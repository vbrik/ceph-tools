# SPDX-License-Identifier: MIT
"""
Propose upmaps that move stuck backfill_toofull shards to emptier OSDs.

When an OSD goes out, a host-level CRUSH rule re-places its shards on the
same host's other OSDs. On a full cluster those cross backfillfull_ratio and
the backfills stall, while other hosts have room.

For every backfill_toofull PG, each shard arriving on an OSD at or above
--toofull-util is re-targeted. Ceph does not say which shard was refused, so
this is a guess, and shards arriving on emptier OSDs are left alone. The
target is the OSD that ends up least utilized among those that:

- are up and in, of the same device class;
- are on a host the PG does not use, and not in its CRUSH mapping;
- are emptier than the OSD the shard was heading for;
- stay at or below --max-target-util, counting the shards already arriving
  there and those proposed in this run;
- have taken fewer than --max-target-uses shards.

Shards whose data sits on the fullest OSD are placed first. If room runs
out, apply the proposals, let them finish and run again.

Table groups: ACTING is where the data is ('none' if its OSD is out), UP
where the stalled backfill is headed, TARGET the proposed OSD. PROJ is the
target's projected utilization once all proposals complete.

Apply the output with pgremapper, not 'ceph osd pg-upmap-items', which
replaces a PG's whole upmap entry:

    backfillctl divert-toofull --pgremapper-mappings > m.json
    pgremapper import-mappings m.json

Pass pgremapper a file, not stdin: it prompts for confirmation. Consider
'ceph balancer off' while the backfills run. Assumes the CRUSH failure
domain is host.
"""

import argparse
import heapq
import math
import sys
from collections import Counter, deque
from typing import NamedTuple

from placement import (
    ArrivingShard,
    FullRatios,
    ProjectedUsage,
    add_target_args,
    build_candidate_osds,
    check_host_failure_domain,
    ec_pool_ids_from,
    fetch_full_ratios,
    find_arriving_shards,
    osd_class,
    pick_target,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
    usage_and_capacity,
)
from shared import (
    HelpFormatter,
    PgidFilter,
    SnapshotStore,
    add_load_state_arg,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    is_real_osd,
    osd_cells,
    pgid_pool_id,
    pgid_sort_key,
    print_table,
    print_upmap_pairs,
    stderr_para,
    utilization_pct,
)

# Live runs read pg_ls_backfill_toofull; --load-state filters pg_dump_pgs
# instead (see fetch_backfill_toofull_pg_stats).
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_ls_backfill_toofull": [
        "ceph",
        "pg",
        "ls",
        "backfill_toofull",
        "--format",
        "json",
    ],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


def fetch_backfill_toofull_pg_stats(store: SnapshotStore) -> list[dict]:
    """Return the backfill_toofull PGs, live or from a capture."""
    if store.load_dir is None:
        return fetch_pg_stats(store, "pg_ls_backfill_toofull")
    return [
        pg
        for pg in fetch_pg_stats(store, "pg_dump_pgs")
        if "backfill_toofull" in pg["state"].split("+")
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "divert-toofull",
        help="Divert backfill_toofull PGs to emptier OSDs.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--toofull-util",
        type=utilization_pct,
        metavar="PERCENT",
        help="Divert only shards arriving on an OSD at least this full "
        "(default: nearfull_ratio).",
    )
    add_target_args(parser)
    parser.add_argument(
        "--pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="Consider only these PGs.",
    )
    add_load_state_arg(parser, after_command=True)
    return parser


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


def filter_toofull_pgs(
    pgs: list[dict], wanted: set[str]
) -> tuple[list[dict], set[str]]:
    """Return (the PGs in wanted, the ids of wanted that matched)."""
    matched = {pg["pgid"] for pg in pgs if pg["pgid"] in wanted}
    kept = [pg for pg in pgs if pg["pgid"] in wanted]
    return kept, matched


def select_stuck_shards(
    shards: list[ArrivingShard],
    osd_df: dict[int, dict],
    toofull_util: float,
) -> tuple[list[ArrivingShard], list[ArrivingShard]]:
    """Split arriving shards into (stuck, skipped), keeping their order.

    Ceph reports backfill_toofull per PG, not per shard, so a shard counts as
    stuck when its OSD is at or above toofull_util. Diverting healthy shards
    would waste target room that stuck ones need.

    The default, nearfull_ratio, is below backfillfull_ratio because Ceph
    refuses on the target's projected usage, not its current usage.
    """
    stuck, skipped = [], []
    for shard in shards:
        util = osd_df.get(shard.up_osd, {}).get("utilization")
        # Unknown utilization cannot rule the OSD out.
        if util is None or util >= toofull_util:
            stuck.append(shard)
        else:
            skipped.append(shard)
    return stuck, skipped


# ---------------------------------------------------------------------------
# Target selection
# ---------------------------------------------------------------------------


class SourcePressure:
    """Projected utilization of each shard's acting OSD, for ordering shards.

    Scarce target room goes to relieving the fullest OSDs first. A placed
    shard's backfill can finish, freeing its acting OSD, so relieve() lowers
    that OSD's figure and priority rotates between OSDs.

    Kept apart from ProjectedUsage: the space frees only after the backfill,
    while Ceph checks targets at reservation.
    """

    def __init__(self, osd_df: dict[int, dict]):
        self._used, self._capacity = usage_and_capacity(osd_df)

    def utilization(self, shard: ArrivingShard) -> float:
        """Return the shard's acting OSD's projected utilization; -inf if unknown."""
        osd_id = shard.acting_osd
        if osd_id not in self._used:
            return -math.inf
        return self._used[osd_id] / self._capacity[osd_id] * 100

    def relieve(self, shard: ArrivingShard) -> None:
        """Record that shard is placed, so its acting OSD will lose it."""
        if shard.acting_osd in self._used:
            self._used[shard.acting_osd] -= shard.size_bytes


class Proposal(NamedTuple):
    shard: ArrivingShard
    target_osd: int
    target_host: str
    target_utilization: float  # current, from 'ceph osd df'
    target_projected: float  # once all proposals have completed, in any row


def assign_targets(
    shards: list[ArrivingShard],
    candidates: dict[str, list[int]],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    upmap_items: dict[str, list[dict]],
    *,
    projection: ProjectedUsage,
    max_uses: int,
    max_target_util: float,
) -> tuple[list[Proposal], list[ArrivingShard]]:
    """Greedily give each stuck shard a target (see pick_target).

    Each turn takes the shard whose acting OSD is fullest (SourcePressure);
    ties and unknown acting OSDs go in the order given. A target must also be
    emptier than the shard's UP OSD. Returns (proposals, unplaceable) in the
    order given; each proposal carries its target's final projection.
    """
    uses: Counter[int] = Counter()
    pressure = SourcePressure(osd_df)
    # Hosts each PG uses, including targets proposed so far.
    blocked_hosts: dict[str, set[str]] = {}
    proposed: dict[int, int] = {}  # shard index -> target OSD
    unplaceable = set()

    # Placing a shard changes only its acting OSD's priority, so shards are
    # queued per acting OSD and a heap ranks the queue heads, one entry per
    # non-empty queue (none goes stale).
    queues: dict[int | None, deque[int]] = {}
    for i, shard in enumerate(shards):
        queues.setdefault(shard.acting_osd, deque()).append(i)
    heap = [
        (-pressure.utilization(shards[q[0]]), q[0], acting)
        for acting, q in queues.items()
    ]
    heapq.heapify(heap)

    while heap:
        _, i, acting = heapq.heappop(heap)
        queue = queues[acting]
        queue.popleft()
        shard = shards[i]

        if shard.pgid not in blocked_hosts:
            blocked_hosts[shard.pgid] = {
                osd_host.get(o) for o in shard.up_set if is_real_osd(o)
            }
        forbidden_hosts = blocked_hosts[shard.pgid]
        raw = raw_crush_osds(shard.up_set, upmap_items.get(shard.pgid, []))
        # See raw_crush_osds for why 'up' alone is not enough.
        forbidden_osds = raw | {o for o in shard.up_set if is_real_osd(o)}

        # Same device class only; an unknown class leaves it unplaceable.
        pool = candidates.get(osd_class(osd_df, shard.up_osd), [])
        # A target must be emptier than the UP OSD (if its utilization is known).
        picked = pick_target(
            pool,
            shard.size_bytes,
            forbidden_hosts=forbidden_hosts,
            forbidden_osds=forbidden_osds,
            osd_host=osd_host,
            osd_df=osd_df,
            projection=projection,
            uses=uses,
            max_uses=max_uses,
            max_target_util=max_target_util,
            below_util=osd_df.get(shard.up_osd, {}).get("utilization"),
        )

        if picked:
            _, target = picked
            projection.redirect(shard, target)
            pressure.relieve(shard)
            uses[target] += 1
            forbidden_hosts.add(osd_host.get(target))
            proposed[i] = target
        else:
            unplaceable.add(i)

        # Requeue: placing may have lowered the queue's rank.
        if queue:
            heapq.heappush(
                heap, (-pressure.utilization(shards[queue[0]]), queue[0], acting)
            )

    # Final projections, including shards placed later.
    return (
        [
            Proposal(
                shards[i],
                proposed[i],
                osd_host.get(proposed[i], "?"),
                osd_df[proposed[i]]["utilization"],
                projection.utilization_after(proposed[i], 0),
            )
            for i in sorted(proposed)
        ],
        [shards[i] for i in sorted(unplaceable)],
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# (group, label), along the shard's path: ACTING (data now), UP (stalled
# destination), TARGET (proposed). PROJ: the target's final projection.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "HOST"),
    ("TARGET", "OSD"),
    ("TARGET", "UTIL"),
    ("TARGET", "PROJ"),
    ("TARGET", "HOST"),
]


def format_row(
    proposal: Proposal,
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
) -> list[str]:
    shard = proposal.shard
    return [
        shard.pgid,
        str(shard.shard),
        *osd_cells(osd_df, osd_host, shard.acting_osd),
        *osd_cells(osd_df, osd_host, shard.up_osd),
        str(proposal.target_osd),
        f"{proposal.target_utilization:.1f}%",
        f"{proposal.target_projected:.1f}%",
        proposal.target_host,
    ]


def print_outcome(proposed: int, unplaceable: int) -> None:
    """Report on stderr how many shards were placed and how many were not."""
    stderr_para(
        f"Proposed {proposed} remap(s); {unplaceable} shard(s) could not be placed"
        + (
            ": targets ran out of room, or the greedy heuristic missed some. "
            "Apply these, let them finish, then re-run."
            if unplaceable
            else "."
        )
    )


def print_pgremapper_mappings(proposals: list[Proposal]) -> None:
    """Print the proposals as JSON for 'pgremapper import-mappings'.

    A JSON array, one {pgid, mapping: {from: UP, to: TARGET}} per line.

    Dry runs of pgremapper 1.0.0 showed import-mappings applies all of a PG's
    pairs as one change and keeps its existing pairs. When UP is itself the
    'to' of an existing pair, it rewrites that pair; a new pair from UP would
    be silently dropped, since Ceph only honors a 'from' CRUSH chose.
    """
    print_upmap_pairs((p.shard.pgid, p.shard.up_osd, p.target_osd) for p in proposals)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class DivertResult(NamedTuple):
    """What plan() decided, for render() to print."""

    proposals: list[Proposal]  # in PG, then shard, order
    unplaceable: list[ArrivingShard]
    toofull_pg_count: int  # backfill_toofull PGs considered (after --pgs)
    pgs_with_shards: int  # of those, the ones with a newly-arriving shard
    arriving_count: int  # arriving shards in all
    stuck_count: int  # arriving on an OSD at or above toofull_util
    left_alone_count: int  # arriving on an OSD below it: not the blocker
    candidates: dict[str, list[int]]  # see build_candidate_osds
    toofull_util: float
    max_target_util: float
    ratios: FullRatios
    pgs_filter: PgidFilter | None  # None without --pgs
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> DivertResult:
    """Fetch the cluster state and work out what to divert where.

    Exits if a PG cannot be analyzed safely. The --pgs note is printed
    first, so it shows even then.
    """
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    upmap_items = fetch_upmap_items(store)
    pools_by_id = fetch_pools(store)
    pools = list(pools_by_id.values())
    ec_pool_ids = ec_pool_ids_from(pools)
    toofull_pgs = fetch_backfill_toofull_pg_stats(store)
    crush_rules = fetch_crush_rules(store)
    ec_profiles = fetch_ec_profiles(store)

    ratios = fetch_full_ratios(store)
    toofull_util = ratios.nearfull if args.toofull_util is None else args.toofull_util
    max_target_util = resolve_max_target_util(args.max_target_util, ratios)

    pgs_filter = None
    if args.pgs:
        wanted_pgs = set(args.pgs)
        toofull_pgs, matched_pgs = filter_toofull_pgs(toofull_pgs, wanted_pgs)
        pgs_filter = PgidFilter(
            len(wanted_pgs), len(matched_pgs), sorted(wanted_pgs - matched_pgs)
        )
        print_pgs_filter(pgs_filter)

    toofull_pool_ids = {pgid_pool_id(pg["pgid"]) for pg in toofull_pgs}
    # An unknown pool would skip the failure-domain check and have its EC
    # shards diffed as replicas: plausible but wrong rows.
    unknown_pools = sorted(toofull_pool_ids - pools_by_id.keys())
    if unknown_pools:
        sys.exit(
            "ERROR: backfill_toofull PGs belong to pool id(s) "
            f"{', '.join(map(str, unknown_pools))}, which 'ceph osd pool ls "
            "detail' does not list."
        )
    check_host_failure_domain(
        [pools_by_id[i] for i in sorted(toofull_pool_ids)],
        crush_rules,
        "with a stuck PG",
    )

    arriving = []
    pgs_with_shards = 0
    for pg in toofull_pgs:
        pool_id = pgid_pool_id(pg["pgid"])
        found = find_arriving_shards(
            pg,
            pool_id in ec_pool_ids,
            shard_size_bytes(pg, pools_by_id[pool_id], ec_profiles),
        )
        arriving.extend(found)
        pgs_with_shards += bool(found)
    shards, not_full_enough = select_stuck_shards(arriving, osd_df, toofull_util)
    shards.sort(
        key=lambda s: (
            pgid_sort_key(s.pgid),
            s.shard if isinstance(s.shard, int) else -1,
        )
    )

    candidates = build_candidate_osds(osd_df)
    projection = ProjectedUsage(osd_df, arriving)
    proposals, unplaceable = assign_targets(
        shards,
        candidates,
        osd_host,
        osd_df,
        upmap_items,
        projection=projection,
        max_uses=args.max_target_uses,
        max_target_util=max_target_util,
    )
    return DivertResult(
        proposals=proposals,
        unplaceable=unplaceable,
        toofull_pg_count=len(toofull_pgs),
        pgs_with_shards=pgs_with_shards,
        arriving_count=len(arriving),
        stuck_count=len(shards),
        left_alone_count=len(not_full_enough),
        candidates=candidates,
        toofull_util=toofull_util,
        max_target_util=max_target_util,
        ratios=ratios,
        pgs_filter=pgs_filter,
        osd_df=osd_df,
        osd_host=osd_host,
    )


def print_pgs_filter(pgs_filter: PgidFilter) -> None:
    """Report on stderr what --pgs matched, naming ids that matched nothing."""
    stderr_para(
        f"NOTE: --pgs: {pgs_filter.matched} of {pgs_filter.given} given PG id(s) "
        "are backfill_toofull and will be the only ones considered"
        + (
            f"; {len(pgs_filter.unmatched)} matched nothing (check for typos): "
            + ", ".join(pgs_filter.unmatched)
            if pgs_filter.unmatched
            else ""
        )
        + "."
    )


def render(result: DivertResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    # Notes go to stderr, keeping stdout parseable.
    osd_df = result.osd_df
    max_target_util = result.max_target_util
    by_class = ", ".join(
        f"{cls}={sum(osd_df[o]['utilization'] <= max_target_util for o in osds)}"
        f"/{len(osds)}"
        for cls, osds in sorted(result.candidates.items())
    )
    stderr_para(
        f"{result.toofull_pg_count} backfill_toofull PG(s), "
        f"{result.pgs_with_shards} with arriving shards. "
        f"{result.arriving_count} arriving shard(s), of which "
        f"{result.stuck_count} on an OSD at or above --toofull-util "
        f"{result.toofull_util:g}% ({result.left_alone_count} left alone as "
        "not the blocker)."
    )
    stderr_para(
        f"Targets: up to --max-target-uses {args.max_target_uses} shard(s) "
        f"each, projected at or below --max-target-util {max_target_util:g}% "
        f"(backfillfull_ratio {result.ratios.backfillfull:g}%). Candidates "
        f"under that now / in all, per device class: {by_class or 'none'}."
    )

    proposals, unplaceable = result.proposals, result.unplaceable
    if args.pgremapper_mappings:
        print_pgremapper_mappings(proposals)
    elif proposals:
        print_table(
            COLUMNS,
            [format_row(p, result.osd_host, osd_df) for p in proposals],
        )

    print_outcome(len(proposals), len(unplaceable))


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
