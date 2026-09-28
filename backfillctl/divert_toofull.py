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
- are on a host the PG's other shards do not use, and not in its up or
  acting set or CRUSH mapping;
- stay at or below --max-target-util, counting every backfill in motion and
  the proposals, but not data leaving (Ceph checks before any is freed);
- end up below the OSD the shard was heading for, once all proposals
  complete.

Utilization here is projected (PROJ in the table): what an OSD will hold
once every backfill in motion and every proposal completes, crediting data
leaving it. Shards whose data sits on the fullest OSD are placed first. If
room runs out, apply the proposals, let them finish and run again. PGs
whose existing upmap pairs chain are left alone, and no proposal makes a
PG's pairs chain: pgremapper would break them.

Table groups: ACTING is where the data is ('none' if its OSD is out), UP
where the stalled backfill is headed, TARGET the proposed OSD.

Apply the output with pgremapper, not 'ceph osd pg-upmap-items', which
replaces a PG's whole upmap entry:

    backfillctl divert-toofull --pgremapper-mappings > m.json
    pgremapper import-mappings m.json

Pass pgremapper a file, not stdin: it prompts for confirmation. Turn off the
upmap balancer ('ceph balancer off') for as long as the upmaps should hold,
or it may undo them. Assumes the CRUSH failure domain is host.
"""

import argparse
import heapq
import math
from collections import deque
from typing import NamedTuple

from messages import (
    RELIEVE_CLAUSE,
    chained_text,
    named_list,
    print_balancer_note,
    print_pgid_filter,
    print_unplaceable,
    stderr_para,
    targets_clause,
)
from placement import (
    ArrivingShard,
    FullRatios,
    add_max_target_util_arg,
    add_toofull_util_arg,
    blocker_projection,
    find_arriving_shards,
    resolve_toofull_util,
    usage_and_capacity,
)
from shared import (
    HelpFormatter,
    PgidFilter,
    SnapshotStore,
    add_load_state_arg,
    add_pgremapper_mappings_arg,
    check_host_failure_domain,
    check_known_pools,
    pgid_sort_key,
)
from shed import (
    SNAPSHOT_COMMANDS,
    Cluster,
    Move,
    PgState,
    Planner,
    is_toofull,
    print_moves,
    project_moves,
    shard_key,
)

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
    add_toofull_util_arg(parser)
    add_max_target_util_arg(parser)
    parser.add_argument(
        "--pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="Consider only these PGs.",
    )
    add_pgremapper_mappings_arg(parser)
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

    Kept apart from the planner's projections: ProjectedUsage never credits
    data leaving (Ceph checks targets at reservation), and FinalUsage
    credits all of it, stuck or not.
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


def divert(
    planner: Planner,
    states: dict[str, PgState],
    shards: list[ArrivingShard],
) -> list[ArrivingShard]:
    """Give each stuck shard the best legal target (Planner.place); return the rest.

    Each turn takes the shard whose acting OSD is fullest (SourcePressure);
    ties and unknown acting OSDs go in the order given. Moves are recorded
    in states (and the planner's projections); the shards left without a
    target are returned in the order given.
    """
    pressure = SourcePressure(planner.cluster.osd_df)
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
        state = states[shard.pgid]
        picked = planner.place(state, shard)
        if picked:
            planner.commit(state, shard, picked[1])
            state.add_move(shard, picked[1])
            pressure.relieve(shard)
        else:
            unplaceable.add(i)
        # Requeue: placing may have lowered the queue's rank.
        if queue:
            heapq.heappush(
                heap, (-pressure.utilization(shards[queue[0]]), queue[0], acting)
            )

    return [shards[i] for i in sorted(unplaceable)]


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def split_left(
    cluster: Cluster, left: list[ArrivingShard], moves: list[Move]
) -> tuple[list[ArrivingShard], list[tuple[ArrivingShard, float]]]:
    """Split the shards divert() left into (unplaceable, (shard, projection) that fit).

    A shard fits if moves take other shards off its UP OSD, and leave that
    OSD's reservation projection below backfillfull_ratio: Ceph may then
    take it as it is, and any target would end up fuller (the relief rule).
    A shard on an OSD no move relieves is unplaceable: Ceph refuses it now.
    """
    unplaceable, fitting = [], []
    reservation, backfillfull = cluster.reservation, cluster.ratios.backfillfull
    relieved = {m.up_osd for m in moves}
    for shard in left:
        if (
            shard.up_osd in relieved
            and reservation.knows(shard.up_osd)
            and blocker_projection(reservation, shard.up_osd, backfillfull) is None
        ):
            fitting.append((shard, reservation.utilization_after(shard.up_osd, 0)))
        else:
            unplaceable.append(shard)
    return unplaceable, fitting


def print_outcome(result: "DivertResult") -> None:
    """Report on stderr how many shards were placed, and name those that were not."""
    r = result
    stderr_para(
        f"Proposed {len(r.moves)} move(s); {len(r.unplaceable)} shard(s) could "
        "not be placed."
    )
    print_unplaceable(
        ((s.pgid, s.shard, f"(headed for osd.{s.up_osd})") for s in r.unplaceable),
        "the OSD it was headed for",
    )
    if r.fitting:
        named = named_list(
            [
                f"{s.pgid} shard {s.shard} (osd.{s.up_osd} {u:.1f}%)"
                for s, u in r.fitting
            ]
        )
        stderr_para(
            f"{len(r.fitting)} more got no target, but the moves off the OSD "
            "each is headed for take it below backfillfull_ratio "
            f"{r.ratios.backfillfull:g}%, so Ceph may take them as they are: {named}."
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class DivertResult(NamedTuple):
    """What plan() decided, for render() to print."""

    moves: list[Move]  # in PG, then shard, order
    unplaceable: list[ArrivingShard]  # likewise
    # Left without a target, with their UP OSD's projection: see split_left.
    fitting: list[tuple[ArrivingShard, float]]
    toofull_pg_count: int  # backfill_toofull PGs considered (after --pgs)
    pgs_with_shards: int  # of those, the ones with a newly-arriving shard
    arriving_count: int  # arriving shards in all
    stuck_count: int  # arriving on an OSD at or above toofull_util
    left_alone_count: int  # arriving on an OSD below it: not the blocker
    chained_pgs: int  # PGs with a stuck shard, left alone as their pairs chain
    candidates: dict[str, list[int]]  # see Cluster.candidates
    toofull_util: float
    max_target_util: float
    ratios: FullRatios
    pgs_filter: PgidFilter | None  # None without --pgs
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> DivertResult:
    """Fetch the cluster state and work out what to divert where.

    Exits on an invalid --max-target-util, or if a PG cannot be analyzed
    safely; the --pgs note is printed before the latter, so it shows even
    then. Without stuck shards, the PGs in motion are not projected: an
    unrelated one cannot make the run exit.
    """
    cluster = Cluster(store, args.max_target_util)
    toofull_util = resolve_toofull_util(args.toofull_util, cluster.ratios)
    toofull_pgs = [pg for pg in cluster.pgs if is_toofull(pg)]

    pgs_filter = None
    if args.pgs:
        wanted = set(args.pgs)
        toofull_pgs, matched = filter_toofull_pgs(toofull_pgs, wanted)
        pgs_filter = PgidFilter.of(wanted, matched)
        print_pgid_filter(
            "--pgs",
            pgs_filter,
            "are backfill_toofull and will be the only ones considered",
            "not backfill_toofull",
        )

    toofull_pgids = [pg["pgid"] for pg in toofull_pgs]
    check_known_pools(toofull_pgids, cluster.pools, "backfill_toofull PGs")
    check_host_failure_domain(
        toofull_pgids, cluster.pools, cluster.crush_rules, "backfill_toofull PGs"
    )

    arriving = []
    pgs_with_shards = 0
    for pg in toofull_pgs:
        found = find_arriving_shards(pg, *cluster.pg_info(pg))
        arriving.extend(found)
        pgs_with_shards += bool(found)
    stuck, not_full_enough = select_stuck_shards(arriving, cluster.osd_df, toofull_util)

    by_pgid = {pg["pgid"]: pg for pg in toofull_pgs}
    chained = {s.pgid for s in stuck if cluster.pairs_chain(by_pgid[s.pgid])}
    shards = sorted(
        (s for s in stuck if s.pgid not in chained),
        key=lambda s: (pgid_sort_key(s.pgid), shard_key(s)),
    )
    states = {s.pgid: cluster.pg_state(by_pgid[s.pgid]) for s in shards}

    moves, unplaceable, fitting = [], [], []
    if shards:
        planner = Planner(cluster, set(), None, None, relieve=True)
        left = divert(planner, states, shards)
        moves = sorted(
            (m for state in states.values() for m in state.moves),
            key=lambda m: (pgid_sort_key(m.pgid), shard_key(m)),
        )
        unplaceable, fitting = split_left(cluster, left, moves)
        moves = project_moves(cluster, moves)
    return DivertResult(
        moves=moves,
        unplaceable=unplaceable,
        fitting=fitting,
        toofull_pg_count=len(toofull_pgs),
        pgs_with_shards=pgs_with_shards,
        arriving_count=len(arriving),
        stuck_count=len(stuck),
        left_alone_count=len(not_full_enough),
        chained_pgs=len(chained),
        candidates=cluster.candidates,
        toofull_util=toofull_util,
        max_target_util=cluster.max_target_util,
        ratios=cluster.ratios,
        pgs_filter=pgs_filter,
        osd_df=cluster.osd_df,
        osd_host=cluster.osd_host,
    )


def render(result: DivertResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    r = result
    if not r.toofull_pg_count:
        among = " among --pgs" if r.pgs_filter is not None else ""
        stderr_para(f"No backfill_toofull PGs{among}.")
        print_moves([], r.osd_host, r.osd_df, args)
        return
    by_class = ", ".join(
        f"{cls}={sum(r.osd_df[o]['utilization'] <= r.max_target_util for o in osds)}"
        f"/{len(osds)}"
        for cls, osds in sorted(r.candidates.items())
    )
    chained = (
        f" PGs left alone: {chained_text(r.chained_pgs)}." if r.chained_pgs else ""
    )
    stderr_para(
        f"{r.toofull_pg_count} backfill_toofull PG(s), "
        f"{r.pgs_with_shards} with arriving shards. "
        f"{r.arriving_count} arriving shard(s), of which "
        f"{r.stuck_count} on an OSD at or above --toofull-util "
        f"{r.toofull_util:g}% ({r.left_alone_count} left alone as "
        "not the blocker)." + chained
    )
    stderr_para(
        "Targets: "
        + targets_clause(r.max_target_util, r.ratios.backfillfull)
        + RELIEVE_CLAUSE
        + ". Candidates under that now / in all, per device class: "
        f"{by_class or 'none'}."
    )
    print_moves(r.moves, r.osd_host, r.osd_df, args)
    print_outcome(r)
    if r.moves:
        print_balancer_note()


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
