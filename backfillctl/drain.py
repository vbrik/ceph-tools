# SPDX-License-Identifier: MIT
"""
Propose upmaps that move every PG shard off the given OSDs, or off every OSD
of the given hosts.

Marking an OSD out lets a host-level CRUSH rule pile its shards onto the same
host's other OSDs. This spreads them over the least-utilized OSDs
cluster-wide instead. Shards still backfilling onto a drained OSD are
redirected too.

Targets are chosen as in divert-toofull, except that drained OSDs are never
targets, the shard's own host is allowed, and the largest shards are placed
first.

backfill_toofull holds back the whole PG, so a moved shard also waits on any
other shard of its PG heading for an OSD projected over --max-target-util or,
if the PG is backfill_toofull now, arriving on an OSD at or above
--toofull-util. Such a blocker is diverted if there is room, otherwise pinned
back to its acting OSD (with companions, as in cancel-backfill). If neither
is possible, the NOTE column says the PG will stay stuck, and why.

Keep the drained OSDs up and in until they are empty: marking one out voids
these upmaps, since Ceph honors only a 'from' that CRUSH chose. Afterwards,
external/upmap-remapped.py can pin CRUSH's new mapping to where the data is.

Apply the output as with divert-toofull. In the JSON, each entry has its
NOTE as 'note', plus 'shard' and 'role': requested (an evacuee) or blocker
(a blocker, or a pinned blocker's companion). Keep a PG's blocker entries
with its evacuees:

    backfillctl drain --hosts host07 --pgremapper-mappings > m.json
    pgremapper import-mappings m.json

Consider 'ceph balancer off' while the drain runs. Assumes the CRUSH failure
domain is host.
"""

import argparse
import sys
from collections import Counter
from typing import NamedTuple

from placement import (
    ArrivingShard,
    FullRatios,
    MappedShard,
    PgPlacement,
    ProjectedUsage,
    add_target_args,
    build_candidate_osds,
    check_host_failure_domain,
    ec_pool_ids_from,
    fetch_full_ratios,
    find_arriving_shards,
    find_mapped_shards,
    osd_class,
    pick_target,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
)
from shared import (
    NOT_APPLICABLE,
    ROLE_BLOCKER,
    ROLE_REQUESTED,
    HelpFormatter,
    SnapshotStore,
    add_load_state_arg,
    close_pins,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_remapped_pg_stats,
    fetch_upmap_items,
    osd_cells,
    parse_osd,
    pgid_pool_id,
    pgid_sort_key,
    pin_replica,
    print_table,
    print_upmap_entries,
    real_osd_set,
    stderr_para,
    upmap_entry,
    utilization_pct,
)

# Live runs add a 'pg ls-by-osd' per drained OSD (ls_by_osd_commands);
# --load-state filters pg_dump_pgs instead of those and pg_ls_remapped.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_ls_remapped": ["ceph", "pg", "ls", "remapped", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


def ls_by_osd_key(osd: int) -> str:
    """Snapshot key for 'ceph pg ls-by-osd' of one OSD (live runs only)."""
    return f"pg_ls_by_osd_{osd}"


def ls_by_osd_commands(osds: set[int]) -> dict[str, list[str]]:
    """Return a 'pg ls-by-osd' snapshot command per drained OSD.

    Added to the store once the OSDs are known (with --hosts, after reading
    'ceph osd tree').
    """
    return {
        ls_by_osd_key(osd): [
            "ceph",
            "pg",
            "ls-by-osd",
            f"osd.{osd}",
            "--format",
            "json",
        ]
        for osd in osds
    }


def fetch_drained_pg_stats(store: SnapshotStore, osds: set[int]) -> list[dict]:
    """Return the PGs with any of osds in 'up' or 'acting', in PG id order."""
    if store.load_dir is None:
        by_pgid = {
            pg["pgid"]: pg
            for osd in sorted(osds)
            for pg in fetch_pg_stats(store, ls_by_osd_key(osd))
        }
        pgs = list(by_pgid.values())
    else:
        pgs = [
            pg
            for pg in fetch_pg_stats(store, "pg_dump_pgs")
            if osds & (real_osd_set(pg["up"]) | real_osd_set(pg["acting"]))
        ]
    return sorted(pgs, key=lambda pg: pgid_sort_key(pg["pgid"]))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "drain",
        help="Move all PG shards off given OSDs or hosts.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        metavar="OSD",
        help="OSDs to drain.",
    )
    which.add_argument(
        "--hosts",
        nargs="+",
        metavar="HOST",
        help="Drain every OSD of these hosts.",
    )
    parser.add_argument(
        "--toofull-util",
        type=utilization_pct,
        metavar="PERCENT",
        help="Blocker threshold for PGs that are backfill_toofull now "
        "(default: nearfull_ratio).",
    )
    add_target_args(parser)
    add_load_state_arg(parser, after_command=True)
    return parser


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


class Move(NamedTuple):
    """One proposed upmap pair, and why it is proposed."""

    pgid: str
    shard: "int | str"
    acting_osd: int | None  # where the shard's data is now
    up_osd: int  # the 'from' of the pair
    target_osd: int  # the 'to' of the pair
    projected: float | None  # target's, once all moves are done; None for pins
    note: str
    role: str = (
        ROLE_REQUESTED  # an evacuee; blockers and their companions: ROLE_BLOCKER
    )


class DrainResult(NamedTuple):
    """What plan() decided, for render() to print."""

    osds: list[int]
    hosts: list[str]  # with --hosts, the (short) host names; else empty
    moves: list[Move]  # in PG order; within a PG, in the order proposed
    unplaceable: list[MappedShard]
    evacuee_count: int
    leaving_count: int  # shards already moving off a drained OSD
    diverted_count: int  # blockers diverted
    pinned_count: int  # blockers pinned back (companions not counted)
    stuck_pgs: list[str]  # PGs with an evacuee that will stay toofull
    unexplained_pgs: list[str]  # toofull now, blocker not identified
    max_target_util: float
    toofull_util: float
    ratios: FullRatios
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


class PgState(PgPlacement):
    """One affected PG, and the moves proposed for it."""

    def __init__(self, pg: dict, is_ec: bool, size_bytes: int, raw: set[int]):
        super().__init__(pg, is_ec, size_bytes, raw)
        self.changed: set[int | str] = set()  # EC slots / replica OSDs moved
        self.moves: list[Move] = []


class Planner:
    """State shared by one run's placements: projection, uses, thresholds."""

    def __init__(
        self,
        osd_df: dict[int, dict],
        osd_host: dict[int, str],
        candidates: dict[str, list[int]],
        projection: ProjectedUsage,
        *,
        max_uses: int,
        max_target_util: float,
        toofull_util: float,
        drained: set[int],
    ):
        self.osd_df = osd_df
        self.osd_host = osd_host
        self.candidates = candidates  # see placement.build_candidate_osds
        self.projection = projection
        self.max_uses = max_uses
        self.max_target_util = max_target_util
        self.toofull_util = toofull_util
        self.drained = drained
        self.uses: Counter[int] = Counter()

    def place(self, state: PgState, from_osd: int) -> tuple[float, int] | None:
        """Pick a target for the PG's shard currently headed for from_osd."""
        return pick_target(
            self.candidates.get(osd_class(self.osd_df, from_osd), []),
            state.size_bytes,
            forbidden_hosts=state.forbidden_hosts(from_osd, self.osd_host),
            forbidden_osds=state.forbidden_osds,
            osd_host=self.osd_host,
            osd_df=self.osd_df,
            projection=self.projection,
            uses=self.uses,
            max_uses=self.max_uses,
            max_target_util=self.max_target_util,
        )

    def commit(self, state: PgState, shard, target: int) -> None:
        """Record that shard (with up_osd, size_bytes) goes to target."""
        self.projection.redirect(shard, target)
        self.uses[target] += 1
        state.retarget(shard.up_osd, target)

    def is_blocker(self, sibling: ArrivingShard, toofull_now: bool) -> str | None:
        """Return why the sibling blocks its PG, or None.

        It blocks if its OSD is projected over max_target_util or, with
        toofull_now, is at or above toofull_util. The reason cites the cap
        first, e.g. 'now 90.5%, projected 91.4% > --max-target-util 90%'.
        """
        if not self.projection.knows(sibling.up_osd):
            return None
        projected = self.projection.utilization_after(sibling.up_osd, 0)
        now = self.osd_df[sibling.up_osd].get("utilization")
        if projected > self.max_target_util:
            now_text = "?" if now is None else f"{now:.1f}%"
            return (
                f"now {now_text}, projected {projected:.1f}% > "
                f"--max-target-util {self.max_target_util:g}%"
            )
        if toofull_now and now is not None and now >= self.toofull_util:
            return (
                f"now {now:.1f}% >= --toofull-util {self.toofull_util:g}% "
                f"and PG is backfill_toofull, projected {projected:.1f}%"
            )
        return None

    def try_pin(
        self, state: PgState, blocker: ArrivingShard
    ) -> tuple[list[tuple["int | str", int, int]], str | None]:
        """Return the (shard, from, to) pins that cancel blocker, or ([], why not).

        The blocker's own pin comes first, then companions. Refused, besides
        the reasons close_pins gives, if a pin would land on a drained OSD,
        undo a move proposed in this run, or chain (pgremapper cannot apply
        chains).
        """
        pg = state.pg
        acting = pg["acting"]
        if state.is_ec:
            if blocker.acting_osd is None:
                return [], "no acting OSD"
            pins, why = close_pins(
                state.new_up, acting, {blocker.shard: blocker.acting_osd}, self.osd_host
            )
            if why is not None:
                return [], why
            moves = [(s, state.new_up[s], a) for s, a in pins.items()]
        else:
            up_set, acting_set = real_osd_set(pg["up"]), real_osd_set(acting)
            departing = acting_set - up_set
            arriving = (up_set - acting_set) - self.drained
            if len(departing) != 1 or len(arriving) != 1:
                return [], "replica pairing is ambiguous"
            (to_osd,) = departing
            why = pin_replica(state.new_up, blocker.up_osd, to_osd, self.osd_host)
            if why is not None:
                return [], why
            moves = [("-", blocker.up_osd, to_osd)]

        for s, from_osd, to_osd in moves:
            if to_osd in self.drained:
                return [], f"it would pin data back onto drained osd.{to_osd}"
            if (s if state.is_ec else from_osd) in state.changed:
                return [], "it would undo a move proposed in this run"
        froms = {m.up_osd for m in state.moves} | {f for _, f, _ in moves}
        tos = {m.target_osd for m in state.moves} | {t for _, _, t in moves}
        if froms & tos:
            return [], "the PG's pairs would chain, which pgremapper cannot apply"
        return moves, None


def shard_key(evacuee: MappedShard) -> int:
    """Order a PG's evacuees: EC by shard index, replicated by drained OSD id."""
    return evacuee.shard if isinstance(evacuee.shard, int) else evacuee.up_osd


def place_evacuees(
    planner: Planner, states: dict[str, PgState], evacuees: list[MappedShard]
) -> list[MappedShard]:
    """Place evacuees, largest first; return the unplaceable ones in PG order."""
    unplaceable = []
    order = sorted(
        evacuees, key=lambda e: (-e.size_bytes, pgid_sort_key(e.pgid), shard_key(e))
    )
    for evacuee in order:
        state = states[evacuee.pgid]
        picked = planner.place(state, evacuee.up_osd)
        if picked is None:
            unplaceable.append(evacuee)
            continue
        projected, target = picked
        planner.commit(state, evacuee, target)
        state.changed.add(evacuee.shard if state.is_ec else evacuee.up_osd)
        state.moves.append(
            Move(
                evacuee.pgid,
                evacuee.shard,
                evacuee.acting_osd,
                evacuee.up_osd,
                target,
                projected,
                "",
            )
        )
    unplaceable.sort(key=lambda e: (pgid_sort_key(e.pgid), shard_key(e)))
    return unplaceable


# resolve_blockers' verdicts on a PG.
STUCK = "stuck"
UNEXPLAINED = "unexplained"


def resolve_blockers(planner: Planner, state: PgState) -> tuple[int, int, str | None]:
    """Divert or pin the PG's blockers; return (diverted, pinned, verdict).

    Called after evacuees are placed, since diverting uses the same room.
    verdict, also noted on each evacuee, is None or:

    - STUCK: a blocker could be neither diverted nor pinned;
    - UNEXPLAINED: the PG is backfill_toofull now, but no sibling is a
      blocker and no evacuee was arriving on a drained OSD.
    """
    toofull_now = "backfill_toofull" in state.pg["state"].split("+")
    evacuated = list(state.moves)
    # What a blocker would hold up, e.g. 'shard 9 leaving osd.231'.
    held_up = " and ".join(
        f"shard {m.shard} leaving osd.{m.up_osd}"
        if state.is_ec
        else f"the replica leaving osd.{m.up_osd}"
        for m in evacuated
    )

    def blocker_note(action: str, sibling: ArrivingShard, blocking: str) -> str:
        return (
            f"{action}: osd.{sibling.up_osd} ({blocking}) would stall the PG, "
            f"holding up {held_up}"
        )

    siblings = [
        s
        for s in find_arriving_shards(state.pg, state.is_ec, state.size_bytes)
        if s.up_osd not in planner.drained
    ]
    diverted = pinned = 0
    stuck_reasons = []
    for sibling in siblings:
        # Pinned as an earlier blocker's companion.
        if (sibling.shard if state.is_ec else sibling.up_osd) in state.changed:
            continue
        blocking = planner.is_blocker(sibling, toofull_now)
        if blocking is None:
            continue
        picked = planner.place(state, sibling.up_osd)
        if picked is not None:
            projected, target = picked
            planner.commit(state, sibling, target)
            state.changed.add(sibling.shard if state.is_ec else sibling.up_osd)
            state.moves.append(
                Move(
                    sibling.pgid,
                    sibling.shard,
                    sibling.acting_osd,
                    sibling.up_osd,
                    target,
                    projected,
                    blocker_note("diverted", sibling, blocking),
                    ROLE_BLOCKER,
                )
            )
            diverted += 1
            continue
        pins, why = planner.try_pin(state, sibling)
        if not pins:
            stuck_reasons.append(
                f"shard {sibling.shard} -> osd.{sibling.up_osd} "
                f"({blocking}; cannot pin: {why})"
            )
            continue
        for k, (s, from_osd, to_osd) in enumerate(pins):
            planner.projection.cancel(
                ArrivingShard(
                    state.pg["pgid"], s, from_osd, to_osd, [], state.size_bytes
                )
            )
            state.new_up[state.new_up.index(from_osd)] = to_osd
            state.changed.add(s if state.is_ec else from_osd)
            note = (
                blocker_note("pinned, no room to divert", sibling, blocking)
                if k == 0
                else f"companion of blocker shard {sibling.shard}"
            )
            state.moves.append(
                Move(
                    state.pg["pgid"],
                    s,
                    to_osd,
                    from_osd,
                    to_osd,
                    None,
                    note,
                    ROLE_BLOCKER,
                )
            )
        pinned += 1

    if stuck_reasons:
        verdict, note = STUCK, "PG stays toofull: " + "; ".join(stuck_reasons)
    elif (
        not diverted
        and not pinned
        and toofull_now
        and all(m.acting_osd == m.up_osd for m in evacuated)
    ):
        verdict = UNEXPLAINED
        note = (
            "PG is backfill_toofull now, but no other shard is arriving on "
            "an OSD at or above --toofull-util or projected over "
            "--max-target-util: blocker unidentified"
        )
    else:
        return diverted, pinned, None
    state.moves = [m._replace(note=note) if m in evacuated else m for m in state.moves]
    return diverted, pinned, verdict


def with_final_projection(moves: list[Move], projection: ProjectedUsage) -> list[Move]:
    """Return moves with each target's final projection, so rows of one OSD agree."""
    return [
        m
        if m.projected is None
        else m._replace(projected=projection.utilization_after(m.target_osd, 0))
        for m in moves
    ]


def host_osds(hosts: list[str], osd_host: dict[int, str]) -> set[int]:
    """Return every OSD of the given hosts, exiting if a host has none."""
    wanted = {h.split(".")[0] for h in hosts}
    osds = {o for o, h in osd_host.items() if h in wanted}
    missing = sorted(wanted - {osd_host[o] for o in osds})
    if missing:
        sys.exit(
            f"ERROR: no OSDs under host(s) in 'ceph osd tree': {', '.join(missing)}"
        )
    return osds


def plan(args: argparse.Namespace, store: SnapshotStore) -> DrainResult:
    """Fetch the cluster state and work out where each shard goes.

    Exits on an unknown OSD or host, an invalid --max-target-util, or a pool
    it cannot analyze safely.
    """
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    if args.hosts:
        drained = host_osds(args.hosts, osd_host)
    else:
        drained = set(args.osds)
        unknown = sorted(drained - osd_df.keys())
        if unknown:
            sys.exit(
                "ERROR: not in 'ceph osd df': " + ", ".join(f"osd.{o}" for o in unknown)
            )
    store.commands.update(ls_by_osd_commands(drained))
    upmap_items = fetch_upmap_items(store)
    pools_by_id = fetch_pools(store)
    ec_pool_ids = ec_pool_ids_from(list(pools_by_id.values()))
    crush_rules = fetch_crush_rules(store)
    ec_profiles = fetch_ec_profiles(store)
    ratios = fetch_full_ratios(store)
    max_target_util = resolve_max_target_util(args.max_target_util, ratios)
    toofull_util = ratios.nearfull if args.toofull_util is None else args.toofull_util

    drained_pgs = fetch_drained_pg_stats(store, drained)
    remapped_pgs = fetch_remapped_pg_stats(store)
    affected_pool_ids = {pgid_pool_id(pg["pgid"]) for pg in drained_pgs}
    unknown_pools = sorted(affected_pool_ids - pools_by_id.keys())
    if unknown_pools:
        sys.exit(
            "ERROR: PGs on the drained OSD(s) belong to pool id(s) "
            f"{', '.join(map(str, unknown_pools))}, which 'ceph osd pool ls "
            "detail' does not list, so their shards cannot be analyzed."
        )
    check_host_failure_domain(
        [pools_by_id[i] for i in sorted(affected_pool_ids)],
        crush_rules,
        "with a PG on a drained OSD",
    )

    def pg_info(pg: dict) -> tuple[bool, int]:
        pool_id = pgid_pool_id(pg["pgid"])
        is_ec = pool_id in ec_pool_ids
        pool = pools_by_id.get(pool_id)
        size = shard_size_bytes(pg, pool, ec_profiles) if pool else 0
        return is_ec, size

    # Every shard in motion counts towards its target.
    arriving = []
    for pg in {p["pgid"]: p for p in [*remapped_pgs, *drained_pgs]}.values():
        arriving.extend(find_arriving_shards(pg, *pg_info(pg)))
    projection = ProjectedUsage(osd_df, arriving)

    states: dict[str, PgState] = {}
    evacuees: list[MappedShard] = []
    leaving = 0
    for pg in drained_pgs:
        is_ec, size = pg_info(pg)
        found, gone = find_mapped_shards(pg, is_ec, drained, size)
        leaving += gone
        if found:
            raw = raw_crush_osds(pg["up"], upmap_items.get(pg["pgid"], []))
            states[pg["pgid"]] = PgState(pg, is_ec, size, raw)
            evacuees.extend(found)

    planner = Planner(
        osd_df,
        osd_host,
        build_candidate_osds(osd_df, exclude=drained),
        projection,
        max_uses=args.max_target_uses,
        max_target_util=max_target_util,
        toofull_util=toofull_util,
        drained=drained,
    )
    unplaceable = place_evacuees(planner, states, evacuees)

    diverted = pinned = 0
    stuck_pgs, unexplained_pgs = [], []
    moves = []
    for pgid, state in states.items():  # PG order, as drained_pgs is
        if not state.moves:
            continue
        d, p, verdict = resolve_blockers(planner, state)
        diverted += d
        pinned += p
        if verdict == STUCK:
            stuck_pgs.append(pgid)
        elif verdict == UNEXPLAINED:
            unexplained_pgs.append(pgid)
        moves.extend(state.moves)
    moves = with_final_projection(moves, projection)

    return DrainResult(
        osds=sorted(drained),
        hosts=sorted({h.split(".")[0] for h in args.hosts or []}),
        moves=moves,
        unplaceable=unplaceable,
        evacuee_count=len(evacuees),
        leaving_count=leaving,
        diverted_count=diverted,
        pinned_count=pinned,
        stuck_pgs=stuck_pgs,
        unexplained_pgs=unexplained_pgs,
        max_target_util=max_target_util,
        toofull_util=toofull_util,
        ratios=ratios,
        osd_df=osd_df,
        osd_host=osd_host,
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# As in divert-toofull. PROJ is '-' for a pin: the data stays where it is.
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
    ("", "NOTE"),
]


def format_row(
    move: Move, osd_host: dict[int, str], osd_df: dict[int, dict]
) -> list[str]:
    return [
        move.pgid,
        str(move.shard),
        *osd_cells(osd_df, osd_host, move.acting_osd),
        *osd_cells(osd_df, osd_host, move.up_osd),
        *osd_cells(osd_df, osd_host, move.target_osd)[:2],
        NOT_APPLICABLE if move.projected is None else f"{move.projected:.1f}%",
        osd_host.get(move.target_osd, "?"),
        move.note,
    ]


def print_pgremapper_mappings(moves: list[Move]) -> None:
    """Print the moves as JSON for 'pgremapper import-mappings', one per line.

    Each entry carries its table row's SHARD, role and NOTE.
    """
    print_upmap_entries(
        upmap_entry(
            m.pgid, m.up_osd, m.target_osd, shard=m.shard, role=m.role, note=m.note
        )
        for m in moves
    )


def render(result: DrainResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    osds = ", ".join(f"osd.{o}" for o in result.osds)
    if result.hosts:
        osds = f"host(s) {', '.join(result.hosts)} ({osds})"
    stderr_para(
        f"Draining {osds}: {result.evacuee_count} shard(s) mapped to them "
        f"({result.leaving_count} more already moving off). Targets: up to "
        f"--max-target-uses {args.max_target_uses} shard(s) each, projected "
        f"at or below --max-target-util {result.max_target_util:g}% "
        f"(backfillfull_ratio {result.ratios.backfillfull:g}%). Blockers: "
        "other shards of a PG heading over that cap or, if the PG is "
        f"backfill_toofull now, onto an OSD at or above --toofull-util "
        f"{result.toofull_util:g}%."
    )
    if args.pgremapper_mappings:
        print_pgremapper_mappings(result.moves)
    elif result.moves:
        print_table(
            COLUMNS,
            [format_row(m, result.osd_host, result.osd_df) for m in result.moves],
        )

    def pg_list(pgids: list[str]) -> str:
        return f"{len(pgids)} ({', '.join(pgids)})" if pgids else "0"

    placed = result.evacuee_count - len(result.unplaceable)
    stuck, unexplained = result.stuck_pgs, result.unexplained_pgs
    stderr_para(
        f"Proposed {placed} evacuation(s), {len(result.unplaceable)} unplaceable; "
        f"{result.diverted_count} blocking shard(s) diverted, "
        f"{result.pinned_count} pinned back. PGs that will stay "
        f"backfill_toofull: {pg_list(stuck)}; for an unidentified reason: "
        f"{pg_list(unexplained)}"
        + (". Their NOTE (JSON: 'note') says why." if stuck or unexplained else ".")
    )
    if result.unplaceable:
        stderr_para(
            "NOTE: targets ran out of room (--max-target-util, "
            "--max-target-uses). Apply these, let them finish, then re-run."
        )


def run(args: argparse.Namespace) -> None:
    # A copy: plan() adds to it.
    store = SnapshotStore.from_args(args, dict(SNAPSHOT_COMMANDS))
    render(plan(args, store), args)
