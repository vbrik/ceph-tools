# SPDX-License-Identifier: MIT
"""
Propose upmaps that move shards off the fullest OSDs of a device class onto
the emptiest ones, lowering the class's highest utilization.

It is not a full balancer. It stops as soon as no move can lower the class's
highest projected utilization, rather than moving data only to even things
out. By default, sources are the fuller half of the class's up and in OSDs.
--min-source-util selects the OSDs at or above a level instead, still at most
half of the class. --osds names the sources; the run then continues while
they can shed data, even if a non-source is fuller.

Utilization here is projected: what an OSD will hold once every backfill in
motion and every proposal completes (PROJ in the table).

Each turn takes the source with the highest projected utilization and moves
its largest shard that has a legal target. The target is the OSD that ends
up least utilized among those that:

- are up and in, of the class, and not sources;
- are on a host the PG does not use (the source's own host is allowed), and
  not in its CRUSH mapping or acting set;
- stay at or below --max-target-util, counting the shards already arriving
  there and those proposed in this run, but not data leaving (Ceph checks
  before any is freed);
- end up less utilized than the source once the move completes;
- have taken fewer than --max-target-uses shards.

So no move raises the class's highest projected utilization. Shards still
backfilling onto a source are redirected. PGs that are not active, or are
degraded, undersized, recovering or peering, are left alone, as are PGs
whose existing upmap pairs chain.

The run stops when the fullest source cannot shed a shard, when a
non-source is as full as it (except with --osds), or at --max-moves. For
more, apply the output, let the backfills finish and run again.

Apply the output with pgremapper, as with divert-toofull:

    backfillctl balance --pgremapper-mappings > m.json
    pgremapper import-mappings m.json

Turn off the upmap balancer ('ceph balancer off') while the backfills run,
or it may undo the moves. Assumes the CRUSH failure domain is host.
"""

import argparse
import heapq
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from typing import NamedTuple

from placement import (
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
    positive_int,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
    usage_and_capacity,
)
from shared import (
    HelpFormatter,
    SnapshotStore,
    add_load_state_arg,
    chain_link,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    format_bytes,
    osd_cells,
    parse_osd,
    pgid_pool_id,
    pgid_sort_key,
    print_table,
    print_upmap_pairs,
    real_osd_set,
    slot,
    stderr_para,
    utilization_pct,
)

SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}

DEFAULT_CLASS = "hdd"

# PG state flags that keep a PG's shards in place: some of its data is
# missing or unsettled, and remapping it would slow or block recovery.
UNSETTLED_FLAGS = frozenset(
    {
        "degraded",
        "undersized",
        "recovering",
        "recovery_wait",
        "recovery_toofull",
        "forced_recovery",
        "peering",
        "down",
        "incomplete",
        "stale",
    }
)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "balance",
        help="Move data off the fullest OSDs of a device class.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--class",
        dest="osd_class",
        default=DEFAULT_CLASS,
        metavar="CLASS",
        help="Device class to balance (default: %(default)s).",
    )
    sources = parser.add_mutually_exclusive_group()
    sources.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        metavar="OSD",
        help="Move data off only these OSDs.",
    )
    sources.add_argument(
        "--min-source-util",
        type=utilization_pct,
        metavar="PERCENT",
        help="Move data off OSDs projected at least this full, at most half "
        "of the class (default: the fuller half).",
    )
    add_target_args(parser)
    parser.add_argument(
        "--max-moves",
        type=positive_int,
        metavar="N",
        help="Propose at most N moves (default: no limit).",
    )
    add_load_state_arg(parser, after_command=True)
    return parser


# ---------------------------------------------------------------------------
# Sources and movable shards
# ---------------------------------------------------------------------------


def select_sources(
    class_osds: list[int],
    utilization: Callable[[int], float],
    *,
    osds: list[int] | None,
    min_source_util: float | None,
) -> tuple[list[int], int]:
    """Return (the sources, fullest first; how many qualified before the cap).

    class_osds: the class's usable OSDs (build_candidate_osds). osds is
    taken as given, exiting if one is not in class_osds. Otherwise sources
    are the OSDs whose utilization is at or above min_source_util (all, if
    None), at most half of class_osds. Ties go to the lower id.
    """

    def fullest_first(ids: Iterable[int]) -> list[int]:
        return sorted(ids, key=lambda o: (-utilization(o), o))

    if osds:
        bad = sorted(set(osds) - set(class_osds))
        if bad:
            sys.exit(
                "ERROR: --osds: not up and in OSDs of this device class, with "
                "a utilization in 'ceph osd df': " + ", ".join(f"osd.{o}" for o in bad)
            )
        return fullest_first(set(osds)), len(set(osds))
    ranked = fullest_first(class_osds)
    if min_source_util is not None:
        ranked = [o for o in ranked if utilization(o) >= min_source_util]
    return ranked[: len(class_osds) // 2], len(ranked)


def is_settled(pg: dict) -> bool:
    """True if the PG is active and none of its UNSETTLED_FLAGS are set."""
    flags = set(pg["state"].split("+"))
    return "active" in flags and not flags & UNSETTLED_FLAGS


def departing_osds(pg: dict, is_ec: bool) -> list[int]:
    """Return the OSDs a copy of the PG is leaving: in 'acting', not 'up'.

    EC slots are compared by position, replicated sets as sets. An EC shard
    whose up slot is empty stays: it has nowhere to go.
    """
    up, acting = pg["up"], pg["acting"]
    if is_ec:
        return [
            osd
            for i in range(len(acting))
            if (osd := slot(acting, i)) is not None and slot(up, i) not in (None, osd)
        ]
    return sorted(real_osd_set(acting) - real_osd_set(up))


# ---------------------------------------------------------------------------
# Projection and target selection
# ---------------------------------------------------------------------------


class FinalUsage:
    """What each OSD will hold once every backfill in motion and every move completes.

    Unlike ProjectedUsage, data leaving an OSD is credited. This is the figure
    balancing is about: it ranks sources and targets. The --max-target-util
    cap still uses ProjectedUsage, since Ceph checks a target when reserving
    the backfill, before the source frees any space.
    """

    def __init__(
        self,
        osd_df: dict[int, dict],
        arriving: Iterable[tuple[int, int]],
        departing: Iterable[tuple[int, int]],
    ):
        """arriving and departing: (OSD, bytes) of each shard in motion."""
        self._used, self._capacity = usage_and_capacity(osd_df)
        for osd_id, size in arriving:
            if osd_id in self._used:
                self._used[osd_id] += size
        for osd_id, size in departing:
            if osd_id in self._used:
                self._used[osd_id] -= size

    def utilization(self, osd_id: int, extra_bytes: int = 0) -> float:
        """Return the OSD's final utilization (percent) with extra_bytes more."""
        return (self._used[osd_id] + extra_bytes) / self._capacity[osd_id] * 100

    def move(self, from_osd: int, to_osd: int, size_bytes: int) -> None:
        """Record that size_bytes will end up on to_osd instead of from_osd."""
        self._used[from_osd] -= size_bytes
        self._used[to_osd] += size_bytes


class Move(NamedTuple):
    """One proposed upmap pair: shard from from_osd to target_osd."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    acting_osd: int | None  # where the shard's data is now, None if unknown
    from_osd: int  # the source: the shard's up OSD, the 'from' of the pair
    target_osd: int
    size_bytes: int
    from_projected: float  # FinalUsage once all moves are done
    target_projected: float


# Why the balancing stopped.
STOP_NO_MOVE = "no-move"  # the fullest source cannot shed a shard
STOP_NO_SHARDS = "no-shards"  # the fullest source has no movable shard left
STOP_NOT_A_SOURCE = "not-a-source"  # a non-source is as full as any source
STOP_MAX_MOVES = "max-moves"
STOP_NO_SOURCES = "no-sources"


class Stop(NamedTuple):
    reason: str  # one of the STOP_* constants
    source: int | None = None  # the fullest source, but for STOP_MAX_MOVES
    source_util: float | None = None  # and its final projection
    other: int | None = None  # with STOP_NOT_A_SOURCE, the non-source
    other_util: float | None = None


class Balancer:
    """One run's placement state: projections, target uses and caps."""

    def __init__(
        self,
        targets: list[int],
        osd_df: dict[int, dict],
        osd_host: dict[int, str],
        *,
        reservation: ProjectedUsage,
        final: FinalUsage,
        max_uses: int,
        max_target_util: float,
    ):
        self.targets = targets  # least utilized first (build_candidate_osds)
        self.osd_df = osd_df
        self.osd_host = osd_host
        self.reservation = reservation
        self.final = final
        self.max_uses = max_uses
        self.max_target_util = max_target_util
        self.uses: Counter[int] = Counter()

    def pick_target(self, shard: MappedShard, state: PgPlacement) -> int | None:
        """Return the legal target that ends up least utilized, or None.

        Legal: see the module docstring. Ties go to the lower id.
        """
        size = shard.size_bytes
        # Strictly below: equal would leave the maximum where it is.
        source_after = self.final.utilization(shard.up_osd, -size)
        forbidden_hosts = state.forbidden_hosts(shard.up_osd, self.osd_host)
        best = None
        for candidate in self.targets:
            # Sorted, and reservation projections never fall below current.
            if self.osd_df[candidate]["utilization"] > self.max_target_util:
                break
            if (
                self.osd_host.get(candidate) in forbidden_hosts
                or candidate in state.forbidden_osds
                or self.uses[candidate] >= self.max_uses
            ):
                continue
            if (
                self.reservation.utilization_after(candidate, size)
                > self.max_target_util
            ):
                continue
            after = self.final.utilization(candidate, size)
            if after < source_after and (best is None or (after, candidate) < best):
                best = (after, candidate)
        return None if best is None else best[1]

    def commit(self, shard: MappedShard, state: PgPlacement, target: int) -> None:
        """Record that shard goes to target."""
        if shard.acting_osd == shard.up_osd:
            self.reservation.add(target, shard.size_bytes)
        else:
            # Still arriving on the source: that backfill is cancelled.
            self.reservation.redirect(shard, target)
        self.final.move(shard.up_osd, target, shard.size_bytes)
        self.uses[target] += 1
        state.retarget(shard.up_osd, target)


def shard_order(shard: MappedShard) -> tuple:
    """Order a source's shards: largest first, then by PG and shard."""
    return (-shard.size_bytes, pgid_sort_key(shard.pgid), shard_key(shard))


def shard_key(shard: MappedShard) -> int:
    """Order a PG's shards: EC by shard index, replicated by up OSD."""
    return shard.shard if isinstance(shard.shard, int) else shard.up_osd


def balance(
    balancer: Balancer,
    by_source: dict[int, list[MappedShard]],
    states: dict[str, PgPlacement],
    *,
    max_moves: int | None,
    stop_below_other: bool,
) -> tuple[list[tuple[MappedShard, int]], Stop]:
    """Move shards off the sources, fullest first; return ((shard, target)s, why it stopped).

    by_source: each source's movable shards, in shard_order; consumed. Each
    turn moves the fullest source's first shard with a legal target. The
    run stops when that source has none, or, with stop_below_other, when a
    non-source is as full: either way the class's maximum cannot go down.
    """
    final = balancer.final
    heap = [(-final.utilization(s), s) for s in by_source]
    heapq.heapify(heap)
    if not heap:
        return [], Stop(STOP_NO_SOURCES)
    moves = []
    while True:
        if max_moves is not None and len(moves) >= max_moves:
            return moves, Stop(STOP_MAX_MOVES)
        neg_util, source = heap[0]
        if stop_below_other and balancer.targets:
            other_util, other = max(
                (final.utilization(t), -t) for t in balancer.targets
            )
            if other_util >= -neg_util:
                return moves, Stop(
                    STOP_NOT_A_SOURCE, source, -neg_util, -other, other_util
                )
        shards = by_source[source]
        if not shards:
            return moves, Stop(STOP_NO_SHARDS, source, -neg_util)
        for i, shard in enumerate(shards):
            state = states[shard.pgid]
            target = balancer.pick_target(shard, state)
            if target is not None:
                break
        else:
            return moves, Stop(STOP_NO_MOVE, source, -neg_util)
        balancer.commit(shard, state, target)
        del shards[i]
        moves.append((shard, target))
        heapq.heapreplace(heap, (-final.utilization(source), source))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class BalanceResult(NamedTuple):
    """What plan() decided, for render() to print."""

    osd_class: str
    moves: list[Move]  # in PG, then shard, order
    stop: Stop
    sources: list[int]  # fullest first
    qualified: int  # sources before the half-of-class cap
    targets: list[int]
    movable_count: int  # shards on the sources, in settled PGs
    unsettled_pgs: int  # PGs with a shard on a source, left alone
    chained_pgs: int  # of the others, left alone as their upmap pairs chain
    max_before: tuple[float, int]  # the class's highest final projection, OSD
    max_after: tuple[float, int]
    max_target_util: float
    ratios: FullRatios
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> BalanceResult:
    """Fetch the cluster state and work out what to move where.

    Exits on an unknown device class, a bad --osds, an invalid
    --max-target-util, or a pool it cannot analyze safely.
    """
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    upmap_items = fetch_upmap_items(store)
    pools_by_id = fetch_pools(store)
    ec_pool_ids = ec_pool_ids_from(list(pools_by_id.values()))
    crush_rules = fetch_crush_rules(store)
    ec_profiles = fetch_ec_profiles(store)
    ratios = fetch_full_ratios(store)
    max_target_util = resolve_max_target_util(args.max_target_util, ratios)

    candidates = build_candidate_osds(osd_df)
    class_osds = candidates.get(args.osd_class)
    if not class_osds:
        sys.exit(
            f"ERROR: --class: no up and in OSDs of class {args.osd_class!r}; "
            f"classes present: {', '.join(sorted(candidates)) or 'none'}."
        )
    pgs = fetch_pg_stats(store, "pg_dump_pgs")
    pool_ids = {pgid_pool_id(pg["pgid"]) for pg in pgs}
    unknown_pools = sorted(pool_ids - pools_by_id.keys())
    if unknown_pools:
        sys.exit(
            f"ERROR: PGs belong to pool id(s) {', '.join(map(str, unknown_pools))}, "
            "which 'ceph osd pool ls detail' does not list, so their shards "
            "cannot be analyzed."
        )

    def pg_info(pg: dict) -> tuple[bool, int]:
        pool_id = pgid_pool_id(pg["pgid"])
        return pool_id in ec_pool_ids, shard_size_bytes(
            pg, pools_by_id[pool_id], ec_profiles
        )

    # Every shard in motion, cluster-wide.
    arriving, departing = [], []
    for pg in pgs:
        if pg["up"] == pg["acting"]:
            continue
        is_ec, size = pg_info(pg)
        arriving.extend(find_arriving_shards(pg, is_ec, size))
        departing.extend((o, size) for o in departing_osds(pg, is_ec))
    reservation = ProjectedUsage(osd_df, arriving)
    final = FinalUsage(osd_df, ((s.up_osd, s.size_bytes) for s in arriving), departing)

    sources, qualified = select_sources(
        class_osds,
        final.utilization,
        osds=args.osds,
        min_source_util=args.min_source_util,
    )
    source_set = set(sources)
    targets = [o for o in class_osds if o not in source_set]

    def pairs_chain(pg: dict) -> bool:
        # pgremapper would drop a link of such a PG's pairs on any change
        # (see shared.avoid_chains), remapping a shard nobody proposed.
        existing = upmap_items.get(pg["pgid"], [])
        return chain_link([(m["from"], m["to"]) for m in existing]) is not None

    on_sources = [pg for pg in pgs if real_osd_set(pg["up"]) & source_set]
    settled = [pg for pg in on_sources if is_settled(pg)]
    chained = [pg for pg in settled if pairs_chain(pg)]
    settled = [pg for pg in settled if not pairs_chain(pg)]
    check_host_failure_domain(
        [pools_by_id[i] for i in sorted({pgid_pool_id(pg["pgid"]) for pg in settled})],
        crush_rules,
        "with a PG to move",
    )

    states: dict[str, PgPlacement] = {}
    by_source: dict[int, list[MappedShard]] = {s: [] for s in sources}
    for pg in settled:
        is_ec, size = pg_info(pg)
        found, _ = find_mapped_shards(pg, is_ec, source_set, size)
        raw = raw_crush_osds(pg["up"], upmap_items.get(pg["pgid"], []))
        state = PgPlacement(pg, is_ec, size, raw)
        # Moving onto an acting OSD would be a pin, not a move.
        state.forbidden_osds |= real_osd_set(pg["acting"])
        states[pg["pgid"]] = state
        for shard in found:
            by_source[shard.up_osd].append(shard)
    for shards in by_source.values():
        shards.sort(key=shard_order)
    movable_count = sum(map(len, by_source.values()))

    def class_max() -> tuple[float, int]:
        return max((final.utilization(o), -o) for o in class_osds)

    max_before = class_max()
    balancer = Balancer(
        targets,
        osd_df,
        osd_host,
        reservation=reservation,
        final=final,
        max_uses=args.max_target_uses,
        max_target_util=max_target_util,
    )
    placed, stop = balance(
        balancer,
        by_source,
        states,
        max_moves=args.max_moves,
        stop_below_other=not args.osds,
    )
    max_after = class_max()

    placed.sort(key=lambda p: (pgid_sort_key(p[0].pgid), shard_key(p[0])))
    moves = [
        Move(
            s.pgid,
            s.shard,
            s.acting_osd,
            s.up_osd,
            target,
            s.size_bytes,
            final.utilization(s.up_osd),
            final.utilization(target),
        )
        for s, target in placed
    ]
    return BalanceResult(
        osd_class=args.osd_class,
        moves=moves,
        stop=stop,
        sources=sources,
        qualified=qualified,
        targets=targets,
        movable_count=movable_count,
        unsettled_pgs=len(on_sources) - len(settled) - len(chained),
        chained_pgs=len(chained),
        max_before=(max_before[0], -max_before[1]),
        max_after=(max_after[0], -max_after[1]),
        max_target_util=max_target_util,
        ratios=ratios,
        osd_df=osd_df,
        osd_host=osd_host,
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# (group, label), along the shard's path: ACTING (data now), FROM (the
# source it is mapped to), TARGET (proposed). PROJ: see the module docstring.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("", "SIZE"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("FROM", "OSD"),
    ("FROM", "UTIL"),
    ("FROM", "PROJ"),
    ("FROM", "HOST"),
    ("TARGET", "OSD"),
    ("TARGET", "UTIL"),
    ("TARGET", "PROJ"),
    ("TARGET", "HOST"),
]


def format_row(
    move: Move, osd_host: dict[int, str], osd_df: dict[int, dict]
) -> list[str]:
    from_cells = osd_cells(osd_df, osd_host, move.from_osd)
    target_cells = osd_cells(osd_df, osd_host, move.target_osd)
    return [
        move.pgid,
        str(move.shard),
        format_bytes(move.size_bytes),
        *osd_cells(osd_df, osd_host, move.acting_osd),
        *from_cells[:2],
        f"{move.from_projected:.1f}%",
        from_cells[2],
        *target_cells[:2],
        f"{move.target_projected:.1f}%",
        target_cells[2],
    ]


def describe_sources(result: BalanceResult, args: argparse.Namespace) -> str:
    """Return how the sources were chosen, e.g. 'the fuller half (12 of 24)'."""
    n, total = len(result.sources), len(result.sources) + len(result.targets)
    if args.osds:
        return f"{n} given by --osds"
    if args.min_source_util is None:
        return f"the fuller half, {n} of {total}"
    capped = f", capped at half of the class ({n})" if result.qualified > n else ""
    return (
        f"{result.qualified} of {total} at or above --min-source-util "
        f"{args.min_source_util:g}%{capped}"
    )


def describe_stop(result: BalanceResult, args: argparse.Namespace) -> str:
    """Return why balancing stopped, as a sentence."""
    stop = result.stop
    if stop.reason == STOP_MAX_MOVES:
        return f"Stopped at --max-moves {args.max_moves}."
    if stop.reason == STOP_NO_SOURCES:
        return "No sources: nothing to move."
    if stop.reason == STOP_NO_SHARDS:
        return (
            f"Stopped: osd.{stop.source}, the fullest source at "
            f"{stop.source_util:.1f}% projected, has no movable shard left (its "
            "other PGs are unsettled, already leaving it, or left alone)."
        )
    if stop.reason == STOP_NOT_A_SOURCE:
        return (
            f"Stopped: osd.{stop.other}, not a source, is projected at "
            f"{stop.other_util:.1f}%, as full as the fullest source "
            f"(osd.{stop.source}, {stop.source_util:.1f}%), so no move can "
            "lower the maximum."
        )
    return (
        f"Stopped: osd.{stop.source}, the fullest source at "
        f"{stop.source_util:.1f}% projected, has no shard with a legal target "
        "(one that would end up less full, within --max-target-util and "
        "--max-target-uses)."
    )


def render(result: BalanceResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    cls = result.osd_class
    stderr_para(
        f"Sources: {describe_sources(result, args)} up and in {cls} OSD(s); "
        f"{result.movable_count} shard(s) on them can move. PGs left alone: "
        f"{result.unsettled_pgs} not active, or degraded, undersized, "
        f"recovering or peering; {result.chained_pgs} whose upmap pairs "
        "chain (A->B, B->C), which pgremapper would break."
    )
    stderr_para(
        f"Targets: the other {len(result.targets)} {cls} OSD(s), up to "
        f"--max-target-uses {args.max_target_uses} shard(s) each, projected at "
        f"or below --max-target-util {result.max_target_util:g}% "
        f"(backfillfull_ratio {result.ratios.backfillfull:g}%)."
    )

    if args.pgremapper_mappings:
        print_upmap_pairs((m.pgid, m.from_osd, m.target_osd) for m in result.moves)
    elif result.moves:
        print_table(
            COLUMNS,
            [format_row(m, result.osd_host, result.osd_df) for m in result.moves],
        )

    (before, before_osd), (after, after_osd) = result.max_before, result.max_after
    moved = sum(m.size_bytes for m in result.moves)
    stderr_para(
        f"Proposed {len(result.moves)} move(s), {format_bytes(moved)}. Highest "
        f"projected {cls} utilization: {before:.1f}% (osd.{before_osd}) -> "
        f"{after:.1f}% (osd.{after_osd}). {describe_stop(result, args)}"
    )


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
