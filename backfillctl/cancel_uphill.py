# SPDX-License-Identifier: MIT
"""
Propose upmaps that cancel "uphill" backfills: those moving data from a
less-utilized OSD to a more-utilized one, often a side effect of a CRUSH or
OSD change.

Each uphill shard is pinned to the OSD that holds it now, with any companions
it needs (see cancel-backfill). A PG's uphill shards are pinned together or
not at all.

A destination's utilization includes what the backfill has copied so far,
while the source keeps its copy until the PG is clean, so a move can look
uphill once partly done. --min-delta filters out such small differences.

A PG whose pins would chain is left out (see cancel-backfill).

Apply the output as with cancel-backfill. Consider 'ceph balancer off' while
the pins are in place. Assumes the CRUSH failure domain is host.
"""

import argparse
from typing import NamedTuple

from shared import (
    COLUMNS,
    POOL_TYPE_ERASURE,
    PROGRESS_APPROX_NOTE,
    Cancellation,
    HelpFormatter,
    Pair,
    PgidFilter,
    Skipped,
    SnapshotStore,
    add_exclude_pgs_arg,
    add_load_state_arg,
    add_pgremapper_mappings_arg,
    avoid_chains,
    check_host_failure_domain,
    check_known_pools,
    close_pins,
    copies_moving,
    ec_shard_moves,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pools,
    fetch_remapped_pg_stats,
    fetch_upmap_items,
    format_bytes,
    format_row,
    order_moves,
    percentage_points,
    pg_progress_pct,
    pgid_pool_id,
    pgid_sort_key,
    pin_replica,
    print_pgid_filter,
    print_pgremapper_mappings,
    print_table,
    real_osd_set,
    shard_size_bytes,
    skipped_sort_key,
    stderr_items,
    stderr_para,
    warn_chains,
    with_exact_progress,
)

# Same as cancel-backfill's.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_ls_remapped": ["ceph", "pg", "ls", "remapped", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cancel-uphill",
        help="Cancel backfills that move data to a fuller OSD.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--min-delta",
        type=percentage_points,
        default=1.0,
        metavar="PERCENT",
        help="Minimum utilization difference, in percentage points, for a "
        "move to count as uphill (default: %(default)s).",
    )
    add_exclude_pgs_arg(parser)
    add_pgremapper_mappings_arg(parser)
    add_load_state_arg(parser, after_command=True)
    return parser


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def find_uphill_shards(
    up: list,
    acting: list,
    is_ec: bool,
    osd_df: dict[int, dict],
    min_delta: float = 1.0,
) -> tuple[list[tuple["int | str", int, int]], list[tuple["int | str", str]]]:
    """Return (candidates, skipped) for a PG's uphill shards.

    candidates: (shard, acting_osd, up_osd) where up is at least min_delta
    points more utilized. skipped: (shard, reason) where that cannot be
    decided (unknown utilization, no acting OSD, ambiguous replica pairing).
    Shards that are not uphill appear in neither.
    """

    def utilization(osd_id: int) -> float | None:
        return osd_df.get(osd_id, {}).get("utilization")

    candidates: list[tuple[int | str, int, int]] = []
    skipped: list[tuple[int | str, str]] = []

    if is_ec:
        for shard, source, destination in ec_shard_moves(up, acting):
            if source is None:
                skipped.append((shard, "no acting OSD for this shard (degraded)"))
                continue
            source_util, dest_util = utilization(source), utilization(destination)
            if source_util is None or dest_util is None:
                skipped.append(
                    (
                        shard,
                        f"utilization unknown for osd.{source} or osd.{destination}",
                    )
                )
            elif dest_util - source_util >= min_delta:
                candidates.append((shard, source, destination))
        return candidates, skipped

    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    arriving, departing = up_set - acting_set, acting_set - up_set
    if len(arriving) == 1 and len(departing) == 1:
        source, destination = next(iter(departing)), next(iter(arriving))
        source_util, dest_util = utilization(source), utilization(destination)
        if source_util is None or dest_util is None:
            skipped.append(
                ("-", f"utilization unknown for osd.{source} or osd.{destination}")
            )
        elif dest_util - source_util >= min_delta:
            candidates.append(("-", source, destination))
    elif not departing:
        # Replicas only being added have nothing to pin back to.
        if arriving:
            skipped.append(("-", "no acting OSD to pin to (missing replica)"))
    elif not arriving:
        pass  # replicas only being removed: nothing to cancel
    else:
        skipped.append(("-", "several replicas moving, pairing is ambiguous"))
    return candidates, skipped


def plan_cancellations(
    pg_stats: list[dict],
    pools: dict[int, dict],
    ec_profiles: dict[str, dict],
    osd_host: dict[int, str],
    crush_rules: dict[int, dict],
    osd_df: dict[int, dict],
    exclude_pgs: frozenset[str] | set[str] = frozenset(),
    min_delta: float = 1.0,
) -> tuple[list[Cancellation], list[Skipped]]:
    """Return (cancellations, skipped) for every PG's uphill shards.

    A PG's uphill shards (find_uphill_shards) are pinned in one close_pins
    pass, so companions and chain order stay consistent; if that fails, all
    of them are skipped. PGs in exclude_pgs are ignored.

    Exits with an error if a pool is unknown or its failure domain is not
    host.
    """
    pg_stats = [pg for pg in pg_stats if pg["pgid"] not in exclude_pgs]
    pgids = [pg["pgid"] for pg in pg_stats]
    check_known_pools(pgids, pools, "remapped PGs")
    check_host_failure_domain(pgids, pools, crush_rules, "remapped PGs")

    cancellations, skipped = [], []
    for pg in pg_stats:
        up, acting = pg["up"], pg["acting"]
        pgid = pg["pgid"]
        pool = pools[pgid_pool_id(pgid)]
        is_ec = pool.get("type") == POOL_TYPE_ERASURE
        size = shard_size_bytes(pg, pool, ec_profiles)
        progress = pg_progress_pct(
            pg, copies_moving(up, acting, is_ec, pool.get("size", 0))
        )

        candidates, unpinnable = find_uphill_shards(
            up, acting, is_ec, osd_df, min_delta
        )
        skipped.extend(Skipped(pgid, shard, why) for shard, why in unpinnable)
        if not candidates:
            continue

        requested = sorted(shard for shard, _, _ in candidates)
        note = requested[0] if len(requested) == 1 else ", ".join(map(str, requested))

        if is_ec:
            pins = {shard: acting_osd for shard, acting_osd, _ in candidates}
            closed, why = close_pins(up, acting, pins, osd_host)
            moves = []
            if why is None:
                moves, why = order_moves([(s, up[s], a) for s, a in closed.items()])
            if why is not None:
                skipped.extend(Skipped(pgid, s, why) for s in requested)
                continue
            cancellations.extend(
                Cancellation(
                    pgid,
                    s,
                    from_osd,
                    to_osd,
                    size,
                    pg["state"],
                    progress,
                    None if s in requested else note,
                )
                for s, from_osd, to_osd in moves
            )
        else:
            # find_uphill_shards yields at most one replicated candidate.
            ((shard, source, destination),) = candidates
            why = pin_replica(up, destination, source, osd_host)
            if why is not None:
                skipped.append(Skipped(pgid, shard, why))
                continue
            cancellations.append(
                Cancellation(
                    pgid, shard, destination, source, size, pg["state"], progress
                )
            )

    return (
        sorted(cancellations, key=lambda c: pgid_sort_key(c.pgid)),
        sorted(skipped, key=skipped_sort_key),
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_summary(cancellations: list[Cancellation], skipped: list[Skipped]) -> None:
    """Summarize the proposal on stderr, and list what cannot be pinned."""
    direct = [c for c in cancellations if c.companion_of is None]
    others = len(cancellations) - len(direct)
    known = [c.size_bytes for c in direct if c.size_bytes is not None]
    total = sum(known)
    unknown = len(direct) - len(known)
    pgs = len({c.pgid for c in cancellations})
    stderr_para(
        f"{len(direct)} uphill shard(s) in {pgs} PG(s) can be pinned back, "
        f"~{format_bytes(total)} of data"
        + (f" (+{unknown} of unknown size)" if unknown else "")
        + f"; {len(skipped)} cannot be judged or pinned."
        + (
            f" {others} more shard(s), moving to other OSDs, are pinned too "
            "(companions)."
            if others
            else ""
        )
    )
    stderr_items(f"cannot pin {s.pgid} shard {s.shard}: {s.reason}" for s in skipped)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class UphillResult(NamedTuple):
    """What plan() decided, for render() to print."""

    cancellations: list[Cancellation]  # in apply order, see plan_cancellations
    skipped: list[Skipped]
    chained: dict[str, list[Pair]]  # see shared.avoid_chains
    exclude_filter: PgidFilter | None  # None without --exclude-pgs
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> UphillResult:
    """Fetch the cluster state and work out which shards to pin back."""
    exclude_pgs = set(args.exclude_pgs)
    osd_df = fetch_osd_df(store)
    osd_host = fetch_osd_hosts(store)
    pg_stats = fetch_remapped_pg_stats(store)
    pools = fetch_pools(store)
    ec_profiles = fetch_ec_profiles(store)
    crush_rules = fetch_crush_rules(store)

    exclude_filter = None
    if exclude_pgs:
        exclude_filter = PgidFilter.of(exclude_pgs, (pg["pgid"] for pg in pg_stats))
        print_pgid_filter(
            "--exclude-pgs",
            exclude_filter,
            "matched a remapped PG and were left alone",
            "not remapped",
        )

    cancellations, skipped = plan_cancellations(
        pg_stats,
        pools,
        ec_profiles,
        osd_host,
        crush_rules,
        osd_df,
        exclude_pgs,
        args.min_delta,
    )
    # A PG's uphill shards go together (plan_cancellations), so no partial.
    resolved = avoid_chains(
        cancellations, fetch_upmap_items(store), pg_stats, osd_host, partial=False
    )
    cancellations = resolved.cancellations
    skipped = sorted(skipped + resolved.skipped, key=skipped_sort_key)
    if not args.pgremapper_mappings:  # JSON has no PROGRESS: skip the queries
        cancellations = with_exact_progress(store, cancellations, pg_stats, pools)
    return UphillResult(
        cancellations=cancellations,
        skipped=skipped,
        chained=resolved.chained,
        exclude_filter=exclude_filter,
        osd_df=osd_df,
        osd_host=osd_host,
    )


def render(result: UphillResult, args: argparse.Namespace) -> None:
    """Print result: pins on stdout in the format args asks for, notes on stderr."""
    cancellations = result.cancellations
    if not cancellations and not result.skipped:
        stderr_para("No uphill backfills.")
        if args.pgremapper_mappings:
            print_pgremapper_mappings([])
        return

    if args.pgremapper_mappings:
        print_pgremapper_mappings(cancellations)
    elif cancellations:
        print_table(
            COLUMNS,
            [format_row(c, result.osd_df, result.osd_host) for c in cancellations],
        )
    print_summary(cancellations, result.skipped)
    if not args.pgremapper_mappings and any(
        c.progress_pct is not None and not c.progress_exact for c in cancellations
    ):
        stderr_para(f"NOTE: {PROGRESS_APPROX_NOTE}")
    if result.chained:
        warn_chains(result.chained)
    stderr_para(
        "NOTE: cancelling a running backfill discards its progress. Consider "
        "'ceph balancer off' while these are pinned."
    )


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
