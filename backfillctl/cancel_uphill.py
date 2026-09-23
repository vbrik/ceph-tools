# SPDX-License-Identifier: MIT
"""
Propose upmaps that cancel backfills moving data "uphill": from a
less-utilized OSD to a more-utilized one.

Why
---
Backfill exists to balance the cluster, so a backfill that moves data the
other way -- onto an OSD that is already fuller than the one the data is
leaving -- is working against that goal. This can happen after a manual
CRUSH or OSD change (a reweight, an OSD added/removed, a rule edit) sends
some shard from a lightly-used OSD to a heavily-used one as a side effect.
Cancelling those specific backfills lets the ones that are actually
improving balance proceed undisturbed.

How
---
Every remapped PG is checked shard by shard (EC by position, replicated by
the single arriving/departing OSD, when unambiguous -- see "Selection"
below). A shard whose acting (current) OSD is less utilized than its up
(destination) OSD is pinned back to where it is now, the same way
cancel-backfill does: an upmap pair '<destination> -> <acting OSD>' makes
'up' equal 'acting' for that shard, so nothing moves. If more than one shard
of a PG is uphill, all of them are pinned together in one pass, so their
companions (below) and chain order are computed consistently rather than one
shard at a time.

Selection
---------
A replicated PG's replicas are interchangeable, so which departing OSD
pairs with which arriving one is only unambiguous when exactly one of each
is moving; with more, the PG's shard is reported as skipped ("ambiguous")
rather than guessed at, the same refusal cancel-backfill's --osd mode makes
in the same situation. A shard whose acting or up OSD has no utilization
figure in 'ceph osd df' (e.g. a down OSD) is also skipped rather than
guessed at. A shard that is moving but not uphill by at least --min-delta
percentage points (default 1.0) is simply not selected; most shards of a
remapped PG fall in this bucket, so it is not reported as skipped -- that
list is only for shards this tool could otherwise identify as uphill but
cannot pin or decide about. --min-delta exists because a destination's
reported utilization already includes whatever this backfill has copied so
far, while the source keeps its full copy until the PG goes clean, so a
move that actually started downhill can read as uphill once it is partway
done; see find_uphill_shards' docstring.

Companion pins and chains
--------------------------
Pinning a shard back can require pinning another shard of the same PG too,
if the pinned shard's acting OSD would otherwise share a host with a shard
still moving there (Ceph would silently drop the upmap): see
cancel_backfill.py's "Companion pins" for why. The two subcommands share
that logic (shared.close_pins and friends), so it behaves identically here;
a companion that is not itself uphill is still pinned, and shown with a
"companion of shard N" note (or "companion of shard N, M" when it was
needed by more than one of the PG's own uphill shards). Chained pairs
(shared.chained_pgs) are handled and reported the same way as in
cancel-backfill too.

Testing against saved cluster state
------------------------------------
Same as cancel-backfill: 'backfillctl --load-state DIR cancel-uphill' replays
a 'backfillctl save-state' capture instead of calling 'ceph'.

Applying the output
--------------------
Same as cancel-backfill: --pgremapper-mappings prints a JSON array for
'pgremapper import-mappings'; review it and keep only the pairs for the
backfills you want cancelled. See cancel_backfill.py's "Applying the
output" and "Chained pairs" for the details (import-mappings usage,
--pgremapper-mappings' care around chains).
"""

import argparse
import sys
from typing import NamedTuple

from shared import (
    COLUMNS,
    POOL_TYPE_ERASURE,
    PROGRESS_APPROX_NOTE,
    Cancellation,
    PgidFilter,
    Skipped,
    SnapshotStore,
    chained_pgs,
    close_pins,
    copies_moving,
    ec_shard_moves,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pools,
    fetch_remapped_pg_stats,
    format_bytes,
    format_row,
    order_moves,
    pg_progress_pct,
    pgid_sort_key,
    pin_replica,
    print_pgremapper_mappings,
    print_table,
    real_osd_set,
    rule_failure_domain,
    shard_size_bytes,
    stderr_para,
    warn_chained_pgs,
    with_exact_progress,
    wrap_text,
)

# Maps each snapshot to the 'ceph ... --format json' command that produces
# it. Same shape as cancel_backfill.py's (see its SNAPSHOT_COMMANDS): a
# --load-state capture from either subcommand, or from save-state, works
# for both.
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
        description="Propose the upmaps needed to cancel backfills that move "
        "data from a less-utilized OSD to a more-utilized one ('uphill'): "
        "the opposite of what backfill is supposed to accomplish. Every "
        "moving shard of every remapped PG is checked (see the docstring at "
        "the top of this script for exactly how); the uphill ones are "
        "pinned to the OSD they are on now, the same way cancel-backfill "
        "does, including any companion shard needed to keep the resulting "
        "'up' set valid. When a PG has more than one uphill shard, they are "
        "pinned together as one unit: if that combined pin is not valid, "
        "none of the PG's uphill shards are proposed, not just the one that "
        "clashed. Prints the proposals only; nothing is changed.",
        epilog="See the docstring at the top of this script, and "
        "cancel_backfill.py's, for companions, chains and how to apply the "
        "output.",
    )
    parser.add_argument(
        "--exclude-pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="PG id(s) to leave alone: skip entirely (no pins, no "
        "companions) even if they have an uphill backfill to cancel. "
        "Space-separated, e.g. --exclude-pgs 19.92e 20.1a3. A given id that "
        "does not match a remapped PG is reported on stderr, since that "
        "usually means a typo.",
    )
    parser.add_argument(
        "--pgremapper-mappings",
        action="store_true",
        help="Print a JSON array for 'pgremapper import-mappings' instead of "
        "the table, one {pgid, mapping} entry per line. This is the reliable "
        "way to apply the proposals: all pairs of a PG go in together.",
    )
    parser.add_argument(
        "--min-delta",
        type=float,
        default=1.0,
        metavar="PERCENT",
        help="Only treat a shard as uphill when its destination OSD is at "
        "least this many percentage points more utilized than its source "
        "(default: 1.0). A destination's reported utilization already "
        "includes whatever this backfill has copied so far, while the "
        "source still holds its full copy until the PG goes clean, so a "
        "move that actually started downhill can read as uphill once it is "
        "partway done; this filters out deltas small enough to plausibly be "
        "that artifact rather than a real difference.",
    )
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
    """Return (candidates, skipped) for the PG's uphill shards.

    candidates holds (shard, acting_osd, up_osd): the EC shard index, or '-'
    for replicated pools, plus the OSD it is on now (less utilized) and the
    OSD it is headed for (more utilized), where up's utilization exceeds
    acting's by at least min_delta percentage points. min_delta exists
    because the destination's reported utilization already includes
    whatever this backfill has copied so far while the source still holds
    its full copy until the PG goes clean, so a move that actually started
    downhill can read as uphill once it is partway done; the default (1.0)
    filters out deltas small enough to plausibly be that artifact rather
    than a real difference. skipped holds (shard, reason) for a shard this
    cannot decide about: unknown utilization on either end, no acting OSD
    (degraded), or -- replicated pools only -- more than one replica moving
    at once, which makes the departing/arriving pairing ambiguous (the same
    refusal cancel-backfill's --osd mode makes).

    A shard that is moving but not uphill by at least min_delta is not
    returned at all: most shards of a remapped PG fall in this bucket, and
    reporting each as "skipped" would bury the ones this tool actually
    cannot decide about.
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
        # Either nothing is moving (both empty) or replicas are only being
        # added (missing replica, nothing to pin back to): neither is a move
        # to report on, uphill or otherwise.
        if arriving:
            skipped.append(("-", "no acting OSD to pin to (missing replica)"))
    elif not arriving:
        pass  # replicas only being removed: nothing is backfilling in
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

    min_delta is passed through to find_uphill_shards (see its docstring).

    A PG's uphill shards (find_uphill_shards) are pinned together in one
    close_pins/order_moves pass, exactly as cancel-backfill's cancel_whole_pg
    pins every moving shard of a PG together: that keeps companions and
    chain order consistent when a PG has more than one uphill shard, rather
    than resolving each independently and risking inconsistent or duplicate
    pins. If the combined set cannot be pinned validly, every one of the
    PG's uphill shards is reported as skipped with the reason; none are
    proposed. A PG whose id is in exclude_pgs is left alone entirely.

    Exits with an error if a PG's pool is missing from 'pools', or its CRUSH
    failure domain is not 'host' (this tool only checks for same-host
    clashes), same as cancel-backfill.
    """
    cancellations, skipped = [], []
    for pg in pg_stats:
        up, acting = pg["up"], pg["acting"]
        pgid = pg["pgid"]
        if pgid in exclude_pgs:
            continue
        pool = pools.get(int(pgid.split(".")[0]))
        if pool is None:
            sys.exit(
                f"ERROR: PG {pgid} belongs to a pool that 'ceph osd pool ls "
                "detail' does not list, so its shards cannot be analyzed."
            )
        domain = rule_failure_domain(crush_rules.get(pool.get("crush_rule")))
        if domain != "host":
            sys.exit(
                f"ERROR: pool {pool['pool_id']} (PG {pgid}) has CRUSH failure "
                f"domain {domain or 'unknown'}; this script only checks for "
                "same-host clashes, so it cannot tell which pins are valid."
            )
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
            # find_uphill_shards' replicated branch only ever appends a
            # candidate inside its "exactly one arriving, one departing"
            # case, so there can be at most one here.
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

    def skipped_order(item: Skipped) -> tuple:
        return (*pgid_sort_key(item.pgid), item.shard if item.shard != "-" else -1)

    return (
        sorted(cancellations, key=lambda c: pgid_sort_key(c.pgid)),
        sorted(skipped, key=skipped_order),
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_exclude_filter(exclude_filter: PgidFilter) -> None:
    """Report on stderr what --exclude-pgs matched, naming the ids that matched nothing."""
    unmatched = exclude_filter.unmatched
    stderr_para(
        f"NOTE: --exclude-pgs: {exclude_filter.matched} of {exclude_filter.given} "
        "given PG id(s) matched a remapped PG and were left alone"
        + (
            f"; {len(unmatched)} matched nothing (check for typos): "
            f"{', '.join(unmatched)}."
            if unmatched
            else "."
        )
    )


def print_summary(cancellations: list[Cancellation], skipped: list[Skipped]) -> None:
    """Report on stderr what the proposal covers."""
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
        + f"; {len(skipped)} cannot be determined or pinned."
        + (
            f" {others} more shard(s) of those PGs, moving to other OSDs, are "
            "pinned back too, to keep the resulting upmap valid."
            if others
            else ""
        )
    )
    for s in skipped:
        print(
            wrap_text(f"cannot pin {s.pgid} shard {s.shard}: {s.reason}", indent="  "),
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class UphillResult(NamedTuple):
    """Everything a run decides, independent of how it is printed.

    plan() computes it and render() prints it, so tests of the planning can
    assert on these fields and survive changes to the output format.
    """

    cancellations: list[Cancellation]  # in apply order, see plan_cancellations
    skipped: list[Skipped]
    chained: dict[str, list[Cancellation]]  # see shared.chained_pgs
    exclude_filter: PgidFilter | None  # None without --exclude-pgs
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> UphillResult:
    """Fetch the cluster state from store and work out which shards to pin back."""
    exclude_pgs = set(args.exclude_pgs)
    osd_df = fetch_osd_df(store)
    osd_host = fetch_osd_hosts(store)
    pg_stats = fetch_remapped_pg_stats(store)
    pools = fetch_pools(store)
    ec_profiles = fetch_ec_profiles(store)
    crush_rules = fetch_crush_rules(store)

    exclude_filter = None
    if exclude_pgs:
        matched = {pg["pgid"] for pg in pg_stats if pg["pgid"] in exclude_pgs}
        exclude_filter = PgidFilter(
            len(exclude_pgs), len(matched), sorted(exclude_pgs - matched)
        )
        print_exclude_filter(exclude_filter)

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
    if not args.pgremapper_mappings:  # the JSON has no PROGRESS: spare the queries
        cancellations = with_exact_progress(store, cancellations, pg_stats, pools)
    return UphillResult(
        cancellations=cancellations,
        skipped=skipped,
        chained=chained_pgs(cancellations),
        exclude_filter=exclude_filter,
        osd_df=osd_df,
        osd_host=osd_host,
    )


def render(result: UphillResult, args: argparse.Namespace) -> None:
    """Print result: pins on stdout in the format args asks for, notes on stderr."""
    cancellations = result.cancellations
    if not cancellations and not result.skipped:
        print("No uphill backfills.", file=sys.stderr)
        if args.pgremapper_mappings:
            print_pgremapper_mappings([])
        return

    print_summary(cancellations, result.skipped)
    chained = result.chained
    machine_format = args.pgremapper_mappings
    printable = (
        [c for c in cancellations if c.pgid not in chained]
        if machine_format
        else cancellations
    )
    if machine_format:
        print_pgremapper_mappings(printable)
    elif cancellations:
        print_table(
            COLUMNS,
            [format_row(c, result.osd_df, result.osd_host) for c in cancellations],
        )
        if any(
            c.progress_pct is not None and not c.progress_exact for c in cancellations
        ):
            stderr_para(f"NOTE: {PROGRESS_APPROX_NOTE}")
    if chained:
        warn_chained_pgs(chained, left_out=machine_format)
    stderr_para(
        "NOTE: cancelling a running backfill discards its progress. Consider "
        "'ceph balancer off' while these are pinned."
    )


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
