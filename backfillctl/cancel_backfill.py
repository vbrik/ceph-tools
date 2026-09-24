# SPDX-License-Identifier: MIT
"""
Propose upmaps that cancel backfills: all of them, or with --osd those into
one OSD.

Each moving shard is pinned to the OSD that holds it now, so nothing moves.
Ceph refuses a backfill whose target would be projected past
backfillfull_ratio, counting every backfill queued for that OSD, so
cancelling backfills into a full OSD makes room for the ones you want.
Without --osd, this freezes all data movement so you can let backfills
through selectively.

Remove the entries of backfills you want to keep, or pass their PGs to
--exclude-pgs. Cancelling a running backfill discards its progress.

pgremapper cannot apply chained pairs (A->B, B->C). Without --osd, the last
pin of a chain is kept and the backfills before it keep running; with
--osd, a PG whose requested pin would chain is left out. Both are listed on
stderr, with 'ceph osd pg-upmap-items' commands that pin everything.

With --osd, the NOTE column marks pins of backfills into other OSDs. In
the JSON, each entry has its NOTE as 'note', plus 'shard' and 'role'
(requested, companion or blocker).

- companion: another shard of the PG moving onto the host of a pinned
  shard. Ceph drops an upmap that would put two shards of a PG on one host,
  so the two can only be cancelled together. Keep or remove them together.
- blocker (--pin-blockers): another shard of the PG whose target would reach
  backfillfull_ratio, and the blocker's own companions. backfill_toofull
  holds back the whole PG, including a backfill you keep. Keep a PG's
  blocker entries when you keep its backfill into --osd.

Apply the output with pgremapper, which adds to a PG's existing upmap pairs:

    backfillctl cancel-backfill --osd 682 --pin-blockers --pgremapper-mappings > m.json
    # keep 19.92e's backfill into osd.682: drop its pin and companions, keep its blockers
    jq 'map(select(.pgid != "19.92e" or .role == "blocker"))' m.json > m2.json
    pgremapper import-mappings m2.json

Pass pgremapper a file, not stdin: it prompts for confirmation.

Consider 'ceph balancer off' while the pins are in place. Assumes the CRUSH
failure domain is host.
"""

import argparse
import sys
from typing import NamedTuple

from shared import (
    COLUMNS,
    KIB,
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
    check_osds_exist,
    close_pins,
    copies_moving,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pools,
    fetch_remapped_pg_stats,
    fetch_upmap_items,
    format_bytes,
    format_row,
    is_real_osd,
    order_moves,
    parse_osd,
    pg_progress_pct,
    pgid_pool_id,
    pgid_sort_key,
    pin_replica,
    pin_with_companions,
    print_pgid_filter,
    print_pgremapper_mappings,
    print_table,
    real_osd_set,
    same_place,
    shard_size_bytes,
    skipped_sort_key,
    slot,
    stderr_items,
    stderr_para,
    warn_chains,
    with_exact_progress,
)

# Live runs read pg_ls_remapped; --load-state filters pg_dump_pgs instead
# (see fetch_remapped_pg_stats).
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
        "cancel-backfill",
        help="Cancel backfills (all, or into one OSD).",
        description=__doc__,
        formatter_class=HelpFormatter,
        # Hand-written: argparse cannot show that --pin-blockers needs --osd.
        usage="%(prog)s [-h] [--osd OSD [--pin-blockers]]\n"
        + " " * len("usage: backfillctl cancel-backfill ")
        + "[--exclude-pgs PGID [PGID ...]] [--pgremapper-mappings]\n"
        + " " * len("usage: backfillctl cancel-backfill ")
        + "[--load-state DIR]",
    )
    parser.add_argument(
        "--osd",
        type=parse_osd,
        help="Cancel only backfills into OSD (and their companions).",
    )
    parser.add_argument(
        "--pin-blockers",
        action="store_true",
        help="Also pin blockers.",
    )
    add_exclude_pgs_arg(parser)
    add_pgremapper_mappings_arg(parser)
    add_load_state_arg(parser, after_command=True)
    return parser


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def fetch_backfillfull_pct(store: SnapshotStore) -> float | None:
    """Return the cluster's backfillfull_ratio as a percentage, or None if absent."""
    ratio = store.json("osd_dump").get("backfillfull_ratio")
    return None if ratio is None else ratio * 100


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


def find_arrivals(
    up: list, acting: list, osd: int, is_ec: bool
) -> tuple[list[tuple["int | str", int]], list[tuple["int | str", str]]]:
    """Return the shards arriving on osd as (pins, skipped).

    pins: (shard, acting_osd) for shards with an OSD to pin back to (valid or
    not, see pin_with_companions). skipped: (shard, reason) for the rest.
    shard is the EC shard index, or '-' for replicated pools.
    """
    pins, skipped = [], []
    if is_ec:
        for i in range(len(up)):
            if slot(up, i) != osd:
                continue
            acting_osd = slot(acting, i)
            if acting_osd == osd:
                continue
            if acting_osd is None:
                skipped.append((i, "no acting OSD for this shard (degraded)"))
            else:
                pins.append((i, acting_osd))
        return pins, skipped

    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    if osd not in up_set or osd in acting_set:
        return pins, skipped
    arriving = up_set - acting_set
    departing = acting_set - up_set
    if len(arriving) == 1 and len(departing) == 1:
        pins.append(("-", next(iter(departing))))
    elif not departing:
        skipped.append(("-", "no acting OSD to pin to (missing replica)"))
    else:
        skipped.append(("-", "several replicas moving, pairing is ambiguous"))
    return pins, skipped


def projected_utilization(
    osd_df: dict[int, dict], osd_id: int, size_bytes: int | None
) -> float | None:
    """Return an OSD's utilization (percent) once one more shard has landed.

    A lower bound of what Ceph projects: other shards arriving on the OSD are
    not counted. None if the OSD's capacity is unknown.
    """
    node = osd_df.get(osd_id)
    if not node or not node.get("kb"):
        return None
    return (node["kb_used"] * KIB + (size_bytes or 0)) / (node["kb"] * KIB) * 100


def find_blockers(
    up: list,
    acting: list,
    pinned: dict[int, int],
    osd_df: dict[int, dict],
    backfillfull_pct: float,
    size_bytes: int | None,
) -> list[int]:
    """Return the EC shards, in order, that would hold the PG in backfill_toofull.

    One refused reservation holds back the whole PG. A blocker is an
    unpinned, moving shard with an acting OSD whose target would reach
    backfillfull_pct.
    """
    blockers = []
    for j, target in enumerate(up):
        if j in pinned or not is_real_osd(target) or slot(acting, j) in (None, target):
            continue
        projected = projected_utilization(osd_df, target, size_bytes)
        if projected is not None and projected >= backfillfull_pct:
            blockers.append(j)
    return blockers


def cancel_whole_pg(
    up: list, acting: list, is_ec: bool, osd_host: dict[int, str]
) -> tuple[list[tuple["int | str", int, int]], list[tuple["int | str", str]]]:
    """Pin every moving shard of a PG back to its acting OSD (no --osd).

    Returns (moves, skipped): moves are (shard, up_osd, acting_osd) in apply
    order (order_moves); skipped are (shard, reason). All-or-nothing per PG.

    Replicated OSDs are paired in sorted order: with every replica pinned,
    any pairing restores the acting set.
    """
    if is_ec:
        pins, skipped = {}, []
        for i, target in enumerate(up):
            current = slot(acting, i)
            if not is_real_osd(target) or current == target:
                continue
            if current is None:
                skipped.append((i, "no acting OSD for this shard (degraded)"))
            else:
                pins[i] = current
        if not pins:
            return [], skipped
        closed, why = close_pins(up, acting, pins, osd_host)
        moves = []
        if why is None:
            moves, why = order_moves([(s, up[s], a) for s, a in closed.items()])
        if why is not None:
            return [], skipped + [(s, why) for s in pins]
        return moves, skipped

    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    arriving = sorted(up_set - acting_set)
    departing = sorted(acting_set - up_set)
    pairs = list(zip(arriving, departing))
    skipped = [
        ("-", f"no acting OSD to pin osd.{osd} to (missing replica)")
        for osd in arriving[len(pairs) :]
    ]
    new_up = (up_set - {a for a, _ in pairs}) | {d for _, d in pairs}
    for _, to_osd in pairs:
        for other in new_up:
            if other != to_osd and same_place(to_osd, other, osd_host):
                why = f"acting osd.{to_osd} shares a host with replica osd.{other}"
                return [], skipped + [("-", why) for _ in pairs]
    return [("-", a, d) for a, d in pairs], skipped


def plan_cancellations(
    pg_stats: list[dict],
    pools: dict[int, dict],
    ec_profiles: dict[str, dict],
    osd: int | None,
    osd_host: dict[int, str],
    crush_rules: dict[int, dict],
    osd_df: dict[int, dict] | None = None,
    backfillfull_pct: float | None = None,
    pin_blockers: bool = False,
    exclude_pgs: frozenset[str] | set[str] = frozenset(),
) -> tuple[list[Cancellation], list[Skipped]]:
    """Return (cancellations, skipped) for the backfills into osd, in PG order.

    With osd None, cancels every backfill (cancel_whole_pg). Otherwise each
    shard arriving on osd is pinned with its companions (close_pins) and,
    with pin_blockers, its blockers (find_blockers). A blocker that cannot be
    pinned is skipped; the requested pin stays. PGs in exclude_pgs are
    ignored.

    Exits with an error if a pool is unknown or its failure domain is not
    host.
    """
    blockers_enabled = (
        pin_blockers and osd_df is not None and backfillfull_pct is not None
    )
    pg_stats = [
        pg
        for pg in pg_stats
        if (osd is None or osd in pg["up"]) and pg["pgid"] not in exclude_pgs
    ]
    pgids = [pg["pgid"] for pg in pg_stats]
    which = "remapped PGs" if osd is None else f"PGs with a backfill into osd.{osd}"
    check_known_pools(pgids, pools, which)
    check_host_failure_domain(pgids, pools, crush_rules, which)

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
        if osd is None:
            moves, unpinnable = cancel_whole_pg(up, acting, is_ec, osd_host)
            skipped.extend(Skipped(pgid, shard, why) for shard, why in unpinnable)
            cancellations.extend(
                Cancellation(pgid, s, from_osd, to_osd, size, pg["state"], progress)
                for s, from_osd, to_osd in moves
            )
            continue
        pins, unpinnable = find_arrivals(up, acting, osd, is_ec)
        skipped.extend(Skipped(pgid, shard, why) for shard, why in unpinnable)
        for shard, acting_osd in pins:
            # Companion shard -> the blocker whose pin pulled it in.
            blocker_of: dict[int, int] = {}
            if is_ec:
                resolved, why = pin_with_companions(up, acting, shard, osd_host)
                if why is None and blockers_enabled:
                    for blocker in find_blockers(
                        up, acting, resolved, osd_df, backfillfull_pct, size
                    ):
                        if blocker in resolved:  # already pulled in as a companion
                            continue
                        trial = {**resolved, blocker: acting[blocker]}
                        closed, blocker_why = close_pins(up, acting, trial, osd_host)
                        if blocker_why is None:
                            _, blocker_why = order_moves(
                                [(s, up[s], a) for s, a in closed.items()]
                            )
                        if blocker_why is None:
                            for s in closed.keys() - resolved.keys() - {blocker}:
                                blocker_of[s] = blocker
                            resolved = closed
                        else:
                            skipped.append(
                                Skipped(pgid, blocker, f"blocker: {blocker_why}")
                            )
                moves, ring = order_moves([(s, up[s], a) for s, a in resolved.items()])
                why = why or ring
            else:
                why = pin_replica(up, osd, acting_osd, osd_host)
                moves = [("-", osd, acting_osd)]
            if why is not None:
                skipped.append(Skipped(pgid, shard, why))
                continue
            for s, from_osd, to_osd in moves:
                projected = None
                if s != shard and blockers_enabled:
                    projected = projected_utilization(osd_df, from_osd, size)
                    if projected is not None and projected < backfillfull_pct:
                        projected = None
                # A companion over the ratio blocks the requested shard itself.
                of_blocker = projected is None and s in blocker_of
                companion_of = (
                    None if s == shard else blocker_of[s] if of_blocker else shard
                )
                cancellations.append(
                    Cancellation(
                        pgid,
                        s,
                        from_osd,
                        to_osd,
                        size,
                        pg["state"],
                        progress,
                        companion_of,
                        projected,
                        of_blocker=of_blocker,
                    )
                )

    # Stable, by PG only: within a PG the order is the one to apply the pairs in.
    return (
        sorted(cancellations, key=lambda c: pgid_sort_key(c.pgid)),
        sorted(skipped, key=skipped_sort_key),
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_summary(
    osd: int | None,
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    cancellations: list[Cancellation],
    skipped: list[Skipped],
) -> None:
    """Summarize the proposal on stderr, and list what cannot be pinned."""
    arriving = [c for c in cancellations if c.companion_of is None]
    others = len(cancellations) - len(arriving)
    blockers = sum(c.blocker_util is not None for c in cancellations)
    known = [c.size_bytes for c in arriving if c.size_bytes is not None]
    total = sum(known)
    unknown = len(arriving) - len(known)
    if osd is None:
        pgs = len({c.pgid for c in cancellations})
        intro = f"{len(arriving)} moving shard(s) in {pgs} PG(s) can be pinned back"
        share = ""
    else:
        node = osd_df[osd]
        capacity = node.get("kb", 0) * KIB
        share = f", {total / capacity * 100:.1f}% of its capacity" if capacity else ""
        intro = (
            f"osd.{osd} ({osd_host.get(osd, '?')}) is at "
            f"{node['utilization']:.1f}%. {len(arriving)} arriving shard(s) "
            "can be pinned back"
        )
    stderr_para(
        f"{intro} (~{format_bytes(total)}"
        + (f" + {unknown} of unknown size" if unknown else "")
        + f"{share}); {len(skipped)} cannot be pinned."
        + (
            f" {others} more shard(s), moving to other OSDs, are pinned too"
            + (f" (blockers: {blockers})" if blockers else "")
            + "."
            if others
            else ""
        )
    )
    stderr_items(f"cannot pin {s.pgid} shard {s.shard}: {s.reason}" for s in skipped)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class StopResult(NamedTuple):
    """What plan() decided, for render() to print."""

    osd: int | None  # None: every backfill in the cluster
    cancellations: list[Cancellation]  # in apply order, see plan_cancellations
    skipped: list[Skipped]
    chained: dict[str, list[Pair]]  # see avoid_chains
    backfillfull_pct: float | None  # None if 'osd dump' has no backfillfull_ratio
    exclude_filter: PgidFilter | None  # None without --exclude-pgs
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> StopResult:
    """Fetch the cluster state and work out which shards to pin back.

    Exits on invalid arguments or a PG it cannot analyze. Notes on the inputs
    (--exclude-pgs matches, a missing backfillfull_ratio) are printed before
    planning, so they show even if it exits.
    """
    osd = args.osd
    exclude_pgs = set(args.exclude_pgs)
    if osd is None and args.pin_blockers:
        sys.exit("ERROR: --pin-blockers requires --osd.")

    osd_df = fetch_osd_df(store)
    osd_host = fetch_osd_hosts(store)
    pg_stats = fetch_remapped_pg_stats(store)
    pools = fetch_pools(store)
    ec_profiles = fetch_ec_profiles(store)
    crush_rules = fetch_crush_rules(store)

    if osd is not None:
        check_osds_exist("--osd", [osd], osd_df)
    backfillfull_pct = fetch_backfillfull_pct(store)
    if backfillfull_pct is None and args.pin_blockers:
        stderr_para(
            "NOTE: 'osd dump' has no backfillfull_ratio (an older capture?); "
            "--pin-blockers is ignored."
        )
    exclude_filter = None
    if exclude_pgs:
        # Matched: remapped and, with --osd, osd in 'up'. Not necessarily a
        # backfill into osd, so the note claims no more.
        exclude_filter = PgidFilter.of(
            exclude_pgs,
            (pg["pgid"] for pg in pg_stats if osd is None or osd in pg["up"]),
        )
        involving = "" if osd is None else f" involving osd.{osd}"
        print_pgid_filter(
            "--exclude-pgs",
            exclude_filter,
            f"matched a remapped PG{involving} and were left alone",
            "not remapped" + ("" if osd is None else f", not involving osd.{osd}"),
        )
    cancellations, skipped = plan_cancellations(
        pg_stats,
        pools,
        ec_profiles,
        osd,
        osd_host,
        crush_rules,
        osd_df,
        backfillfull_pct,
        args.pin_blockers,
        exclude_pgs,
    )
    # Without --osd every pin is wanted for itself, so a chain's last pin
    # stays even when the one before it cannot.
    resolved = avoid_chains(
        cancellations, fetch_upmap_items(store), pg_stats, osd_host, partial=osd is None
    )
    cancellations = resolved.cancellations
    skipped = sorted(skipped + resolved.skipped, key=skipped_sort_key)
    if not args.pgremapper_mappings:  # JSON has no PROGRESS: skip the queries
        cancellations = with_exact_progress(store, cancellations, pg_stats, pools)
    return StopResult(
        osd=osd,
        cancellations=cancellations,
        skipped=skipped,
        chained=resolved.chained,
        backfillfull_pct=backfillfull_pct,
        exclude_filter=exclude_filter,
        osd_df=osd_df,
        osd_host=osd_host,
    )


def render(result: StopResult, args: argparse.Namespace) -> None:
    """Print result: pins on stdout in the format args asks for, notes on stderr."""
    osd, cancellations = result.osd, result.cancellations
    if not cancellations and not result.skipped:
        into = "" if osd is None else f" into osd.{osd}"
        stderr_para(f"No backfills{into}.")
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
    print_summary(osd, result.osd_df, result.osd_host, cancellations, result.skipped)
    if not args.pgremapper_mappings and any(
        c.progress_pct is not None and not c.progress_exact for c in cancellations
    ):
        stderr_para(f"NOTE: {PROGRESS_APPROX_NOTE}")
    if result.chained:
        warn_chains(result.chained)
    if (
        osd is not None
        and result.backfillfull_pct is not None
        and not args.pin_blockers
    ):
        stderr_para(
            "NOTE: --pin-blockers was not given: a backfill you keep can still "
            "be held in backfill_toofull by another shard of its PG."
        )
    stderr_para(
        "NOTE: cancelling a running backfill discards its progress. Consider "
        "'ceph balancer off' while these are pinned."
    )


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
