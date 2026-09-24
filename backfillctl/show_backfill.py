# SPDX-License-Identifier: MIT
"""
Show backfills: the PGs whose 'up' set differs from 'acting', with source
and target OSDs, progress and state.

EC pools get a row per moving shard. Replicated pools get one row per PG,
since replicas are interchangeable; their SHARD is '-'.

FROM_OSD is the OSD losing data. When a missing copy is being rebuilt there
is none, so it shows the PG's primary, marked '*': the primary keeps its copy
but does the work.

TYPE is recovery, backfill, or remapped (not started). PROGRESS is how far
the row's target has got, from its backfill position in 'ceph pg query'
(averaged over a replicated row's targets). '~' marks a fallback on Ceph's
per-PG counters, which can read far too high.

--osds keeps rows involving any of the given OSDs, including a '*' primary;
--pgs keeps rows of the given PGs. Given both, a row must match both.
"""

import argparse
from typing import NamedTuple

from shared import (
    PROGRESS_APPROX_NOTE,
    HelpFormatter,
    PgidFilter,
    SnapshotStore,
    abbreviate_state,
    add_load_state_arg,
    check_osds_exist,
    copy_progress,
    ec_shard_moves,
    fetch_backfill_positions,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    format_progress,
    format_utilization,
    is_erasure,
    is_real_osd,
    parse_osd,
    pg_progress,
    pgid_pool_id,
    pgid_sort_key,
    print_pgid_filter,
    print_table,
    real_osd_set,
    target_peer,
)

# Footnote for the '*' after a FROM_OSD primary (see fmt_from in render).
PRIMARY_NOTE = (
    "* marks the PG's primary where no OSD loses a copy: it keeps its copy, "
    "but drives the recovery."
)

SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "show-backfill",
        help="Show what is moving: source and target OSDs, and progress.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--sort-by",
        choices=["pgid", "from-osd", "to-osd"],
        default="pgid",
        help="Sort rows by this column (default: %(default)s).",
    )
    parser.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        default=[],
        metavar="OSD",
        help="Show only rows involving these OSDs.",
    )
    parser.add_argument(
        "--pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="Show only rows of these PGs.",
    )
    add_load_state_arg(parser, after_command=True)
    return parser


# ---------------------------------------------------------------------------
# Movement classification
# ---------------------------------------------------------------------------


# State flags of active or pending data movement.
_RECOVERY_FLAGS = {"recovering", "recovery_wait", "recovery_toofull"}
_BACKFILL_FLAGS = {"backfilling", "backfill_wait", "backfill_toofull"}


def movement_type(state: str) -> str:
    """Return 'recovery', 'backfill', both joined by '+', or 'remapped' (not started)."""
    flags = set(state.split("+"))
    labels = []
    if flags & _RECOVERY_FLAGS:
        labels.append("recovery")
    if flags & _BACKFILL_FLAGS:
        labels.append("backfill")
    if not labels:
        labels.append("remapped")
    return "+".join(labels)


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------


class MovementRow(NamedTuple):
    """One row: an EC shard's move, or a replicated PG's."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    sources: frozenset  # OSDs losing data; may be empty
    destinations: frozenset  # OSDs gaining data
    move_type: str
    state: str
    primary: "int | None"  # acting primary, shown with '*' (see from_osds)
    needs_primary_marker: bool  # a destination has no matching source
    progress_pct: "float | None" = None  # set by with_progress
    progress_exact: bool = False  # from backfill positions, not counters


def _row_pgid_key(row: MovementRow) -> tuple:
    shard_key = row.shard if isinstance(row.shard, int) else -1
    return (*pgid_sort_key(row.pgid), shard_key)


def from_osds(row: MovementRow) -> set[int]:
    """Return the OSDs the row's FROM_OSD cell shows: sources plus any '*' primary."""
    ids = set(row.sources)
    if (not ids or row.needs_primary_marker) and row.primary is not None:
        ids.add(row.primary)
    return ids


def _row_from_osd_key(row: MovementRow) -> tuple:
    return (tuple(sorted(from_osds(row))), pgid_sort_key(row.pgid))


def _row_to_osd_key(row: MovementRow) -> tuple:
    return (tuple(sorted(row.destinations)), pgid_sort_key(row.pgid))


_SORT_KEYS = {
    "pgid": _row_pgid_key,
    "from-osd": _row_from_osd_key,
    "to-osd": _row_to_osd_key,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class MovementsResult(NamedTuple):
    """What plan() found, for render() to print."""

    rows: list[MovementRow]  # sorted by --sort-by; --osds/--pgs applied
    pgs_filter: PgidFilter | None  # None without --pgs
    filtered: bool  # --osds or --pgs given
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> MovementsResult:
    """Fetch the cluster state and find the PG movements, filtered by --osds/--pgs."""
    pg_stats = fetch_pg_stats(store, "pg_dump_pgs")
    osd_df = fetch_osd_df(store)
    check_osds_exist("--osds", args.osds, osd_df)
    osd_host = fetch_osd_hosts(store)
    pools = fetch_pools(store)

    rows: list[MovementRow] = []

    for pg in pg_stats:
        pgid = pg["pgid"]
        state = pg["state"]
        up = pg["up"]
        acting = pg["acting"]
        pool_id = pgid_pool_id(pgid)

        primary = pg.get("acting_primary")
        if not is_real_osd(primary):
            primary = next(iter(sorted(real_osd_set(acting))), None)

        mtype = movement_type(state)
        pool = pools.get(pool_id)

        if is_erasure(pool):
            # EC: shards are positional, so each moving shard gets its own row.
            moves = ec_shard_moves(up, acting)
            for i, source, destination in moves:
                sources = frozenset() if source is None else frozenset({source})
                rows.append(
                    MovementRow(
                        pgid,
                        i,
                        sources,
                        frozenset({destination}),
                        mtype,
                        state,
                        primary,
                        False,
                    )
                )
        else:
            # Replicated: diff as sets (a reorder is not movement), one row per PG.
            up_set = real_osd_set(up)
            acting_set = real_osd_set(acting)

            if up_set == acting_set:
                continue

            destinations = up_set - acting_set
            sources = acting_set - up_set

            if not destinations:
                continue

            # More destinations than sources: the primary is rebuilding a
            # missing replica.
            needs_primary_marker = len(destinations) > len(sources)

            rows.append(
                MovementRow(
                    pgid,
                    "-",
                    frozenset(sources),
                    frozenset(destinations),
                    mtype,
                    state,
                    primary,
                    needs_primary_marker,
                )
            )

    rows, pgs_filter = filter_rows(rows, set(args.osds), set(args.pgs))
    rows = with_progress(store, rows, pg_stats, pools)
    rows.sort(key=_SORT_KEYS[args.sort_by])
    return MovementsResult(
        rows, pgs_filter, bool(args.osds or args.pgs), osd_df, osd_host
    )


def with_progress(
    store: SnapshotStore,
    rows: list[MovementRow],
    pg_stats: list[dict],
    pools: dict[int, dict],
) -> list[MovementRow]:
    """Return rows with PROGRESS filled in: an EC shard's own, a replicated PG's overall."""
    shown = {r.pgid for r in rows}
    pgs = {pg["pgid"]: pg for pg in pg_stats if pg["pgid"] in shown}
    positions = fetch_backfill_positions(store, pgs)
    result = []
    for r in rows:
        pg, pool = pgs[r.pgid], pools.get(pgid_pool_id(r.pgid))
        pg_positions = positions.get(r.pgid, {})
        if isinstance(r.shard, int):
            (destination,) = r.destinations
            progress = copy_progress(
                pg, pool, pg_positions, target_peer(destination, r.shard)
            )
        else:
            progress = pg_progress(pg, pool, pg_positions)
        result.append(
            r._replace(progress_pct=progress.pct, progress_exact=progress.exact)
        )
    return result


def filter_rows(
    rows: list[MovementRow], osds: set[int], pgids: set[str]
) -> tuple[list[MovementRow], PgidFilter | None]:
    """Return (the rows passing both filters, what pgids matched).

    An empty filter passes everything. pgids matches are counted before the
    osds filter, so only real typos are flagged.
    """
    pgs_filter = None
    if pgids:
        pgs_filter = PgidFilter.of(pgids, (r.pgid for r in rows))
        rows = [r for r in rows if r.pgid in pgids]
    if osds:
        rows = [r for r in rows if osds & (from_osds(r) | r.destinations)]
    return rows, pgs_filter


# (group, label). The unlabeled column holds the '->' from FROM to TO.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("", "FROM_OSD"),
    ("", ""),
    ("", "TO_OSD"),
    ("", "TYPE"),
    ("", "PROGRESS"),
    ("", "STATE"),
]


def osd_label(
    osd_id: int, osd_df: dict[int, dict], osd_host: dict[int, str], util: bool = True
) -> str:
    """Format an OSD as 'ID(host,NN.N%)', or 'ID(host)' without util."""
    host = osd_host.get(osd_id, "?")
    if not util:
        return f"{osd_id}({host})"
    return f"{osd_id}({host},{format_utilization(osd_df, osd_id)})"


def shows_primary(row: MovementRow) -> bool:
    """True if the row's FROM_OSD cell shows the primary, marked '*'.

    It does when the primary rebuilds a missing copy (no source, or
    needs_primary_marker) and is not a source itself.
    """
    return (
        (not row.sources or row.needs_primary_marker)
        and row.primary is not None
        and row.primary not in row.sources
    )


def format_row(
    row: MovementRow, osd_df: dict[int, dict], osd_host: dict[int, str]
) -> list[str]:
    """Return one table row's cells.

    FROM_OSD lists the sources, plus the primary marked '*' (shows_primary),
    without utilization: it loses no data.
    """
    sources = [osd_label(o, osd_df, osd_host) for o in sorted(row.sources)]
    if shows_primary(row):
        sources.append(osd_label(row.primary, osd_df, osd_host, util=False) + "*")
    elif (not row.sources or row.needs_primary_marker) and row.primary is None:
        sources.append("unknown")  # a copy is rebuilt, but by no known primary
    return [
        row.pgid,
        str(row.shard),
        ",".join(sources),
        "->",
        ",".join(osd_label(o, osd_df, osd_host) for o in sorted(row.destinations)),
        row.move_type,
        format_progress(row.progress_pct, row.progress_exact),
        abbreviate_state(row.state),
    ]


def render(result: MovementsResult) -> None:
    """Print the rows as a table, then the footnotes that apply."""
    rows, pgs_filter, filtered, osd_df, osd_host = result
    if pgs_filter is not None:
        print_pgid_filter("--pgs", pgs_filter, "have movement", "not moving")
    if not rows:
        print(
            "No PG movements match --osds/--pgs."
            if filtered
            else "No PG movements detected."
        )
        return

    print_table(COLUMNS, [format_row(r, osd_df, osd_host) for r in rows])

    if any(shows_primary(r) for r in rows):
        print(f"\n{PRIMARY_NOTE}")

    if any(r.progress_pct is not None and not r.progress_exact for r in rows):
        print(f"\n{PROGRESS_APPROX_NOTE}")

    num_pgs = len({r.pgid for r in rows})
    print(f"\n{len(rows)} shard movement(s) across {num_pgs} PG(s).")


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)))
