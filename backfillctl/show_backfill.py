# SPDX-License-Identifier: MIT
"""
Show backfills: the PGs whose 'up' set differs from 'acting', one row per
moving copy, with its acting (source) and up (target) OSD, progress and state.

EC pools get a row per moving shard. Replicated pools diff up and acting as
sets, pairing sources with targets in OSD order (replicas are
interchangeable); their SHARD is '-'.

When a missing copy is being rebuilt, no OSD loses data, so ACTING shows the
PG's primary, marked '*': the primary keeps its copy but does the work. UP
reads 'none' for a replica dropped with nowhere to go.

TYPE is recovery, backfill, or remapped (not started). PROGRESS is how far
the row's target has got, from its backfill position in 'ceph pg query'.
'~' marks a fallback on Ceph's per-PG counters, which can read far too high.

--osds keeps rows involving any of the given OSDs, including a '*' primary;
--pgs keeps rows of the given PGs. Given both, a row must match both.
"""

import argparse
from itertools import zip_longest
from typing import NamedTuple

from messages import (
    print_pgid_filter,
    print_progress_note,
    stderr_para,
)
from shared import (
    NOT_APPLICABLE,
    HelpFormatter,
    PgidFilter,
    Progress,
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
    is_erasure,
    is_real_osd,
    osd_cells,
    osd_columns,
    parse_osd,
    pgid_pool_id,
    pgid_sort_key,
    print_table,
    real_osd_set,
    target_peer,
)

# Footnote for the '*' after an ACTING primary (see MovementRow.primary_marked).
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
        choices=["pgid", "acting-osd", "up-osd"],
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


class MovementRow(NamedTuple):
    """One row: a moving EC shard, or one replica of a replicated PG."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    acting_osd: int | None  # losing the copy, or the primary (primary_marked)
    up_osd: int | None  # gaining it; None for a replica dropped outright
    move_type: str
    state: str
    primary_marked: bool = False  # acting_osd is the primary rebuilding a
    # missing copy, shown with '*'; it loses nothing
    progress_pct: float | None = None  # set by with_progress
    progress_exact: bool = False  # from backfill positions, not counters


def replica_pairs(
    up: list, acting: list, primary: int | None
) -> list[tuple[int | None, int | None, bool]]:
    """Return (acting_osd, up_osd, primary_marked) for a replicated PG's moving copies.

    Sources (acting only) pair with targets (up only) in OSD order. A target
    left over is a missing copy, paired with the primary, marked; a source
    left over is dropped, paired with None. A reorder yields nothing, and so
    does a PG only dropping copies: nothing moves.
    """
    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    sources, targets = sorted(acting_set - up_set), sorted(up_set - acting_set)
    if not targets:
        return []
    return [
        (primary, target, primary is not None)
        if source is None
        else (source, target, False)
        for source, target in zip_longest(sources, targets)
    ]


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------


def _osd_key(osd: int | None) -> int:
    return -1 if osd is None else osd


def _row_pgid_key(row: MovementRow) -> tuple:
    shard_key = row.shard if isinstance(row.shard, int) else -1
    return (*pgid_sort_key(row.pgid), shard_key)


_SORT_KEYS = {
    "pgid": _row_pgid_key,
    "acting-osd": lambda r: (_osd_key(r.acting_osd), _row_pgid_key(r)),
    "up-osd": lambda r: (_osd_key(r.up_osd), _row_pgid_key(r)),
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
        pgid, state, up, acting = pg["pgid"], pg["state"], pg["up"], pg["acting"]
        primary = pg.get("acting_primary")
        if not is_real_osd(primary):
            primary = next(iter(sorted(real_osd_set(acting))), None)
        mtype = movement_type(state)

        if is_erasure(pools.get(pgid_pool_id(pgid))):
            # Shards are positional: an empty acting slot is a missing copy.
            for i, source, target in ec_shard_moves(up, acting):
                acting_osd = primary if source is None else source
                marked = source is None and primary is not None
                rows.append(
                    MovementRow(pgid, i, acting_osd, target, mtype, state, marked)
                )
        else:
            for acting_osd, target, marked in replica_pairs(up, acting, primary):
                rows.append(
                    MovementRow(pgid, "-", acting_osd, target, mtype, state, marked)
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
    """Return rows with PROGRESS filled in: each row's target's own.

    A dropped replica (no target) has none.
    """
    shown = {r.pgid for r in rows}
    pgs = {pg["pgid"]: pg for pg in pg_stats if pg["pgid"] in shown}
    positions = fetch_backfill_positions(store, pgs)
    result = []
    for r in rows:
        if r.up_osd is None:
            progress = Progress(None, True)
        else:
            progress = copy_progress(
                pgs[r.pgid],
                pools.get(pgid_pool_id(r.pgid)),
                positions.get(r.pgid, {}),
                target_peer(r.up_osd, r.shard),
            )
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
        rows = [r for r in rows if {r.acting_osd, r.up_osd} & osds]
    return rows, pgs_filter


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# (group, label). ACTING is where the copy is (or its '*' primary), UP where
# CRUSH wants it.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    *osd_columns("ACTING"),
    *osd_columns("UP"),
    ("", "TYPE"),
    ("", "PROGRESS"),
    ("", "STATE"),
]


def format_row(
    row: MovementRow, osd_df: dict[int, dict], osd_host: dict[int, str]
) -> list[str]:
    """Return one table row's cells.

    A '*' primary's UTIL is '-': it loses no data.
    """
    acting = osd_cells(
        osd_df, osd_host, row.acting_osd, row.acting_osd if row.primary_marked else None
    )
    if row.primary_marked:
        acting[1] = NOT_APPLICABLE
    return [
        row.pgid,
        str(row.shard),
        *acting,
        *osd_cells(osd_df, osd_host, row.up_osd),
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
        stderr_para(
            "No PG movements match --osds/--pgs."
            if filtered
            else "No PG movements detected."
        )
        return

    print_table(COLUMNS, [format_row(r, osd_df, osd_host) for r in rows])
    num_pgs = len({r.pgid for r in rows})
    stderr_para(f"{len(rows)} copy movement(s) across {num_pgs} PG(s).")
    if any(r.primary_marked for r in rows):
        stderr_para(f"NOTE: {PRIMARY_NOTE}")
    print_progress_note((r.progress_pct, r.progress_exact) for r in rows)


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)))
