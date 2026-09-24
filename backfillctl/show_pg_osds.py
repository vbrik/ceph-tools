# SPDX-License-Identifier: MIT
"""
Show the acting and up OSDs of the given PGs, one row per shard, with each
OSD's utilization and host.

EC shards are paired by position. Replicated OSDs in both sets share a row;
the rest are paired in id order. '*' marks each set's primary; 'none' an
empty slot.

PROGRESS is a remapped shard's backfill progress, as in show-backfill.
UPMAPS lists the PG's pg_upmap_items pairs that involve the row's OSDs.
"""

import argparse
import sys
from itertools import zip_longest
from typing import NamedTuple

from shared import (
    NOT_APPLICABLE,
    PROGRESS_APPROX_NOTE,
    HelpFormatter,
    Progress,
    SnapshotStore,
    add_load_state_arg,
    copy_progress,
    extract_backfill_positions,
    fetch_backfill_positions,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    format_progress,
    is_erasure,
    osd_cells,
    pgid_pool_id,
    print_table,
    real_osd_set,
    slot,
    target_peer,
)

# run() adds a 'pg query' per PG; --load-state reads pg_dump_pgs instead.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


# (group, label)
COLUMNS = [
    ("", "SHARD"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "HOST"),
    ("", "PROGRESS"),
    ("", "UPMAPS"),
]


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def pg_query_key(pgid: str) -> str:
    """Return the snapshot key of 'ceph pg <pgid> query'."""
    return f"pg_query_{pgid}"


def fetch_pg_info(store: SnapshotStore, pgid: str) -> dict:
    """Return the PG's up/acting sets, primaries, state, counters and backfill positions.

    Live, from 'ceph pg <pgid> query'; from a capture, from pg_dump_pgs.
    """
    if store.load_dir is None:
        data = store.json(pg_query_key(pgid))
        try:
            stats = data.get("info", {}).get("stats", {})
            return {
                "pgid": pgid,
                "up": data["up"],
                "up_primary": stats["up_primary"],
                "acting": data["acting"],
                "acting_primary": stats["acting_primary"],
                "state": data["state"],
                "stat_sum": stats.get("stat_sum", {}),
                "backfill_positions": extract_backfill_positions(data),
            }
        except KeyError as exc:
            sys.exit(
                f"ERROR: unexpected JSON shape from 'ceph pg query': missing key {exc}"
            )
    for pg in fetch_pg_stats(store, "pg_dump_pgs"):
        if pg["pgid"] == pgid:
            return {
                "pgid": pgid,
                "up": pg["up"],
                "up_primary": pg["up_primary"],
                "acting": pg["acting"],
                "acting_primary": pg["acting_primary"],
                "state": pg["state"],
                "stat_sum": pg.get("stat_sum", {}),
                "backfill_positions": fetch_backfill_positions(store, [pgid]).get(
                    pgid, {}
                ),
            }
    sys.exit(f"ERROR: PG {pgid} not found in --load-state snapshot's pg_dump_pgs.json.")


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------


class ShardRow(NamedTuple):
    """One table row: a shard's acting and up OSD."""

    shard: int | str  # EC shard index, or '-' for replicated pools
    acting: int | None
    up: int | None

    @property
    def remapped(self) -> bool:
        """True if the shard is headed for another OSD."""
        return self.up is not None and self.up != self.acting


def build_rows(up: list[int], acting: list[int], erasure: bool) -> list[ShardRow]:
    """Pair the PG's acting and up OSDs into rows (see module docstring)."""
    if erasure:
        return [
            ShardRow(i, slot(acting, i), slot(up, i))
            for i in range(max(len(up), len(acting)))
        ]
    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    rows = [ShardRow("-", o, o) for o in sorted(up_set & acting_set)]
    rows += [
        ShardRow("-", src, dst)
        for src, dst in zip_longest(
            sorted(acting_set - up_set), sorted(up_set - acting_set)
        )
    ]
    return rows


class PgView(NamedTuple):
    """One PG's rows, and what else its table shows."""

    pgid: str
    pg: dict  # see fetch_pg_info
    rows: list[ShardRow]
    progress: list[Progress | None]  # per row; None if not remapped
    upmap_pairs: list[dict]


class ShowResult(NamedTuple):
    """What plan() looked up, for render() to print."""

    pgs: list[PgView]  # in the order given, deduplicated
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> ShowResult:
    """Look up every PG in args.pgids and pair its OSDs into rows.

    An unknown PG exits before anything is printed.
    """
    pgids = list(dict.fromkeys(args.pgids))  # drop duplicates, keep order
    pgs = {pgid: fetch_pg_info(store, pgid) for pgid in pgids}
    pools = fetch_pools(store)
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    upmap_items = fetch_upmap_items(store)

    views = []
    for pgid, pg in pgs.items():
        pool = pools.get(pgid_pool_id(pgid))
        rows = build_rows(pg["up"], pg["acting"], is_erasure(pool))
        progress = [
            copy_progress(
                pg, pool, pg["backfill_positions"], target_peer(row.up, row.shard)
            )
            if row.remapped
            else None
            for row in rows
        ]
        views.append(PgView(pgid, pg, rows, progress, upmap_items.get(pgid, [])))
    return ShowResult(views, osd_df, osd_host)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def format_upmaps(pairs: list[dict], row: ShardRow) -> str:
    """Return the 'from->to' pairs involving the row's acting or up OSD."""
    osds = {row.acting, row.up} - {None}
    touching = [f"{p['from']}->{p['to']}" for p in pairs if {p["from"], p["to"]} & osds]
    return ",".join(touching) or NOT_APPLICABLE


def format_row(
    row: ShardRow,
    pg: dict,
    progress: Progress | None,
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    pairs: list[dict],
) -> list[str]:
    """Return one table row's cells; progress is None for a row not remapped."""
    return [
        str(row.shard),
        *osd_cells(osd_df, osd_host, row.acting, pg["acting_primary"]),
        *osd_cells(osd_df, osd_host, row.up, pg["up_primary"]),
        format_progress(*progress) if progress else format_progress(None),
        format_upmaps(pairs, row),
    ]


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "show-pg-osds",
        help="Show the acting and up OSDs of PGs, per shard.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument("pgids", nargs="+", metavar="PGID")
    add_load_state_arg(parser, after_command=True)
    return parser


# Footnote for the '*' after an OSD id.
PRIMARY_NOTE = (
    "* marks the primary of each set: acting's now, up's once the PG is clean."
)


def render(result: ShowResult) -> None:
    """Print a table per PG, then the footnotes that apply."""
    any_approx = any_primary = False
    for i, view in enumerate(result.pgs):
        if i:
            print()
        print(f"PG {view.pgid}  state: {view.pg['state']}\n")
        print_table(
            COLUMNS,
            [
                format_row(
                    r, view.pg, p, result.osd_df, result.osd_host, view.upmap_pairs
                )
                for r, p in zip(view.rows, view.progress, strict=True)
            ],
        )
        any_approx |= any(
            p is not None and p.pct is not None and not p.exact for p in view.progress
        )
        any_primary |= any(
            r.acting == view.pg["acting_primary"] or r.up == view.pg["up_primary"]
            for r in view.rows
            if r.acting is not None or r.up is not None
        )

    if any_primary:
        print(f"\n{PRIMARY_NOTE}")
    if any_approx:
        print(f"\n{PROGRESS_APPROX_NOTE}")


def run(args: argparse.Namespace) -> None:
    commands = SNAPSHOT_COMMANDS | {
        pg_query_key(pgid): ["ceph", "pg", pgid, "query", "--format", "json"]
        for pgid in args.pgids
    }
    render(plan(args, SnapshotStore.from_args(args, commands)))
