# SPDX-License-Identifier: MIT
"""
Show the 'acting' and 'up' OSDs of the given Ceph PGs, one row per shard,
with each OSD's utilization and host (CRUSH bucket of type 'host'), the
progress of shards that are being remapped, and the PG's pg_upmap_items pairs
that touch the row.

Usage: backfillctl show-pg-osds <pgid> [<pgid> ...]
  e.g. backfillctl show-pg-osds 3.1a2 3.1a3

Each PG gets its own block (a 'PG <pgid>  state: ...' line and a table), in
the order given, with duplicates dropped; the footnotes follow once, after the
last block. Every PG is looked up before anything is printed, so an unknown
PGID fails the run without partial output.

Columns (same two-line grouped header as the divert-toofull subcommand):

  SHARD      EC shard index, '-' for replicated pools (see below)
  ACTING     OSD holding the shard's data now, with its UTIL and HOST
  UP         OSD CRUSH (plus upmaps) wants it on, with its UTIL and HOST
  PROGRESS   for a remapped shard (UP OSD != ACTING OSD), % of the PG's data
             its UP OSD has already been sent, else '-'
  UPMAPS     pg_upmap_items pairs 'from->to' whose from or to is this row's
             ACTING or UP OSD ('from' is what CRUSH chose, 'to' what is used
             instead), else '-'

An OSD that is the PG's primary in that set is marked with '*'. An empty slot
is shown as 'none'.

Rows are built as in the show-backfill subcommand:

  - EC pools: index i is shard i, a fixed identity, so acting[i] is paired
    with up[i].
  - Replicated pools: replicas are interchangeable, so position carries no
    identity. OSDs in both sets share a row; OSDs only in acting are paired
    (in OSD id order) with OSDs only in up.

PROGRESS is each remapped row's own, as in the show-backfill subcommand
(shared.copy_progress): from its UP OSD's backfill position in 'ceph pg
query', falling back on Ceph's misplaced/degraded counters (marked '~'),
which are per PG, so every such row of the PG shows the same figure.

'backfillctl save-state DIR' captures a cluster's state (anonymized, and
covering every subcommand, not just this one) into DIR; 'backfillctl
--load-state DIR show-pg-osds PGID...' then replays it here instead of
calling 'ceph', reading the given PGs' rows out of the capture's
pg_dump_pgs.json and backfill_positions.json.
"""

import argparse
import sys
from itertools import zip_longest
from typing import NamedTuple

from shared import (
    NOT_APPLICABLE,
    PROGRESS_APPROX_NOTE,
    Progress,
    SnapshotStore,
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

# Maps each snapshot to the 'ceph ... --format json' command that produces
# it. run() adds one pg_query_key(pgid) entry per PGID given: live, those are
# the per-PG commands actually issued (one PG each, not the whole cluster);
# --load-state never looks them up, reading pg_dump_pgs.json instead (what
# 'backfillctl save-state' captures, covering every PG -- see fetch_pg_info).
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


# Two-line header: (group, label). An empty group has no group line.
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
    """Return the SnapshotStore key of 'ceph pg <pgid> query' (see run())."""
    return f"pg_query_{pgid}"


def fetch_pg_info(store: SnapshotStore, pgid: str) -> dict:
    """Return the PG's up/acting sets, primaries, state, counters and
    backfill positions ('backfill_positions', see
    shared.extract_backfill_positions).

    Live, this reads pg_query_key(pgid) (see SNAPSHOT_COMMANDS: 'ceph pg
    <pgid> query', added by run() -- one PG, not the whole cluster). From a
    --load-state snapshot (pg_dump_pgs.json, covering every PG -- see the
    save-state subcommand), the same values are read off that PG's own
    pg_stat entry instead.
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
    shard: int | str  # EC shard index, or '-' for replicated pools
    acting: int | None
    up: int | None

    @property
    def remapped(self) -> bool:
        """True when the shard is headed for an OSD other than its current one."""
        return self.up is not None and self.up != self.acting


def build_rows(up: list[int], acting: list[int], erasure: bool) -> list[ShardRow]:
    """Pair up the PG's acting and up OSDs into rows (see module docstring)."""
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
    progress: list[Progress | None]  # per row: None where not remapped
    upmap_pairs: list[dict]  # the PG's pg_upmap_items pairs


class ShowResult(NamedTuple):
    """Everything a run looked up, independent of how it is printed.

    plan() computes it and render() prints it. osd_df and osd_host are
    carried along only because the tables show them.
    """

    pgs: list[PgView]  # in the order given, without duplicates
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> ShowResult:
    """Look up every PG in args.pgids and pair up its OSDs into rows.

    Every PG is looked up before anything else, so an unknown one exits (see
    fetch_pg_info) before render() prints any output.
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
        # Only a remapped shard has progress of its own to show.
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
    """List 'from->to' of the pairs touching the row's acting or up OSD."""
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
        description="Show acting/up OSDs of Ceph PGs per shard, with "
        "utilization, host, remap progress and upmaps.",
    )
    parser.add_argument("pgids", nargs="+", metavar="pgid", help="PG id, e.g. 3.1a2")
    return parser


def render(result: ShowResult) -> None:
    """Print a table per PG, then the footnotes that apply to any of them."""
    any_approx = False
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

    if any_approx:
        print(f"\n{PROGRESS_APPROX_NOTE}")
    print("\n* primary")


def run(args: argparse.Namespace) -> None:
    commands = SNAPSHOT_COMMANDS | {
        pg_query_key(pgid): ["ceph", "pg", pgid, "query", "--format", "json"]
        for pgid in args.pgids
    }
    render(plan(args, SnapshotStore.from_args(args, commands)))
