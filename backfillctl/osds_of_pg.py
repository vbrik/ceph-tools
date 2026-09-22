# SPDX-License-Identifier: MIT
"""
Show the 'acting' and 'up' OSDs of a given Ceph PG, one row per shard, with
each OSD's utilization and host (CRUSH bucket of type 'host'), the progress of
shards that are being remapped, and the PG's pg_upmap_items pairs that touch
the row.

Usage: backfillctl osds-of-pg <pgid>
  e.g. backfillctl osds-of-pg 3.1a2

Columns (same two-line grouped header as the divert-toofull-backfills subcommand):

  SHARD      EC shard index, '-' for replicated pools (see below)
  ACTING     OSD holding the shard's data now, with its UTIL and HOST
  UP         OSD CRUSH (plus upmaps) wants it on, with its UTIL and HOST
  PROGRESS   for a remapped shard (UP OSD != ACTING OSD), % of the PG's data
             already in its target location, else '-'
  UPMAPS     pg_upmap_items pairs 'from->to' whose from or to is this row's
             ACTING or UP OSD ('from' is what CRUSH chose, 'to' what is used
             instead), else '-'

An OSD that is the PG's primary in that set is marked with '*'. An empty slot
is shown as 'none'.

Rows are built as in the pg-movements subcommand:

  - EC pools: index i is shard i, a fixed identity, so acting[i] is paired
    with up[i].
  - Replicated pools: replicas are interchangeable, so position carries no
    identity. OSDs in both sets share a row; OSDs only in acting are paired
    (in OSD id order) with OSDs only in up.

PROGRESS is estimated from the PG's object counters exactly as in the
pg-movements subcommand (both use shared.pg_progress_pct). It is a per-PG figure, so
every remapped row shows the same value.

'backfillctl save-state DIR' captures a cluster's state (anonymized, and
covering every subcommand, not just this one) into DIR; --load-state DIR
then replays it here instead of calling 'ceph', reading the given PG's row
out of the capture's pg_dump_pgs.json.
"""

import argparse
import sys
from itertools import zip_longest
from typing import NamedTuple

from shared import (
    NOT_APPLICABLE,
    PROGRESS_100_NOTE,
    SnapshotStore,
    add_load_state_arg,
    copies_moving,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    format_progress,
    is_erasure,
    osd_cells,
    pg_progress_pct,
    pgid_pool_id,
    print_table,
    progress_reads_100,
    real_osd_set,
    slot,
)

# Maps each snapshot to the 'ceph ... --format json' command that produces
# it. pg_query is filled in by run() with this invocation's pgid: live, it is
# the one command actually issued (one PG, not the whole cluster); --load-state
# never looks it up, reading pg_dump_pgs.json instead (what 'backfillctl
# save-state' captures, covering every PG -- see fetch_pg_info).
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


def fetch_pg_info(store: SnapshotStore, pgid: str) -> dict:
    """Return the PG's up/acting sets, primaries, state and counters.

    Live, this reads 'pg_query' (see SNAPSHOT_COMMANDS: 'ceph pg <pgid>
    query', filled in by run() -- one PG, not the whole cluster). From a
    --load-state snapshot (pg_dump_pgs.json, covering every PG -- see the
    save-state subcommand), the same values are read off that PG's own
    pg_stat entry instead.
    """
    if store.load_dir is None:
        data = store.json("pg_query")
        try:
            stats = data.get("info", {}).get("stats", {})
            return {
                "up": data["up"],
                "up_primary": stats["up_primary"],
                "acting": data["acting"],
                "acting_primary": stats["acting_primary"],
                "state": data["state"],
                "stat_sum": stats.get("stat_sum", {}),
            }
        except KeyError as exc:
            sys.exit(
                f"ERROR: unexpected JSON shape from 'ceph pg query': missing key {exc}"
            )
    for pg in fetch_pg_stats(store, "pg_dump_pgs"):
        if pg["pgid"] == pgid:
            return {
                "up": pg["up"],
                "up_primary": pg["up_primary"],
                "acting": pg["acting"],
                "acting_primary": pg["acting_primary"],
                "state": pg["state"],
                "stat_sum": pg.get("stat_sum", {}),
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
    pct: float | None,
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    pairs: list[dict],
) -> list[str]:
    return [
        str(row.shard),
        *osd_cells(osd_df, osd_host, row.acting, pg["acting_primary"]),
        *osd_cells(osd_df, osd_host, row.up, pg["up_primary"]),
        # Only a remapped shard has progress of its own to show.
        format_progress(pct if row.remapped else None),
        format_upmaps(pairs, row),
    ]


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "osds-of-pg",
        description="Show acting/up OSDs of a Ceph PG per shard, with "
        "utilization, host, remap progress and upmaps.",
    )
    parser.add_argument("pgid", help="PG id, e.g. 3.1a2")
    add_load_state_arg(parser)
    return parser


def run(args: argparse.Namespace) -> None:
    commands = {
        **SNAPSHOT_COMMANDS,
        "pg_query": ["ceph", "pg", args.pgid, "query", "--format", "json"],
    }
    store = SnapshotStore.from_args(args, commands)
    pg = fetch_pg_info(store, args.pgid)
    pool = fetch_pools(store).get(pgid_pool_id(args.pgid))
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    pairs = fetch_upmap_items(store).get(args.pgid, [])

    erasure = is_erasure(pool)
    rows = build_rows(pg["up"], pg["acting"], erasure)
    pct = pg_progress_pct(
        pg,
        copies_moving(
            pg["up"], pg["acting"], erasure, pool.get("size", 0) if pool else 0
        ),
    )

    print(f"PG {args.pgid}  state: {pg['state']}\n")
    print_table(
        COLUMNS, [format_row(r, pg, pct, osd_df, osd_host, pairs) for r in rows]
    )

    if any(r.remapped for r in rows):
        print(
            "\nPROGRESS is per PG (from its object counters), not per shard: "
            "every remapped row shows the same %."
        )
    if any(r.remapped for r in rows) and progress_reads_100(pct):
        print(f"\n{PROGRESS_100_NOTE}")
    print("\n* primary")
