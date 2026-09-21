#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Show the 'acting' and 'up' OSDs of a given Ceph PG, one row per shard, with
each OSD's utilization and host (CRUSH bucket of type 'host'), the progress of
shards that are being remapped, and the PG's pg_upmap_items pairs that touch
the row.

Usage: osds-of-pg.py <pgid>
  e.g. osds-of-pg.py 3.1a2

Columns (same two-line grouped header as backfill-toofull-unwedge-upmaps.py):

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

Rows are built as in pg-movements.py:

  - EC pools: index i is shard i, a fixed identity, so acting[i] is paired
    with up[i].
  - Replicated pools: replicas are interchangeable, so position carries no
    identity. OSDs in both sets share a row; OSDs only in acting are paired
    (in OSD id order) with OSDs only in up.

PROGRESS is estimated from the PG's object counters exactly as in
pg-movements.py (see pg_progress_pct). It is a per-PG figure, so every
remapped row shows the same value.
"""

import argparse
import json
import math
import subprocess
import sys
from itertools import groupby, zip_longest
from typing import NamedTuple

# Sentinel for "no OSD in this slot" (crush/crush.h), as in pg-movements.py.
CRUSH_ITEM_NONE = 0x7FFFFFFF

POOL_TYPE_ERASURE = 3

# Printed in the UTIL/HOST columns of an empty slot and for absent values.
# Distinct from '?', which means the OSD exists but its data is unavailable.
NOT_APPLICABLE = "-"

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

# Between columns of one group, and between columns of different groups.
COLUMN_SEP = "  "
GROUP_SEP = "    "


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def run_json(cmd: list[str]) -> object:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        sys.exit(f"ERROR: {' '.join(cmd)}\n{exc.stderr.strip()}")
    except FileNotFoundError:
        sys.exit("ERROR: 'ceph' binary not found in PATH.")
    return json.loads(proc.stdout)


def fetch_pg_info(pgid: str) -> dict:
    data = run_json(["ceph", "pg", pgid, "query", "--format", "json"])
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


def fetch_osd_hosts() -> dict[int, str]:
    """Return {osd_id: short_hostname} from 'ceph osd tree'."""
    data = run_json(["ceph", "osd", "tree", "--format", "json"])
    nodes = data.get("nodes", []) + data.get("stray", [])
    by_id = {n["id"]: n for n in nodes}
    result = {}
    for n in nodes:
        if n.get("type") == "host":
            short = n["name"].split(".")[0]
            for child_id in n.get("children", []):
                if by_id.get(child_id, {}).get("type") == "osd":
                    result[child_id] = short
    return result


def fetch_osd_utilization() -> dict[int, float]:
    """Return {osd_id: utilization_pct} from 'ceph osd df'. Down OSDs may be absent."""
    data = run_json(["ceph", "osd", "df", "--format", "json"])
    nodes = data.get("nodes", []) + data.get("stray", [])
    return {n["id"]: n["utilization"] for n in nodes if "utilization" in n}


class Pool(NamedTuple):
    erasure: bool
    size: int  # replica count, or k+m for EC


def fetch_pool(pool_id: int) -> Pool | None:
    """Return the pool with this id from 'ceph osd pool ls detail', if any."""
    data = run_json(["ceph", "osd", "pool", "ls", "detail", "--format", "json"])
    for p in data:
        if p["pool_id"] == pool_id:
            return Pool(p.get("type") == POOL_TYPE_ERASURE, p.get("size", 0))
    return None


def fetch_upmap_pairs(pgid: str) -> list[dict]:
    """Return the PG's pg_upmap_items pairs [{'from': osd, 'to': osd}, ...]."""
    data = run_json(["ceph", "osd", "dump", "--format", "json"])
    for entry in data.get("pg_upmap_items", []):
        if entry["pgid"] == pgid:
            return entry["mappings"]
    return []


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------


def _is_real_osd(osd_id: int) -> bool:
    return osd_id not in (CRUSH_ITEM_NONE, -1)


def _slot(osd_list: list[int], i: int) -> int | None:
    """Return the real OSD at position i of an up/acting array, else None."""
    if i >= len(osd_list):
        return None
    return osd_list[i] if _is_real_osd(osd_list[i]) else None


def _real_osd_set(osd_list: list[int]) -> set[int]:
    return {o for o in osd_list if _is_real_osd(o)}


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
            ShardRow(i, _slot(acting, i), _slot(up, i))
            for i in range(max(len(up), len(acting)))
        ]
    up_set, acting_set = _real_osd_set(up), _real_osd_set(acting)
    rows = [ShardRow("-", o, o) for o in sorted(up_set & acting_set)]
    rows += [
        ShardRow("-", src, dst)
        for src, dst in zip_longest(
            sorted(acting_set - up_set), sorted(up_set - acting_set)
        )
    ]
    return rows


# ---------------------------------------------------------------------------
# Progress (copied from pg-movements.py; keep the two in sync)
# ---------------------------------------------------------------------------


def ec_unassigned_shards(up: list[int], acting: list[int]) -> int:
    """Count EC shards with no OSD in either up or acting.

    Nothing can move for these yet, but Ceph still counts their objects as
    degraded, so they belong in the progress denominator.
    """
    return sum(
        1
        for i in range(max(len(up), len(acting)))
        if _slot(up, i) is None and _slot(acting, i) is None
    )


def replicated_unassigned_copies(up_set: set, acting_set: set, size: int) -> int:
    """Count replicas that are degraded but have no destination OSD yet.

    Ceph counts size - len(acting) copies per object as degraded. Each
    destination (an up OSD outside acting) fills one of those, and any
    remainder is waiting for an OSD to appear.
    """
    return max(0, size - len(acting_set) - len(up_set - acting_set))


def pg_progress_pct(stat_sum: dict, n_copies: int) -> float | None:
    """Estimate % of a PG's data already at its target location.

    num_objects_misplaced (backfill) and num_objects_degraded (recovery) both
    count down to 0 as movement completes. Both are in *copy* units, so the
    PG's total work is num_objects * n_copies, n_copies being the shards or
    replicas that are moving plus those not yet assigned any OSD. An
    object-count approximation, not byte-exact. None if the PG has no objects.
    """
    total = stat_sum.get("num_objects", 0) * n_copies
    if total <= 0:
        return None
    # Misplaced and degraded may overlap, so clamp.
    remaining = stat_sum.get("num_objects_misplaced", 0) + stat_sum.get(
        "num_objects_degraded", 0
    )
    return max(0.0, min(100.0, 100.0 * (1 - remaining / total)))


def copies_in_flight(
    rows: list[ShardRow], up: list[int], acting: list[int], pool: Pool | None
) -> int:
    """Return the progress denominator's copy count, as pg-movements.py does."""
    moving = sum(r.remapped for r in rows)
    if pool is not None and pool.erasure:
        return moving + ec_unassigned_shards(up, acting)
    size = pool.size if pool else 0
    return moving + replicated_unassigned_copies(
        _real_osd_set(up), _real_osd_set(acting), size
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def format_osd(osd_id: int | None, primary: int) -> str:
    if osd_id is None:
        return "none"
    return f"osd.{osd_id}{'*' if osd_id == primary else ''}"


def format_utilization(osd_util: dict[int, float], osd_id: int | None) -> str:
    """Format as 'NN.N%'; '-' for an empty slot, '?' if 'ceph osd df' lacks it."""
    if osd_id is None:
        return NOT_APPLICABLE
    util = osd_util.get(osd_id)
    return f"{util:.1f}%" if util is not None else "?"


def format_host(osd_host: dict[int, str], osd_id: int | None) -> str:
    return NOT_APPLICABLE if osd_id is None else osd_host.get(osd_id, "?")


def format_upmaps(pairs: list[dict], row: ShardRow) -> str:
    """List 'from->to' of the pairs touching the row's acting or up OSD."""
    osds = {row.acting, row.up} - {None}
    touching = [f"{p['from']}->{p['to']}" for p in pairs if {p["from"], p["to"]} & osds]
    return ",".join(touching) or NOT_APPLICABLE


def format_progress(pct: float | None, row: ShardRow) -> str:
    # floor (not round) so a PG that is still moving (99.7%) never reads 100%.
    if not row.remapped or pct is None:
        return NOT_APPLICABLE
    return f"{math.floor(pct)}%"


def format_row(
    row: ShardRow,
    pg: dict,
    pct: float | None,
    osd_util: dict[int, float],
    osd_host: dict[int, str],
    pairs: list[dict],
) -> list[str]:
    return [
        str(row.shard),
        format_osd(row.acting, pg["acting_primary"]),
        format_utilization(osd_util, row.acting),
        format_host(osd_host, row.acting),
        format_osd(row.up, pg["up_primary"]),
        format_utilization(osd_util, row.up),
        format_host(osd_host, row.up),
        format_progress(pct, row),
        format_upmaps(pairs, row),
    ]


def print_table(rows: list[list[str]]) -> None:
    """Print rows under a two-line header: group spans, then column labels.

    A group's name is centered in dashes across the full width of its
    columns. Columns of different groups are separated by the wider GROUP_SEP.
    """
    widths = [
        max([len(label), *(len(row[i]) for row in rows)])
        for i, (_, label) in enumerate(COLUMNS)
    ]
    # seps[i] is what precedes column i.
    seps = [""] + [
        COLUMN_SEP if COLUMNS[i][0] == COLUMNS[i - 1][0] else GROUP_SEP
        for i in range(1, len(COLUMNS))
    ]

    group_line = ""
    for group, indexes in groupby(range(len(COLUMNS)), key=lambda i: COLUMNS[i][0]):
        cols = list(indexes)
        span = sum(widths[i] for i in cols) + sum(len(seps[i]) for i in cols[1:])
        group_line += seps[cols[0]]
        group_line += f" {group} ".center(span, "-") if group else " " * span
    print(group_line.rstrip())

    # Last column is variable-width and rightmost; leave it unpadded.
    def emit(cells: list[str]) -> None:
        last = len(cells) - 1
        print(
            "".join(
                sep + (cell.ljust(widths[i]) if i < last else cell)
                for i, (sep, cell) in enumerate(zip(seps, cells))
            )
        )

    emit([label for _, label in COLUMNS])
    for row in rows:
        emit(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show acting/up OSDs of a Ceph PG per shard, with "
        "utilization, host, remap progress and upmaps.",
    )
    parser.add_argument("pgid", help="PG id, e.g. 3.1a2")
    args = parser.parse_args()

    pg = fetch_pg_info(args.pgid)
    pool = fetch_pool(int(args.pgid.split(".")[0]))
    osd_host = fetch_osd_hosts()
    osd_util = fetch_osd_utilization()
    pairs = fetch_upmap_pairs(args.pgid)

    rows = build_rows(pg["up"], pg["acting"], bool(pool and pool.erasure))
    pct = pg_progress_pct(
        pg["stat_sum"], copies_in_flight(rows, pg["up"], pg["acting"], pool)
    )

    print(f"PG {args.pgid}  state: {pg['state']}\n")
    print_table([format_row(r, pg, pct, osd_util, osd_host, pairs) for r in rows])

    if any(r.remapped for r in rows):
        print(
            "\nPROGRESS is per PG (from its object counters), not per shard: "
            "every remapped row shows the same %."
        )
    print("\n* primary")


if __name__ == "__main__":
    main()
