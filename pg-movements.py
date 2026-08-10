#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
For each PG where 'up' != 'acting', print:
  - shard index (EC pools only — see below)
  - source OSD(s): OSD(s) losing data (see Note)
  - destination OSD(s): OSD(s) gaining data
  - movement type derived from PG state flags
  - abbreviated PG state string

'up'/'acting' are diffed differently depending on pool type:

  - EC pools: index i is shard i — a fixed identity, since each shard holds
    distinct erasure-coded data. Diffed position by position, one row per
    shard whose OSD changed. This is what keeps unrelated shard moves in
    the same PG (e.g. shard 0 remapped A->B while shard 4 is separately
    backfilling into a previously-missing slot) from being merged into one
    misleading multi-destination row.
  - Replicated pools: every slot holds an identical copy, so position
    carries no identity (e.g. a same-OSD-set reorder from primary-affinity
    or pg-upmap-items is not real movement). Diffed as plain sets instead,
    same as pre-shard-column behavior — one aggregate row per PG, SHARD
    column shows '-'.

Two cases are intentionally excluded:
  - In-place recovery (source == destination, or up_set == acting_set):
    the OSD is catching up via log replay on the same OSD(s) — no
    cross-OSD data movement.
  - NONE destination: the target slot is CRUSH_ITEM_NONE (2147483647),
    meaning the cluster is waiting for a suitable OSD to appear.

Note:
For pure degraded recovery (replica OSD lost, CRUSH mapped to a
replacement), the source is CRUSH_ITEM_NONE — there is no source OSD to
report. Instead the FROM_OSD column shows the PG's acting primary marked
with a trailing '*': the primary isn't losing anything (it keeps its own
copy) but it drives the recovery (reads peer shards/objects, reconstructs
if needed, sends to the destination), so it's flagged as a potential load
hotspot rather than left blank.

Recovery/backfill is always primary-driven: the primary reads (or
reconstructs) the object and pushes it to every OSD that needs a copy,
whether the reason is rebalancing or restoring lost redundancy — never
peer-to-peer between OSDs. This means a replicated-pool aggregate row can
have a genuine source (e.g. OSD A, being rebalanced away from) *and* a
separate sourceless destination (a previously-missing replica the primary
is filling) at the same time. In that case FROM_OSD shows both: the real
source(s) plus the primary marked with '*', since the primary is doing
real work here too and showing only the real source would make the row
look like a plain one-to-one move when the primary is quietly also
fan-ing out to a second destination.
"""

import argparse
import json
import math
import subprocess
import sys
from typing import NamedTuple


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sort-by",
        choices=["pgid", "from-osd", "to-osd"],
        default="pgid",
        help="column to sort output rows by (default: pgid)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def _ceph_json(cmd: list[str]) -> object:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        sys.exit(f"ERROR: ceph command failed:\n{exc.stderr.strip()}")
    except FileNotFoundError:
        sys.exit("ERROR: 'ceph' binary not found in PATH.")
    return json.loads(proc.stdout)


def fetch_osd_utilization() -> dict[int, float]:
    """Return {osd_id: utilization_pct} from 'ceph osd df'. Down OSDs may be absent."""
    data = _ceph_json(["ceph", "osd", "df", "--format", "json"])
    return {node["id"]: node["utilization"] for node in data.get("nodes", [])}


POOL_TYPE_ERASURE = 3


def fetch_ec_pool_ids() -> set[int]:
    """Return the set of pool ids that are erasure-coded (type == 3).

    Positional up/acting diffing (shard-index-significant) is only valid
    for EC pools. Replicated pools' replicas are interchangeable, so a
    same-OSD-set reorder (e.g. from primary-affinity or pg-upmap-items)
    would otherwise show up as phantom paired movements.
    """
    data = _ceph_json(["ceph", "osd", "pool", "ls", "detail", "--format", "json"])
    return {p["pool_id"] for p in data if p.get("type") == POOL_TYPE_ERASURE}


def fetch_osd_hosts() -> dict[int, str]:
    """Return {osd_id: short_hostname} from 'ceph osd tree'."""
    data = _ceph_json(["ceph", "osd", "tree", "--format", "json"])
    nodes = data.get("nodes", [])
    by_id = {n["id"]: n for n in nodes}
    result = {}
    for n in nodes:
        if n.get("type") == "host":
            short = n["name"].split(".")[0]
            for child_id in n.get("children", []):
                if by_id.get(child_id, {}).get("type") == "osd":
                    result[child_id] = short
    return result


def fetch_pg_stats() -> list[dict]:
    """Return pg_stat dicts from 'ceph pg dump pgs'. Handles several JSON shapes across releases."""
    raw = _ceph_json(["ceph", "pg", "dump", "pgs", "--format", "json"])
    return _extract_pg_stats(raw)


def _extract_pg_stats(raw) -> list[dict]:
    # Some releases return a bare list directly.
    if isinstance(raw, list):
        return raw

    if isinstance(raw, dict):
        # Quincy typical: top-level 'pg_stats' key
        if "pg_stats" in raw:
            return raw["pg_stats"]

        # Older layout: nested under 'pg_map'
        pg_map = raw.get("pg_map", {})
        if "pg_stats" in pg_map:
            return pg_map["pg_stats"]

        # Last resort: find any list whose first element looks like a pg_stat
        for val in raw.values():
            if (
                isinstance(val, list)
                and val
                and isinstance(val[0], dict)
                and "pgid" in val[0]
            ):
                return val

    raise SystemExit(
        f"ERROR: unrecognised JSON structure from 'ceph pg dump pgs'.\n"
        f"Top-level type: {type(raw).__name__}"
        + (f", keys: {list(raw.keys())}" if isinstance(raw, dict) else "")
    )


# ---------------------------------------------------------------------------
# Movement classification
# ---------------------------------------------------------------------------

# Sentinel used by CRUSH/Ceph for "no OSD in this slot" (crush/crush.h).
# 'ceph pg dump' JSON uses this value, not -1, to mark unfilled up/acting slots.
CRUSH_ITEM_NONE = 0x7FFFFFFF


def _is_real_osd(o: int) -> bool:
    return o not in (CRUSH_ITEM_NONE, -1)


def _real_osd_set(osd_list: list) -> set[int]:
    """Return the set of real (non-placeholder) OSD ids in an up/acting array."""
    return {o for o in osd_list if _is_real_osd(o)}


def pg_progress_pct(pg: dict) -> "float | None":
    """Estimate % of a PG's objects already at their target location.

    Approximated from pg_stat.stat_sum object counts (already fetched via
    'ceph pg dump pgs' — no extra ceph calls needed): num_objects_misplaced
    covers backfill (data relocating, redundancy already satisfied) and
    num_objects_degraded covers recovery (missing copies being restored).
    Both counters count down to 0 as movement completes, unlike
    num_objects_recovered/num_bytes_recovered in the same dict, which are
    lifetime cumulative counters that only ever increase and can't answer
    "how much of this move is left".

    This is an object-count approximation, not a byte-exact figure: Ceph
    doesn't track misplaced/degraded state at byte granularity, so the
    result assumes objects in the PG are roughly similar in size (true
    for RBD-style pools, less so for pools with wildly mixed object
    sizes).
    """
    stat_sum = pg.get("stat_sum", {})
    total = stat_sum.get("num_objects", 0)
    if total <= 0:
        return None
    # Misplaced and degraded are not guaranteed disjoint (a PG can be both
    # at once, e.g. state "degraded+remapped+backfilling"), so an object
    # counted in both would make remaining > total without this clamp.
    remaining = stat_sum.get("num_objects_misplaced", 0) + stat_sum.get(
        "num_objects_degraded", 0
    )
    pct = 100.0 * (1 - remaining / total)
    return max(0.0, min(100.0, pct))

# State flags that indicate active or pending data movement.
# A PG can be in multiple states simultaneously (e.g. degraded+backfilling).
_RECOVERY_FLAGS = {"recovering", "recovery_wait", "recovery_toofull"}
_BACKFILL_FLAGS = {"backfilling", "backfill_wait", "backfill_toofull"}

_STATE_ABBREVS = {
    "active": "act",
    "clean": "cln",
    "degraded": "deg",
    "undersized": "undsz",
    "remapped": "remap",
    "recovering": "rcvr",
    "recovery_wait": "rcvr_wt",
    "recovery_toofull": "rcvr_tf",
    "forced_recovery": "frc_rcvr",
    "backfilling": "bkfl",
    "backfill_wait": "bkfl_wt",
    "backfill_toofull": "bkfl_tf",
    "forced_backfill": "frc_bkfl",
    "peering": "prng",
    "peered": "prd",
    "scrubbing": "scrb",
    "deep": "dp",
    "repair": "rep",
    "inconsistent": "incon",
    "incomplete": "incomp",
    "stale": "stl",
    "down": "dn",
    "creating": "crt",
    "snaptrim": "snptrim",
    "snaptrim_wait": "snptrim_wt",
    "snaptrim_error": "snptrim_err",
    "wait": "wt",
}


def abbreviate_state(state: str) -> str:
    return "+".join(_STATE_ABBREVS.get(f, f) for f in state.split("+"))


def movement_type(state: str) -> str:
    """
    Return a short label for the type of movement based on PG state flags.

    'recovery'  — log-based peer recovery (OSD was briefly down)
    'backfill'  — full-object copy to new/returning OSD
    'remapped'  — CRUSH mapping changed but movement not yet started
                  (e.g. waiting before the first backfill_wait)

    Multiple types are joined with '+' when both flags are present.
    """
    flags = set(state.split("+"))
    labels = []
    if flags & _RECOVERY_FLAGS:
        labels.append("recovery")
    if flags & _BACKFILL_FLAGS:
        labels.append("backfill")
    if not labels:
        # Remapped but pipeline not yet active — common transient state
        labels.append("remapped")
    return "+".join(labels)


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------


def pgid_sort_key(pgid: str) -> tuple[int, int]:
    """Sort PG IDs numerically: pool id (decimal), then pg id (hex)."""
    pool_str, pg_hex = pgid.split(".")
    return (int(pool_str), int(pg_hex, 16))


class MovementRow(NamedTuple):
    pgid: str
    shard: "int | str"  # shard index for EC pools (per-shard row), or "-" for
    # replicated pools (one aggregate row per PG — see main() for why).
    sources: frozenset  # OSD ids losing data; may be empty (see module Note)
    destinations: frozenset  # OSD ids gaining data
    move_type: str
    state: str
    primary: "int | None"  # acting primary OSD id; shown (marked '*') when
    # sources is empty or needs_primary_marker is set
    needs_primary_marker: bool  # True when at least one destination has no
    # counterpart source anywhere in this row (see main() for derivation).
    # Always False for EC rows, where each row is a single shard and can't
    # mix the two cases.
    progress_pct: "float | None"  # % of the PG's objects already in their
    # target location, or None if the PG reports zero objects. Computed
    # per-PG (from pg_stat.stat_sum), not per-shard — see main() for why
    # that matters for EC rows.


def _row_pgid_key(row: MovementRow) -> tuple:
    shard_key = row.shard if isinstance(row.shard, int) else -1
    return (*pgid_sort_key(row.pgid), shard_key)


def _row_from_osd_key(row: MovementRow) -> tuple:
    ids = set(row.sources)
    if (not ids or row.needs_primary_marker) and row.primary is not None:
        ids.add(row.primary)
    return (tuple(sorted(ids)), pgid_sort_key(row.pgid))


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


def main() -> None:
    args = parse_args()

    pg_stats = fetch_pg_stats()
    osd_util = fetch_osd_utilization()
    osd_host = fetch_osd_hosts()
    ec_pool_ids = fetch_ec_pool_ids()

    def _slot(osd_list: list, i: int) -> int | None:
        if i >= len(osd_list):
            return None
        o = osd_list[i]
        return o if _is_real_osd(o) else None

    rows: list[MovementRow] = []

    for pg in pg_stats:
        pgid = pg["pgid"]
        state = pg["state"]
        up = pg["up"]
        acting = pg["acting"]
        pool_id = int(pgid.split(".")[0])

        primary = pg.get("acting_primary")
        if primary in (None, -1, CRUSH_ITEM_NONE):
            primary = next(iter(sorted(_real_osd_set(acting))), None)

        mtype = movement_type(state)
        progress = pg_progress_pct(pg)

        if pool_id in ec_pool_ids:
            # EC: shard identity is positional, so diff up/acting index by
            # index — one row per shard whose OSD changed. This is what
            # keeps unrelated shard moves (e.g. PG 27.96: shard 0 remapped
            # A->B, shard 4 separately backfilled into a previously-missing
            # slot) from being merged into one misleading multi-dest row.
            for i in range(max(len(up), len(acting))):
                source = _slot(acting, i)
                destination = _slot(up, i)

                if source == destination:
                    # No movement at this shard: clean, or in-place
                    # recovery (primary sending missing objects to a
                    # replica on the same OSD). Skip.
                    continue
                if destination is None:
                    # up[i] is CRUSH_ITEM_NONE — cluster is waiting for an
                    # OSD to fill this shard slot; nothing actionable yet.
                    continue

                sources = frozenset() if source is None else frozenset({source})
                rows.append(
                    MovementRow(
                        pgid, i, sources, frozenset({destination}), mtype,
                        state, primary, False, progress,
                    )
                )
        else:
            # Replicated: replicas are interchangeable, so shard position
            # carries no identity — a same-OSD-set reorder is not real
            # movement. Diff as sets instead, one aggregate row per PG.
            up_set = _real_osd_set(up)
            acting_set = _real_osd_set(acting)

            if up_set == acting_set:
                continue

            destinations = up_set - acting_set
            sources = acting_set - up_set

            if not destinations:
                continue

            # Shard position is meaningless for replicated pools (that's
            # the whole reason this branch diffs as sets), so we can't ask
            # "is up[i] paired with a None at acting[i]" — a same-size
            # reshuffle can align a real up[i] against an unrelated None
            # in acting purely by position, with no redundancy change
            # involved. What actually indicates the primary is filling a
            # lost replica (rather than just relocating an existing one)
            # is a net *increase* in real OSD count: if up_set has more
            # real members than acting_set, at least one destination has
            # no counterpart source anywhere, since a pure swap always
            # keeps sources/destinations equal in size.
            needs_primary_marker = len(destinations) > len(sources)

            rows.append(
                MovementRow(
                    pgid, "-", frozenset(sources), frozenset(destinations),
                    mtype, state, primary, needs_primary_marker, progress,
                )
            )

    if not rows:
        print("No PG movements detected.")
        return

    rows.sort(key=_SORT_KEYS[args.sort_by])

    # ------------------------------------------------------------------
    # Tabular output
    # ------------------------------------------------------------------

    SEP = "  ->  "  # separator between FROM and TO columns
    SEP_HDR = " " * len(SEP)  # same width, plain spaces in the header row

    def fmt_osd(o: int, show_util: bool = True) -> str:
        host = osd_host.get(o, "?")
        if not show_util:
            return f"{o}({host})"
        util = f"{osd_util[o]:.0f}%" if o in osd_util else "?%"
        return f"{o}({host},{util})"

    def fmt_osds(osd_ids: frozenset) -> str:
        """Format OSD ids as 'ID(host,util%),...'."""
        return ",".join(fmt_osd(o) for o in sorted(osd_ids))

    used_primary_marker = False

    def fmt_from(sources: frozenset, primary, needs_primary_marker: bool = False) -> str:
        """
        FROM_OSD cell. Normally the OSD(s) actually losing data. When
        empty (pure degraded recovery — the shard/replica slot was
        CRUSH_ITEM_NONE), the primary isn't losing anything — it keeps its
        copy — but it's the one driving recovery reads/reconstruction, so
        flag it as a potential load hotspot instead of leaving the column
        blank.

        A replicated-pool row can mix a genuine vacating source with a
        separate sourceless (degraded) destination — e.g. one replica
        rebalancing away from OSD A while another, previously-missing,
        replica is filled in by the primary. needs_primary_marker flags
        that case so the primary is shown alongside the real source(s)
        instead of being omitted, since it's doing real work here too.

        Utilization is omitted for the primary marker since it won't drop
        as a result of this movement (the primary isn't losing data).

        used_primary_marker is only set when a '*' is actually appended —
        not merely when this branch is entered — since primary can coincide
        with a real source (the primary itself is vacating), in which case
        nothing more is shown and the footnote shouldn't print either.
        """
        nonlocal used_primary_marker
        parts = []
        if sources:
            parts.append(fmt_osds(sources))
        if not sources or needs_primary_marker:
            if primary is None:
                parts.append("unknown")
            elif primary not in sources:
                used_primary_marker = True
                parts.append(f"{fmt_osd(primary, show_util=False)}*")
        return ",".join(parts)

    def fmt_progress(pct: "float | None") -> str:
        # floor (not round) so a PG that's still moving (e.g. 99.7%) never
        # displays as "100%" before it's actually done.
        return "-" if pct is None else f"{math.floor(pct)}%"

    # Column widths fitted to actual data
    col_pg = max(len("PGID"), max(len(r.pgid) for r in rows))
    col_shard = max(len("SHARD"), max(len(str(r.shard)) for r in rows))
    col_from = max(
        len("FROM_OSD"),
        max(len(fmt_from(r.sources, r.primary, r.needs_primary_marker)) for r in rows),
    )
    col_to = max(len("TO_OSD"), max(len(fmt_osds(r.destinations)) for r in rows))
    col_type = max(len("TYPE"), max(len(r.move_type) for r in rows))
    col_progress = max(
        len("PROGRESS"), max(len(fmt_progress(r.progress_pct)) for r in rows)
    )

    def format_row(row: MovementRow) -> str:
        return (
            f"{row.pgid:<{col_pg}}  "
            f"{str(row.shard):<{col_shard}}  "
            f"{fmt_from(row.sources, row.primary, row.needs_primary_marker):<{col_from}}"
            f"{SEP}"
            f"{fmt_osds(row.destinations):<{col_to}}  "
            f"{row.move_type:<{col_type}}  "
            f"{fmt_progress(row.progress_pct):<{col_progress}}  "
            f"{abbreviate_state(row.state)}"
        )

    header = (
        f"{'PGID':<{col_pg}}  "
        f"{'SHARD':<{col_shard}}  "
        f"{'FROM_OSD':<{col_from}}"
        f"{SEP_HDR}"
        f"{'TO_OSD':<{col_to}}  "
        f"{'TYPE':<{col_type}}  "
        f"{'PROGRESS':<{col_progress}}  "
        f"STATE"
    )

    data_lines = [format_row(r) for r in rows]
    separator = "─" * max(len(header), max(len(l) for l in data_lines))

    print(header)
    print(separator)
    for line in data_lines:
        print(line)

    if used_primary_marker:
        print(
            "\n* this is the PG's primary OSD — keeps its copy (not a data source); shown "
            "because\n                 it drives recovery reads/reconstruction and may see "
            "elevated load."
        )

    if any(isinstance(r.shard, int) for r in rows):
        print(
            "\nPROGRESS is computed per PG, not per shard: it comes from the whole PG's "
            "object\ncounts ('ceph pg dump'), so if an EC PG has more than one shard moving "
            "independently\n(see module docstring), every shard row for that PG shows the "
            "same % — the PG's\noverall remaining work, not this shard's individually."
        )

    num_pgs = len({r.pgid for r in rows})
    print(f"\n{len(rows)} shard movement(s) across {num_pgs} PG(s).")


if __name__ == "__main__":
    main()
