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
from typing import NamedTuple

from shared import (
    PROGRESS_100_NOTE,
    PROGRESS_COUNTERS,
    SnapshotStore,
    abbreviate_state,
    add_state_args,
    copies_moving,
    ec_shard_moves,
    extract_pg_stats,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    format_progress,
    is_erasure,
    is_real_osd,
    pg_progress_pct,
    pgid_pool_id,
    pgid_sort_key,
    progress_reads_100,
    real_osd_set,
)
from shared import anonymize_snapshots as anonymize_common

# Maps each snapshot to the 'ceph ... --format json' command that produces it
# and the '<key>.json' filename it is saved/loaded as (--save-state/--load-state).
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}

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
    add_state_args(parser, SNAPSHOT_COMMANDS)
    return parser.parse_args()


# The parts of each pg_stat this script reads.
KEPT_PG_STAT_KEYS = ("pgid", "state", "up", "acting", "acting_primary")


def anonymize_snapshots(snapshots: dict[str, object]) -> None:
    """Anonymize the snapshots for --save-state, in place.

    'ceph pg dump pgs' carries dozens of fields per PG, of which this script
    reads a handful; only those are kept, which also keeps the capture small
    on a big cluster.
    """
    anonymize_common(snapshots)
    pg_stats = extract_pg_stats(snapshots["pg_dump_pgs"], "ceph pg dump pgs")
    snapshots["pg_dump_pgs"] = {
        "pg_stats": [
            {
                **{k: pg[k] for k in KEPT_PG_STAT_KEYS if k in pg},
                "stat_sum": {
                    k: v
                    for k, v in pg.get("stat_sum", {}).items()
                    if k in PROGRESS_COUNTERS
                },
            }
            for pg in pg_stats
        ]
    }


# ---------------------------------------------------------------------------
# Movement classification
# ---------------------------------------------------------------------------


# State flags that indicate active or pending data movement.
# A PG can be in multiple states simultaneously (e.g. degraded+backfilling).
_RECOVERY_FLAGS = {"recovering", "recovery_wait", "recovery_toofull"}
_BACKFILL_FLAGS = {"backfilling", "backfill_wait", "backfill_toofull"}


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

    store = SnapshotStore.from_args(
        args, SNAPSHOT_COMMANDS, anonymize=anonymize_snapshots
    )
    pg_stats = fetch_pg_stats(store, "pg_dump_pgs")
    osd_df = fetch_osd_df(store)
    osd_host = fetch_osd_hosts(store)
    pools = fetch_pools(store)
    store.save()

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
            # EC: shard identity is positional, so diff up/acting index by
            # index — one row per shard whose OSD changed. This is what
            # keeps unrelated shard moves (e.g. PG 27.96: shard 0 remapped
            # A->B, shard 4 separately backfilled into a previously-missing
            # slot) from being merged into one misleading multi-dest row.
            moves = ec_shard_moves(up, acting)
            progress = pg_progress_pct(pg, copies_moving(up, acting, True, 0))
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
                        progress,
                    )
                )
        else:
            # Replicated: replicas are interchangeable, so shard position
            # carries no identity — a same-OSD-set reorder is not real
            # movement. Diff as sets instead, one aggregate row per PG.
            up_set = real_osd_set(up)
            acting_set = real_osd_set(acting)

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
            progress = pg_progress_pct(
                pg,
                copies_moving(up, acting, False, pool.get("size", 0) if pool else 0),
            )

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
                    progress,
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
        util_pct = osd_df.get(o, {}).get("utilization")
        util = f"{util_pct:.0f}%" if util_pct is not None else "?%"
        return f"{o}({host},{util})"

    def fmt_osds(osd_ids: frozenset) -> str:
        """Format OSD ids as 'ID(host,util%),...'."""
        return ",".join(fmt_osd(o) for o in sorted(osd_ids))

    used_primary_marker = False

    def fmt_from(
        sources: frozenset, primary, needs_primary_marker: bool = False
    ) -> str:
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
        len("PROGRESS"), max(len(format_progress(r.progress_pct)) for r in rows)
    )

    def format_row(row: MovementRow) -> str:
        return (
            f"{row.pgid:<{col_pg}}  "
            f"{row.shard!s:<{col_shard}}  "
            f"{fmt_from(row.sources, row.primary, row.needs_primary_marker):<{col_from}}"
            f"{SEP}"
            f"{fmt_osds(row.destinations):<{col_to}}  "
            f"{row.move_type:<{col_type}}  "
            f"{format_progress(row.progress_pct):<{col_progress}}  "
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

    if any(progress_reads_100(r.progress_pct) for r in rows):
        print(f"\n{PROGRESS_100_NOTE}")

    num_pgs = len({r.pgid for r in rows})
    print(f"\n{len(rows)} shard movement(s) across {num_pgs} PG(s).")


if __name__ == "__main__":
    main()
