# SPDX-License-Identifier: MIT
"""
Show backfills: for each PG where 'up' != 'acting', print:
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

Filters (all rows by default; given together, a row must pass both):
  --osds  keep rows showing any of the given OSDs in FROM_OSD or TO_OSD,
          the '*'-marked primary included, since it carries recovery load.
          Filters rows, not PGs: for an EC PG, only the shards that touch
          one of the OSDs are shown.
  --pgs   keep only rows of the given PGs. A given PG with no movement is
          named on stderr, since that usually means a typo.
"""

import argparse
import sys
from typing import NamedTuple

from shared import (
    PROGRESS_100_NOTE,
    PgidFilter,
    SnapshotStore,
    abbreviate_state,
    copies_moving,
    ec_shard_moves,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    format_progress,
    is_erasure,
    is_real_osd,
    parse_osd,
    pg_progress_pct,
    pgid_pool_id,
    pgid_sort_key,
    progress_reads_100,
    real_osd_set,
)

# Maps each snapshot to the 'ceph ... --format json' command that produces it
# and the '<key>.json' filename it is read back from under --load-state (see
# the save-state subcommand, which captures this same file).
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
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sort-by",
        choices=["pgid", "from-osd", "to-osd"],
        default="pgid",
        help="column to sort output rows by (default: pgid)",
    )
    parser.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        default=[],
        metavar="OSD",
        help="Show only rows involving any of these OSDs: as a source, a "
        "destination, or the '*'-marked primary driving recovery. "
        "Space-separated, e.g. --osds 12 osd.34.",
    )
    parser.add_argument(
        "--pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="Show only rows of these PG id(s). Space-separated, e.g. --pgs "
        "19.92e 20.1a3. A given id with no movement is reported on stderr, "
        "since that usually means a typo.",
    )
    return parser


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
    # replicated pools (one aggregate row per PG — see plan() for why).
    sources: frozenset  # OSD ids losing data; may be empty (see module Note)
    destinations: frozenset  # OSD ids gaining data
    move_type: str
    state: str
    primary: "int | None"  # acting primary OSD id; shown (marked '*') when
    # sources is empty or needs_primary_marker is set
    needs_primary_marker: bool  # True when at least one destination has no
    # counterpart source anywhere in this row (see plan() for derivation).
    # Always False for EC rows, where each row is a single shard and can't
    # mix the two cases.
    progress_pct: "float | None"  # % of the PG's objects already in their
    # target location, or None if the PG reports zero objects. Computed
    # per-PG (from pg_stat.stat_sum), not per-shard — see plan() for why
    # that matters for EC rows.


def _row_pgid_key(row: MovementRow) -> tuple:
    shard_key = row.shard if isinstance(row.shard, int) else -1
    return (*pgid_sort_key(row.pgid), shard_key)


def from_osds(row: MovementRow) -> set[int]:
    """OSD ids the row's FROM_OSD cell shows: sources plus any '*' primary."""
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
    """Everything a run found, independent of how it is printed.

    plan() computes it and render() prints it. osd_df and osd_host are
    carried along only because the table shows them.
    """

    rows: list[MovementRow]  # sorted by --sort-by; --osds/--pgs applied
    pgs_filter: PgidFilter | None  # None without --pgs
    filtered: bool  # --osds or --pgs given
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> MovementsResult:
    """Fetch the cluster state from store and find the PG movements in it.

    All of them, unless narrowed down by --osds and --pgs (see
    filter_rows).
    """
    pg_stats = fetch_pg_stats(store, "pg_dump_pgs")
    osd_df = fetch_osd_df(store)
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

    rows, pgs_filter = filter_rows(rows, set(args.osds), set(args.pgs))
    rows.sort(key=_SORT_KEYS[args.sort_by])
    return MovementsResult(
        rows, pgs_filter, bool(args.osds or args.pgs), osd_df, osd_host
    )


def filter_rows(
    rows: list[MovementRow], osds: set[int], pgids: set[str]
) -> tuple[list[MovementRow], PgidFilter | None]:
    """Keep the rows that pass both filters; an empty filter passes everything.

    A row passes osds if any OSD it shows (FROM_OSD, '*' primary included, or
    TO_OSD) is in it, and passes pgids if its PG is in it. Returns the kept
    rows and, if pgids is non-empty, which of them matched a moving PG at all
    (regardless of osds, so that only real typos are flagged as unmatched).
    """
    pgs_filter = None
    if pgids:
        matched = pgids & {r.pgid for r in rows}
        pgs_filter = PgidFilter(len(pgids), len(matched), sorted(pgids - matched))
        rows = [r for r in rows if r.pgid in pgids]
    if osds:
        rows = [r for r in rows if osds & (from_osds(r) | r.destinations)]
    return rows, pgs_filter


def print_pgs_filter(pgs_filter: PgidFilter) -> None:
    """Report on stderr what --pgs matched, naming the ids that matched nothing."""
    print(
        f"--pgs: {pgs_filter.matched} of {pgs_filter.given} given PG id(s) "
        "have movement"
        + (
            f"; {len(pgs_filter.unmatched)} matched nothing (not moving, or "
            "a typo): " + ", ".join(pgs_filter.unmatched)
            if pgs_filter.unmatched
            else ""
        ),
        file=sys.stderr,
    )


def render(result: MovementsResult) -> None:
    """Print result's rows as a table, then the footnotes that apply to them.

    The --pgs note, if any, goes to stderr first.
    """
    rows, pgs_filter, filtered, osd_df, osd_host = result
    if pgs_filter is not None:
        print_pgs_filter(pgs_filter)
    if not rows:
        print(
            "No PG movements match --osds/--pgs."
            if filtered
            else "No PG movements detected."
        )
        return

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


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)))
