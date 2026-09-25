# SPDX-License-Identifier: MIT
"""
Measure the rates at which backfill destinations (UP OSDs) receive data:
sample the movements show-backfill shows twice, at least --interval seconds
apart, and give each copy's RATE and ETA.

RATE is objects and MiB per second arriving at the row's UP OSD, from how far
its PROGRESS moved; MiB assume the PG's objects are of even size. ETA is the
time left at that rate. The stderr summary totals the rates.

The interval starts once the first sample is complete, and is timed per PG
query, so every row is measured over at least --interval seconds.

'~' marks a RATE and ETA from Ceph's counters, which are as unreliable as the
PROGRESS they come from. RATE is '-' for a copy that started moving (or was
re-targeted) during the interval, whose progress came from its backfill
position in one sample and the counters in the other, or went down (restarted,
or counters reset).

Two more tables sum the rates per UP OSD and per host of the first. COPIES
counts every row, with a RATE or not. All three tables sort by host, in human
order ('ceph1-2' before 'ceph1-10'), then OSD, PG and shard; --sort-by obj/s
or mib/s sorts them all fastest first, progress and eta just the copies.

Copies without a destination (a replica dropped outright) are left out.
--osds and --hosts keep rows whose UP OSD they match; --pgs keeps rows of the
given PGs. --save-state DIR
also saves both samples, anonymized, for replay with --load-state DIR; a
'save-state' capture holds just one, so it can't be replayed here.
"""

import argparse
import json
import math
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

import show_backfill as sb
from messages import (
    format_bytes,
    print_movement_summary,
    print_no_movements,
    print_pgid_filter,
    print_progress_note,
    print_query_failed,
    stderr_para,
)
from save_state import trim_osd_dump, trim_pg_dump
from shared import (
    BACKFILL_POSITIONS_FILE,
    NOT_APPLICABLE,
    UNKNOWN_HOST,
    HelpFormatter,
    PgidFilter,
    SnapshotStore,
    TimedPositions,
    abbreviate_state,
    add_load_state_arg,
    anonymize_snapshots,
    fetch_backfill_positions,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    format_progress,
    natural_sort_key,
    osd_cells,
    osd_columns,
    pgid_pool_id,
    pgid_sort_key,
    print_table,
    query_backfill_positions_timed,
    resolve_save_dir,
    shard_size_bytes,
)

# Read once. A capture keeps these in its top directory...
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
}

# ...and each sample's PG dump and backfill positions in a subdirectory.
SAMPLE_COMMANDS: dict[str, list[str]] = {
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}
SAMPLE_DIRS = ("1", "2")

# When each sample was read, in seconds from the first sample's PG dump:
# {"samples": [{"dump": T, "queries": {pgid: T}}, ...]}.
TIMES_FILE = "times.json"

DEFAULT_INTERVAL = 30.0

MIB = 1024 * 1024

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def positive_seconds(text: str) -> float:
    """argparse type: a finite duration in seconds, more than 0."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number, got {text!r}") from None
    if not (value > 0 and math.isfinite(value)):  # also rejects nan
        raise argparse.ArgumentTypeError(f"must be a finite number over 0, got {text}")
    return value


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "measure-rate",
        help="Show how fast backfill destinations receive data, per copy, OSD "
        "and host, and when they will finish.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--interval",
        type=positive_seconds,
        default=DEFAULT_INTERVAL,
        metavar="SECONDS",
        help="Wait at least this long between the samples (default: %(default)g). "
        "A replay uses the capture's.",
    )
    parser.add_argument(
        "--sort-by",
        choices=SORT_CHOICES,
        default="host",
        help="Sort by UP host (then OSD, PG, shard), fastest rate, or the "
        "copies by most progress or soonest ETA (default: %(default)s).",
    )
    sb.add_filter_args(parser)
    state = parser.add_mutually_exclusive_group()
    state.add_argument(
        "--save-state",
        metavar="DIR",
        help="Also save both samples to DIR (created if missing, must be empty), "
        "for --load-state.",
    )
    add_load_state_arg(
        state,
        after_command=True,
        help="Replay a 'measure-rate --save-state' capture instead of sampling "
        "the live cluster.",
    )
    return parser


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


class Sample(NamedTuple):
    """One reading of the PGs' progress. Times are seconds on the sampler's clock."""

    pg_stats: list[dict]
    positions: dict[str, dict[str, str]]  # {pgid: {peer: position}}
    dump_time: float  # when 'ceph pg dump pgs' was read
    query_times: dict[str, float]  # {pgid: when its positions were read}
    queried: frozenset[str]  # PGs whose positions were asked for
    failed: frozenset[str]  # of those, the ones whose query failed


# Given a sample's pg_stats, the PGs to read the backfill positions of.
Select = Callable[[list[dict]], set[str]]


class LiveSampler:
    """Samples the live cluster. If store has a save_dir, save() writes a capture there."""

    def __init__(
        self,
        store: SnapshotStore,
        interval: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        query: Callable[[set[str]], TimedPositions] = query_backfill_positions_timed,
    ):
        """store serves SNAPSHOT_COMMANDS, anonymized by anonymize_static."""
        self.store = store
        self.interval = interval
        self.clock, self.sleep, self.query = clock, sleep, query
        self.samples: list[Sample] = []
        self._sample_stores: list[SnapshotStore] = []

    def take(self, select: Select) -> Sample:
        """Read the PG dump, then the positions of the PGs select picks from it.

        The dump's time is the middle of its read; clock must be the one
        query times its replies with (time.monotonic).
        """
        save_dir = self.store.save_dir
        store = SnapshotStore(
            SAMPLE_COMMANDS,
            save_dir=None
            if save_dir is None
            else save_dir / SAMPLE_DIRS[len(self.samples)],
            anonymize=trim_sample,
        )
        start = self.clock()
        pg_stats = fetch_pg_stats(store, "pg_dump_pgs")
        dump_time = (start + self.clock()) / 2
        pgids = select(pg_stats)
        reply = self.query(pgids)
        sample = Sample(
            pg_stats,
            reply.positions,
            dump_time,
            reply.times,
            frozenset(pgids),
            frozenset(reply.failed),
        )
        self.samples.append(sample)
        self._sample_stores.append(store)
        return sample

    def wait(self) -> None:
        """Sleep --interval seconds, saying so on stderr."""
        stderr_para(f"Sampling again in {self.interval:g} s.")
        self.sleep(self.interval)

    def save(self) -> None:
        """Write the snapshots and samples taken as a capture (a no-op without save_dir).

        Times are saved relative to the first sample's dump, to the microsecond,
        so a replay reproduces the run's ETAs.
        """
        if self.store.save_dir is None:
            return
        self.store.save()
        origin = self.samples[0].dump_time
        times = []
        for store, sample in zip(self._sample_stores, self.samples, strict=True):
            store.save_dir.mkdir()
            store.save()
            (store.save_dir / BACKFILL_POSITIONS_FILE).write_text(
                json.dumps(sample.positions, separators=(",", ":"))
            )
            times.append(
                {
                    "dump": round(sample.dump_time - origin, 6),
                    "queries": {
                        pgid: round(t - origin, 6)
                        for pgid, t in sample.query_times.items()
                    },
                }
            )
        (self.store.save_dir / TIMES_FILE).write_text(json.dumps({"samples": times}))


class ReplaySampler:
    """Replays the samples of a --save-state capture, in order."""

    def __init__(self, store: SnapshotStore):
        """store reads the capture's top directory; exits unless it has TIMES_FILE."""
        self.store = store
        path = Path(store.load_dir) / TIMES_FILE
        try:
            self.times = json.loads(path.read_text())["samples"]
        except FileNotFoundError:
            sys.exit(
                f"ERROR: --load-state: {store.load_dir} is not a measure-rate "
                f"capture (no {TIMES_FILE}). Make one with 'measure-rate "
                "--save-state DIR'."
            )
        self.taken = 0

    def take(self, select: Select) -> Sample:
        """Return the next saved sample, with the positions of the PGs select picks."""
        times = self.times[self.taken]
        store = SnapshotStore(
            SAMPLE_COMMANDS,
            load_dir=Path(self.store.load_dir) / SAMPLE_DIRS[self.taken],
        )
        self.taken += 1
        pg_stats = fetch_pg_stats(store, "pg_dump_pgs")
        pgids = select(pg_stats)
        return Sample(
            pg_stats,
            fetch_backfill_positions(store, pgids),
            times["dump"],
            times["queries"],
            frozenset(pgids),
            frozenset(),
        )

    def wait(self) -> None:
        """Nothing to wait for: the capture has the times."""

    def save(self) -> None:
        """Nothing to save: --save-state and --load-state are exclusive."""


def anonymize_static(snapshots: dict[str, object]) -> None:
    """Anonymize SNAPSHOT_COMMANDS' snapshots and trim them, as save-state does."""
    anonymize_snapshots(snapshots)
    snapshots["osd_dump"] = trim_osd_dump(snapshots["osd_dump"])


def trim_sample(snapshots: dict[str, object]) -> None:
    """Trim a sample's PG dump, as save-state does; it has nothing to anonymize."""
    snapshots["pg_dump_pgs"] = trim_pg_dump(snapshots["pg_dump_pgs"])


# ---------------------------------------------------------------------------
# Rates
# ---------------------------------------------------------------------------


class Rate(NamedTuple):
    """How fast one copy moves to its target."""

    pct_per_s: float  # of the copy, by its PROGRESS
    objects_per_s: float
    bytes_per_s: float | None  # None: the shard's size is unknown
    eta_s: float | None  # None: not moving
    exact: bool  # from backfill positions in both samples
    seconds: float  # measured over


# Why a row has no Rate.
NEW = "new"  # not moving to this target at the first sample
MIXED = "mixed"  # progress from a position in one sample, counters in the other
BACKWARDS = "backwards"  # progress went down
UNKNOWN = "unknown"  # no progress (no target, or no objects): PROGRESS is '-'


class RateRow(NamedTuple):
    """A row of the second sample, and its rate since the first."""

    move: sb.MovementRow  # progress filled in
    rate: Rate | None
    no_rate: str | None  # why rate is None: NEW, MIXED, BACKWARDS or UNKNOWN


def row_key(row: sb.MovementRow) -> tuple:
    """Identify a copy's movement across samples: its PG, shard and target."""
    return (row.pgid, row.shard, row.up_osd)


def progress_time(sample: Sample, row: sb.MovementRow) -> float | None:
    """Return when row's progress was read: its PG's query, or the PG dump for counters."""
    return sample.query_times.get(row.pgid) if row.progress_exact else sample.dump_time


def copy_rate(
    before: sb.MovementRow | None,
    after: sb.MovementRow,
    seconds: float | None,
    pg: dict,
    shard_bytes: int | None,
) -> tuple[Rate | None, str | None]:
    """Return (the rate from before to after, None) or (None, why there is none).

    seconds is the time between the two progress readings; pg is after's
    'ceph pg dump pgs' entry, shard_bytes its copy's size. A copy's objects
    and bytes arrive in proportion to its PROGRESS.
    """
    if before is None:
        return None, NEW
    if before.progress_pct is None or after.progress_pct is None or not seconds:
        return None, UNKNOWN
    if before.progress_exact != after.progress_exact:
        return None, MIXED
    delta = after.progress_pct - before.progress_pct
    if delta < 0:
        return None, BACKWARDS
    pct_per_s = delta / seconds
    remaining = 100.0 - after.progress_pct
    if remaining <= 0:
        eta = 0.0
    else:
        eta = remaining / pct_per_s if pct_per_s > 0 else None
    objects = pg.get("stat_sum", {}).get("num_objects", 0)
    return (
        Rate(
            pct_per_s,
            pct_per_s / 100 * objects,
            None if shard_bytes is None else pct_per_s / 100 * shard_bytes,
            eta,
            after.progress_exact,
            seconds,
        ),
        None,
    )


def rate_rows(
    first: Sample,
    first_rows: list[sb.MovementRow],
    second: Sample,
    second_rows: list[sb.MovementRow],
    pools: dict[int, dict],
    ec_profiles: dict[str, dict],
) -> tuple[list[RateRow], int]:
    """Return (second_rows with their rates, how many first_rows are gone).

    A first row is gone if no second row has its key (row_key): it finished,
    or was re-targeted.
    """
    earlier = {row_key(r): r for r in first_rows}
    pgs = {pg["pgid"]: pg for pg in second.pg_stats}
    result = []
    for row in second_rows:
        before = earlier.get(row_key(row))
        seconds = None
        if before is not None:
            t_before, t_after = progress_time(first, before), progress_time(second, row)
            if t_before is not None and t_after is not None:
                seconds = t_after - t_before
        pg = pgs[row.pgid]
        pool = pools.get(pgid_pool_id(row.pgid))
        shard_bytes = (
            shard_size_bytes(pg, pool, ec_profiles)
            if "num_bytes" in pg.get("stat_sum", {})
            else None
        )
        result.append(RateRow(row, *copy_rate(before, row, seconds, pg, shard_bytes)))
    gone = len(earlier.keys() - {row_key(r) for r in second_rows})
    return result, gone


class Flow(NamedTuple):
    """The copies arriving at an OSD or host, and their summed rate."""

    copies: int  # every row, with a rate or not
    measured: int  # of those, the rows with a rate
    objects_per_s: float  # summed over the rows with a rate
    bytes_per_s: float | None  # None if a row with a rate has an unknown size
    exact: bool  # no summed rate is counter-based


NO_FLOW = Flow(0, 0, 0.0, 0.0, True)


def add_to_flow(flow: Flow, rate: Rate | None) -> Flow:
    """Return flow with one more copy, arriving at rate (None: unmeasured)."""
    if rate is None:
        return flow._replace(copies=flow.copies + 1)
    return Flow(
        flow.copies + 1,
        flow.measured + 1,
        flow.objects_per_s + rate.objects_per_s,
        None
        if flow.bytes_per_s is None or rate.bytes_per_s is None
        else flow.bytes_per_s + rate.bytes_per_s,
        flow.exact and rate.exact,
    )


class FlowRow(NamedTuple):
    """What arrives at an UP OSD, or at the UP OSDs of a host."""

    key: "int | str"  # OSD id or host name
    flow: Flow


def aggregate(
    rows: list[RateRow], key_of: Callable[[int], "int | str"]
) -> list[FlowRow]:
    """Sum rows' rates per key_of(UP OSD), unsorted (see flow_sort_key).

    rows must all have an UP OSD (see plan()).
    """
    flows: dict[int | str, Flow] = {}
    for row in rows:
        key = key_of(row.move.up_osd)
        flows[key] = add_to_flow(flows.get(key, NO_FLOW), row.rate)
    return [FlowRow(key, flow) for key, flow in flows.items()]


def _rate_field(name: str) -> Callable[[RateRow], float | None]:
    return lambda row: None if row.rate is None else getattr(row.rate, name)


def _flow_field(name: str) -> Callable[[FlowRow], float | None]:
    return lambda f: getattr(f.flow, name) if f.flow.measured else None


# --sort-by's choices, bar the default order ('host'): the value a copy, OSD
# or host sorts by, None if it has none. Larger values come first, except
# eta's. The OSD and host tables have no progress or ETA: they stay in the
# default order.
SORT_VALUES: dict[str, Callable[[RateRow], float | None]] = {
    "obj/s": _rate_field("objects_per_s"),
    "mib/s": _rate_field("bytes_per_s"),
    "progress": lambda row: row.move.progress_pct,
    "eta": _rate_field("eta_s"),
}
FLOW_SORT_VALUES: dict[str, Callable[[FlowRow], float | None]] = {
    "obj/s": _flow_field("objects_per_s"),
    "mib/s": _flow_field("bytes_per_s"),
}
ASCENDING = {"eta"}
SORT_CHOICES = ["host", *SORT_VALUES]


def _by_value(
    sort_by: str,
    values: dict[str, Callable],
    default: Callable[..., tuple],
) -> Callable[..., tuple]:
    """Return the key sorting by values[sort_by], valueless last, ties by default.

    Just default if values has no sort_by.
    """
    value_of = values.get(sort_by)
    if value_of is None:
        return default
    sign = 1 if sort_by in ASCENDING else -1

    def key(item: RateRow | FlowRow) -> tuple:
        value = value_of(item)
        return (value is None, 0.0 if value is None else sign * value, default(item))

    return key


def host_key(osd_host: dict[int, str], osd: int) -> tuple:
    """Sort key of osd's host, in human order (UNKNOWN_HOST if unknown)."""
    return natural_sort_key(osd_host.get(osd, UNKNOWN_HOST))


def sort_key(sort_by: str, osd_host: dict[int, str]) -> Callable[[RateRow], tuple]:
    """Return the copy table's key for --sort-by.

    'host' (the default order) sorts by the UP OSD's host (host_key), then
    the OSD, PG and shard. The others sort by SORT_VALUES, rows without a
    value last, and break ties in the default order.
    """

    def default(row: RateRow) -> tuple:
        osd = row.move.up_osd
        return (host_key(osd_host, osd), osd, sb.SORT_KEYS["pgid"](row.move))

    return _by_value(sort_by, SORT_VALUES, default)


def flow_sort_key(
    sort_by: str, default: Callable[[FlowRow], tuple]
) -> Callable[[FlowRow], tuple]:
    """Return an OSD or host table's key for --sort-by, as sort_key does.

    default is the table's default order; it is also the order under the
    choices without FLOW_SORT_VALUES.
    """
    return _by_value(sort_by, FLOW_SORT_VALUES, default)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class RatesResult(NamedTuple):
    """What plan() found, for render() to print."""

    rows: list[RateRow]  # the second sample's, sorted by --sort-by
    osd_flows: list[FlowRow]  # per UP OSD of rows, sorted by --sort-by
    host_flows: list[FlowRow]  # per host of those (UNKNOWN_HOST if unknown), sorted too
    gone: int  # movements of the first sample the second no longer has
    pgs_filter: PgidFilter | None  # None without --pgs
    filter_options: tuple[str, ...]  # sb.RowFilter.options
    queried: int  # PGs whose positions were asked for, in either sample
    query_failed: list[str]  # of those, the ones a query failed for, in PG order
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, sampler: LiveSampler | ReplaySampler) -> RatesResult:
    """Take two samples of the filtered movements and measure their rates.

    Movements without a destination are dropped before filtering. Skips the
    second sample if the first has nothing to measure.
    """
    store = sampler.store
    osd_df = fetch_osd_df(store)
    osd_host = fetch_osd_hosts(store)
    row_filter = sb.RowFilter.from_args(args, osd_df, osd_host)
    pools = fetch_pools(store)
    ec_profiles = fetch_ec_profiles(store)

    def movements(pg_stats: list[dict]):
        found = [r for r in sb.find_movements(pg_stats, pools) if r.up_osd is not None]
        return row_filter.apply(found, up_only=True)

    def select(pg_stats: list[dict]) -> set[str]:
        return {r.pgid for r in movements(pg_stats)[0]}

    samples: list[Sample] = []
    rows: list[list[sb.MovementRow]] = []
    for i in range(len(SAMPLE_DIRS)):
        if i:
            if not rows[0]:
                break  # nothing to measure
            sampler.wait()
        sample = sampler.take(select)
        found, pgs_filter = movements(sample.pg_stats)
        samples.append(sample)
        rows.append(sb.with_progress(found, sample.pg_stats, pools, sample.positions))

    measured, gone = [], 0
    if len(samples) == len(SAMPLE_DIRS):
        measured, gone = rate_rows(
            samples[0], rows[0], samples[1], rows[1], pools, ec_profiles
        )
    measured.sort(key=sort_key(args.sort_by, osd_host))
    osd_flows = aggregate(measured, lambda osd: osd)
    osd_flows.sort(
        key=flow_sort_key(args.sort_by, lambda f: (host_key(osd_host, f.key), f.key))
    )
    host_flows = aggregate(measured, lambda osd: osd_host.get(osd, UNKNOWN_HOST))
    host_flows.sort(key=flow_sort_key(args.sort_by, lambda f: natural_sort_key(f.key)))
    queried = frozenset().union(*(s.queried for s in samples))
    failed = frozenset().union(*(s.failed for s in samples))
    return RatesResult(
        measured,
        osd_flows,
        host_flows,
        gone,
        pgs_filter,
        row_filter.options,
        len(queried),
        sorted(failed, key=pgid_sort_key),
        osd_df,
        osd_host,
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# (group, label). show-backfill's, less ACTING, plus the rate after PROGRESS.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    *osd_columns("UP"),
    ("", "TYPE"),
    ("", "PROGRESS"),
    ("RATE", "OBJ/S"),
    ("RATE", "MiB/S"),
    ("", "ETA"),
    ("", "STATE"),
]

# The rate columns of the OSD and host tables.
FLOW_COLUMNS = [("", "COPIES"), ("", "OBJ/S"), ("", "MiB/S")]
OSD_COLUMNS = [*osd_columns(""), *FLOW_COLUMNS]
HOST_COLUMNS = [("", "HOST"), *FLOW_COLUMNS]

RATE_APPROX_NOTE = (
    "~ marks RATE and ETA from Ceph's counters, as unreliable as the PROGRESS "
    "they come from, and the OSD and host sums that include one."
)

# Why RATE is '-', as a clause after a count of rows (UNKNOWN goes unexplained:
# its PROGRESS is '-' too).
NO_RATE_REASONS = {
    NEW: "started moving (or were re-targeted) during the interval",
    MIXED: "had progress from a backfill position in one sample and the "
    "counters in the other",
    BACKWARDS: "went backwards (restarted, or counters reset)",
}


def format_duration(seconds: float) -> str:
    """Format seconds in its two largest units, e.g. '45s', '12m05s', '3h05m', '2d04h'."""
    s = round(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    if s < 86400:
        return f"{s // 3600}h{s % 3600 // 60:02d}m"
    return f"{s // 86400}d{s % 86400 // 3600:02d}h"


def speed_cells(
    objects_per_s: float, bytes_per_s: float | None, exact: bool
) -> list[str]:
    """Return the [OBJ/S, MiB/S] cells; '~' marks a counter-based rate."""
    mark = "" if exact else "~"
    obj = f"{objects_per_s:.1f}" if objects_per_s < 10 else f"{objects_per_s:.0f}"
    return [
        mark + obj,
        "?" if bytes_per_s is None else f"{mark}{bytes_per_s / MIB:.1f}",
    ]


def rate_cells(rate: Rate | None) -> list[str]:
    """Return the [OBJ/S, MiB/S, ETA] cells; '~' marks a counter-based rate."""
    if rate is None:
        return [NOT_APPLICABLE] * 3
    eta = rate.eta_s
    return [
        *speed_cells(rate.objects_per_s, rate.bytes_per_s, rate.exact),
        NOT_APPLICABLE
        if eta is None
        else ("" if rate.exact else "~") + format_duration(eta),
    ]


def flow_cells(flow: Flow) -> list[str]:
    """Return the [COPIES, OBJ/S, MiB/S] cells; the rates are '-' if none is measured."""
    if not flow.measured:
        return [str(flow.copies), NOT_APPLICABLE, NOT_APPLICABLE]
    return [
        str(flow.copies),
        *speed_cells(flow.objects_per_s, flow.bytes_per_s, flow.exact),
    ]


def print_flow_tables(result: "RatesResult") -> None:
    """Print the per-OSD and per-host tables, each after a blank line."""
    print()
    print_table(
        OSD_COLUMNS,
        [
            [
                *osd_cells(result.osd_df, result.osd_host, f.key),
                *flow_cells(f.flow),
            ]
            for f in result.osd_flows
        ],
    )
    print()
    print_table(
        HOST_COLUMNS,
        [[f.key, *flow_cells(f.flow)] for f in result.host_flows],
    )


def format_row(
    row: RateRow, osd_df: dict[int, dict], osd_host: dict[int, str]
) -> list[str]:
    """Return one table row's cells (COLUMNS)."""
    move = row.move
    return [
        move.pgid,
        str(move.shard),
        *osd_cells(osd_df, osd_host, move.up_osd),
        move.move_type,
        format_progress(move.progress_pct, move.progress_exact),
        *rate_cells(row.rate),
        abbreviate_state(move.state),
    ]


def print_totals(rows: list[RateRow]) -> None:
    """Sum up the measured rates on stderr, with the interval they span."""
    rates = [r.rate for r in rows if r.rate is not None]
    if not rates:
        return
    objects = sum(r.objects_per_s for r in rates)
    size = sum(r.bytes_per_s for r in rates if r.bytes_per_s is not None)
    lo, hi = min(r.seconds for r in rates), max(r.seconds for r in rates)
    span = f"{lo:.1f} s" if round(lo, 1) == round(hi, 1) else f"{lo:.1f}-{hi:.1f} s"
    of = (
        f" of the {len(rates)} copy movement(s) with one"
        if len(rates) < len(rows)
        else ""
    )
    stderr_para(
        f"Total RATE{of}: {objects:.0f} objects/s, {format_bytes(round(size))}/s, "
        f"measured over {span}."
    )


def print_no_rates(rows: list[RateRow], gone: int) -> None:
    """Say on stderr which rows have no RATE and why, and how many rows are gone."""
    counts = {
        reason: sum(r.no_rate == reason for r in rows) for reason in NO_RATE_REASONS
    }
    clauses = [f"{n} {NO_RATE_REASONS[reason]}" for reason, n in counts.items() if n]
    if clauses:
        stderr_para(f"RATE is '-' for copy movements that: {'; '.join(clauses)}.")
    if gone:
        stderr_para(
            f"{gone} copy movement(s) of the first sample finished or were "
            "re-targeted during the interval; not shown, nor counted in the total."
        )


def render(result: RatesResult) -> None:
    """Print the rows as a table, then the OSD and host tables, the totals and
    the footnotes that apply."""
    rows = result.rows
    if result.pgs_filter is not None:
        print_pgid_filter("--pgs", result.pgs_filter, "have movement", "not moving")
    if not rows:
        print_no_movements(result.filter_options)
    else:
        print_table(
            COLUMNS, [format_row(r, result.osd_df, result.osd_host) for r in rows]
        )
        print_flow_tables(result)
        print_movement_summary(len(rows), len({r.move.pgid for r in rows}))
        print_totals(rows)
    print_no_rates(rows, result.gone)
    print_progress_note((r.move.progress_pct, r.move.progress_exact) for r in rows)
    if any(r.rate is not None and not r.rate.exact for r in rows):
        stderr_para(f"NOTE: {RATE_APPROX_NOTE}")
    print_query_failed(
        result.query_failed,
        result.queried,
        "their progress comes from Ceph's counters in that sample",
    )


def run(args: argparse.Namespace) -> None:
    if args.load_state and args.save_state:
        sys.exit("ERROR: --save-state and --load-state are exclusive.")
    save_dir = resolve_save_dir(args.save_state) if args.save_state else None
    store = SnapshotStore.from_args(
        args, SNAPSHOT_COMMANDS, save_dir=save_dir, anonymize=anonymize_static
    )
    if store.load_dir is not None:
        sampler = ReplaySampler(store)
    else:
        sampler = LiveSampler(store, args.interval)
    result = plan(args, sampler)
    render(result)
    sampler.save()
    if args.save_state:
        stderr_para(
            f"Saved the samples to {args.save_state}; replay them with "
            f"'measure-rate --load-state {args.save_state}'."
        )
