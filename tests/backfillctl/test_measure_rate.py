"""Unit tests for backfillctl's measure-rate subcommand.

Rows, filters and progress are show-backfill's (test_show_backfill.py). Here:
the rate arithmetic (copy_rate) and when there is no rate; matching rows
across samples (rate_rows); the live sampler's order and timing, on a fake
clock, so no test sleeps; saving a capture and replaying it; and the table
and notes render() prints. FixtureReplayTest measures a real capture's rows
against a second sample made from it.
"""

import contextlib
import copy
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from _support import TEST_DATA, flat, parse_args, run_command, shared

from backfillctl import measure_rate as mr
from backfillctl import show_backfill as sb

MIB = 1024 * 1024


def position_at(pgid: str, pg_num: int, fraction: float) -> str:
    """The backfill position (normalized last_backfill) fraction of the way through a PG.

    The inverse of shared.backfill_fraction.
    """
    seed = int(pgid.split(".")[1], 16)
    bits = shared.pg_hash_bits(seed, pg_num)
    prefix = int(f"{seed & ((1 << bits) - 1):0{bits}b}"[::-1], 2) if bits else 0
    span = 1 << (32 - bits)
    return f"{(prefix << (32 - bits)) | int(fraction * span):08x}"


class PositionAtTest(unittest.TestCase):
    def test_inverts_backfill_fraction(self):
        for pgid, pg_num in (("27.500", 4096), ("27.1", 16), ("5.1f", 32)):
            for fraction in (0.0, 0.25, 0.5):
                with self.subTest(pgid=pgid, fraction=fraction):
                    position = position_at(pgid, pg_num, fraction)
                    seed = int(pgid.split(".")[1], 16)
                    self.assertEqual(
                        fraction, shared.backfill_fraction(position, seed, pg_num)
                    )


def pg(pgid, up, acting, state="active+remapped+backfilling", **stat):
    return {
        "pgid": pgid,
        "up": up,
        "acting": acting,
        "state": state,
        "acting_primary": acting[0],
        "stat_sum": {"num_objects": 1000, "num_bytes": 2000 * MIB, **stat},
    }


# Hosts h1 (osd.0, osd.1), h2 (2, 3), h3 (4, 5). Pool 27 is EC 2+2, pool 5
# replicated.
SNAPSHOTS = {
    "osd_tree": {
        "nodes": [
            *(
                {"id": -h, "type": "host", "name": f"h{h}", "children": [o, o + 1]}
                for h, o in ((1, 0), (2, 2), (3, 4))
            ),
            *({"id": o, "type": "osd"} for o in range(6)),
        ]
    },
    "osd_df": {"nodes": [{"id": o, "utilization": 50.0} for o in range(6)]},
    "osd_dump": {"erasure_code_profiles": {"p": {"k": "2", "m": "2"}}},
    "pool_ls_detail": [
        {"pool_id": 5, "type": 1, "size": 3, "pg_num": 32},
        {
            "pool_id": 27,
            "type": 3,
            "size": 4,
            "pg_num": 16,
            "erasure_code_profile": "p",
        },
    ],
}

PGS = [
    # EC shard 1: 3 -> 2. A shard is 1000 MiB (k=2).
    pg("27.1", [0, 2, 4, 1], [0, 3, 4, 1]),
    # Replicated: 2 -> 3, 2000 MiB a copy.
    pg("5.3", [0, 3, 4], [0, 2, 4]),
    # Replicated: 4 -> 5, waiting.
    pg("5.5", [1, 5, 2], [1, 4, 2], "active+remapped+backfill_wait"),
]


def positions(ec: float, rep: float) -> dict:
    """Backfill positions: 27.1's target at fraction ec, 5.3's at rep, 5.5's at MIN."""
    return {
        "27.1": {"2(1)": position_at("27.1", 16, ec)},
        "5.3": {"3": position_at("5.3", 32, rep)},
        "5.5": {"5": "MIN"},
    }


class FakeClock:
    """time.monotonic stand-in: advanced only by sleep(), which it logs."""

    def __init__(self, now=1000.0):
        self.now = now
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class Cluster:
    """A cluster whose PG dump and positions change between samples.

    samples lists (pg_stats, positions, {pgid: query delay}) per sample: a
    PG's positions are read that many seconds after the sample's dump.
    """

    def __init__(self, samples, snapshots=SNAPSHOTS):
        self.samples = samples
        self.snapshots = snapshots
        self.clock = FakeClock()
        self.events: list[str] = []
        self.queried: list[set[str]] = []
        self.failed: set[str] = set()

    def ceph_json(self, cmd):
        assert cmd == mr.SAMPLE_COMMANDS["pg_dump_pgs"], cmd
        self.events.append("dump")
        return self.samples[len(self.queried)][0]

    def query(self, pgids):
        _, pos, delays = self.samples[len(self.queried)]
        self.events.append("query")
        self.queried.append(set(pgids))
        ok = [p for p in pgids if p in pos and p not in self.failed]
        return shared.TimedPositions(
            {p: pos[p] for p in ok},
            {p: self.clock.now + delays.get(p, 0.0) for p in ok},
            sorted(set(pgids) - set(ok)),
        )

    def sleep(self, seconds):
        self.events.append("sleep")
        self.clock.sleep(seconds)

    def plan(self, *argv, save_dir=None):
        """Run plan() with argv on a live sampler; keep it as self.last_sampler."""
        args = parse_args(mr, list(argv))
        store = shared.SnapshotStore(
            mr.SNAPSHOT_COMMANDS, save_dir=save_dir, anonymize=mr.anonymize_static
        )
        store._cache.update(self.snapshots)
        self.last_sampler = mr.LiveSampler(
            store, args.interval, clock=self.clock, sleep=self.sleep, query=self.query
        )
        with (
            mock.patch.object(shared, "ceph_json", self.ceph_json),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            return mr.plan(args, self.last_sampler)


def two_samples(first=None, second=None):
    """27.1's shard goes 25% -> 62.5% (read 40 s apart), 5.3 stalls, 5.5 waits."""
    first = first or positions(0.25, 0.5)
    second = second or positions(0.625, 0.5)
    return Cluster(
        [
            (PGS, first, {"27.1": 1.0, "5.3": 5.0}),
            (PGS, second, {"27.1": 11.0, "5.3": 11.0}),
        ]
    )


def human_hosts(first=None, second=None):
    """two_samples, with hosts h1, h2, h3 renamed n1, n10, n9."""
    cluster = two_samples(first, second)
    tree = copy.deepcopy(SNAPSHOTS["osd_tree"])
    for node in tree["nodes"]:
        if node["type"] == "host":
            node["name"] = {"h1": "n1", "h2": "n10", "h3": "n9"}[node["name"]]
    cluster.snapshots = {**SNAPSHOTS, "osd_tree": tree}
    return cluster


def by_pgid(result) -> dict[str, mr.RateRow]:
    return {r.move.pgid: r for r in result.rows}


PG_OF_400 = {"stat_sum": {"num_objects": 400}}


class CopyRateTest(unittest.TestCase):
    @staticmethod
    def row(pct, exact=True):
        return sb.MovementRow(
            "1.0", 0, 1, 2, "backfill", "s", progress_pct=pct, progress_exact=exact
        )

    def test_objects_bytes_and_eta_follow_progress(self):
        rate, why = mr.copy_rate(
            self.row(20.0), self.row(50.0), 60.0, PG_OF_400, 1000 * MIB
        )
        self.assertIsNone(why)
        self.assertAlmostEqual(0.5, rate.pct_per_s)
        self.assertAlmostEqual(2.0, rate.objects_per_s)  # 0.5% of 400
        self.assertAlmostEqual(5 * MIB, rate.bytes_per_s)  # 0.5% of 1000 MiB
        self.assertAlmostEqual(100.0, rate.eta_s)  # 50% left at 0.5%/s
        self.assertEqual((True, 60.0), (rate.exact, rate.seconds))

    def test_counter_rates_are_not_exact(self):
        rate, _ = mr.copy_rate(
            self.row(20.0, False), self.row(50.0, False), 60.0, PG_OF_400, 1
        )
        self.assertFalse(rate.exact)

    def test_stalled_copy_has_a_zero_rate_and_no_eta(self):
        rate, _ = mr.copy_rate(self.row(20.0), self.row(20.0), 60.0, PG_OF_400, 1)
        self.assertEqual((0.0, None), (rate.objects_per_s, rate.eta_s))

    def test_finished_copy_is_due_now_even_if_stalled(self):
        rate, _ = mr.copy_rate(self.row(100.0), self.row(100.0), 60.0, PG_OF_400, 1)
        self.assertEqual(0.0, rate.eta_s)

    def test_unknown_shard_size_leaves_bytes_unknown(self):
        rate, _ = mr.copy_rate(self.row(20.0), self.row(50.0), 60.0, PG_OF_400, None)
        self.assertIsNone(rate.bytes_per_s)
        self.assertAlmostEqual(2.0, rate.objects_per_s)

    def test_no_rate_and_why(self):
        cases = {
            "new": (None, self.row(50.0), 60.0),
            "mixed": (self.row(20.0, False), self.row(50.0), 60.0),
            "backwards": (self.row(50.0), self.row(20.0), 60.0),
            "unknown progress": (self.row(None), self.row(50.0), 60.0),
            "unknown time": (self.row(20.0), self.row(50.0), None),
        }
        expected = {
            "new": mr.NEW,
            "mixed": mr.MIXED,
            "backwards": mr.BACKWARDS,
            "unknown progress": mr.UNKNOWN,
            "unknown time": mr.UNKNOWN,
        }
        for name, (before, after, seconds) in cases.items():
            with self.subTest(name):
                self.assertEqual(
                    (None, expected[name]),
                    mr.copy_rate(before, after, seconds, PG_OF_400, 1),
                )


class RateRowsTest(unittest.TestCase):
    @staticmethod
    def sample(dump_time=0.0, query_times=None):
        return mr.Sample(
            PGS, {}, dump_time, query_times or {}, frozenset(), frozenset()
        )

    @staticmethod
    def row(pgid, up, pct, exact=True, shard=0):
        return sb.MovementRow(
            pgid, shard, 9, up, "backfill", "s", progress_pct=pct, progress_exact=exact
        )

    def rates(self, first_rows, second_rows, first=None, second=None):
        return mr.rate_rows(
            first or self.sample(0.0, {"27.1": 3.0}),
            first_rows,
            second or self.sample(30.0, {"27.1": 43.0}),
            second_rows,
            {27: SNAPSHOTS["pool_ls_detail"][1]},
            SNAPSHOTS["osd_dump"]["erasure_code_profiles"],
        )

    def test_exact_progress_is_timed_by_its_pgs_query(self):
        (row,), gone = self.rates(
            [self.row("27.1", 2, 10.0)], [self.row("27.1", 2, 50.0)]
        )
        self.assertEqual(0, gone)
        self.assertEqual(40.0, row.rate.seconds)  # 43 - 3, not the dumps' 30

    def test_counter_progress_is_timed_by_the_dump(self):
        (row,), _ = self.rates(
            [self.row("27.1", 2, 10.0, False)], [self.row("27.1", 2, 50.0, False)]
        )
        self.assertEqual(30.0, row.rate.seconds)

    def test_shard_bytes_are_the_pgs_over_k(self):
        (row,), _ = self.rates([self.row("27.1", 2, 10.0)], [self.row("27.1", 2, 50.0)])
        # 1% of 1000 MiB a second: 40% in 40 s.
        self.assertAlmostEqual(10 * MIB, row.rate.bytes_per_s)

    def test_a_retargeted_copy_is_new_and_its_old_target_gone(self):
        (row,), gone = self.rates(
            [self.row("27.1", 2, 10.0)], [self.row("27.1", 5, 0.0)]
        )
        self.assertEqual((None, mr.NEW, 1), (row.rate, row.no_rate, gone))

    def test_shards_of_one_pg_are_matched_by_shard(self):
        rows, gone = self.rates(
            [self.row("27.1", 2, 10.0, shard=1), self.row("27.1", 2, 30.0, shard=3)],
            [self.row("27.1", 2, 50.0, shard=3)],
        )
        self.assertEqual((1, 0.5), (gone, rows[0].rate.pct_per_s))  # (50-30)/40

    def test_exact_progress_without_its_query_time_has_no_rate(self):
        (row,), _ = self.rates(
            [self.row("27.1", 2, 10.0)],
            [self.row("27.1", 2, 50.0)],
            second=self.sample(30.0, {}),
        )
        self.assertEqual((None, mr.UNKNOWN), (row.rate, row.no_rate))


class LivePlanTest(unittest.TestCase):
    def test_samples_then_sleeps_the_interval_then_samples_again(self):
        cluster = two_samples()
        cluster.plan("--interval", "12.5")
        self.assertEqual(["dump", "query", "sleep", "dump", "query"], cluster.events)
        self.assertEqual([12.5], cluster.clock.slept)

    def test_rates_are_per_copy_and_timed_per_pg(self):
        rows = by_pgid(two_samples().plan())
        ec = rows["27.1"].rate
        # 37.5% in 40 s (queried at +1 s, then after 30 s of sleep at +11 s).
        self.assertEqual(40.0, ec.seconds)
        self.assertAlmostEqual(9.375, ec.objects_per_s)  # 0.9375% of 1000
        self.assertAlmostEqual(9.375 * MIB, ec.bytes_per_s)  # of 1000 MiB
        self.assertAlmostEqual(40.0, ec.eta_s)  # 37.5% left
        self.assertTrue(ec.exact)
        stalled = rows["5.3"].rate
        self.assertEqual(
            (36.0, 0.0, None), (stalled.seconds, stalled.pct_per_s, stalled.eta_s)
        )

    def test_only_the_filtered_pgs_are_queried(self):
        cluster = two_samples()
        cluster.plan("--hosts", "h2")  # osd.2 or osd.3; 5.5 moves 4 -> 5
        self.assertEqual([{"27.1", "5.3"}] * 2, cluster.queried)
        cluster = two_samples()
        result = cluster.plan("--hosts", "h2", "--pgs", "5.3", "5.5")
        self.assertEqual([{"5.3"}] * 2, cluster.queried)
        self.assertEqual(["5.3"], [r.move.pgid for r in result.rows])
        self.assertEqual(("--pgs", "--hosts"), result.filter_options)

    def test_osds_and_hosts_match_only_the_destination(self):
        # 5.5 moves 4 -> 5; osd.4 is only ever a source.
        self.assertEqual([], two_samples().plan("--osds", "4").rows)
        for argv in (["--osds", "5"], ["--hosts", "h3"]):
            with self.subTest(argv=argv):
                rows = two_samples().plan(*argv).rows
                self.assertEqual(["5.5"], [r.move.pgid for r in rows])

    def test_copies_without_a_destination_are_dropped(self):
        # 5.2: replica 1 moves to 3, replica 2 is dropped outright.
        dump = [pg("5.2", [0, 3], [0, 1, 2])]
        rows = Cluster([(dump, {}, {}), (dump, {}, {})]).plan().rows
        self.assertEqual([(1, 3)], [(r.move.acting_osd, r.move.up_osd) for r in rows])

    def test_nothing_to_measure_skips_the_second_sample(self):
        cluster = two_samples()
        result = cluster.plan("--pgs", "9.9")
        self.assertEqual(["dump", "query"], cluster.events)
        self.assertEqual([], result.rows)
        self.assertEqual(["9.9"], result.pgs_filter.unmatched)

    def test_counters_where_a_query_fails_and_failures_reported_once(self):
        cluster = two_samples()
        cluster.failed = {"5.3"}
        result = cluster.plan()
        row = by_pgid(result)["5.3"]
        self.assertFalse(row.move.progress_exact)
        self.assertEqual((["5.3"], 3), (result.query_failed, result.queried))
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            mr.render(result)
        self.assertEqual(1, err.getvalue().count("'ceph pg query' failed"))
        self.assertIn("failed for 1 of 3 PG(s) (5.3)", flat(err.getvalue()))

    def test_sort_by_eta_soonest_first_and_none_last(self):
        cluster = two_samples(second=positions(0.625, 0.875))
        rows = cluster.plan("--sort-by", "eta").rows
        # 5.3: 12 s (12.5% left at 37.5%/36 s), 27.1: 40 s; 5.5 isn't moving.
        self.assertEqual(["5.3", "27.1", "5.5"], [r.move.pgid for r in rows])

    def test_sorted_by_up_host_by_default(self):
        rows = two_samples().plan().rows
        # 27.1 -> osd.2 and 5.3 -> osd.3 on h2, 5.5 -> osd.5 on h3; not PG order.
        self.assertEqual(["27.1", "5.3", "5.5"], [r.move.pgid for r in rows])

    def test_sort_by_rates_and_progress_highest_first(self):
        # 5.3: 10.4 obj/s, 20.8 MiB/s, 87.5%; 27.1: 9.4, 9.4, 62.5%; 5.5: 0, 0, 0%.
        for sort_by in ("obj/s", "mib/s", "progress"):
            with self.subTest(sort_by=sort_by):
                cluster = two_samples(second=positions(0.625, 0.875))
                rows = cluster.plan("--sort-by", sort_by).rows
                self.assertEqual(["5.3", "27.1", "5.5"], [r.move.pgid for r in rows])

    def test_all_tables_sorted_by_host_in_human_order_by_default(self):
        # h2 -> n10 (osd.2, osd.3; 27.1, 5.3), h3 -> n9 (osd.5; 5.5).
        result = human_hosts().plan()
        self.assertEqual(["5.5", "27.1", "5.3"], [r.move.pgid for r in result.rows])
        self.assertEqual([5, 2, 3], [f.key for f in result.osd_flows])
        self.assertEqual(["n9", "n10"], [f.key for f in result.host_flows])

    def test_rate_sorts_resort_the_osd_and_host_tables(self):
        # osd.3 (5.3): 10.4 obj/s, 20.8 MiB/s; osd.2 (27.1): 9.4, 9.4; osd.5: 0.
        for sort_by in ("obj/s", "mib/s"):
            with self.subTest(sort_by=sort_by):
                result = human_hosts(second=positions(0.625, 0.875)).plan(
                    "--sort-by", sort_by
                )
                self.assertEqual([3, 2, 5], [f.key for f in result.osd_flows])
                self.assertEqual(["n10", "n9"], [f.key for f in result.host_flows])

    def test_progress_and_eta_leave_the_osd_and_host_tables_by_host(self):
        for sort_by in ("progress", "eta"):
            with self.subTest(sort_by=sort_by):
                result = human_hosts(second=positions(0.625, 0.875)).plan(
                    "--sort-by", sort_by
                )
                self.assertEqual(
                    ["5.3", "27.1", "5.5"], [r.move.pgid for r in result.rows]
                )
                self.assertEqual([5, 2, 3], [f.key for f in result.osd_flows])
                self.assertEqual(["n9", "n10"], [f.key for f in result.host_flows])

    def test_the_old_sort_choices_are_gone(self):
        for sort_by in ("pgid", "up-osd"):
            with (
                self.subTest(sort_by=sort_by),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parse_args(mr, ["--sort-by", sort_by])


SORT_OSD_HOST = {1: "b10", 2: "b2", 3: "b2"}  # osd.9's host is unknown


class SortKeyTest(unittest.TestCase):
    @staticmethod
    def row(pgid, shard, up, progress=None, rate=None):
        move = sb.MovementRow(pgid, shard, 0, up, "backfill", "s", False, progress)
        return mr.RateRow(move, rate, None if rate else mr.UNKNOWN)

    @staticmethod
    def rate(objects, size=MIB, eta=None):
        return mr.Rate(1.0, objects, size, eta, True, 30.0)

    def order(self, sort_by, rows):
        key = mr.sort_key(sort_by, SORT_OSD_HOST)
        return [rows.index(r) for r in sorted(rows, key=key)]

    def test_host_then_osd_then_pg_then_shard(self):
        rows = [
            self.row("27.1", 1, 1),  # host b10, after b2
            self.row("27.1", 1, 3),  # host b2, osd.3
            self.row("27.10", 0, 2),  # host b2, osd.2: 27.10 after 27.2 (hex)
            self.row("27.2", 3, 2),
            self.row("27.2", 1, 2),
            self.row("5.1", "-", 2),  # pool 5 before pool 27
            self.row("1.0", "-", 9),  # host '?' sorts as its cell does, first
        ]
        self.assertEqual([6, 5, 4, 3, 2, 1, 0], self.order("host", rows))

    def test_rates_highest_first_unknown_last_ties_in_host_order(self):
        rows = [
            self.row("1.0", "-", 1, rate=self.rate(5, size=None)),
            self.row("1.1", "-", 1),  # no rate
            self.row("1.2", "-", 1, rate=self.rate(9, size=2 * MIB)),
            self.row("1.3", "-", 2, rate=self.rate(5, size=MIB)),  # host b2
        ]
        self.assertEqual([2, 3, 0, 1], self.order("obj/s", rows))
        self.assertEqual([2, 3, 0, 1], self.order("mib/s", rows))  # 0: size unknown

    def test_progress_highest_first_unknown_last(self):
        rows = [
            self.row("1.0", "-", 1, progress=10.0),
            self.row("1.1", "-", 1),
            self.row("1.2", "-", 1, progress=100.0),
            self.row("1.3", "-", 1, progress=0.0),  # 0 is known
        ]
        self.assertEqual([2, 0, 3, 1], self.order("progress", rows))

    def test_eta_soonest_first_unknown_last(self):
        rows = [
            self.row("1.0", "-", 1, rate=self.rate(1, eta=60.0)),
            self.row("1.1", "-", 1, rate=self.rate(0)),  # not moving: no ETA
            self.row("1.2", "-", 1, rate=self.rate(1, eta=0.0)),  # done
            self.row("1.3", "-", 1),
        ]
        self.assertEqual([2, 0, 1, 3], self.order("eta", rows))

    def test_flows_fastest_first_unmeasured_last_ties_by_default(self):
        flows = [
            mr.FlowRow("a", mr.Flow(1, 0, 0.0, 0.0, True)),  # nothing measured
            mr.FlowRow("b", mr.Flow(2, 2, 5.0, None, True)),  # a size unknown
            mr.FlowRow("c", mr.Flow(1, 1, 5.0, 3 * MIB, True)),
            mr.FlowRow("d", mr.Flow(1, 1, 9.0, MIB, True)),
        ]
        for sort_by, want in (
            ("obj/s", ["d", "b", "c", "a"]),
            ("mib/s", ["c", "d", "a", "b"]),
            ("progress", ["a", "b", "c", "d"]),  # no progress: the default
            ("host", ["a", "b", "c", "d"]),
        ):
            with self.subTest(sort_by=sort_by):
                key = mr.flow_sort_key(sort_by, lambda f: f.key)
                self.assertEqual(want, [f.key for f in sorted(flows, key=key)])


class AggregateTest(unittest.TestCase):
    @staticmethod
    def row(acting, up, rate=None):
        move = sb.MovementRow("1.0", 0, acting, up, "backfill", "s")
        return mr.RateRow(move, rate, None if rate else mr.UNKNOWN)

    @staticmethod
    def rate(objects, size=MIB, exact=True):
        return mr.Rate(1.0, objects, size, None, exact, 30.0)

    def test_sums_per_up_osd_only(self):
        rows = [
            self.row(1, 2, self.rate(10)),
            self.row(3, 2, self.rate(4)),
            self.row(2, 3, self.rate(1)),
        ]
        flows = {f.key: f.flow for f in mr.aggregate(rows, lambda osd: osd)}
        self.assertEqual([2, 3], list(flows))  # no ACTING OSD of its own
        self.assertEqual(mr.Flow(2, 2, 14.0, 2 * MIB, True), flows[2])

    def test_per_host_of_the_up_osd(self):
        rows = [self.row(1, 2, self.rate(10)), self.row(9, 3, self.rate(4))]
        (host,) = mr.aggregate(rows, lambda osd: "h")
        self.assertEqual(
            ("h", 2, 14.0), (host.key, host.flow.copies, host.flow.objects_per_s)
        )

    def test_unmeasured_rows_count_as_copies_only(self):
        (row,) = mr.aggregate(
            [self.row(1, 2, self.rate(10)), self.row(1, 2)], lambda osd: osd
        )
        self.assertEqual(mr.Flow(2, 1, 10.0, MIB, True), row.flow)

    def test_an_unknown_size_or_counter_rate_taints_the_sum(self):
        rows = [
            self.row(1, 2, self.rate(10, size=None)),
            self.row(1, 2, self.rate(1, exact=False)),
            self.row(1, 2, self.rate(1)),
        ]
        (row,) = mr.aggregate(rows, lambda osd: osd)
        self.assertEqual(mr.Flow(3, 3, 12.0, None, False), row.flow)

    def test_plan_gives_per_osd_and_per_host_flows(self):
        result = two_samples().plan()
        self.assertEqual([2, 3, 5], [f.key for f in result.osd_flows])
        hosts = {f.key: f.flow for f in result.host_flows}
        self.assertEqual(["h2", "h3"], list(hosts))
        # h2 receives 27.1's shard (9.375 objects/s) and 5.3's (stalled).
        self.assertEqual((2, 2), (hosts["h2"].copies, hosts["h2"].measured))
        self.assertAlmostEqual(9.375, hosts["h2"].objects_per_s)


class SaveReplayTest(unittest.TestCase):
    def test_replay_measures_what_the_live_run_did(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture = Path(tmp, "capture")
            capture.mkdir()
            cluster = two_samples()
            live = cluster.plan(save_dir=capture)
            cluster.last_sampler.save()
            self.assertEqual(
                {"1", "2", mr.TIMES_FILE, *(f"{k}.json" for k in mr.SNAPSHOT_COMMANDS)},
                {p.name for p in capture.iterdir()},
            )
            times = json.loads((capture / mr.TIMES_FILE).read_text())["samples"]
            self.assertEqual(
                {"dump": 30.0, "queries": {"27.1": 41.0, "5.3": 41.0, "5.5": 30.0}},
                times[1],
            )
            args = parse_args(mr, [], load_state=str(capture))
            with mock.patch.object(shared, "ceph_json", side_effect=AssertionError):
                store = shared.SnapshotStore.from_args(args, mr.SNAPSHOT_COMMANDS)
                replayed = mr.plan(args, mr.ReplaySampler(store))
        self.assertEqual(live.rows, replayed.rows)

    def test_a_sample_is_trimmed_like_a_save_state_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture = Path(tmp)
            dump = [
                {**PGS[0], "stat_sum": {**PGS[0]["stat_sum"], "num_object_clones": 3}}
            ]
            cluster = Cluster([(dump, {}, {}), (dump, {}, {})])
            cluster.plan(save_dir=capture)
            cluster.last_sampler.save()
            saved = json.loads((capture / "1" / "pg_dump_pgs.json").read_text())
        self.assertNotIn("num_object_clones", saved["pg_stats"][0]["stat_sum"])

    def test_a_save_state_capture_is_refused(self):
        result = run_command(
            mr, load_state=TEST_DATA / "ceph1-resumed-backfills-exact-progress"
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("is not a measure-rate capture", flat(result.stderr))
        self.assertIn("measure-rate --save-state DIR", flat(result.stderr))

    def test_save_and_load_state_are_exclusive_in_the_usage(self):
        with (
            contextlib.redirect_stderr(io.StringIO()) as err,
            self.assertRaises(SystemExit),
        ):
            parse_args(mr, ["--save-state", "x", "--load-state", "y"])
        self.assertIn("not allowed with argument", err.getvalue())

    def test_save_and_load_state_are_exclusive(self):
        # The global --load-state, before the subcommand, gets past argparse.
        with tempfile.TemporaryDirectory() as tmp:
            result = run_command(mr, "--save-state", Path(tmp, "x"), load_state=tmp)
            self.assertFalse(Path(tmp, "x").exists())
        self.assertEqual(1, result.returncode)
        self.assertIn("--save-state and --load-state are exclusive", result.stderr)

    def test_interval_must_be_positive(self):
        for bad in ("0", "-1", "nan", "inf", "1e400", "x"):
            with (
                self.subTest(bad),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parse_args(mr, ["--interval", bad])


class RenderTest(unittest.TestCase):
    def render(self, result):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            mr.render(result)
        return out.getvalue(), flat(err.getvalue())

    def test_rate_columns_follow_progress_and_acting_is_gone(self):
        out, _ = self.render(two_samples().plan())
        group_line, label_line, *lines = out.split("\n\n")[0].splitlines()
        self.assertRegex(group_line, r"^\s+-+ UP -+\s+-+ RATE -+$")
        self.assertRegex(
            label_line,
            r"^PGID\s+SHARD\s+OSD\s+UTIL\s+HOST\s+TYPE\s+PROGRESS\s+OBJ/S\s+MiB/S"
            r"\s+ETA\s+STATE$",
        )
        ec = next(ln for ln in lines if ln.startswith("27.1 "))
        self.assertRegex(
            ec,
            r"^27\.1\s+1\s+2\s+50\.0%\s+h2\s+backfill\s+62%\s+9\.4\s+9\.4\s+40s\s+act\+",
        )
        waiting = next(ln for ln in lines if ln.startswith("5.5 "))
        self.assertRegex(waiting, r"\s0%\s+0\.0\s+0\.0\s+-\s+act\+")

    def test_a_rebuilding_primary_is_not_mentioned(self):
        dump = [pg("5.7", [0, 1, 2], [0, 1], "active+undersized+degraded+recovering")]
        out, err = self.render(Cluster([(dump, {}, {}), (dump, {}, {})]).plan())
        self.assertRegex(out, r"\n5\.7\s+-\s+2\s")  # its row: 0* -> 2
        self.assertNotIn("*", out)
        self.assertNotIn("* marks", err)

    def test_osd_and_host_tables_follow_the_main_one(self):
        out, _ = self.render(two_samples().plan())
        _, osds, hosts = (part.splitlines() for part in out.split("\n\n"))
        self.assertRegex(osds[0], r"^OSD\s+UTIL\s+HOST\s+COPIES\s+OBJ/S\s+MiB/S$")
        self.assertEqual(["2", "3", "5"], [ln.split()[0] for ln in osds[1:]])
        self.assertRegex(osds[1], r"^2\s+50\.0%\s+h2\s+1\s+9\.4\s+9\.4$")
        self.assertRegex(osds[2], r"^3\s+50\.0%\s+h2\s+1\s+0\.0\s+0\.0$")
        self.assertRegex(hosts[0], r"^HOST\s+COPIES\s+OBJ/S\s+MiB/S$")
        self.assertRegex(hosts[1], r"^h2\s+2\s+9\.4\s+9\.4$")
        self.assertRegex(hosts[2], r"^h3\s+1\s+0\.0\s+0\.0$")

    def test_counter_sums_are_marked(self):
        dumps = [
            [pg("27.1", [0, 2, 4, 1], [0, 3, 4, 1], num_objects_misplaced=n)]
            for n in (800, 500)
        ]
        out, _ = self.render(Cluster([(dumps[0], {}, {}), (dumps[1], {}, {})]).plan())
        hosts = out.split("\n\n")[2].splitlines()
        self.assertRegex(hosts[-1], r"^h2\s+1\s+~10\s+~10\.0$")

    def test_totals_and_interval_on_stderr(self):
        out, err = self.render(two_samples().plan())
        self.assertIn("3 copy movement(s) across 3 PG(s).", err)
        self.assertIn(
            "Total RATE: 9 objects/s, 9.4 MiB/s, measured over 30.0-40.0 s.",
            err,
        )
        self.assertNotIn("Total", out)

    def test_totals_say_how_many_rows_they_cover_if_not_all(self):
        second = positions(0.625, 0.5)
        del second["5.5"]  # by the counters now: mixed, no rate
        _, err = self.render(two_samples(second=second).plan())
        self.assertIn("Total RATE of the 2 copy movement(s) with one: 9 objects/s", err)

    def test_counter_rates_are_marked_and_explained(self):
        # No positions: 27.1's shard by the counters, 20% -> 50% in 30 s.
        dumps = [
            [pg("27.1", [0, 2, 4, 1], [0, 3, 4, 1], num_objects_misplaced=n)]
            for n in (800, 500)
        ]
        cluster = Cluster([(dumps[0], {}, {}), (dumps[1], {}, {})])
        out, err = self.render(cluster.plan())
        (line,) = [ln for ln in out.splitlines() if ln.startswith("27.1 ")]
        self.assertRegex(line, r"\s~50%\s+~10\s+~10\.0\s+~50s\s")
        self.assertIn("NOTE: ~ marks RATE and ETA from Ceph's counters", err)

    def test_rows_without_a_rate_are_counted_by_reason(self):
        # 27.1's target moves to osd.5 (new, and its old one gone); 5.3's
        # progress goes backwards; 5.5 loses its position (mixed).
        moved = pg("27.1", [0, 5, 4, 1], [0, 3, 4, 1])
        second = {"5.3": {"3": position_at("5.3", 32, 0.1)}}
        cluster = Cluster(
            [(PGS, positions(0.25, 0.5), {}), ([moved, *PGS[1:]], second, {})]
        )
        result = cluster.plan()
        self.assertEqual(1, result.gone)
        _, err = self.render(result)
        self.assertIn(
            "RATE is '-' for copy movements that: 1 started moving (or were "
            "re-targeted) during the interval; 1 had progress from a backfill "
            "position in one sample and the counters in the other; 1 went "
            "backwards (restarted, or counters reset).",
            err,
        )
        self.assertIn("1 copy movement(s) of the first sample finished", err)
        self.assertNotIn("Total RATE", err)  # no row has one

    def test_no_movements(self):
        _, err = self.render(two_samples().plan("--pgs", "9.9"))
        self.assertIn("No PG movements match --pgs.", err)

    def test_format_duration(self):
        for seconds, text in (
            (0, "0s"),
            (59.4, "59s"),
            (65, "1m05s"),
            (3 * 3600 + 5 * 60 + 9, "3h05m"),
            (2 * 86400 + 4 * 3600, "2d04h"),
        ):
            with self.subTest(seconds):
                self.assertEqual(text, mr.format_duration(seconds))

    def test_rate_cells(self):
        rate = mr.Rate(1.0, 123.4, None, 30.0, True, 30.0)
        self.assertEqual(["123", "?", "30s"], mr.rate_cells(rate))
        self.assertEqual(["-", "-", "-"], mr.rate_cells(None))


FIXTURE_RESUMED = TEST_DATA / "ceph1-resumed-backfills-exact-progress"


class FixtureReplayTest(unittest.TestCase):
    """The real capture as the first sample, and a second 30 s later in which
    27.500's targets (at 6.6%, see the fixture's README.txt) got 10% further."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        capture = Path(cls.tmp.name)
        for key in mr.SNAPSHOT_COMMANDS:
            shutil.copy(FIXTURE_RESUMED / f"{key}.json", capture)
        before = json.loads(
            (FIXTURE_RESUMED / shared.BACKFILL_POSITIONS_FILE).read_text()
        )
        pools = {
            p["pool_id"]: p
            for p in json.loads((capture / "pool_ls_detail.json").read_text())
        }
        pg_num = pools[27]["pg_num"]
        after = {
            **before,
            "27.500": {
                peer: position_at(
                    "27.500",
                    pg_num,
                    shared.backfill_fraction(p, 0x500, pg_num) + 0.1,
                )
                for peer, p in before["27.500"].items()
            },
        }
        times = []
        for name, positions_, t in (("1", before, 0.0), ("2", after, 30.0)):
            sample = capture / name
            sample.mkdir()
            shutil.copy(FIXTURE_RESUMED / "pg_dump_pgs.json", sample)
            (sample / shared.BACKFILL_POSITIONS_FILE).write_text(json.dumps(positions_))
            times.append({"dump": t, "queries": dict.fromkeys(before, t)})
        (capture / mr.TIMES_FILE).write_text(json.dumps({"samples": times}))
        cls.capture = capture

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_only_the_advanced_pg_moves(self):
        args = parse_args(mr, [], load_state=str(self.capture))
        store = shared.SnapshotStore.from_args(args, mr.SNAPSHOT_COMMANDS)
        result = mr.plan(args, mr.ReplaySampler(store))
        pgs = json.loads((FIXTURE_RESUMED / "pg_dump_pgs.json").read_text())["pg_stats"]
        objects = next(p for p in pgs if p["pgid"] == "27.500")["stat_sum"][
            "num_objects"
        ]
        for row in result.rows:
            self.assertTrue(row.rate.exact)
            if row.move.pgid == "27.500":
                self.assertAlmostEqual(10 / 30, row.rate.pct_per_s, 4)
                self.assertAlmostEqual(objects * 0.1 / 30, row.rate.objects_per_s, 0)
                self.assertAlmostEqual(
                    (100 - row.move.progress_pct) * 3, row.rate.eta_s, 0
                )
            else:
                self.assertEqual(0.0, row.rate.pct_per_s)
        self.assertEqual(0, result.gone)

    def test_every_osd_and_host_table_sums_to_the_total(self):
        args = parse_args(mr, [], load_state=str(self.capture))
        store = shared.SnapshotStore.from_args(args, mr.SNAPSHOT_COMMANDS)
        result = mr.plan(args, mr.ReplaySampler(store))
        total = sum(r.rate.objects_per_s for r in result.rows)
        self.assertGreater(total, 0)
        for flows in (result.osd_flows, result.host_flows):
            with self.subTest(keys=type(flows[0].key).__name__):
                self.assertAlmostEqual(
                    total, sum(f.flow.objects_per_s for f in flows), 6
                )
                self.assertEqual(len(result.rows), sum(f.flow.copies for f in flows))

    def test_replayed_through_the_command(self):
        result = run_command(mr, "--pgs", "27.500", load_state=self.capture, check=True)
        self.assertIn("27.500", result.stdout)
        self.assertIn("measured over 30.0 s", flat(result.stderr))
        self.assertNotIn("~", result.stdout)
        self.assertNotIn("Sampling again", result.stderr)


class DrainCaptureTest(unittest.TestCase):
    """Replay the real two-sample capture of a host drain (see its README.txt)."""

    FIXTURE = TEST_DATA / "ceph1-28-being-drained"

    @classmethod
    def setUpClass(cls):
        args = parse_args(mr, [], load_state=str(cls.FIXTURE))
        store = shared.SnapshotStore.from_args(args, mr.SNAPSHOT_COMMANDS)
        cls.result = mr.plan(args, mr.ReplaySampler(store))
        dump = json.loads((cls.FIXTURE / "2" / "pg_dump_pgs.json").read_text())
        cls.pgs = {pg["pgid"]: pg for pg in dump["pg_stats"]}

    def test_every_copy_has_an_exact_rate_over_at_least_the_interval(self):
        rows = self.result.rows
        self.assertEqual((674, 0), (len(rows), self.result.gone))
        self.assertTrue(all(r.rate is not None and r.rate.exact for r in rows))
        self.assertGreaterEqual(min(r.rate.seconds for r in rows), mr.DEFAULT_INTERVAL)

    def test_counted_as_ceph_does_the_rates_match_ceph_s(self):
        # 'ceph -s' at the time: "recovery: 2.5 GiB/s, 1.45k objects/s", which
        # counts an object once per PG, at its logical size (num_bytes).
        objects, size = {}, {}
        for r in self.result.rows:
            pg = self.pgs[r.move.pgid]
            objects[r.move.pgid] = max(
                objects.get(r.move.pgid, 0), r.rate.objects_per_s
            )
            logical = r.rate.pct_per_s / 100 * pg["stat_sum"]["num_bytes"]
            size[r.move.pgid] = max(size.get(r.move.pgid, 0), logical)
        self.assertAlmostEqual(1.45e3, sum(objects.values()), delta=0.05e3)
        self.assertAlmostEqual(2.5, sum(size.values()) / 2**30, delta=0.1)

    def test_osd_and_host_tables_sum_to_the_total(self):
        total = sum(r.rate.bytes_per_s for r in self.result.rows)
        self.assertAlmostEqual(544.6, total / MIB, 1)
        for flows in (self.result.osd_flows, self.result.host_flows):
            self.assertAlmostEqual(total, sum(f.flow.bytes_per_s for f in flows), 3)

    def test_nothing_arrives_at_the_drained_host(self):
        self.assertNotIn("host28", {f.key for f in self.result.host_flows})


if __name__ == "__main__":
    unittest.main()
