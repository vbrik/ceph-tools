"""Unit tests for shared.py, the code the backfillctl scripts have in common.

The risky parts: the progress denominator (num_objects_misplaced and
num_objects_degraded are counted in copy units, so a PG moving k copies starts
at k * num_objects; dividing by num_objects alone reads 0% until more than 1/k
of the work is done, which looks like a plausible "hasn't started" rather than
an obvious failure), the EC/replicated slot counting that feeds it, turning a
backfill position into a share of the PG (checked against a model of Ceph's
ceph_stable_mod, including for pg_num that is not a power of two), and the
snapshot layer whose saved output must be safe to share and loadable again.
"""

import argparse
import contextlib
import io
import json
import pathlib
import random
import sys
import tempfile
import unittest
from typing import ClassVar
from unittest import mock

from _support import FakeStore, real_query_backfill_positions, shared

NONE = shared.CRUSH_ITEM_NONE


def pg(num_objects: int, misplaced: int = 0, degraded: int = 0) -> dict:
    return {
        "stat_sum": {
            "num_objects": num_objects,
            "num_objects_misplaced": misplaced,
            "num_objects_degraded": degraded,
        }
    }


class OsdSlotTest(unittest.TestCase):
    def test_placeholders_are_not_real_osds(self):
        self.assertFalse(shared.is_real_osd(NONE))
        self.assertFalse(shared.is_real_osd(-1))
        self.assertFalse(shared.is_real_osd(None))
        self.assertTrue(shared.is_real_osd(0))  # osd.0 is a real OSD

    def test_slot_is_none_for_placeholder_or_missing_position(self):
        self.assertEqual(shared.slot([5, NONE], 0), 5)
        self.assertIsNone(shared.slot([5, NONE], 1))
        self.assertIsNone(shared.slot([5, -1], 1))
        self.assertIsNone(shared.slot([5], 3))

    def test_real_osd_set_drops_placeholders(self):
        self.assertEqual(shared.real_osd_set([3, NONE, 4, -1, 3]), {3, 4})
        self.assertEqual(shared.real_osd_set([]), set())


class PgidTest(unittest.TestCase):
    def test_pool_id(self):
        self.assertEqual(shared.pgid_pool_id("19.2a1"), 19)
        self.assertEqual(shared.pgid_pool_id("5.0"), 5)

    def test_sort_is_numeric_pool_then_hex_pg(self):
        pgids = ["19.a", "5.1f", "19.9", "5.2", "19.10"]
        self.assertEqual(
            sorted(pgids, key=shared.pgid_sort_key),
            ["5.2", "5.1f", "19.9", "19.a", "19.10"],
        )


class PoolTest(unittest.TestCase):
    def test_is_erasure(self):
        self.assertTrue(shared.is_erasure({"type": shared.POOL_TYPE_ERASURE}))
        self.assertFalse(shared.is_erasure({"type": 1}))
        self.assertFalse(shared.is_erasure({}))
        self.assertFalse(shared.is_erasure(None))  # a PG whose pool is unknown

    def test_failure_domain_is_the_first_choose_step(self):
        rule = {
            "steps": [
                {"op": "take", "item": -1},
                {"op": "choose_indep", "num": 0, "type": "host"},
                {"op": "chooseleaf_indep", "num": 1, "type": "osd"},
                {"op": "emit"},
            ]
        }
        self.assertEqual(shared.rule_failure_domain(rule), "host")

    def test_failure_domain_unknown(self):
        self.assertIsNone(shared.rule_failure_domain(None))
        self.assertIsNone(shared.rule_failure_domain({"steps": [{"op": "take"}]}))

    def test_shard_size_replica_is_whole_pg_and_ec_is_a_rounded_up_kth(self):
        stats = {"stat_sum": {"num_bytes": 1001}}
        ec = {"type": shared.POOL_TYPE_ERASURE, "erasure_code_profile": "p"}
        self.assertEqual(shared.shard_size_bytes(stats, {"type": 1}, {}), 1001)
        self.assertEqual(shared.shard_size_bytes(stats, ec, {"p": {"k": "4"}}), 251)

    def test_shard_size_unknown_profile_is_none(self):
        stats = {"stat_sum": {"num_bytes": 1001}}
        ec = {"type": shared.POOL_TYPE_ERASURE, "erasure_code_profile": "p"}
        self.assertIsNone(shared.shard_size_bytes(stats, ec, {}))
        self.assertIsNone(shared.shard_size_bytes(stats, ec, {"p": {}}))


class ProgressTest(unittest.TestCase):
    def test_single_copy(self):
        self.assertEqual(shared.pg_progress_pct(pg(100, misplaced=100), 1), 0.0)
        self.assertEqual(shared.pg_progress_pct(pg(100, misplaced=25), 1), 75.0)

    def test_multi_copy_not_stuck_at_zero(self):
        # Regression: 2 copies moving, one quarter done. Misplaced is 150 of
        # 200 copy-units; dividing by num_objects alone gave 1 - 1.5 -> 0%.
        self.assertEqual(shared.pg_progress_pct(pg(100, misplaced=150), 2), 25.0)

    def test_multi_copy_just_started(self):
        self.assertEqual(shared.pg_progress_pct(pg(100, misplaced=200), 2), 0.0)

    def test_done(self):
        self.assertEqual(shared.pg_progress_pct(pg(100), 3), 100.0)

    def test_degraded_and_misplaced_are_summed(self):
        # 3 copies: 300 copy-units total, 60 + 90 left.
        self.assertEqual(
            shared.pg_progress_pct(pg(100, misplaced=60, degraded=90), 3), 50.0
        )

    def test_overlap_clamps_to_zero(self):
        self.assertEqual(
            shared.pg_progress_pct(pg(100, misplaced=200, degraded=200), 2), 0.0
        )

    def test_no_objects_or_no_copies_is_unknown(self):
        self.assertIsNone(shared.pg_progress_pct(pg(0), 2))
        self.assertIsNone(shared.pg_progress_pct(pg(100, misplaced=5), 0))
        self.assertIsNone(shared.pg_progress_pct({}, 1))


def stable_mod(x: int, pg_num: int) -> int:
    """Model of Ceph's ceph_stable_mod(x, pg_num, pg_num_mask): object hash -> PG seed."""
    mask = (1 << (pg_num - 1).bit_length()) - 1
    return x & mask if (x & mask) < pg_num else x & (mask >> 1)


def sort_key(hash32: int) -> int:
    """The hobject sort key a backfill position prints: the hash bit-reversed."""
    return int(f"{hash32:032b}"[::-1], 2)


# Power-of-two and not, including the degenerate 1 and 2.
PG_NUMS = (1, 2, 3, 5, 8, 12, 13, 64, 100, 512, 1000)


class PgHashBitsTest(unittest.TestCase):
    def test_power_of_two_fixes_all_bits_for_every_pg(self):
        self.assertEqual({9}, {shared.pg_hash_bits(s, 512) for s in range(512)})

    def test_pg_without_a_sibling_fixes_one_bit_less(self):
        # pg_num 12: seeds 4-7 have no seed + 8 sibling (12-15 don't exist),
        # so each also takes those hashes and covers twice the key range.
        bits = [shared.pg_hash_bits(s, 12) for s in range(12)]
        self.assertEqual([4] * 4 + [3] * 4 + [4] * 4, bits)

    def test_single_pg_fixes_nothing(self):
        self.assertEqual(0, shared.pg_hash_bits(0, 1))

    def test_matches_ceph_stable_mod(self):
        # A PG gets exactly the hashes whose low pg_hash_bits bits are its seed's.
        for pg_num in PG_NUMS:
            n = (pg_num - 1).bit_length()
            for seed in range(pg_num):
                with self.subTest(pg_num=pg_num, seed=seed):
                    bits = shared.pg_hash_bits(seed, pg_num)
                    low = (1 << bits) - 1
                    self.assertEqual(
                        {h for h in range(1 << n) if stable_mod(h, pg_num) == seed},
                        {h for h in range(1 << n) if h & low == seed & low},
                    )


class BackfillPositionTest(unittest.TestCase):
    def test_normalize(self):
        norm = shared.normalize_last_backfill
        self.assertEqual("MIN", norm("MIN"))
        self.assertEqual("MAX", norm("MAX"))
        self.assertEqual("b87d166c", norm("18:b87d166c:::eceaf007.36421.651:head"))
        self.assertEqual("b87d166c", norm("18:B87D166C:::x:head"))
        self.assertIsNone(norm("18:zz:::x:head"))
        self.assertIsNone(norm("garbage"))
        self.assertIsNone(norm(""))

    def test_min_and_max(self):
        self.assertEqual(0.0, shared.backfill_fraction("MIN", 5, 12))
        self.assertEqual(1.0, shared.backfill_fraction("MAX", 5, 12))

    def test_real_positions(self):
        # Read off ceph1 (pool 18: pg_num 512), where PG 18.0's misplaced
        # counter agreed (2.9%) and 18.1d's read 0 (see the resumed-backfills
        # fixture's README.txt).
        self.assertAlmostEqual(0.0296, shared.backfill_fraction("0003c84c", 0, 512), 4)
        self.assertAlmostEqual(
            0.977, shared.backfill_fraction("b87d166c", 0x1D, 512), 3
        )

    def test_key_of_another_pg_is_rejected(self):
        # 18.1d's position, read against a different seed or pg_num.
        self.assertIsNone(shared.backfill_fraction("b87d166c", 0x1C, 512))
        self.assertIsNone(shared.backfill_fraction("b87d166c", 0x1D, 1024))

    def test_fraction_is_share_of_the_pgs_objects_before_the_position(self):
        # Scatter objects over the hash space, and check that the share of a
        # PG's objects whose sort key is below a position is what
        # backfill_fraction says, for PGs with and without a sibling. Small
        # pg_nums only, so every PG gets thousands of objects; the tolerance
        # is 4 standard deviations of that sampling.
        rng = random.Random(42)
        hashes = [rng.getrandbits(32) for _ in range(40_000)]
        for pg_num in (p for p in PG_NUMS if p <= 16):
            for seed in range(pg_num):
                keys = sorted(
                    sort_key(h) for h in hashes if stable_mod(h, pg_num) == seed
                )
                delta = 4 * (0.25 / len(keys)) ** 0.5
                with self.subTest(pg_num=pg_num, seed=seed):
                    for i in (len(keys) // 10, len(keys) // 2, 9 * len(keys) // 10):
                        frac = shared.backfill_fraction(f"{keys[i]:08x}", seed, pg_num)
                        self.assertAlmostEqual(i / len(keys), frac, delta=delta)

    def test_every_key_of_the_pg_fits_including_the_siblingless_half(self):
        # pg_num 12, seed 5: hashes ending in 0101 or (sibling 13 missing) 1101.
        for low4 in (0b0101, 0b1101):
            key = sort_key(0xABCDE000 | low4)
            self.assertIsNotNone(shared.backfill_fraction(f"{key:08x}", 5, 12))


def query(up, acting, peers):
    """A 'ceph pg query' with peer_info for (peer, last_backfill) pairs."""
    return {
        "up": up,
        "acting": acting,
        "peer_info": [{"peer": p, "last_backfill": lb} for p, lb in peers],
    }


class ExtractBackfillPositionsTest(unittest.TestCase):
    def test_ec_keeps_only_the_targets(self):
        q = query(
            [1, 5, 3],
            [1, 2, 3],
            [
                ("1(0)", "MAX"),  # acting, not moving
                ("2(1)", "MAX"),  # source
                ("5(1)", "18:8000abcd:::obj:head"),  # the target
                ("9(1)", "18:1000abcd:::obj:head"),  # stray from an older mapping
                ("3(2)", "MAX"),
            ],
        )
        self.assertEqual({"5(1)": "8000abcd"}, shared.extract_backfill_positions(q))

    def test_ec_same_osd_in_another_shard_is_not_a_target(self):
        # OSD 5 is in up at shard 1, but this peer entry is its shard 2.
        q = query([1, 5, 3], [1, 2, 3], [("5(2)", "MIN")])
        self.assertEqual({}, shared.extract_backfill_positions(q))

    def test_replicated(self):
        q = query([1, 4], [1, 2], [("1", "MAX"), ("2", "MAX"), ("4", "MIN")])
        self.assertEqual({"4": "MIN"}, shared.extract_backfill_positions(q))

    def test_unparsable_entries_are_skipped(self):
        q = query([1, 4], [1, 2], [("4", "garbage"), ("osd.4", "MIN")])
        self.assertEqual({}, shared.extract_backfill_positions(q))
        self.assertEqual({}, shared.extract_backfill_positions({}))

    def test_target_peers_named_like_pg_query(self):
        self.assertEqual(
            ["5(1)", "7(3)"],
            shared.backfill_target_peers([1, 5, 3, 7], [1, 2, 3, NONE], True),
        )
        self.assertEqual(
            ["4", "6"], shared.backfill_target_peers([6, 1, 4], [1, 2], False)
        )


class PgProgressTest(unittest.TestCase):
    EC = {"type": shared.POOL_TYPE_ERASURE, "size": 3, "pg_num": 16}  # noqa: RUF012
    REP = {"type": 1, "size": 2, "pg_num": 16}  # noqa: RUF012

    @staticmethod
    def pg(up, acting, pgid="1.3", misplaced=0):
        return {
            "pgid": pgid,
            "up": up,
            "acting": acting,
            "stat_sum": {"num_objects": 100, "num_objects_misplaced": misplaced},
        }

    @staticmethod
    def key(seed, pg_num, frac):
        """The position frac of the way through PG seed's key range."""
        bits = shared.pg_hash_bits(seed, pg_num)
        top = int(f"{seed & ((1 << bits) - 1):0{bits}b}"[::-1], 2) if bits else 0
        span = 1 << (32 - bits)
        return f"{(top << (32 - bits)) | int(frac * span):08x}"

    def test_positions_average_over_targets(self):
        # The counters say done; the positions say 25% and 75%.
        pg = self.pg([4, 5, 3], [1, 2, 3])
        positions = {"4(0)": self.key(3, 16, 0.25), "5(1)": self.key(3, 16, 0.75)}
        progress = shared.pg_progress(pg, self.EC, positions)
        self.assertTrue(progress.exact)
        self.assertAlmostEqual(50.0, progress.pct, 3)

    def test_finished_target_counts_as_done(self):
        pg = self.pg([4, 5, 3], [1, 2, 3], misplaced=200)
        progress = shared.pg_progress(pg, self.EC, {"4(0)": "MAX", "5(1)": "MIN"})
        self.assertEqual(shared.Progress(50.0, True), progress)

    def test_replicated(self):
        pg = self.pg([1, 4], [1, 2])
        progress = shared.pg_progress(pg, self.REP, {"4": self.key(3, 16, 0.5)})
        self.assertTrue(progress.exact)
        self.assertAlmostEqual(50.0, progress.pct, 3)

    def test_falls_back_on_counters(self):
        pg = self.pg([4, 5, 3], [1, 2, 3], misplaced=50)  # 75% by the counters
        counters = shared.Progress(75.0, False)
        cases = {
            "no positions": (self.EC, {}),
            "a target without one": (self.EC, {"4(0)": "MIN"}),
            "a position of another PG": (
                self.EC,
                {"4(0)": "MIN", "5(1)": self.key(2, 16, 0.5)},
            ),
            "unknown pool": (None, {"4(0)": "MIN", "5(1)": "MIN"}),
            "unknown pg_num": (
                {"type": shared.POOL_TYPE_ERASURE, "size": 3},
                {"4(0)": "MIN", "5(1)": "MIN"},
            ),
        }
        for name, (pool, positions) in cases.items():
            with self.subTest(name):
                # With the pool unknown, EC shards are diffed as replicas: the
                # counters then give a different, still counter-based, figure.
                progress = shared.pg_progress(pg, pool, positions)
                self.assertFalse(progress.exact)
                if pool is not None:
                    self.assertEqual(counters, progress)

    def test_shard_with_no_osd_yet_falls_back_on_counters(self):
        # Shard 2 has nowhere to go: its copies are work no position covers.
        pg = self.pg([4, 1, NONE], [1, 2, NONE])
        pg["up"], pg["acting"] = [4, 2, NONE], [1, 2, NONE]
        progress = shared.pg_progress(pg, self.EC, {"4(0)": "MAX"})
        self.assertFalse(progress.exact)


class CopyProgressTest(unittest.TestCase):
    """One moving copy's own progress (EC shard or replica)."""

    EC = PgProgressTest.EC
    REP = PgProgressTest.REP
    key = staticmethod(PgProgressTest.key)

    def test_target_peer(self):
        self.assertEqual("5(1)", shared.target_peer(5, 1))
        self.assertEqual("5(0)", shared.target_peer(5, 0))
        self.assertEqual("5", shared.target_peer(5, "-"))

    def test_each_shard_its_own(self):
        pg = PgProgressTest.pg([4, 5, 3], [1, 2, 3])
        positions = {"4(0)": self.key(3, 16, 0.9), "5(1)": self.key(3, 16, 0.1)}
        pcts = [
            shared.copy_progress(pg, self.EC, positions, peer).pct
            for peer in ("4(0)", "5(1)")
        ]
        self.assertAlmostEqual(90.0, pcts[0], 3)
        self.assertAlmostEqual(10.0, pcts[1], 3)

    def test_replica(self):
        pg = PgProgressTest.pg([1, 4], [1, 2])
        progress = shared.copy_progress(pg, self.REP, {"4": "MAX"}, "4")
        self.assertEqual(shared.Progress(100.0, True), progress)

    def test_shard_beside_one_with_no_osd_yet(self):
        # pg_progress can't cover shard 2 (nowhere to go), but shard 0 has a
        # position of its own.
        pg = PgProgressTest.pg([4, 2, NONE], [1, 2, NONE])
        progress = shared.copy_progress(pg, self.EC, {"4(0)": "MIN"}, "4(0)")
        self.assertEqual(shared.Progress(0.0, True), progress)
        self.assertFalse(shared.pg_progress(pg, self.EC, {"4(0)": "MIN"}).exact)

    def test_falls_back_on_the_pgs_counters(self):
        pg = PgProgressTest.pg([4, 5, 3], [1, 2, 3], misplaced=50)  # 75% by them
        counters = shared.Progress(75.0, False)
        self.assertEqual(counters, shared.counter_progress(pg, self.EC))
        cases = {
            "no position": (self.EC, {"5(1)": "MIN"}),
            "a position of another PG": (self.EC, {"4(0)": self.key(2, 16, 0.5)}),
            "unknown pg_num": (
                {"type": shared.POOL_TYPE_ERASURE, "size": 3},
                {"4(0)": "MIN"},
            ),
        }
        for name, (pool, positions) in cases.items():
            with self.subTest(name):
                self.assertEqual(
                    counters, shared.copy_progress(pg, pool, positions, "4(0)")
                )


class WithExactProgressTest(unittest.TestCase):
    """cancel-backfill/cancel-uphill's per-shard PROGRESS."""

    def cancellation(self, pgid, shard, up_osd, acting_osd):
        return shared.Cancellation(pgid, shard, up_osd, acting_osd, 0, "s", 75.0)

    def test_each_cancellation_gets_its_shards_progress(self):
        ec = PgProgressTest.pg([4, 5, 3], [1, 2, 3], misplaced=50)
        rep = PgProgressTest.pg([1, 6], [1, 2], pgid="2.3", misplaced=50)
        pools = {1: PgProgressTest.EC, 2: PgProgressTest.REP}
        cancellations = [
            self.cancellation("1.3", 0, 4, 1),
            self.cancellation("1.3", 1, 5, 2),
            self.cancellation("2.3", "-", 6, 2),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            pathlib.Path(tmp, shared.BACKFILL_POSITIONS_FILE).write_text(
                json.dumps({"1.3": {"4(0)": "MAX", "5(1)": "MIN"}})
            )
            store = FakeStore({}, load_dir=pathlib.Path(tmp))
            result = shared.with_exact_progress(store, cancellations, [ec, rep], pools)
        self.assertEqual(
            [(100.0, True), (0.0, True), (50.0, False)],
            [(c.progress_pct, c.progress_exact) for c in result],
        )


class PositionsFromOutputTest(unittest.TestCase):
    """A bad 'ceph pg query' output fails only that PG (None), not the run."""

    def test_valid(self):
        q = query([1, 4], [1, 2], [("4", "MIN")])
        self.assertEqual({"4": "MIN"}, shared._positions_from_output(json.dumps(q)))
        self.assertEqual(
            {"4": "MIN"}, shared._positions_from_output(json.dumps(q).encode())
        )

    def test_invalid(self):
        for output in ("", "not json", "[1, 2]", "null", b"\xff\xfe"):
            with self.subTest(output=output):
                self.assertIsNone(shared._positions_from_output(output))


class QueryBackfillPositionsTest(unittest.TestCase):
    """The live query, with librados and the CLI both faked."""

    def run_query(self, pgids, *, rados, rados_result=None, cli_result=None):
        """Call the real query_backfill_positions; return (result, stderr, cli mock)."""
        err = io.StringIO()
        cli = mock.Mock(return_value=cli_result)
        with (
            mock.patch.dict(sys.modules, {"rados": rados}),
            mock.patch.object(
                shared, "_query_positions_rados", return_value=rados_result
            ) as via_rados,
            mock.patch.object(shared, "_query_positions_cli", cli),
            contextlib.redirect_stderr(err),
        ):
            if isinstance(rados_result, Exception):
                via_rados.side_effect = rados_result
            result = real_query_backfill_positions(pgids)
        return result, err.getvalue(), cli

    def fake_rados(self):
        return type("rados", (), {"Error": type("Error", (Exception,), {})})

    def test_uses_librados_when_available(self):
        result, err, cli = self.run_query(
            ["1.2", "1.1"],
            rados=self.fake_rados(),
            rados_result={"1.1": {}, "1.2": {"4": "MIN"}},
        )
        self.assertEqual({"1.1": {}, "1.2": {"4": "MIN"}}, result)
        cli.assert_not_called()
        self.assertEqual("", err)

    def test_falls_back_on_the_cli_without_librados(self):
        # None in sys.modules makes 'import rados' raise ImportError.
        result, _, cli = self.run_query(["1.1"], rados=None, cli_result={"1.1": {}})
        self.assertEqual({"1.1": {}}, result)
        cli.assert_called_once_with(["1.1"])

    def test_falls_back_on_the_cli_when_librados_cannot_connect(self):
        rados = self.fake_rados()
        result, _, cli = self.run_query(
            ["1.1"],
            rados=rados,
            rados_result=rados.Error("no keyring"),
            cli_result={"1.1": {}},
        )
        self.assertEqual({"1.1": {}}, result)
        cli.assert_called_once()

    def test_failed_pgs_are_left_out_and_noted(self):
        result, err, _ = self.run_query(
            ["1.1", "1.2"], rados=None, cli_result={"1.1": None, "1.2": {"4": "MIN"}}
        )
        self.assertEqual({"1.2": {"4": "MIN"}}, result)
        self.assertIn("'ceph pg query' failed for 1 of 2 PG(s) (1.1)", err)

    def test_nothing_to_query(self):
        result, _, cli = self.run_query([], rados=None)
        self.assertEqual({}, result)
        cli.assert_not_called()


class FetchBackfillPositionsTest(unittest.TestCase):
    def test_from_a_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp, shared.BACKFILL_POSITIONS_FILE)
            path.write_text(json.dumps({"1.1": {"4": "MIN"}, "1.2": {"5": "MAX"}}))
            store = FakeStore({}, load_dir=pathlib.Path(tmp))
            self.assertEqual(
                {"1.1": {"4": "MIN"}},
                shared.fetch_backfill_positions(store, ["1.1", "1.3"]),
            )

    def test_older_capture_without_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore({}, load_dir=pathlib.Path(tmp))
            self.assertEqual({}, shared.fetch_backfill_positions(store, ["1.1"]))


class EcShardMovesTest(unittest.TestCase):
    def test_no_movement(self):
        self.assertEqual(shared.ec_shard_moves([1, 2, 3], [1, 2, 3]), [])

    def test_remap_is_positional(self):
        # Same OSD set, swapped positions: both shards genuinely move.
        self.assertEqual(
            shared.ec_shard_moves([2, 1, 3], [1, 2, 3]), [(0, 1, 2), (1, 2, 1)]
        )

    def test_mixed_remap_and_degraded(self):
        # PG 27.96 shape: shard 0 remapped, shard 4 filling a missing slot.
        up = [10, 2, 3, 4, 50]
        acting = [1, 2, 3, 4, NONE]
        self.assertEqual(shared.ec_shard_moves(up, acting), [(0, 1, 10), (4, None, 50)])

    def test_none_destination_is_skipped(self):
        self.assertEqual(shared.ec_shard_moves([1, NONE, 3], [1, 2, 3]), [])

    def test_length_mismatch(self):
        self.assertEqual(shared.ec_shard_moves([1, 2, 3], [1, 2]), [(2, None, 3)])
        self.assertEqual(shared.ec_shard_moves([1, 2], [1, 2, 3]), [])

    def test_minus_one_placeholder(self):
        self.assertEqual(shared.ec_shard_moves([1, 2], [1, -1]), [(1, None, 2)])


class EcUnassignedShardsTest(unittest.TestCase):
    def test_none_in_both(self):
        self.assertEqual(shared.ec_unassigned_shards([1, NONE, 3], [1, NONE, 3]), 1)

    def test_none_only_in_up_is_not_counted(self):
        # acting still holds a copy, so Ceph does not count it degraded.
        self.assertEqual(shared.ec_unassigned_shards([1, NONE], [1, 2]), 0)

    def test_none_only_in_acting_is_a_move_not_unassigned(self):
        self.assertEqual(shared.ec_unassigned_shards([1, 2], [1, NONE]), 0)

    def test_short_lists_and_minus_one(self):
        self.assertEqual(shared.ec_unassigned_shards([1, -1], [1]), 1)


class ReplicatedUnassignedTest(unittest.TestCase):
    def test_full_swap_has_none(self):
        self.assertEqual(
            shared.replicated_unassigned_copies({1, 2, 4}, {1, 2, 3}, 3), 0
        )

    def test_missing_replica_filled_by_destination(self):
        self.assertEqual(shared.replicated_unassigned_copies({1, 2, 3}, {1, 2}, 3), 0)

    def test_replica_with_no_osd(self):
        # size 3, one copy left, one destination: the third replica has none.
        self.assertEqual(shared.replicated_unassigned_copies({2}, {1}, 3), 1)

    def test_unknown_size_is_zero(self):
        self.assertEqual(shared.replicated_unassigned_copies({2}, {1}, 0), 0)


class CopiesMovingTest(unittest.TestCase):
    """The progress denominator: what pg_progress_pct multiplies num_objects by."""

    def test_ec_counts_moves_and_unassigned_slots(self):
        # One shard moving plus one shard with no OSD at all.
        self.assertEqual(shared.copies_moving([1, 9, NONE], [1, 4, NONE], True, 3), 2)

    def test_ec_mixed_moves_and_unassigned_slots(self):
        # slot 0 moves, slot 1 stays, slot 2 has no OSD anywhere, slot 3 moves
        self.assertEqual(
            shared.copies_moving([5, 2, NONE, 7], [1, 2, NONE, 4], True, 4), 3
        )

    def test_ec_ignores_pool_size(self):
        self.assertEqual(shared.copies_moving([1, 9], [1, 4], True, 0), 1)

    def test_ec_slot_empty_in_up_only_is_not_a_copy_to_place(self):
        self.assertEqual(shared.copies_moving([1, NONE], [1, 2], True, 2), 0)

    def test_replicated_counts_destinations(self):
        self.assertEqual(shared.copies_moving([1, 7], [1, 5], False, 2), 1)

    def test_replicated_counts_missing_replica(self):
        # One destination plus one replica still lacking an OSD.
        self.assertEqual(shared.copies_moving([1, 7], [1], False, 3), 2)

    def test_replicated_reorder_is_not_movement(self):
        self.assertEqual(shared.copies_moving([3, 1, 2], [1, 2, 3], False, 3), 0)

    def test_replicated_unknown_size_counts_only_destinations(self):
        self.assertEqual(shared.copies_moving([1, 7], [1, 5], False, 0), 1)

    def test_denominator_scales_progress(self):
        # 1 shard moving + 1 unassigned = 2 copies; 150 of 200 units left.
        n = shared.copies_moving([10, NONE], [1, NONE], True, 0)
        self.assertEqual(
            shared.pg_progress_pct(pg(100, misplaced=50, degraded=100), n), 25.0
        )
        # 1 destination + 1 unassigned = 2 copies; 50 of 200 units left.
        n = shared.copies_moving([2], [1], False, 3)
        self.assertEqual(shared.pg_progress_pct(pg(100, degraded=50), n), 75.0)


class ExtractPgStatsTest(unittest.TestCase):
    def test_accepted_shapes(self):
        stats = [{"pgid": "1.0"}]
        for raw in (
            stats,
            {"pg_stats": stats},
            {"pg_map": {"pg_stats": stats}},
            {"whatever": stats},
        ):
            self.assertEqual(shared.extract_pg_stats(raw, "ceph pg ls"), stats)

    def test_no_matching_pgs_returns_only_pg_ready(self):
        self.assertEqual(shared.extract_pg_stats({"pg_ready": True}, "ceph pg ls"), [])

    def test_not_ready_is_an_error_naming_the_command(self):
        with self.assertRaises(SystemExit) as ctx:
            shared.extract_pg_stats({"pg_ready": False}, "ceph pg ls remapped")
        self.assertIn("not ready", str(ctx.exception))
        self.assertIn("ceph pg ls remapped", str(ctx.exception))

    def test_unrecognised_shape_is_an_error_naming_the_command(self):
        for raw in ({"what": 1}, "text", 3):
            with self.assertRaises(SystemExit) as ctx:
                shared.extract_pg_stats(raw, "ceph pg dump pgs")
            self.assertIn("ceph pg dump pgs", str(ctx.exception))

    def test_fetch_labels_errors_with_the_stored_command_minus_format_flag(self):
        store = FakeStore(
            {"pgs": {"what": 1}},
            commands={"pgs": ["ceph", "pg", "ls", "remapped", "--format", "json"]},
        )
        with self.assertRaises(SystemExit) as ctx:
            shared.fetch_pg_stats(store, "pgs")
        self.assertIn("'ceph pg ls remapped'", str(ctx.exception))


TREE = {
    "nodes": [
        {"id": -2, "type": "host", "name": "ceph1.example.org", "children": [1, 2]},
        {"id": 1, "type": "osd"},
        {"id": 2, "type": "osd"},
        {"id": -3, "type": "host", "name": "ceph2", "children": [3, -9]},
        {"id": 3, "type": "osd"},
        {"id": -9, "type": "rack"},  # a non-OSD child is ignored
    ],
    "stray": [{"id": 4, "type": "osd"}],
}


class FetchTest(unittest.TestCase):
    def test_hosts_are_short_names_and_only_cover_osds_under_a_host(self):
        self.assertEqual(
            shared.fetch_osd_hosts(FakeStore({"osd_tree": TREE})),
            {1: "ceph1", 2: "ceph1", 3: "ceph2"},
        )

    def test_osd_df_includes_stray_osds(self):
        df = {"nodes": [{"id": 1, "utilization": 50.0}], "stray": [{"id": 9}]}
        self.assertEqual(
            shared.fetch_osd_df(FakeStore({"osd_df": df})),
            {1: {"id": 1, "utilization": 50.0}, 9: {"id": 9}},
        )

    def test_pools_crush_rules_are_keyed_by_id(self):
        store = FakeStore(
            {
                "pool_ls_detail": [{"pool_id": 7, "type": 1}],
                "crush_rule_dump": [{"rule_id": 2, "rule_name": "r"}],
            }
        )
        self.assertEqual(shared.fetch_pools(store), {7: {"pool_id": 7, "type": 1}})
        self.assertEqual(
            shared.fetch_crush_rules(store), {2: {"rule_id": 2, "rule_name": "r"}}
        )

    def test_osd_dump_derived_tables_tolerate_missing_keys(self):
        store = FakeStore({"osd_dump": {}})
        self.assertEqual(shared.fetch_ec_profiles(store), {})
        self.assertEqual(shared.fetch_upmap_items(store), {})

    def test_upmap_items_are_keyed_by_pgid(self):
        dump = {"pg_upmap_items": [{"pgid": "1.a", "mappings": [{"from": 1, "to": 2}]}]}
        self.assertEqual(
            shared.fetch_upmap_items(FakeStore({"osd_dump": dump})),
            {"1.a": [{"from": 1, "to": 2}]},
        )


class CellTest(unittest.TestCase):
    DF: ClassVar = {1: {"utilization": 92.46}, 2: {}}

    def test_utilization(self):
        self.assertEqual(shared.format_utilization(self.DF, 1), "92.5%")
        self.assertEqual(shared.format_utilization(self.DF, 2), "?")  # no figure
        self.assertEqual(shared.format_utilization(self.DF, 3), "?")  # unknown OSD
        self.assertEqual(shared.format_utilization(self.DF, None), "-")  # empty slot

    def test_progress_is_floored_so_a_moving_pg_never_reads_100(self):
        self.assertEqual(shared.format_progress(41.9), "41%")
        self.assertEqual(shared.format_progress(99.7), "99%")
        self.assertEqual(shared.format_progress(0.0), "0%")
        self.assertEqual(shared.format_progress(None), "-")

    def test_counter_progress_is_marked_approximate(self):
        self.assertEqual(shared.format_progress(41.9, exact=False), "~41%")
        self.assertEqual(shared.format_progress(100.0, exact=False), "~100%")
        self.assertEqual(shared.format_progress(None, exact=False), "-")

    def test_osd_cells_are_bare_ids(self):
        host = {1: "h1"}
        self.assertEqual(shared.osd_cells(self.DF, host, 1), ["1", "92.5%", "h1"])
        self.assertEqual(shared.osd_cells(self.DF, host, 2), ["2", "?", "?"])

    def test_osd_cells_primary_marker(self):
        self.assertEqual(shared.osd_cells(self.DF, {}, 1, primary=1)[0], "1*")
        self.assertEqual(shared.osd_cells(self.DF, {}, 1, primary=2)[0], "1")

    def test_abbreviate_state(self):
        self.assertEqual(
            shared.abbreviate_state("active+remapped+backfill_wait+brand_new"),
            "act+remap+bkfl_wt+brand_new",
        )
        self.assertEqual(shared.abbreviate_state(""), "")

    def test_osd_cells_empty_slot(self):
        self.assertEqual(shared.osd_cells(self.DF, {}, None), ["none", "-", "-"])


class PrintTableTest(unittest.TestCase):
    COLUMNS: ClassVar = [("", "PG"), ("UP", "OSD"), ("UP", "UTIL"), ("", "NOTE")]

    def render(self, rows, columns=None):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            shared.print_table(columns or self.COLUMNS, rows)
        return out.getvalue().splitlines()

    def test_two_line_header_and_group_span(self):
        group, labels, _ = self.render([["1.a", "osd.12", "88.1%", "x"]])
        self.assertEqual(labels.split(), ["PG", "OSD", "UTIL", "NOTE"])
        # The group name is centered in dashes over exactly its own columns.
        self.assertIn(" UP ", group)
        self.assertEqual(group.index("-"), labels.index("OSD"))
        self.assertEqual(len(group), labels.index("NOTE") - len(shared.GROUP_SEP))

    def test_groups_are_set_apart_by_a_wider_gap(self):
        _, labels, _ = self.render([["1.a", "osd.12", "88.1%", "x"]])
        # Column widths here are 3 (1.a), 6 (osd.12), 5 (88.1%).
        self.assertEqual(
            labels.index("OSD") - (labels.index("PG") + 3), len(shared.GROUP_SEP)
        )
        self.assertEqual(
            labels.index("UTIL") - (labels.index("OSD") + 6), len(shared.COLUMN_SEP)
        )
        self.assertEqual(
            labels.index("NOTE") - (labels.index("UTIL") + 5), len(shared.GROUP_SEP)
        )

    def test_columns_widen_to_the_widest_cell_and_the_last_is_unpadded(self):
        _, labels, row1, row2 = self.render(
            [
                ["1.a", "osd.12", "88.1%", "short"],
                ["1.1234", "osd.3", "9.0%", "longer note"],
            ]
        )
        for line in (labels, row1, row2):
            self.assertEqual(line, line.rstrip())
        self.assertEqual(row1.index("osd.12"), row2.index("osd.3"))

    def test_no_rows_prints_just_the_header(self):
        self.assertEqual(len(self.render([])), 2)


class SnapshotStoreTest(unittest.TestCase):
    COMMANDS: ClassVar = {"a": ["ceph", "a", "--format", "json"], "b": ["ceph", "b"]}

    def test_live_runs_each_command_once(self):
        store = shared.SnapshotStore(self.COMMANDS)
        with mock.patch.object(shared, "ceph_json", return_value={"x": 1}) as run:
            self.assertEqual(store.json("a"), {"x": 1})
            self.assertEqual(store.json("a"), {"x": 1})
        run.assert_called_once_with(self.COMMANDS["a"])

    def test_load_reads_the_key_file_and_never_runs_ceph(self):
        with tempfile.TemporaryDirectory() as tmp:
            (pathlib.Path(tmp) / "a.json").write_text('{"x": 2}')
            store = shared.SnapshotStore(self.COMMANDS, load_dir=pathlib.Path(tmp))
            with mock.patch.object(shared, "ceph_json", side_effect=AssertionError):
                self.assertEqual(store.json("a"), {"x": 2})

    def test_load_with_a_missing_file_names_the_file_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = shared.SnapshotStore(self.COMMANDS, load_dir=pathlib.Path(tmp))
            with self.assertRaises(SystemExit) as ctx:
                store.json("a")
        self.assertIn("a.json", str(ctx.exception))
        self.assertIn("ceph a --format json", str(ctx.exception))

    def test_save_writes_every_key_anonymized_and_leaves_the_run_data_alone(self):
        data = {
            "a": {
                "nodes": [{"id": -1, "type": "host", "name": "real.example.org"}],
            },
            "b": {"kept": True},
        }
        commands = {"osd_tree": ["ceph", "x"], "b": ["ceph", "y"]}
        with tempfile.TemporaryDirectory() as tmp:
            store = shared.SnapshotStore(commands, save_dir=pathlib.Path(tmp))
            with mock.patch.object(
                shared,
                "ceph_json",
                side_effect=lambda cmd: (
                    data["a"] if cmd == commands["osd_tree"] else data["b"]
                ),
            ):
                store.save()
                saved = {
                    p.stem: json.loads(p.read_text())
                    for p in pathlib.Path(tmp).glob("*.json")
                }
        self.assertEqual(set(saved), {"osd_tree", "b"})
        self.assertNotIn("real", json.dumps(saved))
        self.assertEqual(saved["b"], {"kept": True})
        self.assertEqual(data["a"]["nodes"][0]["name"], "real.example.org")

    def test_save_fetches_keys_the_run_never_asked_for(self):
        # The capture is complete by construction, not by what main() happened to read.
        with tempfile.TemporaryDirectory() as tmp:
            store = shared.SnapshotStore(self.COMMANDS, save_dir=pathlib.Path(tmp))
            with mock.patch.object(shared, "ceph_json", return_value={}):
                store.save()
            self.assertEqual(
                {p.stem for p in pathlib.Path(tmp).glob("*.json")}, set(self.COMMANDS)
            )

    def test_save_without_a_directory_is_a_no_op(self):
        with mock.patch.object(shared, "ceph_json", side_effect=AssertionError):
            shared.SnapshotStore(self.COMMANDS).save()

    def test_custom_anonymizer_is_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = shared.SnapshotStore(
                {"b": ["ceph", "b"]},
                save_dir=pathlib.Path(tmp),
                anonymize=lambda snaps: snaps.update(b="redacted"),
            )
            with mock.patch.object(shared, "ceph_json", return_value={"secret": 1}):
                store.save()
            self.assertEqual((pathlib.Path(tmp) / "b.json").read_text(), '"redacted"')


class HelpFormatterTest(unittest.TestCase):
    TEXT = """
    First paragraph, long enough that it has to be reflowed onto several
    lines at this width.

    - item one, which is also long enough to wrap onto a second line
      here
    - item two

        indented example   kept   as written
    """

    def fill(self, width=40):
        formatter = shared.HelpFormatter("prog", width=width)
        return formatter._fill_text(self.TEXT, width, "")

    def test_paragraphs_stay_separate(self):
        self.assertEqual(3, len(self.fill().split("\n\n")))

    def test_paragraph_is_reflowed_to_width(self):
        first = self.fill().split("\n\n")[0].splitlines()
        self.assertGreater(len(first), 1)
        self.assertTrue(all(len(line) <= 40 for line in first))

    def test_list_items_get_a_hanging_indent(self):
        items = self.fill().split("\n\n")[1].splitlines()
        self.assertTrue(items[0].startswith("- item one"))
        self.assertTrue(items[1].startswith("  "))
        self.assertTrue(items[-1].startswith("- item two"))

    def test_indented_block_is_kept_verbatim(self):
        self.assertEqual(
            "    indented example   kept   as written", self.fill().split("\n\n")[2]
        )


class StateArgsTest(unittest.TestCase):
    def parse(self, *argv):
        parser = argparse.ArgumentParser()
        shared.add_load_state_arg(parser)
        return parser.parse_args(argv)

    def test_default_is_off(self):
        self.assertIsNone(self.parse().load_state)

    def test_help_points_at_save_state(self):
        parser = argparse.ArgumentParser()
        shared.add_load_state_arg(parser)
        self.assertIn("save-state", parser.format_help())

    def test_from_args_requires_an_existing_load_directory(self):
        with self.assertRaises(SystemExit) as ctx:
            shared.SnapshotStore.from_args(
                self.parse("--load-state", "/nonexistent-dir"), {}
            )
        self.assertIn("not found", str(ctx.exception))


class ResolveSaveDirTest(unittest.TestCase):
    def test_creates_a_missing_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = pathlib.Path(tmp) / "new" / "dir"
            self.assertEqual(shared.resolve_save_dir(str(target)), target)
            self.assertTrue(target.is_dir())

    def test_refuses_a_non_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            (pathlib.Path(tmp) / "old.json").write_text("{}")
            with self.assertRaises(SystemExit) as ctx:
                shared.resolve_save_dir(tmp)
        self.assertIn("not empty", str(ctx.exception))


class AnonymizeTest(unittest.TestCase):
    def snapshots(self):
        return {
            "osd_tree": {
                "nodes": [
                    {
                        "id": -1,
                        "type": "host",
                        "name": "ceph2-11",
                        "children": [1],
                    },
                    {"id": 1, "type": "osd"},
                ],
                "stray": [
                    {"id": -2, "type": "host", "name": "ceph3-4", "children": []}
                ],
            },
            "osd_dump": {
                "fsid": "real-fsid",
                "pools": [{"pool": 3, "pool_name": "secret"}],
                "osds": [
                    {
                        "osd": 7,
                        "uuid": "real-uuid",
                        "public_addr": "10.1.2.33:6800/123",
                        "public_addrs": {"addrvec": [{"addr": "10.1.2.33:6800"}]},
                    }
                ],
            },
            "pool_ls_detail": [{"pool_id": 3, "pool_name": "secret"}],
            "crush_rule_dump": [{"rule_id": 1, "rule_name": "secret"}],
        }

    def test_scrubs_everything_that_identifies_the_cluster(self):
        snaps = self.snapshots()
        snaps["osd_tree"]["nodes"].append(
            {"id": -3, "type": "host", "name": "node.site.org", "children": []}
        )
        shared.anonymize_snapshots(snaps)
        text = json.dumps(snaps)
        for leaked in (
            "site.org",
            "ceph2",
            "real-fsid",
            "real-uuid",
            "10.1.2",
            "secret",
        ):
            self.assertNotIn(leaked, text)
        names = [n["name"] for n in snaps["osd_tree"]["nodes"] if n["type"] == "host"]
        self.assertEqual(names[0], "host11")  # keyed off the trailing number
        self.assertRegex(names[1], r"^host-[0-9a-f]{8}$")  # no number: hashed
        self.assertEqual(snaps["osd_tree"]["stray"][0]["name"], "host04")
        self.assertEqual(snaps["pool_ls_detail"][0]["pool_name"], "pool3")
        self.assertEqual(snaps["osd_dump"]["pools"][0]["pool_name"], "pool3")
        self.assertEqual(snaps["crush_rule_dump"][0]["rule_name"], "rule1")
        self.assertEqual(snaps["osd_dump"]["osds"][0]["osd"], 7)  # ids are kept

    def test_idempotent(self):
        once = self.snapshots()
        shared.anonymize_snapshots(once)
        twice = json.loads(json.dumps(once))
        shared.anonymize_snapshots(twice)
        self.assertEqual(once, twice)

    def test_works_on_whichever_keys_are_present(self):
        # show-pg-osds and show-backfill snapshot different keys from the others.
        for keys in (["osd_tree"], ["osd_dump"], ["pool_ls_detail"], []):
            snaps = {k: v for k, v in self.snapshots().items() if k in keys}
            shared.anonymize_snapshots(snaps)
        snaps = {"pg_dump_pgs": {"pg_stats": [{"pgid": "1.0"}]}}
        shared.anonymize_snapshots(snaps)
        self.assertEqual(snaps, {"pg_dump_pgs": {"pg_stats": [{"pgid": "1.0"}]}})

    def test_hosts_with_one_trailing_number_stay_distinct(self):
        # ceph1-5 and ceph2-5 would both become host05 if mapped independently.
        fakes = shared._fake_hostnames({"ceph1-5", "ceph2-5", "ceph2-6"})
        self.assertEqual(len(set(fakes.values())), 3)
        self.assertEqual(fakes["ceph2-6"], "host06")

    def test_fake_hostnames_are_stable(self):
        self.assertEqual(shared._fake_hostname("ceph2-7"), "host07")
        fake = shared._fake_hostname("nodigits.example")
        self.assertRegex(fake, r"^host-[0-9a-f]{8}$")
        self.assertEqual(shared._fake_hostname(fake), fake)


class ParseOsdTest(unittest.TestCase):
    def test_bare_and_prefixed(self):
        self.assertEqual(shared.parse_osd("682"), 682)
        self.assertEqual(shared.parse_osd("osd.682"), 682)

    def test_rejects_garbage_and_negatives(self):
        for text in ("", "osd.", "x", "-1", "6.8"):
            with self.subTest(text=text), self.assertRaises(argparse.ArgumentTypeError):
                shared.parse_osd(text)


class PoolChecksTest(unittest.TestCase):
    """check_known_pools and check_host_failure_domain."""

    HOST: ClassVar = {"steps": [{"op": "chooseleaf_indep", "type": "host"}]}
    RACK: ClassVar = {"steps": [{"op": "chooseleaf_indep", "type": "rack"}]}
    POOLS: ClassVar = {
        1: {"pool_id": 1, "pool_name": "a", "crush_rule": 0},
        2: {"pool_id": 2, "pool_name": "b", "crush_rule": 1},
        3: {"pool_id": 3, "pool_name": "c", "crush_rule": 9},
    }

    def test_known_pools_pass(self):
        shared.check_known_pools(["1.0", "2.a"], self.POOLS, "PGs")

    def test_unknown_pools_are_all_named(self):
        with self.assertRaises(SystemExit) as cm:
            shared.check_known_pools(["1.0", "7.1", "5.2", "7.3"], self.POOLS, "PGs")
        self.assertIn("PGs belong to pool id(s) 5, 7,", str(cm.exception))

    def test_host_pools_pass(self):
        shared.check_host_failure_domain(["1.0"], self.POOLS, {0: self.HOST}, "PGs")

    def test_every_other_failure_domain_is_listed(self):
        rules = {0: self.HOST, 1: self.RACK}  # rule 9 is missing
        with self.assertRaises(SystemExit) as cm:
            shared.check_host_failure_domain(
                ["1.0", "3.1", "2.0"], self.POOLS, rules, "stuck PGs"
            )
        lines = str(cm.exception).splitlines()
        self.assertIn("pools of these stuck PGs use another", lines[0])
        self.assertEqual(
            lines[1:],
            [
                "  pool 2 (b): crush rule 1, failure domain rack",
                "  pool 3 (c): crush rule 9, failure domain unknown",
            ],
        )


class PercentTypesTest(unittest.TestCase):
    """percentage_points and utilization_pct: argparse types of the PERCENT options."""

    def test_percentage_points_accepts_0_to_100(self):
        for text, value in (("0", 0.0), ("0.5", 0.5), ("1", 1.0), ("100", 100.0)):
            with self.subTest(text=text):
                self.assertEqual(shared.percentage_points(text), value)

    def test_percentage_points_rejects_out_of_range_and_garbage(self):
        for text in ("-0.1", "100.1", "nan", "inf", "", "85%", "x"):
            with self.subTest(text=text), self.assertRaises(argparse.ArgumentTypeError):
                shared.percentage_points(text)

    def test_utilization_accepts_0_and_1_to_100(self):
        for text, value in (("0", 0.0), ("1.01", 1.01), ("85", 85.0), ("100", 100.0)):
            with self.subTest(text=text):
                self.assertEqual(shared.utilization_pct(text), value)

    def test_utilization_rejects_what_looks_like_a_ratio(self):
        for text in ("0.01", "0.85", "0.9", "1", "1.0"):
            with self.subTest(text=text):
                with self.assertRaises(argparse.ArgumentTypeError) as cm:
                    shared.utilization_pct(text)
                self.assertIn("not a ratio like 0.85", str(cm.exception))

    def test_utilization_rejects_out_of_range(self):
        for text in ("-5", "101", "nan"):
            with self.subTest(text=text), self.assertRaises(argparse.ArgumentTypeError):
                shared.utilization_pct(text)


if __name__ == "__main__":
    unittest.main()
