"""Unit tests for pg-movements.py.

The progress column is the risky part: num_objects_misplaced and
num_objects_degraded are counted in copy units, so a PG moving k copies
starts at k * num_objects. Dividing by num_objects alone reads 0% until
more than 1/k of the work is done, which looks like a plausible "hasn't
started" rather than an obvious failure. The tests pin the denominator
(num_objects * copies being moved) and the EC/replicated slot counting
that feeds it.
"""

import importlib.util
import os
import unittest

SCRIPT = os.path.join(os.path.dirname(__file__), "pg-movements.py")
spec = importlib.util.spec_from_file_location("pg_movements", SCRIPT)
pm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pm)

NONE = pm.CRUSH_ITEM_NONE


def pg(num_objects: int, misplaced: int = 0, degraded: int = 0) -> dict:
    return {
        "stat_sum": {
            "num_objects": num_objects,
            "num_objects_misplaced": misplaced,
            "num_objects_degraded": degraded,
        }
    }


class ProgressTest(unittest.TestCase):
    def test_single_copy(self):
        self.assertEqual(pm.pg_progress_pct(pg(100, misplaced=100), 1), 0.0)
        self.assertEqual(pm.pg_progress_pct(pg(100, misplaced=25), 1), 75.0)

    def test_multi_copy_not_stuck_at_zero(self):
        # Regression: 2 copies moving, one quarter done. Misplaced is 150 of
        # 200 copy-units; dividing by num_objects alone gave 1 - 1.5 -> 0%.
        self.assertEqual(pm.pg_progress_pct(pg(100, misplaced=150), 2), 25.0)

    def test_multi_copy_just_started(self):
        self.assertEqual(pm.pg_progress_pct(pg(100, misplaced=200), 2), 0.0)

    def test_done(self):
        self.assertEqual(pm.pg_progress_pct(pg(100), 3), 100.0)

    def test_degraded_and_misplaced_are_summed(self):
        # 3 copies: 300 copy-units total, 60 + 90 left.
        self.assertEqual(
            pm.pg_progress_pct(pg(100, misplaced=60, degraded=90), 3), 50.0
        )

    def test_overlap_clamps_to_zero(self):
        self.assertEqual(
            pm.pg_progress_pct(pg(100, misplaced=200, degraded=200), 2), 0.0
        )

    def test_no_objects_or_no_copies_is_unknown(self):
        self.assertIsNone(pm.pg_progress_pct(pg(0), 2))
        self.assertIsNone(pm.pg_progress_pct(pg(100, misplaced=5), 0))
        self.assertIsNone(pm.pg_progress_pct({}, 1))


class EcShardMovesTest(unittest.TestCase):
    def test_no_movement(self):
        self.assertEqual(pm.ec_shard_moves([1, 2, 3], [1, 2, 3]), [])

    def test_remap_is_positional(self):
        # Same OSD set, swapped positions: both shards genuinely move.
        self.assertEqual(
            pm.ec_shard_moves([2, 1, 3], [1, 2, 3]), [(0, 1, 2), (1, 2, 1)]
        )

    def test_mixed_remap_and_degraded(self):
        # PG 27.96 shape: shard 0 remapped, shard 4 filling a missing slot.
        up = [10, 2, 3, 4, 50]
        acting = [1, 2, 3, 4, NONE]
        self.assertEqual(pm.ec_shard_moves(up, acting), [(0, 1, 10), (4, None, 50)])

    def test_none_destination_is_skipped(self):
        self.assertEqual(pm.ec_shard_moves([1, NONE, 3], [1, 2, 3]), [])

    def test_length_mismatch(self):
        self.assertEqual(pm.ec_shard_moves([1, 2, 3], [1, 2]), [(2, None, 3)])
        self.assertEqual(pm.ec_shard_moves([1, 2], [1, 2, 3]), [])

    def test_minus_one_placeholder(self):
        self.assertEqual(pm.ec_shard_moves([1, 2], [1, -1]), [(1, None, 2)])


class EcUnassignedShardsTest(unittest.TestCase):
    def test_none_in_both(self):
        self.assertEqual(pm.ec_unassigned_shards([1, NONE, 3], [1, NONE, 3]), 1)

    def test_none_only_in_up_is_not_counted(self):
        # acting still holds a copy, so Ceph does not count it degraded.
        self.assertEqual(pm.ec_unassigned_shards([1, NONE], [1, 2]), 0)

    def test_none_only_in_acting_is_a_move_not_unassigned(self):
        self.assertEqual(pm.ec_unassigned_shards([1, 2], [1, NONE]), 0)

    def test_short_lists_and_minus_one(self):
        self.assertEqual(pm.ec_unassigned_shards([1, -1], [1]), 1)

    def test_progress_denominator_includes_unassigned(self):
        # 1 shard moving + 1 unassigned = 2 copies; 150 of 200 units left.
        up, acting = [10, NONE], [1, NONE]
        n = len(pm.ec_shard_moves(up, acting)) + pm.ec_unassigned_shards(up, acting)
        self.assertEqual(
            pm.pg_progress_pct(pg(100, misplaced=50, degraded=100), n), 25.0
        )


class ReplicatedUnassignedTest(unittest.TestCase):
    def test_full_swap_has_none(self):
        self.assertEqual(pm.replicated_unassigned_copies({1, 2, 4}, {1, 2, 3}, 3), 0)

    def test_missing_replica_filled_by_destination(self):
        self.assertEqual(pm.replicated_unassigned_copies({1, 2, 3}, {1, 2}, 3), 0)

    def test_replica_with_no_osd(self):
        # size 3, one copy left, one destination: the third replica has none.
        self.assertEqual(pm.replicated_unassigned_copies({2}, {1}, 3), 1)

    def test_unknown_size_is_zero(self):
        self.assertEqual(pm.replicated_unassigned_copies({2}, {1}, 0), 0)

    def test_progress_denominator(self):
        # 1 destination + 1 unassigned = 2 copies; 50 of 200 units left.
        n = 1 + pm.replicated_unassigned_copies({2}, {1}, 3)
        self.assertEqual(pm.pg_progress_pct(pg(100, degraded=50), n), 75.0)


if __name__ == "__main__":
    unittest.main()
