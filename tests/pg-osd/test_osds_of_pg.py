"""Unit tests for osds-of-pg.py.

The row pairing (EC positional vs replicated set-diff) and the progress
denominator mirror pg-movements.py; the tests pin those, plus the UPMAPS
matching and the table layout that this script adds.
"""

import contextlib
import importlib.util
import io
import os
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(REPO_ROOT, "pg-osd", "osds-of-pg.py")
spec = importlib.util.spec_from_file_location("osds_of_pg", SCRIPT)
op = importlib.util.module_from_spec(spec)
spec.loader.exec_module(op)

NONE = op.CRUSH_ITEM_NONE
Row = op.ShardRow


class BuildRowsTest(unittest.TestCase):
    def test_ec_pairs_by_position(self):
        rows = op.build_rows([1, 2, 9], [1, 5, 3], erasure=True)
        self.assertEqual([Row(0, 1, 1), Row(1, 5, 2), Row(2, 3, 9)], rows)
        self.assertEqual([False, True, True], [r.remapped for r in rows])

    def test_ec_empty_slots(self):
        rows = op.build_rows([1, NONE, 9, NONE], [1, 4, NONE, NONE], erasure=True)
        self.assertEqual(
            [Row(0, 1, 1), Row(1, 4, None), Row(2, None, 9), Row(3, None, None)],
            rows,
        )
        # Only a shard headed for a real OSD counts as remapped.
        self.assertEqual([False, False, True, False], [r.remapped for r in rows])

    def test_ec_unequal_lengths(self):
        rows = op.build_rows([1], [1, 2], erasure=True)
        self.assertEqual([Row(0, 1, 1), Row(1, 2, None)], rows)

    def test_replicated_reorder_is_not_movement(self):
        rows = op.build_rows([3, 1, 2], [1, 2, 3], erasure=False)
        self.assertEqual([Row("-", o, o) for o in (1, 2, 3)], rows)
        self.assertFalse(any(r.remapped for r in rows))

    def test_replicated_pairs_departing_with_arriving(self):
        rows = op.build_rows([1, 7, 8], [1, 5, 6], erasure=False)
        self.assertEqual([Row("-", 1, 1), Row("-", 5, 7), Row("-", 6, 8)], rows)

    def test_replicated_uneven_sets(self):
        # One replica lost (no source), one moving: acting is shorter.
        rows = op.build_rows([1, 7, 8], [1, 5], erasure=False)
        self.assertEqual([Row("-", 1, 1), Row("-", 5, 7), Row("-", None, 8)], rows)
        # Shrinking: a departing replica with nowhere to go.
        rows = op.build_rows([1], [1, 5], erasure=False)
        self.assertEqual([Row("-", 1, 1), Row("-", 5, None)], rows)
        self.assertFalse(rows[1].remapped)

    def test_replicated_ignores_placeholders(self):
        rows = op.build_rows([1, NONE], [1, -1], erasure=False)
        self.assertEqual([Row("-", 1, 1)], rows)

    def test_empty(self):
        self.assertEqual([], op.build_rows([], [], erasure=True))
        self.assertEqual([], op.build_rows([], [], erasure=False))


STATS = {"num_objects": 100, "num_objects_misplaced": 100}


class ProgressTest(unittest.TestCase):
    def test_scaled_by_copies(self):
        self.assertEqual(op.pg_progress_pct(STATS, 2), 50.0)

    def test_no_objects(self):
        self.assertIsNone(op.pg_progress_pct({"num_objects": 0}, 3))
        self.assertIsNone(op.pg_progress_pct({}, 3))

    def test_clamped(self):
        stats = {"num_objects": 10, "num_objects_misplaced": 30}
        self.assertEqual(op.pg_progress_pct(stats, 1), 0.0)

    def test_copies_ec_counts_unassigned(self):
        up, acting = [1, 9, NONE], [1, 4, NONE]
        rows = op.build_rows(up, acting, erasure=True)
        pool = op.Pool(True, 3)
        # One moving shard plus one shard with no OSD at all.
        self.assertEqual(2, op.copies_in_flight(rows, up, acting, pool))

    def test_copies_replicated_counts_missing_replica(self):
        up, acting = [1, 7], [1]
        rows = op.build_rows(up, acting, erasure=False)
        pool = op.Pool(False, 3)
        # One destination plus one replica still lacking an OSD.
        self.assertEqual(2, op.copies_in_flight(rows, up, acting, pool))

    def test_copies_unknown_pool(self):
        up, acting = [1, 7], [1, 5]
        rows = op.build_rows(up, acting, erasure=False)
        self.assertEqual(1, op.copies_in_flight(rows, up, acting, None))


class FormatTest(unittest.TestCase):
    def test_upmaps_touching_row(self):
        pairs = [
            {"from": 148, "to": 134},
            {"from": 830, "to": 556},
            {"from": 226, "to": 59},
        ]
        self.assertEqual("226->59", op.format_upmaps(pairs, Row(3, 226, 59)))
        # Either side matches; several pairs are comma-joined.
        self.assertEqual("148->134,830->556", op.format_upmaps(pairs, Row(0, 134, 830)))
        self.assertEqual("-", op.format_upmaps(pairs, Row(1, 1, 2)))
        self.assertEqual("-", op.format_upmaps([], Row(1, 1, 2)))

    def test_upmaps_empty_slot_matches_nothing(self):
        self.assertEqual(
            "-", op.format_upmaps([{"from": 1, "to": 2}], Row(0, None, None))
        )

    def test_upmaps_chained_pair(self):
        pairs = [{"from": 1, "to": 2}, {"from": 2, "to": 3}]
        self.assertEqual("1->2,2->3", op.format_upmaps(pairs, Row(0, 2, 2)))

    def test_progress_only_for_remapped(self):
        self.assertEqual("-", op.format_progress(41.0, Row(0, 1, 1)))
        self.assertEqual("-", op.format_progress(41.0, Row(0, 1, None)))
        self.assertEqual("-", op.format_progress(None, Row(0, 1, 2)))
        self.assertEqual("41%", op.format_progress(41.9, Row(0, 1, 2)))
        # Never displays 100% while still moving.
        self.assertEqual("99%", op.format_progress(99.7, Row(0, 1, 2)))

    def test_osd_primary_marker_and_none(self):
        self.assertEqual("osd.3*", op.format_osd(3, 3))
        self.assertEqual("osd.4", op.format_osd(4, 3))
        self.assertEqual("none", op.format_osd(None, 3))

    def test_utilization_and_host(self):
        util, host = {1: 92.46}, {1: "h1"}
        self.assertEqual("92.5%", op.format_utilization(util, 1))
        self.assertEqual("?", op.format_utilization(util, 2))
        self.assertEqual("-", op.format_utilization(util, None))
        self.assertEqual("h1", op.format_host(host, 1))
        self.assertEqual("?", op.format_host(host, 2))
        self.assertEqual("-", op.format_host(host, None))


PG = {"up_primary": 59, "acting_primary": 712}


class TableTest(unittest.TestCase):
    def render(self, rows):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            op.print_table(rows)
        return out.getvalue().splitlines()

    def test_layout(self):
        util = {712: 88.1, 226: 92.5, 59: 85.0}
        host = {712: "h4", 226: "h9", 59: "h12"}
        pairs = [{"from": 226, "to": 59}]
        rows = [
            op.format_row(r, PG, 41.0, util, host, pairs)
            for r in (Row(0, 712, 712), Row(3, 226, 59))
        ]
        lines = self.render(rows)
        self.assertEqual(4, len(lines))
        group, header, first, second = lines
        self.assertIn("-- ACTING --", group)
        self.assertIn("-- UP --", group)
        self.assertEqual(
            "SHARD    OSD       UTIL   HOST    OSD      UTIL   HOST    PROGRESS  UPMAPS",
            header,
        )
        self.assertEqual(
            "0        osd.712*  88.1%  h4      osd.712  88.1%  h4      -         -",
            first,
        )
        self.assertEqual(
            "3        osd.226   92.5%  h9      osd.59*  85.0%  h12     41%       226->59",
            second,
        )
        # The group name sits over the first column of its group.
        self.assertEqual(header.index("OSD"), group.index("-"))

    def test_no_rows_prints_header_only(self):
        lines = self.render([])
        self.assertEqual(2, len(lines))


if __name__ == "__main__":
    unittest.main()
