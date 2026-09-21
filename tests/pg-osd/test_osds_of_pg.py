"""Unit tests for osds-of-pg.py.

The row pairing (EC positional vs replicated set-diff), the UPMAPS matching and
the table layout are this script's own; the progress arithmetic and cell
formatting it shares with the other scripts are tested in test_shared.py.
"""

import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from _support import load_script, shared

op = load_script("osds-of-pg.py")

NONE = shared.CRUSH_ITEM_NONE
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

    def test_row_progress_only_for_remapped(self):
        def progress(pct, row):
            return op.format_row(row, PG, pct, {}, {}, [])[7]

        self.assertEqual("-", progress(41.0, Row(0, 1, 1)))
        self.assertEqual("-", progress(41.0, Row(0, 1, None)))
        self.assertEqual("-", progress(None, Row(0, 1, 2)))
        self.assertEqual("41%", progress(41.9, Row(0, 1, 2)))
        # Never displays 100% while still moving.
        self.assertEqual("99%", progress(99.7, Row(0, 1, 2)))

    def test_row_marks_each_side_s_own_primary(self):
        row = op.format_row(Row(0, 712, 59), PG, None, {}, {}, [])
        self.assertEqual(("osd.712*", "osd.59*"), (row[1], row[4]))
        row = op.format_row(Row(0, 59, 712), PG, None, {}, {}, [])
        self.assertEqual(("osd.59", "osd.712"), (row[1], row[4]))

    def test_row_empty_slot(self):
        row = op.format_row(Row(3, None, 9), PG, None, {}, {}, [])
        self.assertEqual(["none", "-", "-"], row[1:4])


PG = {"up_primary": 59, "acting_primary": 712}


class TableTest(unittest.TestCase):
    def render(self, rows):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            shared.print_table(op.COLUMNS, rows)
        return out.getvalue().splitlines()

    def test_layout(self):
        util = {
            712: {"utilization": 88.1},
            226: {"utilization": 92.5},
            59: {"utilization": 85.0},
        }
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


class MainTest(unittest.TestCase):
    """main() end to end over canned snapshots, including --load/--save-state."""

    SNAPSHOTS = {  # noqa: RUF012
        "pg_query": {
            "state": "active+remapped+backfilling",
            "up": [1, 4],
            "acting": [1, 2],
            "info": {
                "stats": {
                    "up_primary": 1,
                    "acting_primary": 1,
                    "stat_sum": {
                        "num_objects": 100,
                        "num_objects_misplaced": 50,
                        "num_bytes": 4096,
                    },
                    "reported_epoch": 7,
                }
            },
            # Fields the script never reads, and that may name hosts or addresses.
            "peer_info": [{"peer": "1", "addr": "10.1.2.3:6800"}],
            "recovery_state": [{"name": "Started/Primary/Active"}],
        },
        "osd_tree": {
            "nodes": [
                {"id": -2, "type": "host", "name": "ceph1-1", "children": [1, 2]},
                {"id": -3, "type": "host", "name": "ceph1-2", "children": [4]},
                {"id": 1, "type": "osd"},
                {"id": 2, "type": "osd"},
                {"id": 4, "type": "osd"},
            ]
        },
        "osd_df": {
            "nodes": [
                {"id": 1, "utilization": 50.0},
                {"id": 2, "utilization": 91.0},
                {"id": 4, "utilization": 20.0},
            ]
        },
        "osd_dump": {
            "pg_upmap_items": [{"pgid": "5.3", "mappings": [{"from": 2, "to": 4}]}],
            "blocklist": {"10.9.8.7:0/12345": 1700000000},
        },
        "pool_ls_detail": [{"pool_id": 5, "pool_name": "rep", "type": 1, "size": 2}],
    }

    def run_main(self, *argv):
        out = io.StringIO()
        with (
            mock.patch("sys.argv", ["osds-of-pg.py", *argv]),
            contextlib.redirect_stdout(out),
        ):
            op.main()
        return out.getvalue()

    def test_replicated_pg_from_a_saved_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            for key, data in self.SNAPSHOTS.items():
                (pathlib.Path(tmp) / f"{key}.json").write_text(json.dumps(data))
            out = self.run_main("--load-state", tmp, "5.3")
        lines = out.splitlines()
        self.assertEqual("PG 5.3  state: active+remapped+backfilling", lines[0])
        rows = [ln.split() for ln in lines[4:6]]
        self.assertEqual(
            [
                [
                    "-",
                    "osd.1*",
                    "50.0%",
                    "ceph1-1",
                    "osd.1*",
                    "50.0%",
                    "ceph1-1",
                    "-",
                    "-",
                ],
                [
                    "-",
                    "osd.2",
                    "91.0%",
                    "ceph1-1",
                    "osd.4",
                    "20.0%",
                    "ceph1-2",
                    "50%",
                    "2->4",
                ],
            ],
            rows,
        )

    def test_save_state_writes_the_pg_specific_capture_anonymized(self):
        by_command = {
            tuple(cmd): self.SNAPSHOTS[key]
            for key, cmd in op.snapshot_commands("5.3").items()
        }

        def fake(cmd):
            return by_command[tuple(cmd)]

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(shared, "ceph_json", fake),
        ):
            out = self.run_main("--save-state", tmp + "/snap", "5.3")
            saved = {
                p.stem: json.loads(p.read_text())
                for p in pathlib.Path(tmp, "snap").glob("*.json")
            }
        self.assertIn("PG 5.3", out)
        self.assertEqual(set(saved), set(op.snapshot_commands("5.3")))
        hosts = [n["name"] for n in saved["osd_tree"]["nodes"] if n["type"] == "host"]
        self.assertEqual(["host01", "host02"], hosts)
        # Cut down to the six values fetch_pg_info reads, so nothing else in
        # the (release-dependent) document can identify the cluster.
        self.assertEqual(
            saved["pg_query"],
            {
                "state": "active+remapped+backfilling",
                "up": [1, 4],
                "acting": [1, 2],
                "info": {
                    "stats": {
                        "up_primary": 1,
                        "acting_primary": 1,
                        "stat_sum": {"num_objects": 100, "num_objects_misplaced": 50},
                    }
                },
            },
        )
        self.assertNotIn("10.1.2.3", json.dumps(saved))
        # osd dump is cut down to the upmaps this script reads: no client
        # blocklist entries, no other cluster-wide state.
        self.assertEqual(
            saved["osd_dump"],
            {"pg_upmap_items": self.SNAPSHOTS["osd_dump"]["pg_upmap_items"]},
        )
        self.assertNotIn("10.9.8.7", json.dumps(saved))

    def test_saved_state_replays_to_the_same_output(self):
        # Hostnames already in their anonymized form map to themselves, so the
        # replay of a capture is comparable to the live run it came from.
        snaps = {
            **self.SNAPSHOTS,
            "osd_tree": json.loads(
                json.dumps(self.SNAPSHOTS["osd_tree"])
                .replace("ceph1-1", "host01")
                .replace("ceph1-2", "host02")
            ),
        }
        by_command = {
            tuple(cmd): snaps[key] for key, cmd in op.snapshot_commands("5.3").items()
        }
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(shared, "ceph_json", lambda c: by_command[tuple(c)]):
                live = self.run_main("--save-state", tmp + "/snap", "5.3")
            with mock.patch.object(shared, "ceph_json", side_effect=AssertionError):
                replayed = self.run_main("--load-state", tmp + "/snap", "5.3")
        self.assertIn("host01", live)
        self.assertEqual(live, replayed)

    def test_pool_missing_from_pool_ls_is_treated_as_replicated_of_unknown_size(self):
        snaps = {**self.SNAPSHOTS, "pool_ls_detail": []}
        with mock.patch.object(
            shared.SnapshotStore, "json", lambda self, key: snaps[key]
        ):
            out = self.run_main("5.3")
        self.assertIn("50%", out)


if __name__ == "__main__":
    unittest.main()
