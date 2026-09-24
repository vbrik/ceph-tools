"""Unit tests for backfillctl's show-pg-osds subcommand.

The row pairing (EC positional vs replicated set-diff), the UPMAPS matching and
the table layout are this subcommand's own; the progress arithmetic and cell
formatting it shares with the others are tested in test_shared.py.
"""

import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from _support import parse_args, plan_from_state, shared

from backfillctl import show_pg_osds as op

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

    def test_row_progress(self):
        def progress(p):
            return op.format_row(Row(0, 1, 2), PG, p, {}, {}, [])[7]

        self.assertEqual("-", progress(None))  # not remapped (see plan())
        self.assertEqual("-", progress(shared.Progress(None, True)))
        self.assertEqual("41%", progress(shared.Progress(41.9, True)))
        self.assertEqual("~41%", progress(shared.Progress(41.9, False)))
        # Only finished copying reads 100%.
        self.assertEqual("99%", progress(shared.Progress(99.7, True)))

    def test_row_marks_each_side_s_own_primary(self):
        row = op.format_row(Row(0, 712, 59), PG, None, {}, {}, [])
        self.assertEqual(("712*", "59*"), (row[1], row[4]))
        row = op.format_row(Row(0, 59, 712), PG, None, {}, {}, [])
        self.assertEqual(("59", "712"), (row[1], row[4]))

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
            op.format_row(Row(0, 712, 712), PG, None, util, host, pairs),
            op.format_row(
                Row(3, 226, 59), PG, shared.Progress(41.0, True), util, host, pairs
            ),
        ]
        lines = self.render(rows)
        self.assertEqual(4, len(lines))
        group, header, first, second = lines
        self.assertIn("-- ACTING --", group)
        self.assertIn("-- UP --", group)
        self.assertEqual(
            "SHARD    OSD   UTIL   HOST    OSD  UTIL   HOST    PROGRESS  UPMAPS",
            header,
        )
        self.assertEqual(
            "0        712*  88.1%  h4      712  88.1%  h4      -         -",
            first,
        )
        self.assertEqual(
            "3        226   92.5%  h9      59*  85.0%  h12     41%       226->59",
            second,
        )
        # The group name sits over the first column of its group.
        self.assertEqual(header.index("OSD"), group.index("-"))

    def test_no_rows_prints_header_only(self):
        lines = self.render([])
        self.assertEqual(2, len(lines))


class RenderFootnoteTest(unittest.TestCase):
    """render()'s footnotes, on results built by hand."""

    def render(self, acting_primary, up_primary):
        pg = {"state": "s", "acting_primary": acting_primary, "up_primary": up_primary}
        rows = [op.ShardRow(0, 1, 1), op.ShardRow(1, 2, 3)]
        view = op.PgView("1.0", pg, rows, [None, None], [])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            op.render(op.ShowResult([view], {}, {}))
        return out.getvalue()

    def test_primary_note_when_a_primary_is_shown(self):
        self.assertIn("1*", self.render(1, 1))
        self.assertIn(op.PRIMARY_NOTE, self.render(1, 1))

    def test_no_primary_note_when_no_primary_is_shown(self):
        # e.g. an incomplete PG whose primary is none of the rows' OSDs
        out = self.render(-1, 99)
        self.assertNotIn("*", out)


class MainTest(unittest.TestCase):
    """plan() and run() end to end: a live run (mocked SnapshotStore.json) and
    --load-state (a pg_dump_pgs.json-based directory, what 'backfillctl
    save-state' produces -- see op.fetch_pg_info)."""

    # 'ceph pg <pgid> query''s shape, used for live runs (fetch_pg_info's
    # SnapshotStore.load_dir-is-None branch).
    PG_QUERY = {  # noqa: RUF012
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
        # Of peer_info, only the backfill target's (osd.4's) last_backfill is
        # read: halfway through PG 5.3's key range (pg_num 8). The rest may
        # name hosts or addresses and is never read.
        "peer_info": [
            {"peer": "1", "addr": "10.1.2.3:6800", "last_backfill": "MAX"},
            {"peer": "4", "last_backfill": "5:d0000000:::obj:head"},
        ],
        "recovery_state": [{"name": "Started/Primary/Active"}],
    }

    # The same PG's data in pg_dump_pgs.json's pg_stat shape (what
    # --load-state reads instead -- see fetch_pg_info's other branch): flat,
    # not wrapped in 'info.stats', and up_primary/acting_primary are siblings
    # of up/acting rather than nested.
    PG_DUMP_PGS = {  # noqa: RUF012
        "pg_stats": [
            {
                "pgid": "5.3",
                "state": "active+remapped+backfilling",
                "up": [1, 4],
                "up_primary": 1,
                "acting": [1, 2],
                "acting_primary": 1,
                "stat_sum": {"num_objects": 100, "num_objects_misplaced": 50},
            }
        ]
    }

    SNAPSHOTS = {  # noqa: RUF012
        "pg_query_5.3": PG_QUERY,
        "pg_dump_pgs": PG_DUMP_PGS,
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
        "pool_ls_detail": [
            {"pool_id": 5, "pool_name": "rep", "type": 1, "size": 2, "pg_num": 8}
        ],
    }

    # A second, clean PG of the same pool (in pg_dump_pgs.json's shape).
    CLEAN_PG = {  # noqa: RUF012
        "pgid": "5.4",
        "state": "active+clean",
        "up": [2, 4],
        "up_primary": 2,
        "acting": [2, 4],
        "acting_primary": 2,
        "stat_sum": {"num_objects": 10, "num_objects_misplaced": 0},
    }

    def write_snapshots(self, tmp, snaps=None):
        for key, data in (snaps or self.SNAPSHOTS).items():
            (pathlib.Path(tmp) / f"{key}.json").write_text(json.dumps(data))

    def two_pg_snapshots(self):
        snaps = json.loads(json.dumps(self.SNAPSHOTS))  # deep copy
        snaps["pg_dump_pgs"]["pg_stats"].append(self.CLEAN_PG)
        return snaps

    def run_main(self, *argv, load_state=None):
        out = io.StringIO()
        args = parse_args(op, argv, load_state=load_state)
        with contextlib.redirect_stdout(out):
            op.run(args)
        return out.getvalue()

    def test_replicated_pg_from_a_saved_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp)
            result = plan_from_state(op, tmp, "5.3")
        (view,) = result.pgs
        self.assertEqual("5.3", view.pgid)
        self.assertEqual("active+remapped+backfilling", view.pg["state"])
        # osd.1 stays; osd.2's copy is headed for osd.4
        self.assertEqual([Row("-", 1, 1), Row("-", 2, 4)], view.rows)
        # Only the remapped row has progress: the counters' 50% (the
        # directory has no backfill_positions.json).
        self.assertEqual([None, shared.Progress(50.0, False)], view.progress)
        self.assertEqual([{"from": 2, "to": 4}], view.upmap_pairs)
        self.assertEqual("ceph1-2", result.osd_host[4])

    def test_saved_state_renders_one_line_per_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp)
            out = self.run_main("5.3", load_state=tmp)
        lines = out.splitlines()
        self.assertEqual("PG 5.3  state: active+remapped+backfilling", lines[0])
        # a blank line, the two header lines, then the rows
        self.assertEqual(["-", "-"], [ln.split()[0] for ln in lines[4:6]])
        self.assertIn("2->4", lines[5])

    def test_saved_backfill_position_wins_over_counters(self):
        # The counters say 100%, the saved position 25%.
        snaps = json.loads(json.dumps(self.SNAPSHOTS))  # deep copy
        snaps["pg_dump_pgs"]["pg_stats"][0]["stat_sum"]["num_objects_misplaced"] = 0
        snaps["backfill_positions"] = {"5.3": {"4": "c8000000"}}
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp, snaps)
            (view,) = plan_from_state(op, tmp, "5.3").pgs
            out = self.run_main("5.3", load_state=tmp)
        self.assertEqual([None, shared.Progress(25.0, True)], view.progress)
        self.assertIn(" 25% ", out)
        self.assertNotIn("~", out)

    def test_approx_note_appears_when_progress_comes_from_counters(self):
        # A capture without backfill_positions.json.
        snaps = json.loads(json.dumps(self.SNAPSHOTS))  # deep copy
        snaps["pg_dump_pgs"]["pg_stats"][0]["stat_sum"]["num_objects_misplaced"] = 0
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp, snaps)
            out = self.run_main("5.3", load_state=tmp)
        self.assertIn(" ~100%", out)
        self.assertIn("~ marks PROGRESS from Ceph's misplaced/degraded", out)

    def test_live_progress_from_the_pg_query(self):
        # The query run() makes anyway carries the position: 50% exact, and
        # no separate position query.
        with (
            mock.patch.object(
                shared.SnapshotStore, "json", lambda store, key: self.SNAPSHOTS[key]
            ),
            mock.patch.object(shared, "query_backfill_positions") as query,
        ):
            args = parse_args(op, ["5.3"])
            (view,) = op.plan(args, shared.SnapshotStore(op.SNAPSHOT_COMMANDS)).pgs
        query.assert_not_called()
        self.assertEqual([None, shared.Progress(50.0, True)], view.progress)

    def test_load_state_reports_a_pgid_missing_from_the_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp)
            with self.assertRaises(SystemExit) as ctx:
                self.run_main("99.99", load_state=tmp)
        self.assertIn("99.99", str(ctx.exception))
        self.assertIn("pg_dump_pgs.json", str(ctx.exception))

    def test_pool_missing_from_pool_ls_is_treated_as_replicated_of_unknown_size(self):
        snaps = {**self.SNAPSHOTS, "pool_ls_detail": []}
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp, snaps)
            (view,) = plan_from_state(op, tmp, "5.3").pgs
        self.assertEqual([Row("-", 1, 1), Row("-", 2, 4)], view.rows)
        self.assertEqual([None, shared.Progress(50.0, False)], view.progress)

    def test_several_pgs_print_a_block_each_and_footnotes_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp, self.two_pg_snapshots())
            out = self.run_main("5.4", "5.3", load_state=tmp)
        headers = [ln for ln in out.splitlines() if ln.startswith("PG ")]
        self.assertEqual(
            [
                "PG 5.4  state: active+clean",
                "PG 5.3  state: active+remapped+backfilling",
            ],
            headers,
        )
        # Blocks are separated by a blank line.
        self.assertIn("\n\nPG 5.3  state:", out)
        self.assertEqual(1, out.count("* marks the primary"))
        self.assertEqual(1, out.count("~ marks PROGRESS"))
        # Both footnotes start with the mark they explain, '*' first.
        self.assertLess(out.index("* marks"), out.index("~ marks"))

    def test_footnote_on_progress_only_when_some_pg_is_remapped(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp, self.two_pg_snapshots())
            out = self.run_main("5.4", load_state=tmp)
        self.assertNotIn("~ marks PROGRESS", out)
        self.assertIn("* marks the primary", out)

    def test_duplicate_pgids_are_shown_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp)
            result = plan_from_state(op, tmp, "5.3", "5.3")
        self.assertEqual(["5.3"], [view.pgid for view in result.pgs])

    def test_unknown_pgid_among_several_fails_before_any_output(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            self.write_snapshots(tmp)
            args = parse_args(op, ["5.3", "99.99"], load_state=tmp)
            with contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as ctx:
                op.run(args)
        self.assertIn("99.99", str(ctx.exception))
        self.assertEqual("", out.getvalue())

    def test_live_queries_each_pg_once(self):
        clean_query = {
            "state": "active+clean",
            "up": [2, 4],
            "acting": [2, 4],
            "info": {"stats": {"up_primary": 2, "acting_primary": 2}},
        }
        snaps = {**self.SNAPSHOTS, "pg_query_5.4": clean_query}
        requested = []

        def fake_json(store, key):
            requested.append(key)
            return snaps[key]

        with mock.patch.object(shared.SnapshotStore, "json", fake_json):
            out = self.run_main("5.3", "5.4", "5.3")
        self.assertIn("PG 5.3  state:", out)
        self.assertIn("PG 5.4  state: active+clean", out)
        pg_queries = [k for k in requested if k.startswith("pg_query_")]
        self.assertEqual(["pg_query_5.3", "pg_query_5.4"], pg_queries)
        self.assertNotIn("pg_dump_pgs", requested)

    def test_live_command_per_pgid(self):
        args = parse_args(op, ["5.3", "5.4"])
        with mock.patch.object(op.SnapshotStore, "from_args") as from_args:
            from_args.side_effect = SystemExit  # stop right after building it
            with self.assertRaises(SystemExit):
                op.run(args)
        commands = from_args.call_args.args[1]
        self.assertEqual(
            ["ceph", "pg", "5.4", "query", "--format", "json"],
            commands["pg_query_5.4"],
        )
        self.assertIn("pg_query_5.3", commands)

    def test_at_least_one_pgid_required(self):
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            parse_args(op, [])


if __name__ == "__main__":
    unittest.main()
