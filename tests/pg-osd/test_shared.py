"""Unit tests for shared.py, the code the pg-osd scripts have in common.

The risky parts: the progress denominator (num_objects_misplaced and
num_objects_degraded are counted in copy units, so a PG moving k copies starts
at k * num_objects; dividing by num_objects alone reads 0% until more than 1/k
of the work is done, which looks like a plausible "hasn't started" rather than
an obvious failure), the EC/replicated slot counting that feeds it, and the
snapshot layer whose saved output must be safe to share and loadable again.
"""

import argparse
import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from typing import ClassVar
from unittest import mock

from _support import FakeStore, shared

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

    def test_osd_cells(self):
        host = {1: "h1"}
        self.assertEqual(shared.osd_cells(self.DF, host, 1), ["osd.1", "92.5%", "h1"])
        self.assertEqual(shared.osd_cells(self.DF, host, 2), ["osd.2", "?", "?"])

    def test_osd_cells_primary_marker(self):
        self.assertEqual(shared.osd_cells(self.DF, {}, 1, primary=1)[0], "osd.1*")
        self.assertEqual(shared.osd_cells(self.DF, {}, 1, primary=2)[0], "osd.1")

    def test_osd_cells_bare_id(self):
        host = {1: "h1"}
        self.assertEqual(
            shared.osd_cells(self.DF, host, 1, bare_id=True), ["1", "92.5%", "h1"]
        )
        self.assertEqual(
            shared.osd_cells(self.DF, {}, 1, primary=1, bare_id=True)[0], "1*"
        )
        self.assertEqual(
            shared.osd_cells(self.DF, {}, None, bare_id=True), ["none", "-", "-"]
        )

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


class StateArgsTest(unittest.TestCase):
    def parse(self, *argv):
        parser = argparse.ArgumentParser()
        shared.add_state_args(parser, {"a": ["ceph", "a"], "b": ["ceph", "b"]})
        return parser.parse_args(argv)

    def test_defaults_are_off(self):
        args = self.parse()
        self.assertIsNone(args.load_state)
        self.assertIsNone(args.save_state)

    def test_load_and_save_are_mutually_exclusive(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.parse("--load-state", "x", "--save-state", "y")

    def test_help_lists_the_files_the_script_reads(self):
        parser = argparse.ArgumentParser()
        shared.add_state_args(parser, {"a": ["ceph", "a"], "b": ["ceph", "b"]})
        self.assertIn("a.json, b.json", " ".join(parser.format_help().split()))

    def test_from_args_requires_an_existing_load_directory(self):
        with self.assertRaises(SystemExit) as ctx:
            shared.SnapshotStore.from_args(
                self.parse("--load-state", "/nonexistent-dir"), {}
            )
        self.assertIn("not found", str(ctx.exception))

    def test_from_args_creates_a_missing_save_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = pathlib.Path(tmp) / "new" / "dir"
            store = shared.SnapshotStore.from_args(
                self.parse("--save-state", str(target)), {}
            )
            self.assertTrue(target.is_dir())
            self.assertEqual(store.save_dir, target)

    def test_from_args_refuses_a_non_empty_save_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            (pathlib.Path(tmp) / "old.json").write_text("{}")
            with self.assertRaises(SystemExit) as ctx:
                shared.SnapshotStore.from_args(self.parse("--save-state", tmp), {})
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
        # osds-of-pg and pg-movements snapshot different keys from the others.
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


if __name__ == "__main__":
    unittest.main()
