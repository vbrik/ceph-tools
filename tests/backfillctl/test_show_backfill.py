"""Unit tests for backfillctl's show-backfill subcommand.

The progress arithmetic and OSD-slot helpers it shares with the others
are tested in test_shared.py. Here: how PG states are classified and
abbreviated; plan() over canned snapshots, which pins how EC (positional)
and replicated (set-difference) PGs turn into rows; the --osds/--pgs
filters; and how render() prints them.
"""

import contextlib
import io
import json
import pathlib
import re
import tempfile
import unittest
from unittest import mock

from _support import REPO_ROOT, FakeStore, parse_args, plan_from_state, shared

from backfillctl import show_backfill as pm

NONE = shared.CRUSH_ITEM_NONE


class MovementTypeTest(unittest.TestCase):
    def test_recovery_backfill_and_both(self):
        self.assertEqual(pm.movement_type("active+recovering"), "recovery")
        self.assertEqual(pm.movement_type("active+backfill_wait"), "backfill")
        self.assertEqual(
            pm.movement_type("active+recovery_wait+backfilling"), "recovery+backfill"
        )

    def test_remapped_without_an_active_pipeline(self):
        self.assertEqual(pm.movement_type("active+remapped"), "remapped")


def pg(pgid, up, acting, state, **stat):
    return {
        "pgid": pgid,
        "up": up,
        "acting": acting,
        "state": state,
        "acting_primary": acting[0],
        "stat_sum": {"num_objects": 100, **stat},
    }


PGS = [
    # EC: shard 1 moves 3 -> 2.
    pg(
        "27.10",
        [0, 2, 4, 1],
        [0, 3, 4, 1],
        "active+remapped+backfilling",
        num_objects_misplaced=150,
    ),
    # EC: shard 3 fills an empty slot, shard 4 has no OSD anywhere yet.
    pg(
        "27.9",
        [0, 2, NONE, 1, NONE],
        [0, 2, 4, NONE, NONE],
        "active+undersized+degraded+remapped+backfill_wait",
        num_objects_degraded=100,
    ),
    pg("27.a", [1, 2, 3, 4], [1, 2, 3, 4], "active+clean"),
    # Replicated: 2 -> 3.
    pg(
        "5.3",
        [0, 3, 4],
        [0, 2, 4],
        "active+remapped+backfilling",
        num_objects_misplaced=60,
    ),
    # Replicated: same OSD set, reordered. Not movement.
    pg("5.4", [3, 2, 4], [2, 3, 4], "active+clean"),
    # Replicated: a replica missing entirely, filled from the primary.
    pg("5.1f", [0, 1, 2], [0, 1], "active+undersized+degraded+recovering"),
]

SNAPSHOTS = {
    "pg_dump_pgs": {"pg_map": {"pg_stats": PGS}},
    "osd_tree": {
        "nodes": [
            {"id": -2, "type": "host", "name": "ceph1.example.org", "children": [0, 1]},
            {"id": -3, "type": "host", "name": "ceph2.example.org", "children": [2, 3]},
            *({"id": o, "type": "osd"} for o in range(4)),
        ],
        # An OSD outside the CRUSH tree still has a host if it hangs off one.
        "stray": [{"id": 4, "type": "osd"}],
    },
    "osd_df": {
        "nodes": [{"id": o, "utilization": u} for o, u in enumerate((10, 20, 70, 89))]
    },
    "pool_ls_detail": [
        {"pool_id": 5, "type": 1, "size": 3, "pg_num": 32},
        {"pool_id": 27, "type": 3, "size": 4, "pg_num": 16},
    ],
}


class MainTest(unittest.TestCase):
    def run_main(self, *argv, snapshots=SNAPSHOTS):
        out = io.StringIO()
        args = parse_args(pm, argv)
        with (
            mock.patch.object(
                shared.SnapshotStore, "json", lambda self, key: snapshots[key]
            ),
            contextlib.redirect_stdout(out),
        ):
            pm.run(args)
        return out.getvalue()

    def plan(self, *argv, snapshots=SNAPSHOTS):
        store = FakeStore(snapshots, commands=pm.SNAPSHOT_COMMANDS)
        return pm.plan(parse_args(pm, argv), store)

    def row(self, pgid, snapshots=SNAPSHOTS):
        return next(r for r in self.plan(snapshots=snapshots).rows if r.pgid == pgid)

    def test_only_moving_pgs_get_rows_in_pg_order(self):
        rows = self.plan().rows
        self.assertEqual(["5.3", "5.1f", "27.9", "27.10"], [r.pgid for r in rows])

    def test_ec_row_names_the_shard_and_both_osds(self):
        row = self.row("27.10")
        self.assertEqual(
            (1, {3}, {2}, "backfill"),
            (row.shard, row.sources, row.destinations, row.move_type),
        )

    def test_ec_row_rendering(self):
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("27.10"))
        self.assertRegex(
            line,
            r"^27\.10\s+1\s+3\(ceph2,89\.0%\)\s+->\s+2\(ceph2,70\.0%\)\s+backfill\s+~0%",
        )

    def test_unknown_osds_are_an_error_not_an_empty_match(self):
        with self.assertRaises(SystemExit) as cm:
            self.plan("--osds", "2", "99", "osd.98")
        self.assertEqual(
            str(cm.exception), "ERROR: --osds: not in 'ceph osd df': osd.98, osd.99"
        )

    def test_table_matches_the_other_commands(self):
        # print_table's layout: the label line first (no groups, no rule),
        # and utilization to one decimal, so 88.6% never reads as 89%.
        row = pm.MovementRow(
            "1.0", 0, frozenset({3}), frozenset({2}), "backfill", "s", 3, False
        )
        cells = pm.format_row(row, {3: {"utilization": 88.6}, 2: {}}, {3: "a", 2: "b"})
        self.assertEqual(cells[2:5], ["3(a,88.6%)", "->", "2(b,?)"])
        out = self.run_main()
        self.assertTrue(out.startswith("PGID "))
        self.assertNotIn("─", out)

    def test_progress_denominator_counts_unassigned_shards(self):
        # 27.9: one shard moving plus one with no OSD anywhere = 2 copies to
        # place, so 100 degraded objects of 200 copy-units is 50% done; counting
        # only the moving shard would clamp to 0%.
        self.assertEqual(50.0, self.row("27.9").progress_pct)

    def test_progress_denominator_of_a_single_moving_shard(self):
        # 27.10: 150 of 100 * 1 copy-units misplaced is clamped to 0%; wrongly
        # counting a second copy would read 25%.
        self.assertEqual(0.0, self.row("27.10").progress_pct)

    def test_replicated_reorder_is_not_reported(self):
        self.assertNotIn("5.4", [r.pgid for r in self.plan().rows])

    def test_degraded_replica_has_no_source_but_the_primary(self):
        row = self.row("5.1f")
        self.assertEqual((set(), {2}), (row.sources, row.destinations))
        self.assertEqual(0, row.primary)
        self.assertTrue(row.needs_primary_marker)

    def test_degraded_replica_shows_the_primary_as_the_worker(self):
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("5.1f"))
        self.assertIn("0(ceph1)*", line)
        self.assertIn("* marks the PG's primary", out)

    def test_counter_progress_is_marked_and_explained(self):
        # No backfill positions (the tests' stubbed live query returns none):
        # 5.1f has no misplaced/degraded objects in its stat_sum, so its lone
        # moving copy reads ~100% while still up != acting (see pg()/PGS above).
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("5.1f"))
        self.assertIn(" ~100% ", line)
        self.assertIn("~ marks PROGRESS from Ceph's misplaced/degraded", out)

    def test_shard_beside_one_with_no_osd_yet_gets_its_own_progress(self):
        # 27.9's shard 3 is moving; its shard 4 has no OSD anywhere, which
        # would keep a per-PG figure on the counters, but not shard 3's own.
        query = mock.Mock(return_value={"27.9": {"1(3)": "MAX"}})
        store = FakeStore(SNAPSHOTS, commands=pm.SNAPSHOT_COMMANDS, load_dir=None)
        with mock.patch.object(shared, "query_backfill_positions", query):
            rows = pm.plan(parse_args(pm, ["--pgs", "27.9"]), store).rows
        (row,) = rows
        self.assertEqual((100.0, True), (row.progress_pct, row.progress_exact))

    def test_progress_from_backfill_positions(self):
        # Every target at MIN: 0% however done the counters say it is. 27.9
        # is left out: a shard with no OSD anywhere keeps it on the counters.
        pools = {5: False, 27: True}
        positions = {
            p["pgid"]: dict.fromkeys(
                shared.backfill_target_peers(
                    p["up"], p["acting"], pools[shared.pgid_pool_id(p["pgid"])]
                ),
                "MIN",
            )
            for p in PGS
        }
        query = mock.Mock(return_value=positions)
        with mock.patch.object(shared, "query_backfill_positions", query):
            out = self.run_main("--pgs", "5.3", "5.1f", "27.10")
        # Only the PGs shown are queried.
        self.assertEqual({"5.3", "5.1f", "27.10"}, set(query.call_args.args[0]))
        rows = [ln for ln in out.splitlines() if re.match(r"\d+\.[0-9a-f]+ ", ln)]
        self.assertEqual(3, len(rows))
        for line in rows:
            self.assertRegex(line, r" 0%\s")
        self.assertNotIn("~", out)

    def test_stray_osd_hosts_and_utilization_resolve(self):
        snaps = {
            **SNAPSHOTS,
            "pg_dump_pgs": [pg("5.3", [0, 4, 1], [0, 3, 1], "active+remapped")],
            "osd_tree": {
                "nodes": [
                    {"id": -2, "type": "host", "name": "h1", "children": [0, 1]},
                ],
                "stray": [
                    {"id": -3, "type": "host", "name": "h2", "children": [3, 4]},
                    {"id": 3, "type": "osd"},
                    {"id": 4, "type": "osd"},
                ],
            },
            "osd_df": {"nodes": [], "stray": [{"id": 4, "utilization": 33.0}]},
        }
        result = self.plan(snapshots=snaps)
        self.assertEqual(("h2", "h2"), (result.osd_host[3], result.osd_host[4]))
        self.assertEqual(33.0, result.osd_df[4]["utilization"])
        self.assertNotIn(3, result.osd_df)
        out = self.run_main(snapshots=snaps)
        self.assertIn("4(h2,33.0%)", out)
        self.assertIn("3(h2,?)", out)

    def test_no_movement(self):
        snaps = {**SNAPSHOTS, "pg_dump_pgs": [PGS[2]]}
        self.assertEqual([], self.plan(snapshots=snaps).rows)
        self.assertEqual("No PG movements detected.\n", self.run_main(snapshots=snaps))

    def test_pg_stats_not_ready_is_an_error_naming_the_command(self):
        snaps = {**SNAPSHOTS, "pg_dump_pgs": {"pg_ready": False}}
        with self.assertRaises(SystemExit) as ctx:
            self.run_main(snapshots=snaps)
        self.assertIn("ceph pg dump pgs", str(ctx.exception))

    def test_sort_by_destination(self):
        rows = self.plan("--sort-by", "to-osd").rows
        self.assertEqual("5.3", rows[-1].pgid)  # osd.3 is the highest destination

    def test_load_state_reads_the_saved_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            for key, data in SNAPSHOTS.items():
                (pathlib.Path(tmp) / f"{key}.json").write_text(json.dumps(data))
            with mock.patch.object(shared, "ceph_json", side_effect=AssertionError):
                result = plan_from_state(pm, tmp)
        self.assertEqual(4, len(result.rows))


class FilterTest(unittest.TestCase):
    """--osds and --pgs, over PGS: rows 5.3 (2->3), 5.1f (0*->2), 27.9 shard 3
    (0*->1) and 27.10 shard 1 (3->2); osd.0 is every PG's primary."""

    def pgids(self, *argv, snapshots=SNAPSHOTS):
        store = FakeStore(snapshots, commands=pm.SNAPSHOT_COMMANDS)
        return [r.pgid for r in pm.plan(parse_args(pm, argv), store).rows]

    def run_main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(
                shared.SnapshotStore, "json", lambda self, key: SNAPSHOTS[key]
            ),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            pm.run(parse_args(pm, argv))
        return out.getvalue(), err.getvalue()

    def test_osds_matches_sources_and_destinations(self):
        self.assertEqual(["5.3", "27.10"], self.pgids("--osds", "3"))
        self.assertEqual(["5.3", "5.1f", "27.10"], self.pgids("--osds", "2"))

    def test_osds_matches_the_marked_primary_only_where_it_is_shown(self):
        # osd.0 is primary everywhere, but only 5.1f and 27.9 show it with '*'.
        self.assertEqual(["5.1f", "27.9"], self.pgids("--osds", "0"))

    def test_osds_is_any_of_and_accepts_osd_prefix(self):
        self.assertEqual(["5.3", "27.9", "27.10"], self.pgids("--osds", "osd.1", "3"))

    def test_osds_rejects_garbage(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(pm, ["--osds", "x"])

    def test_osds_filters_ec_rows_not_pgs(self):
        # Shard 0 moves 3->0 and shard 3 moves 0->1; only shard 0 touches osd.3.
        snaps = {
            **SNAPSHOTS,
            "pg_dump_pgs": [
                pg("27.20", [0, 2, 4, 1], [3, 2, 4, 0], "active+remapped+backfilling")
            ],
        }
        store = FakeStore(snaps, commands=pm.SNAPSHOT_COMMANDS)
        rows = pm.plan(parse_args(pm, ["--osds", "3"]), store).rows
        self.assertEqual([("27.20", 0)], [(r.pgid, r.shard) for r in rows])

    def test_pgs_keeps_only_the_given_pgs(self):
        self.assertEqual(["5.1f", "27.10"], self.pgids("--pgs", "27.10", "5.1f"))

    def test_pgs_and_osds_must_both_match(self):
        self.assertEqual(["5.3"], self.pgids("--pgs", "5.3", "5.1f", "--osds", "3"))

    def test_pgs_names_ids_with_no_movement_on_stderr(self):
        # 27.a is clean, 99.1 does not exist: both matched nothing.
        out, err = self.run_main("--pgs", "5.3", "27.a", "99.1")
        err = " ".join(err.split())
        self.assertIn("NOTE: --pgs: 1 of 3 given PG id(s) have movement", err)
        self.assertIn("2 matched nothing (not moving, or a typo): 27.a, 99.1.", err)
        self.assertTrue(out.startswith("PGID"))

    def test_pgs_unmatched_is_judged_before_osds(self):
        # 5.3 moves, so it is not a typo even though --osds filters it out.
        out, err = self.run_main("--pgs", "5.3", "--osds", "1")
        self.assertIn("1 of 1 given PG id(s) have movement", err)
        self.assertNotIn("matched nothing", err)
        self.assertEqual("No PG movements match --osds/--pgs.\n", out)

    def test_no_note_without_pgs(self):
        _, err = self.run_main("--osds", "3")
        self.assertEqual("", err)


FIXTURE_STUCK_AT_100 = (
    REPO_ROOT
    / "tests"
    / "backfillctl"
    / "test-data"
    / "ceph1-backfills-stuck-at-100-pct"
)
FIXTURE_RESUMED = (
    REPO_ROOT
    / "tests"
    / "backfillctl"
    / "test-data"
    / "ceph1-resumed-backfills-exact-progress"
)


class FixtureReplayTest(unittest.TestCase):
    """Replay the real-cluster snapshot in tests/backfillctl/test-data (see its README.txt)."""

    def test_capture_without_positions_falls_back_on_counters(self):
        # 27.126 has two shards genuinely still backfilling despite its
        # counters reading 100% (see the fixture's README.txt). The capture
        # predates backfill_positions.json, so that is all there is to show.
        rows = [
            r
            for r in plan_from_state(pm, FIXTURE_STUCK_AT_100).rows
            if r.pgid == "27.126"
        ]
        self.assertEqual(
            [(100.0, False)] * 2, [(r.progress_pct, r.progress_exact) for r in rows]
        )

    def test_approx_note_appears_for_the_capture_without_positions(self):
        out = io.StringIO()
        args = parse_args(pm, [], load_state=str(FIXTURE_STUCK_AT_100))
        with contextlib.redirect_stdout(out):
            pm.run(args)
        self.assertIn("~ marks PROGRESS from Ceph's misplaced/degraded", out.getvalue())

    def test_resumed_backfills_show_their_real_progress(self):
        # 27.500's counters read 100%, its backfill position 6.6% (see the
        # fixture's README.txt); every PG of the capture has a position.
        rows = plan_from_state(pm, FIXTURE_RESUMED).rows
        self.assertTrue(all(r.progress_exact for r in rows))
        (pct,) = {r.progress_pct for r in rows if r.pgid == "27.500"}
        self.assertAlmostEqual(6.6, pct, 1)

    def test_shards_of_one_pg_show_their_own_progress(self):
        # 27.ae2: shard 9 almost done, shard 4 barely started (see the
        # fixture's README.txt); a per-PG average would show both at 53.8%.
        rows = [
            r for r in plan_from_state(pm, FIXTURE_RESUMED).rows if r.pgid == "27.ae2"
        ]
        pcts = {r.shard: round(r.progress_pct, 1) for r in rows}
        self.assertEqual({4: 9.9, 9: 97.7}, pcts)

    def test_no_approx_note_when_every_position_is_known(self):
        out = io.StringIO()
        args = parse_args(pm, [], load_state=str(FIXTURE_RESUMED))
        with contextlib.redirect_stdout(out):
            pm.run(args)
        self.assertNotIn("~ marks PROGRESS", out.getvalue())


if __name__ == "__main__":
    unittest.main()
