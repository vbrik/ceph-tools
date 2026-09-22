"""Unit tests for pg-movements.py.

The progress arithmetic and OSD-slot helpers it shares with the other scripts
are tested in test_shared.py. Here: how PG states are classified and
abbreviated, and main() end to end over canned snapshots, which pins how
EC (positional) and replicated (set-difference) PGs turn into rows.
"""

import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from _support import load_script, shared

pm = load_script("pg-movements.py")

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
        {"pool_id": 5, "type": 1, "size": 3},
        {"pool_id": 27, "type": 3, "size": 4},
    ],
}


class MainTest(unittest.TestCase):
    def run_main(self, *argv, snapshots=SNAPSHOTS):
        out = io.StringIO()
        with (
            mock.patch.object(
                shared.SnapshotStore, "json", lambda self, key: snapshots[key]
            ),
            mock.patch("sys.argv", ["pg-movements.py", *argv]),
            contextlib.redirect_stdout(out),
        ):
            pm.main()
        return out.getvalue()

    def rows(self, out):
        lines = out.splitlines()
        return [
            line.split() for line in lines[2 : lines.index("") if "" in lines else None]
        ]

    def test_only_moving_pgs_get_rows_in_pg_order(self):
        rows = self.rows(self.run_main())
        self.assertEqual(["5.3", "5.1f", "27.9", "27.10"], [r[0] for r in rows])

    def test_ec_row_names_the_shard_and_both_osds(self):
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("27.10"))
        self.assertRegex(
            line, r"^27\.10\s+1\s+3\(ceph2,89%\)\s+->\s+2\(ceph2,70%\)\s+backfill\s+0%"
        )

    def test_progress_denominator_counts_unassigned_shards(self):
        # 27.9: one shard moving plus one with no OSD anywhere = 2 copies to
        # place, so 100 degraded objects of 200 copy-units is 50% done; counting
        # only the moving shard would clamp to 0%.
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("27.9"))
        self.assertIn(" 50% ", line)

    def test_progress_denominator_of_a_single_moving_shard(self):
        # 27.10: 150 of 100 * 1 copy-units misplaced is clamped to 0%; wrongly
        # counting a second copy would read 25%.
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("27.10"))
        self.assertIn(" 0% ", line)

    def test_replicated_reorder_is_not_reported(self):
        self.assertNotIn("5.4", self.run_main())

    def test_degraded_replica_shows_the_primary_as_the_worker(self):
        out = self.run_main()
        line = next(ln for ln in out.splitlines() if ln.startswith("5.1f"))
        self.assertIn("0(ceph1)*", line)
        self.assertIn("* this is the PG's primary OSD", out)

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
        out = self.run_main(snapshots=snaps)
        self.assertIn("4(h2,33%)", out)
        self.assertIn("3(h2,?%)", out)

    def test_no_movement(self):
        snaps = {**SNAPSHOTS, "pg_dump_pgs": [PGS[2]]}
        self.assertEqual("No PG movements detected.\n", self.run_main(snapshots=snaps))

    def test_pg_stats_not_ready_is_an_error_naming_the_command(self):
        snaps = {**SNAPSHOTS, "pg_dump_pgs": {"pg_ready": False}}
        with self.assertRaises(SystemExit) as ctx:
            self.run_main(snapshots=snaps)
        self.assertIn("ceph pg dump pgs", str(ctx.exception))

    def test_sort_by_destination(self):
        rows = self.rows(self.run_main("--sort-by", "to-osd"))
        self.assertEqual("5.3", rows[-1][0])  # osd.3 is the highest destination

    def test_save_state_keeps_only_what_the_script_reads_and_replays_identically(self):
        # Hostnames already in anonymized form map to themselves, so the
        # replay is comparable to the live run.
        snaps = {
            **SNAPSHOTS,
            "osd_tree": json.loads(
                json.dumps(SNAPSHOTS["osd_tree"])
                .replace("ceph1.example.org", "host01")
                .replace("ceph2.example.org", "host02")
            ),
            "pg_dump_pgs": {
                "pg_stats": [
                    {
                        **p,
                        "stat_sum": {**p["stat_sum"], "num_bytes": 4096},
                        "acting_recovery_backfill": ["0(0)"],
                        "last_scrub_stamp": "2026-01-01T00:00:00",
                    }
                    for p in PGS
                ]
            },
        }
        by_command = {
            tuple(cmd): snaps[key] for key, cmd in pm.SNAPSHOT_COMMANDS.items()
        }
        with tempfile.TemporaryDirectory() as tmp:
            out = io.StringIO()
            with (
                mock.patch.object(shared, "ceph_json", lambda c: by_command[tuple(c)]),
                mock.patch(
                    "sys.argv", ["pg-movements.py", "--save-state", tmp + "/snap"]
                ),
                contextlib.redirect_stdout(out),
            ):
                pm.main()
            saved = json.loads(
                (pathlib.Path(tmp) / "snap" / "pg_dump_pgs.json").read_text()
            )
            self.assertEqual(
                {"pgid", "state", "up", "acting", "acting_primary", "stat_sum"},
                set().union(*(pg.keys() for pg in saved["pg_stats"])),
            )
            self.assertEqual(
                {"num_objects", "num_objects_misplaced", "num_objects_degraded"},
                set().union(*(pg["stat_sum"].keys() for pg in saved["pg_stats"])),
            )
            replayed = io.StringIO()
            with (
                mock.patch.object(shared, "ceph_json", side_effect=AssertionError),
                mock.patch(
                    "sys.argv", ["pg-movements.py", "--load-state", tmp + "/snap"]
                ),
                contextlib.redirect_stdout(replayed),
            ):
                pm.main()
        self.assertEqual(out.getvalue(), replayed.getvalue())

    def test_load_state_reads_the_saved_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            for key, data in SNAPSHOTS.items():
                (pathlib.Path(tmp) / f"{key}.json").write_text(json.dumps(data))
            out = io.StringIO()
            with (
                mock.patch.object(shared, "ceph_json", side_effect=AssertionError),
                mock.patch("sys.argv", ["pg-movements.py", "--load-state", tmp]),
                contextlib.redirect_stdout(out),
            ):
                pm.main()
        self.assertIn("4 shard movement(s) across 4 PG(s).", out.getvalue())


if __name__ == "__main__":
    unittest.main()
