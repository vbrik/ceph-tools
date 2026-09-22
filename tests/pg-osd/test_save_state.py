"""Unit tests for backfillctl's save-state subcommand.

anonymize_snapshots is tested here for the union of fields every subcommand
reads (see save_state.py's module docstring); shared.anonymize_snapshots'
own general scrub (hostnames, fsid, ...) is tested in test_shared.py.

CrossSubcommandFixtureTest is the one that matters most: it replays a real,
already-committed fixture (tests/pg-osd/test-data/
cancel-backfill-ceph2-osd896-host-clash-companions/) that happens to
contain a genuine mix of PG states (688 remapped, of which 585 are also
backfill_toofull -- see its README.txt), so filtering pg_dump_pgs.json for
one flag or the other actually has something to exclude. That is the risk in
this design: a fixture containing only PGs that already match a filter can't
tell a correct filter from one that just returns everything.
"""

import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from _support import REPO_ROOT, FakeStore, parse_args, shared

from backfillctl import cancel_backfill as cb
from backfillctl import divert_toofull as dt
from backfillctl import save_state as ss

FIXTURE = (
    REPO_ROOT
    / "tests"
    / "pg-osd"
    / "test-data"
    / "cancel-backfill-ceph2-osd896-host-clash-companions"
)


def pg(pgid, state, num_bytes=1000, **extra):
    return {
        "pgid": pgid,
        "state": state,
        "up": [1, 2],
        "up_primary": 1,
        "acting": [1, 2],
        "acting_primary": 1,
        "stat_sum": {
            "num_objects": 10,
            "num_objects_misplaced": 0,
            "num_objects_degraded": 0,
            "num_bytes": num_bytes,
            "num_object_clones": 3,  # not in the keep-set: must be dropped
        },
        **extra,
    }


class AnonymizeTest(unittest.TestCase):
    def snapshots(self):
        return {
            "osd_tree": {"nodes": [], "stray": []},
            "osd_dump": {
                "fsid": "real-fsid",
                "erasure_code_profiles": {"p": {"k": "4", "m": "2"}},
                "full_ratio": 0.95,
                "backfillfull_ratio": 0.91,
                "nearfull_ratio": 0.85,
                "pg_upmap_items": [{"pgid": "19.1", "mappings": []}],
                "client_blocklist": {"10.1.2.3:0/1": 1},
            },
            "pool_ls_detail": [],
            "crush_rule_dump": [],
            "pg_dump_pgs": {"pg_stats": [pg("19.1", "active+remapped+backfill_wait")]},
        }

    def test_osd_dump_is_reduced_to_the_union_every_subcommand_reads(self):
        snaps = self.snapshots()
        ss.anonymize_snapshots(snaps)
        self.assertEqual(
            snaps["osd_dump"],
            {
                "erasure_code_profiles": {"p": {"k": "4", "m": "2"}},
                "full_ratio": 0.95,
                "backfillfull_ratio": 0.91,
                "nearfull_ratio": 0.85,
                "pg_upmap_items": [{"pgid": "19.1", "mappings": []}],
            },
        )

    def test_pg_stat_is_reduced_to_the_union_every_subcommand_reads(self):
        snaps = self.snapshots()
        ss.anonymize_snapshots(snaps)
        (kept,) = snaps["pg_dump_pgs"]["pg_stats"]
        self.assertEqual(
            kept,
            {
                "pgid": "19.1",
                "state": "active+remapped+backfill_wait",
                "up": [1, 2],
                "up_primary": 1,
                "acting": [1, 2],
                "acting_primary": 1,
                "stat_sum": {
                    "num_objects": 10,
                    "num_objects_misplaced": 0,
                    "num_objects_degraded": 0,
                    "num_bytes": 1000,
                },
            },
        )

    def test_idempotent(self):
        once = self.snapshots()
        ss.anonymize_snapshots(once)
        twice = json.loads(json.dumps(once))
        ss.anonymize_snapshots(twice)
        self.assertEqual(once, twice)

    def test_hosts_and_fsid_are_scrubbed(self):
        snaps = self.snapshots()
        snaps["osd_tree"]["nodes"].append(
            {"id": -1, "type": "host", "name": "real.example.org", "children": []}
        )
        ss.anonymize_snapshots(snaps)
        text = json.dumps(snaps)
        self.assertNotIn("real.example.org", text)
        self.assertNotIn("real-fsid", text)


class RunTest(unittest.TestCase):
    """run() end to end with a fake ceph_json, no live cluster or subprocess."""

    def canned(self):
        return {
            tuple(cmd): (
                {"pg_stats": [pg("19.1", "active+remapped+backfill_wait")]}
                if key == "pg_dump_pgs"
                else {"nodes": [], "stray": []}
                if key == "osd_tree"
                else {"fsid": "real-fsid", "erasure_code_profiles": {}}
                if key == "osd_dump"
                else []
            )
            for key, cmd in ss.SNAPSHOT_COMMANDS.items()
        }

    def test_writes_one_file_per_command_anonymized(self):
        canned = self.canned()
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(shared, "ceph_json", lambda cmd: canned[tuple(cmd)]),
        ):
            args = parse_args(ss, [tmp + "/snap"])
            ss.run(args)
            saved = {
                p.stem: json.loads(p.read_text())
                for p in pathlib.Path(tmp, "snap").glob("*.json")
            }
        self.assertEqual(set(saved), set(ss.SNAPSHOT_COMMANDS))
        self.assertNotIn("real-fsid", json.dumps(saved))

    def test_refuses_a_non_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            (pathlib.Path(tmp) / "old.json").write_text("{}")
            args = parse_args(ss, [tmp])
            with self.assertRaises(SystemExit) as ctx:
                ss.run(args)
        self.assertIn("not empty", str(ctx.exception))


class CrossSubcommandFixtureTest(unittest.TestCase):
    """One real capture, containing a genuine mix of PG states, replayed by
    two different subcommands' client-side filters (see module docstring)."""

    def load(self):
        return {p.stem: json.loads(p.read_text()) for p in FIXTURE.glob("*.json")}

    def test_fixture_actually_mixes_remapped_and_backfill_toofull_pgs(self):
        # Ground truth this test depends on (see the fixture's README.txt):
        # not every remapped PG here is backfill_toofull, so a filter that
        # just returned everything would be caught by the next two tests.
        pgs = self.load()["pg_dump_pgs"]["pg_stats"]
        remapped = [p for p in pgs if "remapped" in p["state"].split("+")]
        toofull = [p for p in pgs if "backfill_toofull" in p["state"].split("+")]
        self.assertEqual(len(remapped), 688)
        self.assertEqual(len(toofull), 585)
        self.assertLess(len(toofull), len(remapped))

    def test_cancel_backfill_filters_for_remapped(self):
        store = FakeStore(self.load())
        pgs = cb.fetch_remapped_pg_stats(store)
        self.assertEqual(len(pgs), 688)

    def test_divert_toofull_filters_for_backfill_toofull(self):
        store = FakeStore(self.load())
        pgs = dt.fetch_backfill_toofull_pg_stats(store)
        self.assertEqual(len(pgs), 585)

    def test_one_directory_serves_a_second_subcommand_it_was_not_captured_for(self):
        # This fixture's README documents its capture as a
        # cancel-backfill run; divert-toofull was never
        # involved, yet the same directory (now pg_dump_pgs.json-based) is
        # enough for it too -- the point of the unified save-state format.
        out = io.StringIO()
        args = parse_args(dt, [], load_state=str(FIXTURE))
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            dt.run(args)
        self.assertIn("585 backfill_toofull PG(s) cluster-wide", out.getvalue())


if __name__ == "__main__":
    unittest.main()
