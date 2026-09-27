"""Unit tests for backfillctl's drain subcommand.

How shards are placed is shed's, tested in test_shed. These cover what drain
adds: which OSDs it empties (--osds, --hosts), its options, and what it
prints; then, by invariant, every proposal on cluster-sized captures.
"""

import contextlib
import io
import json
import subprocess
import sys
import unittest

from _support import (
    REPO_ROOT,
    TEST_DATA,
    FakeStore,
    SyntheticCluster,
    check_own_moves_in_or_out,
    check_pairs_apply,
    check_reservation_cap,
    flat,
    parse_args,
    shared,
)
from _support import shed as sh

from backfillctl import drain as dr

FIXTURE = TEST_DATA / "ceph1-backfills-stuck-at-100-pct"


class Cluster(SyntheticCluster):
    def plan(self, *argv) -> dr.DrainResult:
        """Run plan() on this cluster; leading bare OSD ids go to --osds."""
        return self.plan_with(dr, *argv)

    def rendered(self, *argv) -> tuple[str, str]:
        """Return (stdout, stderr with whitespace collapsed) of render() for plan(*argv)."""
        argv = [str(a) for a in argv]
        result = self.plan(*argv)
        if argv and not argv[0].startswith("--"):
            argv.insert(0, "--osds")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            dr.render(result, parse_args(dr, argv))
        return out.getvalue(), flat(err.getvalue())


class SourcesTest(unittest.TestCase):
    def test_osds_are_drained_in_full(self):
        c = Cluster()
        c.pg("1.0", [0, 10, 20]).pg("1.1", [0, 11, 21])
        result = c.plan(0).shed
        self.assertEqual((result.sources, result.level), ([0], None))
        self.assertEqual(len(result.moves), 2)

    def test_every_osd_of_the_host_is_drained(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.pg("1.0", [0, 10, 20]).pg("1.1", [11, 21, 30])
        result = c.plan("--hosts", "h1")
        self.assertEqual((result.shed.sources, result.hosts), ([10, 11], ["h1"]))
        self.assertEqual({m.up_osd for m in result.shed.moves}, {10, 11})
        self.assertFalse({Cluster.host(m.target_osd) for m in result.shed.moves} & {1})

    def test_fully_qualified_name_matches_the_short_one(self):
        result = Cluster().pg("1.0", [0, 10, 20]).plan("--hosts", "h1.example.org")
        self.assertEqual((result.shed.sources, result.hosts), ([10, 11], ["h1"]))

    def test_several_hosts(self):
        result = Cluster().pg("1.0", [0, 10, 20]).plan("--hosts", "h0", "h1")
        self.assertEqual(result.shed.sources, [0, 1, 10, 11])

    def test_host_of_mixed_classes_uses_targets_of_each_shards_class(self):
        c = Cluster()
        c.classes[1] = c.classes[31] = c.classes[41] = "ssd"
        c.util[31] = 30.0  # the emptiest ssd
        c.util[40] = 20.0  # the emptiest hdd
        c.pg("1.0", [0, 10, 20]).pg("1.1", [1, 11, 21])
        result = c.plan("--hosts", "h0").shed
        self.assertEqual(
            [(m.up_osd, m.target_osd) for m in result.moves], [(0, 40), (1, 31)]
        )

    def test_until_util_sets_the_level(self):
        result = Cluster().pg("1.0", [0, 10, 20]).plan(0, "--until-util", 60).shed
        self.assertEqual((result.level, result.kept_count), (60.0, 1))

    def test_unknown_osd_is_an_error(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan(99)
        self.assertEqual(
            str(cm.exception), "ERROR: --osds: not in 'ceph osd df': osd.99"
        )

    def test_unknown_host_is_an_error_naming_it(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--hosts", "h1", "nosuch")
        self.assertTrue(str(cm.exception).startswith("ERROR: --hosts: "))
        self.assertIn("nosuch", str(cm.exception))
        self.assertNotIn("h1", str(cm.exception).split(":")[-1])

    def test_max_target_util_above_backfillfull_is_an_error(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().pg("1.0", [0, 10, 20]).plan(0, "--max-target-util", 91)
        self.assertIn("backfillfull_ratio", str(cm.exception))


class ArgsTest(unittest.TestCase):
    def test_osds_and_hosts_are_mutually_exclusive_and_one_is_required(self):
        for argv in (["--osds", "0", "--hosts", "h1"], []):
            with (
                self.subTest(argv=argv),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as cm,
            ):
                parse_args(dr, argv)
            self.assertEqual(cm.exception.code, 2)

    def test_percent_options_refuse_a_ratio(self):
        for option in ("--until-util", "--max-target-util"):
            with self.subTest(option=option):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
                    parse_args(dr, ["--osds", "0", option, "0.7"])
                self.assertIn("not a ratio", err.getvalue())

    def test_until_util_refuses_zero(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
            parse_args(dr, ["--osds", "0", "--until-util", "0"])
        self.assertIn("must be above 0", err.getvalue())

    def test_bad_osds_and_hosts_fail_before_the_pg_dump(self):
        snapshots = Cluster().snapshots()
        del snapshots["pg_dump_pgs"]  # a KeyError if it were read
        for argv, error in (
            (["--hosts", "nosuch"], "ERROR: --hosts: "),
            (["--osds", "99"], "ERROR: --osds: "),
        ):
            with self.subTest(argv=argv), self.assertRaises(SystemExit) as cm:
                dr.plan(parse_args(dr, argv), FakeStore(snapshots))
            self.assertTrue(str(cm.exception).startswith(error))

    def test_pg_of_an_unknown_pool_elsewhere_is_ignored(self):
        c = Cluster().pg("1.0", [0, 10, 20])
        snapshots = c.snapshots()
        snapshots["pg_dump_pgs"].append(
            {
                "pgid": "9.0",
                "state": "active+clean",
                "up": [30, 40, 50],
                "acting": [30, 40, 50],
                "stat_sum": {"num_bytes": 1},
            }
        )
        result = dr.plan(parse_args(dr, ["--osds", "0"]), FakeStore(snapshots))
        self.assertEqual(len(result.shed.moves), 1)

    def test_removed_options_are_refused(self):
        for option in ("--max-target-uses", "--toofull-util"):
            with (
                self.subTest(option=option),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parse_args(dr, ["--osds", "0", option, "5"])


class RenderTest(unittest.TestCase):
    def test_summary_names_the_osds_targets_and_blockers(self):
        _, err = Cluster().pg("1.0", [0, 10, 20]).rendered(0)
        self.assertIn(
            "Draining osd.0: 1 shard(s) mapped to them (0 more already moving off). "
            "Targets: projected at or below --max-target-util 89%",
            err,
        )
        self.assertIn("at or above nearfull_ratio 85%.", err)
        self.assertIn(
            "Proposed 1 move(s) off the drained OSDs, 9.8 MiB, 0 unplaceable;", err
        )
        self.assertNotIn("level", err)
        self.assertNotIn("left alone", err)

    def test_summary_names_the_hosts(self):
        _, err = Cluster().pg("1.0", [0, 10, 20]).rendered("--hosts", "h1")
        self.assertIn("Draining host(s) h1 (osd.10, osd.11):", err)

    def test_summary_names_the_level_and_the_shards_left(self):
        c = Cluster(default_util=40.0)
        c.util[0] = 60.0
        c.pg("1.0", [0, 10, 20], shard_pct=6).pg("1.1", [0, 11, 21])
        _, err = c.rendered(0, "--until-util", 55)
        self.assertIn("Draining osd.0 to below --until-util 55%:", err)
        self.assertIn(
            "Proposed 1 move(s) off the drained OSDs, 58.6 MiB, 0 unplaceable, 1 "
            "left in place (their OSD is below the level);",
            err,
        )
        self.assertNotIn("stay at or above", err)

    def test_summary_names_osds_left_above_the_level(self):
        c = Cluster(default_util=95.0)
        c.util[0] = 70.0
        _, err = c.pg("1.0", [0, 10, 20]).rendered(0, "--until-util", 60)
        self.assertIn(
            "1 drained OSD(s) projected to stay at or above the 60% level: "
            "osd.0 (70.0%).",
            err,
        )
        self.assertIn("NOTE: 1 shard(s) found no target", err)

    def test_osd_without_a_size_is_named_as_drained_in_full(self):
        c = Cluster().pg("1.0", [0, 10, 20])
        snapshots = c.snapshots()
        for node in snapshots["osd_df"]["nodes"]:
            if node["id"] == 0:
                node["kb"] = 0
        args = parse_args(dr, ["--osds", "0", "--until-util", "60"])
        result = dr.plan(args, FakeStore(snapshots))
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            dr.render(result, args)
        self.assertIn(
            "No size in 'ceph osd df', so drained in full despite --until-util: osd.0.",
            flat(err.getvalue()),
        )

    def test_pgs_left_alone_are_counted(self):
        c = Cluster()
        c.pg("1.0", [0, 10, 20], state="active+undersized+degraded")
        _, err = c.pg("1.1", [0, 11, 21]).rendered(0)
        self.assertIn("PGs left alone: 1 not active, or degraded,", err)

    def test_nothing_to_drain_says_so(self):
        for argv, out_text in ((["0"], ""), (["0", "--pgremapper-mappings"], "[]\n")):
            with self.subTest(argv=argv):
                out, err = Cluster().rendered(*argv)
                self.assertEqual(out, out_text)
                self.assertEqual(err, "Nothing to drain: no shard is mapped to osd.0.")

    def test_nothing_to_drain_but_pgs_left_alone(self):
        c = Cluster().pg("1.0", [0, 10, 20], state="peering")
        _, err = c.rendered(0)
        self.assertTrue(
            err.startswith(
                "Nothing to drain: every PG with a shard mapped to osd.0 is left "
                "alone. PGs left alone: 1"
            ),
            err,
        )

    def test_table_and_json(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20])
        out, _ = c.rendered(0)
        self.assertEqual(
            out.splitlines()[1].split(),
            ["PGID", "SHARD", "SIZE", "OSD", "UTIL", "HOST"]
            + ["OSD", "UTIL", "PROJ", "HOST"] * 2
            + ["NOTE"],
        )
        out, _ = c.rendered(0, "--pgremapper-mappings")
        self.assertEqual(
            [(e["mapping"]["from"], e["role"]) for e in json.loads(out)],
            [(0, shared.ROLE_REQUESTED), (10, shared.ROLE_BLOCKER)],
        )


class FixtureInvariants:
    """Drain part of the capture in FIXTURE_DIR and check every proposal is sane.

    A mixin: each subclass names a capture and the arguments (ARGV) that say
    what to drain.
    """

    FIXTURE_DIR = FIXTURE
    ARGV: tuple[str, ...] = ()

    @classmethod
    def setUpClass(cls):
        args = parse_args(dr, list(cls.ARGV), load_state=str(cls.FIXTURE_DIR))
        store = shared.SnapshotStore.from_args(args, dr.SNAPSHOT_COMMANDS)
        cls.drain = dr.plan(args, store)
        cls.result = cls.drain.shed
        cls.OSDS = set(cls.result.sources)
        cls.pgs = {pg["pgid"]: pg for pg in shared.fetch_pg_stats(store, "pg_dump_pgs")}

    def by_pg(self) -> dict[str, list]:
        by_pg: dict[str, list] = {}
        for m in self.result.moves:
            by_pg.setdefault(m.pgid, []).append(m)
        return by_pg

    def test_every_shard_is_moved_unplaceable_or_kept(self):
        r = self.result
        moved = sum(m.up_osd in self.OSDS for m in r.moves)
        self.assertGreater(moved, 0)
        self.assertEqual(moved + len(r.unplaceable) + r.kept_count, r.mapped_count)

    def test_no_target_is_drained_acting_or_over_the_cap(self):
        # Covers pins too: none may send data back onto a drained OSD.
        for m in self.result.moves:
            self.assertNotIn(m.target_osd, self.OSDS)
            if m.target_projected is not None:
                self.assertNotIn(m.target_osd, self.pgs[m.pgid]["acting"])
        check_reservation_cap(self, self.FIXTURE_DIR, self.result)

    def test_new_up_sets_have_one_shard_per_host(self):
        host_of = self.result.osd_host
        for pgid, moves in self.by_pg().items():
            up = list(self.pgs[pgid]["up"])
            for m in moves:
                up[up.index(m.up_osd)] = m.target_osd
            real = [o for o in up if shared.is_real_osd(o)]
            with self.subTest(pgid=pgid):
                self.assertEqual(len({host_of[o] for o in real}), len(real))

    def test_no_pg_has_chained_pairs(self):
        check_pairs_apply(self, self.FIXTURE_DIR, self.result)

    def test_moves_take_data_off_an_osd_or_put_it_on_never_both(self):
        check_own_moves_in_or_out(self, self.result)

    def test_no_pg_left_alone_is_moved(self):
        for pgid in self.by_pg():
            with self.subTest(pgid=pgid):
                self.assertTrue(sh.is_settled(self.pgs[pgid]))


class Ceph1FixtureTest(FixtureInvariants, unittest.TestCase):
    """A calm cluster: nothing over the cap, so no blockers."""

    ARGV = ("--osds", "418", "511")

    def test_drained_in_full(self):
        r = self.result
        self.assertEqual((r.kept_count, r.unplaceable), (0, []))


class Ceph1UntilUtilFixtureTest(FixtureInvariants, unittest.TestCase):
    """The calm cluster, drained only to below 72% (from about 75%)."""

    ARGV = ("--osds", "418", "511", "--until-util", "72")

    def test_each_osd_sheds_only_while_at_or_above_the_level(self):
        # A shard is only moved off an OSD at or above the level, so before
        # its last (at most its largest) the OSD was still there. Blockers
        # diverted or pinned off it come after.
        r = self.result
        self.assertGreater(r.kept_count, 0)
        self.assertEqual(r.still_above, [])
        for osd in self.OSDS:
            with self.subTest(osd=osd):
                off = [m for m in r.moves if m.up_osd == osd]
                shed = [m.size_bytes for m in off if m.role == shared.ROLE_REQUESTED]
                blockers = sum(
                    m.size_bytes for m in off if m.role == shared.ROLE_BLOCKER
                )
                self.assertTrue(shed)
                capacity = r.osd_df[osd]["kb"] * shared.KIB
                final = r.final_util[osd]
                self.assertLess(final, 72)
                self.assertGreaterEqual(
                    final + (blockers + max(shed)) / capacity * 100, 72
                )

    def test_targets_end_below_the_level(self):
        for m in self.result.moves:
            if m.role == shared.ROLE_REQUESTED and m.target_projected is not None:
                self.assertLess(m.target_projected, 72)


class Ceph2EmergencyFixtureTest(FixtureInvariants, unittest.TestCase):
    """A cluster in a fullness emergency, where blockers abound."""

    FIXTURE_DIR = TEST_DATA / "divert-toofull-ceph2-util-emergency-2-new-hosts"
    ARGV = ("--osds", "883")

    def test_blockers_are_diverted_and_pinned(self):
        r = self.result
        self.assertGreater(r.diverted_count, 0)
        self.assertGreater(r.pinned_count, 0)
        self.assertTrue(any("companion of" in m.note for m in r.moves))

    def test_pg_unblocked_by_a_companion_is_not_reported_stuck(self):
        # 19.1f88: shard 6 is pinned as shard 4's companion, so the PG is
        # unblocked; revisiting shard 6 as a blocker of its own once called it
        # stuck.
        self.assertNotIn("19.1f88", self.result.stuck_pgs)


class Ceph2HostFixtureTest(FixtureInvariants, unittest.TestCase):
    """A whole host of the emergency cluster, osd.883's."""

    FIXTURE_DIR = Ceph2EmergencyFixtureTest.FIXTURE_DIR
    ARGV = ("--hosts", "host50")

    def test_nothing_goes_to_the_drained_host(self):
        host_of = self.result.osd_host
        self.assertGreater(len(self.OSDS), 1)
        for m in self.result.moves:
            self.assertNotEqual(host_of[m.target_osd], "host50")


class Ceph2HostUntilUtilFixtureTest(FixtureInvariants, unittest.TestCase):
    """The host, relieved to below 80% though most targets are fuller."""

    FIXTURE_DIR = Ceph2EmergencyFixtureTest.FIXTURE_DIR
    ARGV = ("--hosts", "host50", "--until-util", "80")

    def test_targets_may_end_above_the_level(self):
        # Most OSDs are fuller than 80%: a target need only end up below the
        # drained OSD it relieves.
        requested = [m for m in self.result.moves if m.role == shared.ROLE_REQUESTED]
        self.assertTrue(requested)
        self.assertTrue(all(m.target_projected >= 80 for m in requested))


class CliTest(unittest.TestCase):
    def test_runs_end_to_end_and_prints_json(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "backfillctl"),
                "--load-state",
                str(FIXTURE),
                "drain",
                "--osds",
                "418",
                "--pgremapper-mappings",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        entries = json.loads(proc.stdout)
        self.assertTrue(entries)
        self.assertTrue(all(e["mapping"]["from"] == 418 for e in entries))
        self.assertIn("Draining osd.418", proc.stderr)


if __name__ == "__main__":
    unittest.main()
