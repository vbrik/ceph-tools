"""Unit tests for backfillctl's balance subcommand.

How shards are placed is shed's, tested in test_shed. These cover what
balance adds: the level (the class mean plus --max-deviation, or
--until-util), which OSDs are sources, and what it prints; then, by
invariant, every proposal on cluster-sized captures. Most tests run plan()
on a small synthetic cluster (_support.SyntheticCluster), whose 12 hdd OSDs
are at 50% unless a test says otherwise.

What drives the tests: the promise that no move raises the class's highest
projected utilization, or takes another OSD to the level, is easy to break
silently.
"""

import contextlib
import io
import json
import unittest
from itertools import pairwise
from typing import ClassVar
from unittest import mock

from _support import (
    EC_POOL_DETAIL,
    EC_PROFILES,
    KB,
    NONE,
    PCT,
    REP_POOL_DETAIL,
    TEST_DATA,
    FakeStore,
    SyntheticCluster,
    check_one_shard_per_host,
    check_own_moves_in_or_out,
    check_pairs_apply,
    check_reservation_cap,
    flat,
    mapped_share_pct,
    messages,
    osd_df_of,
    parse_args,
    placement,
    plan_from_state,
    replayed_cluster,
    run_command,
    shared,
    stderr_of,
)
from _support import shed as sh

from backfillctl import balance as bal


class Cluster(SyntheticCluster):
    def plan(self, *argv) -> bal.BalanceResult:
        """Run plan() on this cluster; leading bare OSD ids go to --osds."""
        return self.plan_with(bal, *argv)

    def rendered(self, *argv) -> tuple[str, str]:
        """render()'s (stdout, stderr) for plan(*argv)."""
        return self.rendered_with(bal, *argv)


def pairs(result: bal.BalanceResult) -> list[tuple[str, object, int, int]]:
    return [(m.pgid, m.shard, m.up_osd, m.target_osd) for m in result.shed.moves]


# ---------------------------------------------------------------------------
# The level and the sources
# ---------------------------------------------------------------------------


class LevelTest(unittest.TestCase):
    def cluster(self) -> Cluster:
        c = Cluster()
        c.util[0] = 80.0  # the class mean: (11 * 50 + 80) / 12 = 52.5%
        return c.pg("1.0", [0, 10, 20])

    def test_default_is_the_class_mean(self):
        # As even as possible, though the mean itself is out of reach.
        result = self.cluster().plan()
        self.assertEqual(bal.DEFAULT_MAX_DEVIATION, 0)
        self.assertEqual((result.mean, result.shed.level), (52.5, 52.5))
        self.assertEqual(result.shed.sources, [0])
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])  # own host allowed

    def test_max_deviation(self):
        self.assertEqual(self.cluster().plan("--max-deviation", 0.5).shed.level, 53.0)
        self.assertEqual(self.cluster().plan("--max-deviation", 2).shed.level, 54.5)

    def test_until_util_overrides(self):
        result = self.cluster().plan("--until-util", 70)
        self.assertEqual((result.shed.level, result.shed.sources), (70.0, [0]))

    def test_until_util_and_max_deviation_are_mutually_exclusive(self):
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as cm,
        ):
            parse_args(bal, ["--until-util", "70", "--max-deviation", "1"])
        self.assertEqual(cm.exception.code, 2)

    def test_percent_options_refuse_a_ratio(self):
        for option in ("--until-util", "--max-target-util"):
            with self.subTest(option=option):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
                    parse_args(bal, [option, "0.9"])
                self.assertIn("not a ratio", err.getvalue())

    def test_removed_options_are_refused(self):
        for option in ("--max-target-uses", "--max-moves", "--min-source-util"):
            with (
                self.subTest(option=option),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parse_args(bal, [option, "5"])

    def test_mean_is_of_final_projections(self):
        # A backfill in motion within the class leaves the mean where it is,
        # but its target counts as full as it will be.
        c = Cluster()
        c.util[51] = 10.0  # (11 * 50 + 10) / 12 = 46.7%
        c.pg("1.0", [51, 10, 20], [0, 10, 20], shard_pct=45.0)
        result = c.plan("--until-util", 54)
        self.assertAlmostEqual(result.mean, 560 / 12)
        self.assertEqual(result.shed.sources, [51])  # 55% once it lands


class SourcesTest(unittest.TestCase):
    def test_osds_are_the_sources_even_below_the_level(self):
        c = Cluster().pg("1.0", [0, 10, 20])
        result = c.plan(0, "--max-deviation", 2)
        self.assertEqual(result.shed.sources, [0])
        self.assertEqual((result.shed.moves, result.shed.kept_count), ([], 1))
        c.util[0] = 80.0
        self.assertEqual(pairs(c.plan(0, "--max-deviation", 2)), [("1.0", 0, 0, 1)])

    def test_osds_not_in_the_class_are_named(self):
        c = Cluster()
        c.classes[40] = "ssd"
        with self.assertRaises(SystemExit) as cm:
            c.plan(0, 40)
        self.assertIn("not among the up and in hdd OSDs", str(cm.exception))
        self.assertIn("osd.40", str(cm.exception))
        self.assertNotIn("osd.0,", str(cm.exception))

    def test_osds_unknown_to_ceph_are_named_as_elsewhere(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--osds", "0", "999")
        self.assertEqual(
            str(cm.exception), "ERROR: --osds: not in 'ceph osd df': osd.999"
        )

    def test_bad_class_and_osds_fail_before_the_pg_dump(self):
        snapshots = Cluster().snapshots()
        del snapshots["pg_dump_pgs"]  # a KeyError if it were read
        for argv, error in (
            (["--class", "ssd"], "ERROR: --class: "),
            (["--osds", "999"], "ERROR: --osds: "),
        ):
            with self.subTest(argv=argv), self.assertRaises(SystemExit) as cm:
                bal.plan(parse_args(bal, argv), FakeStore(snapshots))
            self.assertTrue(str(cm.exception).startswith(error))

    def test_level_binds_the_balanced_class_only(self):
        c = Cluster()
        c.util[0] = 80.0
        self.assertEqual(c.pg("1.0", [0, 10, 20]).plan().shed.level_class, "hdd")

    def test_unknown_class_names_the_classes_present(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--class", "ssd")
        self.assertIn("hdd", str(cm.exception))

    def test_no_osd_at_or_above_the_level(self):
        result = Cluster().pg("1.0", [0, 10, 20]).plan("--max-deviation", 2)
        self.assertEqual((result.shed.sources, result.shed.moves), ([], []))

    def test_other_classes_are_neither_sources_targets_nor_the_max(self):
        c = Cluster()
        c.util[0] = 80.0  # hdd mean: (9 * 50 + 80) / 10 = 53%
        c.util[40] = 95.0  # fullest, but ssd
        c.util[41] = 1.0  # emptiest, but ssd
        c.classes[40] = c.classes[41] = "ssd"
        result = c.pg("1.0", [0, 10, 20]).plan()
        self.assertEqual((result.mean, result.class_size), (53.0, 10))
        self.assertEqual(result.shed.sources, [0])
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])
        self.assertEqual(result.max_before, (80.0, 0))

    def test_ssd_class(self):
        c = Cluster()
        for o in (40, 41, 50, 51):
            c.classes[o] = "ssd"
        c.util[40] = 80.0
        result = c.pg("1.0", [40, 10, 20]).plan("--class", "ssd")
        self.assertEqual(result.shed.sources, [40])
        self.assertEqual(pairs(result), [("1.0", 0, 40, 41)])


# ---------------------------------------------------------------------------
# Shards in motion (placement's projections, which balance's level rests on)
# ---------------------------------------------------------------------------


class DepartingOsdsTest(unittest.TestCase):
    def test_ec_slot_moving_departs_its_acting_osd(self):
        pg = {"up": [4, 2, 3], "acting": [1, 2, 3]}
        self.assertEqual(placement.departing_osds(pg, True), [1])

    def test_ec_shard_with_empty_up_slot_stays(self):
        pg = {"up": [NONE, 2, 3], "acting": [1, 2, 3]}
        self.assertEqual(placement.departing_osds(pg, True), [])

    def test_ec_empty_acting_slot_departs_nothing(self):
        pg = {"up": [4, 2, 3], "acting": [NONE, 2, 3]}
        self.assertEqual(placement.departing_osds(pg, True), [])

    def test_replicated_compares_sets(self):
        pg = {"up": [3, 2, 4], "acting": [1, 2, 3]}
        self.assertEqual(placement.departing_osds(pg, False), [1])


class FinalUsageTest(unittest.TestCase):
    def test_arrivals_added_departures_credited(self):
        final = placement.FinalUsage(
            osd_df_of({1: 50.0, 2: 50.0}), [(1, 10 * PCT)], [(2, 5 * PCT)]
        )
        self.assertEqual(final.utilization(1), 60.0)
        self.assertEqual(final.utilization(2), 45.0)
        self.assertEqual(final.utilization(2, 3 * PCT), 48.0)

    def test_move(self):
        final = placement.FinalUsage(osd_df_of({1: 50.0, 2: 50.0}), [], [])
        final.move(1, 2, 10 * PCT)
        self.assertEqual((final.utilization(1), final.utilization(2)), (40.0, 60.0))

    def test_project_usage_credits_departures_in_final_only(self):
        pg = {
            "pgid": "19.0",
            "up": [4, 2, 3],
            "acting": [1, 2, 3],
            "stat_sum": {"num_bytes": 40 * PCT},  # 10% shards (k=4)
        }
        osd_df = osd_df_of({1: 50.0, 2: 50.0, 3: 50.0, 4: 50.0})
        reservation, final = placement.project_usage(
            osd_df, [pg], {19: EC_POOL_DETAIL}, EC_PROFILES, {}
        )
        self.assertEqual(reservation.utilization_after(4, 0), 60.0)
        self.assertEqual(reservation.utilization_after(1, 0), 50.0)
        self.assertEqual((final.utilization(4), final.utilization(1)), (60.0, 40.0))

    def test_move_skips_an_osd_without_a_capacity(self):
        final = placement.FinalUsage(osd_df_of({1: 50.0}), [], [])
        final.move(1, 99, 10 * PCT)
        final.move(99, 1, 5 * PCT)
        self.assertEqual(final.utilization(1), 45.0)
        self.assertFalse(final.knows(99))

    def test_mean_is_capacity_weighted(self):
        osd_df = osd_df_of({1: 50.0, 2: 80.0, 3: None})
        osd_df[2].update(kb=3 * KB, kb_used=2.4 * KB)
        osd_df[3]["kb"] = 0  # no size: left out
        final = placement.FinalUsage(osd_df, [], [])
        self.assertEqual(final.mean_utilization([1, 2, 3]), 72.5)

    def test_mean_of_nothing_sized_is_an_error(self):
        final = placement.FinalUsage(osd_df_of({1: 50.0}), [], [])
        with self.assertRaises(SystemExit):
            final.mean_utilization([99])


class PartlyCopiedTest(unittest.TestCase):
    """An arriving shard's part copied so far is in kb_used already: counted once.

    19.0 (EC 4+2, pg_num 16) moves its 10% shard 0 from osd.1 to osd.4, whose
    position 08000000 is halfway through the PG, so the 55% osd.4 uses
    includes 5 points of it.
    """

    PG: ClassVar = {
        "pgid": "19.0",
        "up": [4, 2, 3],
        "acting": [1, 2, 3],
        "stat_sum": {"num_bytes": 40 * PCT},
    }
    SHARD = placement.ArrivingShard("19.0", 0, 4, 1, [], 10 * PCT)

    def project(self, position: str | None = "08000000"):
        osd_df = osd_df_of({1: 50.0, 2: 50.0, 3: 50.0, 4: 55.0})
        positions = {} if position is None else {"19.0": {"4(0)": position}}
        return placement.project_usage(
            osd_df, [self.PG], {19: EC_POOL_DETAIL}, EC_PROFILES, positions
        )

    def test_only_the_part_still_to_come_is_added(self):
        reservation, final = self.project()
        self.assertAlmostEqual(reservation.utilization_after(4, 0), 60.0)
        self.assertAlmostEqual(final.utilization(4), 60.0)
        # The source keeps its whole copy until the backfill completes.
        self.assertAlmostEqual(reservation.utilization_after(1, 0), 50.0)
        self.assertAlmostEqual(final.utilization(1), 40.0)

    def test_a_finished_copy_adds_nothing(self):
        reservation, final = self.project("MAX")
        self.assertAlmostEqual(reservation.utilization_after(4, 0), 55.0)
        self.assertAlmostEqual(final.utilization(4), 55.0)

    def test_no_known_position_adds_the_whole_shard(self):
        # MIN; none; a key outside 19.0's range (its hash bits read 9, not 0).
        for position in ("MIN", None, "98000000"):
            with self.subTest(position):
                reservation, final = self.project(position)
                self.assertAlmostEqual(reservation.utilization_after(4, 0), 65.0)
                self.assertAlmostEqual(final.utilization(4), 65.0)

    def test_replica_positions_are_by_osd(self):
        pg = {**self.PG, "pgid": "7.0", "stat_sum": {"num_bytes": 10 * PCT}}
        osd_df = osd_df_of({1: 50.0, 2: 50.0, 3: 50.0, 4: 55.0})
        pool = {**REP_POOL_DETAIL, "pg_num": 16}
        reservation, _ = placement.project_usage(
            osd_df, [pg], {7: pool}, EC_PROFILES, {"7.0": {"4": "08000000"}}
        )
        self.assertAlmostEqual(reservation.utilization_after(4, 0), 60.0)

    def test_cancel_takes_off_only_the_part_still_to_come(self):
        # The copied part is data leaving osd.4: the reservation never credits
        # that. The shard is looked up, as pins and moves are built afresh.
        reservation, _ = self.project()
        reservation.cancel(self.SHARD)
        self.assertAlmostEqual(reservation.utilization_after(4, 0), 55.0)
        reservation.uncancel(self.SHARD)
        self.assertAlmostEqual(reservation.utilization_after(4, 0), 60.0)

    def test_redirect_puts_the_whole_shard_on_its_new_target(self):
        reservation, _ = self.project()
        mapped = placement.MappedShard("19.0", 0, 4, 1, 10 * PCT)
        reservation.redirect(mapped, 2)
        self.assertAlmostEqual(reservation.utilization_after(4, 0), 55.0)
        self.assertAlmostEqual(reservation.utilization_after(2, 0), 60.0)

    def test_an_osd_filled_by_copies_under_way_is_not_a_source(self):
        # osd.41's 55% holds half of a 10% shard still arriving: it ends at
        # 60%, below the level, where counting the shard in full gives 65%.
        c = Cluster()
        c.util[41] = 55.0
        c.pg("1.9", [41, 11, 21], [51, 11, 21], shard_pct=10)
        c.positions = {"1.9": {"41(0)": "98000000"}}
        result = c.plan("--until-util", "62")
        self.assertEqual(result.shed.sources, [])
        c.positions = {}
        self.assertEqual(c.plan("--until-util", "62").shed.sources, [41])


# ---------------------------------------------------------------------------
# plan() on the synthetic cluster
# ---------------------------------------------------------------------------


class PlanTest(unittest.TestCase):
    def test_settled_shard_moves_to_least_utilized_legal_osd(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20]).plan("--max-deviation", 2)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.max_before, (80.0, 0))
        self.assertEqual(result.max_after, (79.0, 0))
        self.assertEqual(result.shed.still_above, [(0, 79.0)])

    def test_ties_at_the_max_are_both_relieved(self):
        c = Cluster()
        c.util[0] = c.util[10] = 80.0
        c.pg("1.0", [0, 20, 30]).pg("1.1", [10, 20, 30])
        result = c.plan()
        self.assertEqual({m.up_osd for m in result.shed.moves}, {0, 10})
        self.assertEqual(result.max_after, (79.0, 0))

    def test_unsettled_pgs_are_left_alone(self):
        c = Cluster()
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20], state="active+undersized+degraded")
        result = c.plan()
        self.assertEqual((result.shed.moves, result.shed.unsettled_pgs), ([], 1))

    def test_blocker_of_a_moved_shard_is_diverted(self):
        # osd.10 is not a source (--osds), and is full: shard 1's backfill
        # onto it would hold the PG, and shard 0's move, in backfill_toofull.
        c = Cluster()
        c.util[0] = 80.0
        c.util[10] = 95.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1), ("1.0", 1, 10, 30)])
        self.assertEqual(result.shed.moves[1].role, shared.ROLE_BLOCKER)
        self.assertEqual(result.shed.diverted_count, 1)

    def test_no_target_is_taken_to_the_level(self):
        # Level 54.5%: a 5% shard would take any OSD at 50% to 55%.
        c = Cluster()
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20], shard_pct=5.0)
        result = c.plan("--max-deviation", 2)
        self.assertEqual((result.shed.moves, len(result.shed.unplaceable)), ([], 1))
        c.util[31] = 49.0  # (11 * 50 + 80 - 1) / 12 + 2 = 54.42%; 54% after
        self.assertEqual(pairs(c.plan("--max-deviation", 2)), [("1.0", 0, 0, 31)])


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


class RenderTest(unittest.TestCase):
    def cluster(self) -> Cluster:
        c = Cluster()
        c.util[0] = 80.0
        return c.pg("1.0", [0, 10, 20]).pg("2.0", [10, 0, 20])

    def test_table_and_summary(self):
        out, err = self.cluster().rendered("--max-deviation", 2)
        lines = out.splitlines()
        self.assertEqual(
            lines[1].split(),
            ["PGID", "SHARD", "SIZE", "OSD", "UTIL", "HOST"]
            + ["OSD", "UTIL", "PROJ", "HOST"] * 2
            + ["NOTE"],
        )
        self.assertEqual(lines[2].split()[:2], ["1.0", "0"])
        self.assertIn(
            "Balancing hdd to below 54.5% (the class mean 52.5% + --max-deviation 2). "
            "Sources: 1 of 12 up and in hdd OSD(s), at or above it; 2 shard(s) "
            "mapped to them (0 more already moving off).",
            err,
        )
        self.assertIn("Proposed 2 move(s) off the sources, 19.5 MiB,", err)
        self.assertIn("80.0% (osd.0) -> 78.0% (osd.0).", err)
        self.assertIn(
            "1 source(s) projected to stay at or above the 54.5% level: osd.0 (78.0%).",
            err,
        )

    def test_until_util_and_osds_are_named(self):
        _, err = self.cluster().rendered(0, "--until-util", 70)
        self.assertIn("Balancing hdd to below 70% (--until-util).", err)
        self.assertIn("1 of 12 up and in hdd OSD(s), given by --osds;", err)

    def test_pgremapper_mappings(self):
        out, _ = self.cluster().rendered("--pgremapper-mappings")
        self.assertEqual(
            [(e["pgid"], e["mapping"], e["role"]) for e in json.loads(out)],
            [
                ("1.0", {"from": 0, "to": 1}, shared.ROLE_REQUESTED),
                # osd.1 is taken by then: 51% final, and h1 is the PG's.
                ("2.0", {"from": 0, "to": 30}, shared.ROLE_REQUESTED),
            ],
        )

    def test_balancer_note_closes_a_run_that_moves_something(self):
        note = stderr_of(messages.print_balancer_note)
        for argv in ([], ["--pgremapper-mappings"]):
            with self.subTest(argv=argv):
                _, err = self.cluster().rendered(*argv)
                self.assertTrue(err.endswith(note), err)

    def test_no_sources_says_so(self):
        for argv, out_text in (([], ""), (["--pgremapper-mappings"], "[]\n")):
            with self.subTest(argv=argv):
                out, err = (
                    Cluster()
                    .pg("1.0", [0, 10, 20])
                    .rendered("--max-deviation", 2, *argv)
                )
                self.assertEqual(out, out_text)
                self.assertTrue(
                    err.endswith(
                        "No hdd OSD is at or above the level: nothing to move."
                    ),
                    err,
                )


# ---------------------------------------------------------------------------
# Invariants on captured clusters
# ---------------------------------------------------------------------------


class FixtureInvariantTest(unittest.TestCase):
    """Replay balance on real captures and check every proposal is sane."""

    FIXTURES: ClassVar = [
        "ceph1-resumed-backfills-exact-progress",
        "ceph2-37-filling-with-partial-backfills",
        "divert-toofull-ceph2-util-emergency-2-new-hosts",
        "divert-toofull-osd263-existing-upmap-chain",
    ]

    @classmethod
    def setUpClass(cls):
        # Each replay takes a fraction of a second; the tests share them.
        cls.traced = {fixture: cls.run_traced(fixture) for fixture in cls.FIXTURES}

    @staticmethod
    def run_traced(fixture, *argv):
        """Return (result, [(the class max after each change, made by a move?)]).

        Traced at FinalUsage.move, so pins count too: they cancel a departure
        Ceph is refusing, which may leave the acting OSD fuller than projected.
        """
        store = shared.SnapshotStore.from_args(
            parse_args(bal, list(argv), load_state=str(TEST_DATA / fixture)),
            bal.SNAPSHOT_COMMANDS,
        )
        candidates = placement.build_candidate_osds(shared.fetch_osd_df(store))
        osds = candidates[bal.DEFAULT_CLASS]
        trace = []
        in_commit = [False]
        move, commit = placement.FinalUsage.move, sh.Planner.commit

        def traced_move(final, from_osd, to_osd, size_bytes):
            move(final, from_osd, to_osd, size_bytes)
            trace.append((max(final.utilization(o) for o in osds), in_commit[0]))

        def traced_commit(*args):
            in_commit[0] = True
            try:
                commit(*args)
            finally:
                in_commit[0] = False

        with (
            mock.patch.object(placement.FinalUsage, "move", traced_move),
            mock.patch.object(sh.Planner, "commit", traced_commit),
        ):
            result = plan_from_state(bal, TEST_DATA / fixture, *argv)
        return result, trace

    def test_invariants(self):
        for fixture in self.FIXTURES:
            with self.subTest(fixture):
                self.check(fixture, *self.traced[fixture])

    def check(self, fixture, result, trace):
        before = result.max_before[0]
        # No move raises the class max; only a pin may.
        for (prev, _), (cur, by_move) in pairwise([(before, True), *trace]):
            if by_move:
                self.assertLessEqual(cur, prev + 1e-9)
        self.assertLessEqual(result.max_after[0], before)

        r = result.shed
        fixture_dir = TEST_DATA / fixture
        check_own_moves_in_or_out(self, r)
        check_pairs_apply(self, fixture_dir, r)
        check_reservation_cap(self, fixture_dir, r)
        check_one_shard_per_host(self, fixture_dir, r)
        cluster = replayed_cluster(fixture_dir)
        pgs = {pg["pgid"]: pg for pg in cluster.pgs}
        sources = set(r.sources)
        for m in r.moves:
            if sh.is_pin(m):  # a pin may land on a source
                continue
            self.assertNotIn(m.target_osd, sources)
            # The relief rule, once everything is placed.
            self.assertLess(m.target_projected, m.up_projected, m)
            if m.role == shared.ROLE_REQUESTED:
                self.assertIn(m.up_osd, sources)
                self.assertNotIn(m.target_osd, pgs[m.pgid]["acting"])
                self.assertNotIn(m.target_osd, pgs[m.pgid]["up"])
                # Nor in the raw CRUSH mapping: Ceph drops such an upmap.
                existing = cluster.existing_pairs(pgs[m.pgid])
                self.assertNotIn(m.target_osd, {f for f, _ in existing})
                self.assertLess(m.target_projected, r.level)

    def test_a_busy_cluster_gets_relieved(self):
        result, trace = self.traced["ceph1-resumed-backfills-exact-progress"]
        self.assertGreater(len(trace), 10)
        self.assertLess(result.max_after[0], result.max_before[0])

    def test_a_pin_may_leave_its_acting_osd_above_the_level(self):
        # A blocker leaving for a full OSD, pinned back, stays where it is:
        # there, the class max. Refusing the pin would leave the PG stuck. At
        # the default level, a source left above it is the max instead.
        result, trace = self.run_traced(
            "divert-toofull-ceph2-util-emergency-2-new-hosts", "--max-deviation", "2"
        )
        self.assertTrue(any(not by_move for _, by_move in trace))
        util, osd = result.max_after
        self.assertGreater(util, result.shed.level)
        self.assertIn(
            osd,
            {
                m.target_osd
                for m in result.shed.moves
                if m.role == shared.ROLE_BLOCKER and sh.is_pin(m)
            },
        )


class PartlyCopiedFixtureTest(unittest.TestCase):
    """Backfills under way, on real captures.

    FIXTURE: host37, added empty and half filled by balance. Counting the part
    copied twice put its OSDs at 93-99.8% (see the fixture's README.txt).
    """

    FIXTURE = TEST_DATA / "ceph2-37-filling-with-partial-backfills"
    NEW_HOST = range(900, 926)

    def test_new_osds_end_up_where_their_shards_take_them(self):
        final = replayed_cluster(self.FIXTURE).final
        for osd, pct in mapped_share_pct(self.FIXTURE, self.NEW_HOST).items():
            with self.subTest(osd=osd):
                self.assertAlmostEqual(final.utilization(osd), pct, delta=1.0)

    def test_the_new_host_is_not_a_source(self):
        result = plan_from_state(bal, self.FIXTURE)
        self.assertTrue(result.shed.sources)
        self.assertFalse(set(result.shed.sources) & set(self.NEW_HOST))

    def test_a_capture_without_positions_is_noted_once(self):
        fixture = TEST_DATA / "divert-toofull-ceph2-util-emergency-2-new-hosts"
        err = flat(run_command(bal, load_state=fixture, check=True).stderr)
        self.assertEqual(1, err.count(f"has no {shared.BACKFILL_POSITIONS_FILE}"))
        self.assertIn(flat(messages.PROJECTION_QUERY_EFFECT), err)


if __name__ == "__main__":
    unittest.main()
