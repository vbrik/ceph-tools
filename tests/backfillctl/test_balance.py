"""Unit tests for backfillctl's balance subcommand.

Most tests run plan() on a small synthetic cluster (_support.SyntheticCluster).
By default all 12 OSDs are at 50%, so sources (the fuller half) are the
fullest OSD and, among ties, the lowest ids.

What drives the tests: the one promise balance makes, that no move raises
the class's highest projected utilization, is easy to break silently. The
two projections must not be confused: the --max-target-util cap counts
arrivals only (Ceph checks at reservation), while ranking and no-inversion
use the final state, with departures credited. A shard in flight onto a
source must be redirected, not counted twice. So these are checked on
hand-built corner cases and, by invariant, on cluster-sized captures.
"""

import contextlib
import io
import json
import unittest
from itertools import pairwise
from typing import ClassVar
from unittest import mock

from _support import (
    KB,
    PCT,
    REPO_ROOT,
    SyntheticCluster,
    parse_args,
    placement,
    plan_from_state,
    shared,
)

from backfillctl import balance as ut

TEST_DATA = REPO_ROOT / "tests" / "backfillctl" / "test-data"
NONE = shared.CRUSH_ITEM_NONE


def flat(text: str) -> str:
    """Collapse whitespace, so a substring check survives stderr's line wrapping."""
    return " ".join(text.split())


def host(osd: int) -> int:
    return osd // 10


class Cluster(SyntheticCluster):
    def plan(self, *argv) -> ut.BalanceResult:
        """Run plan() on this cluster; leading bare OSD ids go to --osds."""
        return self.plan_with(ut, *argv)


def pairs(result: ut.BalanceResult) -> list[tuple[str, object, int, int]]:
    return [(m.pgid, m.shard, m.from_osd, m.target_osd) for m in result.moves]


def osd_df_of(utils: dict[int, float]) -> dict[int, dict]:
    """A minimal 'ceph osd df' map from {osd: utilization}, all of class hdd."""
    return {
        o: {
            "id": o,
            "utilization": u,
            "device_class": "hdd",
            "kb": KB,
            "kb_used": u / 100 * KB,
        }
        for o, u in utils.items()
    }


# ---------------------------------------------------------------------------
# Choosing sources
# ---------------------------------------------------------------------------


class SelectSourcesTest(unittest.TestCase):
    UTILS: ClassVar = {1: 90.0, 2: 80.0, 3: 80.0, 4: 70.0, 5: 60.0, 6: 50.0, 7: 40.0}

    def select(self, **kw):
        return ut.select_sources(
            sorted(self.UTILS),
            self.UTILS.__getitem__,
            osds=kw.get("osds"),
            min_source_util=kw.get("min_source_util"),
        )

    def test_default_is_the_fuller_half_ties_to_lower_id(self):
        self.assertEqual(self.select(), ([1, 2, 3], 7))

    def test_min_source_util_selects_at_or_above(self):
        self.assertEqual(self.select(min_source_util=80.0), ([1, 2, 3], 3))

    def test_min_source_util_is_capped_at_half(self):
        self.assertEqual(self.select(min_source_util=60.0), ([1, 2, 3], 5))

    def test_osds_are_taken_as_given_fullest_first(self):
        self.assertEqual(self.select(osds=[7, 4, 5, 6, 1]), ([1, 4, 5, 6, 7], 5))

    def test_osds_not_in_the_class_are_named(self):
        with self.assertRaises(SystemExit) as cm:
            self.select(osds=[1, 99])
        self.assertIn("osd.99", str(cm.exception))
        self.assertNotIn("osd.1,", str(cm.exception))

    def test_one_osd_class_has_no_sources(self):
        self.assertEqual(
            ut.select_sources([1], {1: 90.0}.get, osds=None, min_source_util=None),
            ([], 1),
        )


class SourcesFromClusterTest(unittest.TestCase):
    def test_sources_ranked_by_final_projection_not_current(self):
        # osd.51 is emptiest now but has a big backfill arriving.
        c = Cluster()
        c.util[51] = 10.0
        c.pg("1.0", [51, 10, 20], [0, 10, 20], shard_pct=45.0)
        result = c.plan()
        self.assertIn(51, result.sources)
        self.assertEqual(result.sources[0], 51)

    def test_osds_and_min_source_util_are_mutually_exclusive(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(ut, ["--osds", "1", "--min-source-util", "80"])

    def test_percent_options_refuse_a_ratio(self):
        for argv in (["--min-source-util", "0.8"], ["--max-target-util", "0.9"]):
            with self.subTest(argv=argv):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
                    parse_args(ut, argv)
                self.assertIn("not a ratio", err.getvalue())

    def test_osds_unknown_to_ceph_are_named_as_elsewhere(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--osds", "0", "999")
        self.assertEqual(
            str(cm.exception), "ERROR: --osds: not in 'ceph osd df': osd.999"
        )

    def test_unknown_class_names_the_classes_present(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--class", "ssd")
        self.assertIn("hdd", str(cm.exception))


# ---------------------------------------------------------------------------
# Shards in motion
# ---------------------------------------------------------------------------


class DepartingOsdsTest(unittest.TestCase):
    def test_ec_slot_moving_departs_its_acting_osd(self):
        pg = {"up": [4, 2, 3], "acting": [1, 2, 3]}
        self.assertEqual(ut.departing_osds(pg, True), [1])

    def test_ec_shard_with_empty_up_slot_stays(self):
        pg = {"up": [NONE, 2, 3], "acting": [1, 2, 3]}
        self.assertEqual(ut.departing_osds(pg, True), [])

    def test_ec_empty_acting_slot_departs_nothing(self):
        pg = {"up": [4, 2, 3], "acting": [NONE, 2, 3]}
        self.assertEqual(ut.departing_osds(pg, True), [])

    def test_replicated_compares_sets(self):
        pg = {"up": [3, 2, 4], "acting": [1, 2, 3]}
        self.assertEqual(ut.departing_osds(pg, False), [1])


class IsSettledTest(unittest.TestCase):
    def test_states(self):
        cases = {
            "active+clean": True,
            "active+remapped+backfilling": True,
            "active+remapped+backfill_toofull": True,
            "active+undersized+degraded+remapped+backfilling": False,
            "active+recovering": False,
            "peering": False,
            "down": False,
        }
        for state, settled in cases.items():
            with self.subTest(state):
                self.assertEqual(ut.is_settled({"state": state}), settled)


class FinalUsageTest(unittest.TestCase):
    def test_arrivals_added_departures_credited(self):
        final = ut.FinalUsage(
            osd_df_of({1: 50.0, 2: 50.0}), [(1, 10 * PCT)], [(2, 5 * PCT)]
        )
        self.assertEqual(final.utilization(1), 60.0)
        self.assertEqual(final.utilization(2), 45.0)
        self.assertEqual(final.utilization(2, 3 * PCT), 48.0)

    def test_move(self):
        final = ut.FinalUsage(osd_df_of({1: 50.0, 2: 50.0}), [], [])
        final.move(1, 2, 10 * PCT)
        self.assertEqual((final.utilization(1), final.utilization(2)), (40.0, 60.0))


# ---------------------------------------------------------------------------
# Target selection
# ---------------------------------------------------------------------------


class PickTargetTest(unittest.TestCase):
    """Balancer.pick_target on hand-made projections: source osd.1 (on h0)."""

    def balancer(self, utils, *, arriving=(), departing=(), max_uses=5, cap=89.0):
        osd_df = osd_df_of(utils)
        return ut.Balancer(
            sorted((o for o in utils if o != 1), key=lambda o: (utils[o], o)),
            osd_df,
            {o: f"h{o}" for o in utils},
            reservation=placement.ProjectedUsage(osd_df, arriving),
            final=ut.FinalUsage(
                osd_df, ((s.up_osd, s.size_bytes) for s in arriving), departing
            ),
            max_uses=max_uses,
            max_target_util=cap,
        )

    def shard(self, pct):
        return placement.MappedShard("1.0", 0, 1, 1, pct * PCT)

    def state(self, up=(1,), raw=()):
        pg = {"pgid": "1.0", "up": list(up), "acting": list(up)}
        return placement.PgPlacement(pg, True, 0, set(raw))

    def test_least_final_wins_ties_to_lower_id(self):
        b = self.balancer({1: 80.0, 3: 40.0, 2: 40.0, 4: 30.0})
        self.assertEqual(b.pick_target(self.shard(1), self.state()), 4)
        b = self.balancer({1: 80.0, 3: 40.0, 2: 40.0})
        self.assertEqual(b.pick_target(self.shard(1), self.state()), 2)

    def test_no_inversion_is_strict(self):
        # Source 80 - 10 = 70; target 60 + 10 = 70: equal is refused.
        b = self.balancer({1: 80.0, 2: 60.0})
        self.assertIsNone(b.pick_target(self.shard(10), self.state()))
        self.assertEqual(b.pick_target(self.shard(9), self.state()), 2)

    def test_cap_uses_reservation_not_final(self):
        # osd.2: 85% now, 40% leaving: final 45%, but Ceph sees 85 + 5 > 89.
        b = self.balancer({1: 80.0, 2: 85.0, 3: 60.0}, departing=[(2, 40 * PCT)])
        self.assertEqual(b.pick_target(self.shard(5), self.state()), 3)

    def test_cap_counts_arrivals(self):
        # osd.2: 10% now, 30% arriving, 1% more: 41%.
        arriving = [placement.ArrivingShard("9.0", 0, 2, 7, [2], 30 * PCT)]
        b = self.balancer({1: 80.0, 2: 10.0}, arriving=arriving, cap=40.0)
        self.assertIsNone(b.pick_target(self.shard(1), self.state()))
        b = self.balancer({1: 80.0, 2: 10.0}, arriving=arriving, cap=41.0)
        self.assertEqual(b.pick_target(self.shard(1), self.state()), 2)

    def test_departures_make_a_target_preferred(self):
        b = self.balancer({1: 80.0, 2: 60.0, 3: 50.0}, departing=[(2, 20 * PCT)])
        self.assertEqual(b.pick_target(self.shard(1), self.state()), 2)

    def test_max_uses(self):
        b = self.balancer({1: 80.0, 2: 10.0, 3: 20.0}, max_uses=1)
        b.uses[2] = 1
        self.assertEqual(b.pick_target(self.shard(1), self.state()), 3)

    def test_forbidden_hosts_and_osds(self):
        b = self.balancer({1: 80.0, 2: 10.0, 3: 20.0, 4: 30.0})
        state = self.state(up=[1, 2], raw={3})
        self.assertEqual(b.pick_target(self.shard(1), state), 4)


# ---------------------------------------------------------------------------
# plan() on the synthetic cluster
# ---------------------------------------------------------------------------


class PlanTest(unittest.TestCase):
    def test_settled_shard_moves_to_least_utilized_legal_osd(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20]).plan()
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.moves[0].acting_osd, 0)
        self.assertEqual(result.stop, ut.Stop(ut.STOP_NO_SHARDS, 0, 79.0))
        self.assertEqual(result.max_before, (80.0, 0))
        self.assertEqual(result.max_after, (79.0, 0))

    def test_targets_on_the_pgs_other_hosts_are_excluded(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[11] = c.util[21] = 1.0  # emptiest, but on the PG's other hosts
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])  # own host allowed

    def test_shard_arriving_on_source_is_redirected_not_double_counted(self):
        c = Cluster()
        c.util[0] = 80.0
        # Shard 0 is backfilling from osd.31 onto osd.0.
        result = c.pg("1.0", [0, 10, 20], [31, 10, 20]).plan(0)
        (move,) = result.moves
        self.assertEqual((move.acting_osd, move.from_osd), (31, 0))
        # osd.31 is emptiest (its copy leaves) but is acting: not a target.
        self.assertEqual(move.target_osd, 1)
        self.assertEqual(move.from_projected, 80.0)  # 80 + 1 arriving - 1
        self.assertEqual(move.target_projected, 51.0)

    def test_shard_already_leaving_a_source_is_not_moved(self):
        c = Cluster()
        c.util[0] = 80.0
        result = c.pg("1.0", [31, 10, 20], [0, 10, 20]).plan(0)
        self.assertEqual((result.moves, result.movable_count), ([], 0))
        self.assertEqual(result.max_before, (79.0, 0))

    def test_unsettled_pgs_are_left_alone(self):
        c = Cluster()
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20], state="active+undersized+degraded")
        result = c.plan(0)
        self.assertEqual((result.moves, result.unsettled_pgs), ([], 1))
        self.assertEqual(result.chained_pgs, 0)

    def test_two_moves_in_one_pg_keep_hosts_distinct(self):
        c = Cluster()
        c.util[0] = c.util[10] = 80.0
        result = c.pg("1.0", [0, 10, 20]).plan(0, 10)
        targets = [m.target_osd for m in result.moves]
        self.assertEqual(len(targets), 2)
        self.assertEqual(len({host(t) for t in targets} | {2}), 3)

    def test_osd_in_the_raw_crush_mapping_is_not_a_target(self):
        c = Cluster()
        c.util[0] = 80.0
        # CRUSH chose osd.1 for shard 2; an existing upmap sends it to 20.
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 1, "to": 20}]})
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 30)])

    def test_replicated_replica_is_moved(self):
        c = Cluster()
        c.util[0] = 80.0
        result = c.pg("2.0", [10, 0, 20]).plan(0)
        self.assertEqual(pairs(result), [("2.0", "-", 0, 1)])

    def test_largest_shard_goes_first(self):
        c = Cluster()
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20], shard_pct=1.0).pg("1.1", [0, 10, 20], shard_pct=3.0)
        result = c.plan(0, "--max-moves", 1)
        self.assertEqual(pairs(result), [("1.1", 0, 0, 1)])
        self.assertEqual(result.stop.reason, ut.STOP_MAX_MOVES)

    def test_ties_at_the_max_are_both_relieved(self):
        c = Cluster()
        c.util[0] = c.util[10] = 80.0
        c.pg("1.0", [0, 20, 30]).pg("1.1", [10, 20, 30])
        result = c.plan()
        self.assertEqual({m.from_osd for m in result.moves}, {0, 10})
        self.assertEqual(result.max_after, (79.0, 0))

    def test_other_classes_are_neither_sources_targets_nor_the_max(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[40] = 95.0  # fullest, but ssd
        c.util[41] = 1.0  # emptiest, but ssd
        c.classes[40] = c.classes[41] = "ssd"
        result = c.pg("1.0", [0, 10, 20]).plan()
        self.assertFalse({40, 41} & {*result.sources, *result.targets})
        # 10 hdd OSDs: sources 0, 1, 10, 11 and 20.
        self.assertEqual(pairs(result), [("1.0", 0, 0, 30)])
        self.assertEqual(result.max_before, (80.0, 0))

    def test_ssd_class(self):
        c = Cluster()
        for o in (40, 41, 50, 51):
            c.classes[o] = "ssd"
        c.util[40] = 80.0
        result = c.pg("1.0", [40, 10, 20]).plan("--class", "ssd")
        self.assertEqual((result.sources, result.targets), ([40, 41], [50, 51]))
        self.assertEqual(pairs(result), [("1.0", 0, 40, 50)])

    def test_pg_whose_upmap_pairs_chain_is_left_alone(self):
        c = Cluster()
        c.util[0] = 80.0
        c.upmaps.append(
            {
                "pgid": "1.0",
                "mappings": [{"from": 31, "to": 30}, {"from": 30, "to": 20}],
            }
        )
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual((result.moves, result.chained_pgs), ([], 1))
        self.assertEqual(result.stop.reason, ut.STOP_NO_SHARDS)

    def test_source_whose_shards_have_no_legal_target_stops(self):
        c = Cluster()
        c.util[0] = 80.0
        # Any target would end at 70%, above the source's 60%.
        result = c.pg("1.0", [0, 10, 20], shard_pct=20.0).plan()
        self.assertEqual(result.moves, [])
        self.assertEqual(result.stop, ut.Stop(ut.STOP_NO_MOVE, 0, 80.0))

    def test_source_with_nothing_movable_stops(self):
        c = Cluster()
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20], state="active+undersized+degraded")
        result = c.plan()
        self.assertEqual(result.stop, ut.Stop(ut.STOP_NO_SHARDS, 0, 80.0))

    def test_no_sources(self):
        c = Cluster()
        result = c.plan("--min-source-util", 90)
        self.assertEqual((result.moves, result.stop), ([], ut.Stop(ut.STOP_NO_SOURCES)))


class StopBelowOtherTest(unittest.TestCase):
    """Seven OSDs at 70% or more, so osd.30 (70%) cannot be a source."""

    def cluster(self):
        c = Cluster()
        c.util[0] = 80.0
        for o in (1, 10, 11, 20, 21, 30):
            c.util[o] = 70.0
        for i in range(3):
            c.pg(f"1.{i}", [0, 10, 20], shard_pct=5.0)
        return c

    def test_default_stops_when_a_non_source_is_as_full(self):
        result = self.cluster().plan()
        self.assertEqual(len(result.moves), 2)
        self.assertEqual(result.stop, ut.Stop(ut.STOP_NOT_A_SOURCE, 0, 70.0, 30, 70.0))

    def test_osds_keeps_relieving_them(self):
        result = self.cluster().plan(0)
        self.assertEqual(len(result.moves), 3)
        self.assertEqual(result.stop.reason, ut.STOP_NO_SHARDS)

    def test_max_target_uses(self):
        result = self.cluster().plan(0, "--max-target-uses", 1)
        self.assertEqual(len({m.target_osd for m in result.moves}), 3)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


class RenderTest(unittest.TestCase):
    def render(self, *argv):
        c = Cluster()
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20]).pg("2.0", [10, 0, 20])
        args = parse_args(ut, ["--osds", "0", *argv])
        result = c.plan(0, *argv)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            ut.render(result, args)
        return out.getvalue(), flat(err.getvalue())

    def test_table(self):
        out, err = self.render()
        lines = out.splitlines()
        self.assertEqual(
            lines[1].split(),
            ["PGID", "SHARD", "SIZE"]
            + ["OSD", "UTIL", "HOST"]
            + ["OSD", "UTIL", "PROJ", "HOST"] * 2,
        )
        self.assertEqual(lines[2].split()[:2], ["1.0", "0"])
        self.assertIn("Proposed 2 move(s)", err)
        self.assertIn("80.0% (osd.0) -> 78.0% (osd.0)", err)
        self.assertIn("osd.0, the fullest source at 78.0%", err)

    def test_pgremapper_mappings(self):
        out, _ = self.render("--pgremapper-mappings")
        self.assertEqual(
            json.loads(out),
            [
                {"pgid": "1.0", "mapping": {"from": 0, "to": 1}},
                # osd.1 is taken by then: 51% final, and h1 is the PG's.
                {"pgid": "2.0", "mapping": {"from": 0, "to": 30}},
            ],
        )

    def test_row_matches_columns(self):
        c = Cluster()
        c.util[0] = 80.0
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        row = ut.format_row(result.moves[0], result.osd_host, result.osd_df)
        self.assertEqual(len(row), len(ut.COLUMNS))


# ---------------------------------------------------------------------------
# Invariants on captured clusters
# ---------------------------------------------------------------------------


class FixtureInvariantTest(unittest.TestCase):
    """Replay balance on real captures and check every proposal is sane."""

    FIXTURES: ClassVar = [
        "ceph1-resumed-backfills-exact-progress",
        "divert-toofull-ceph2-util-emergency-2-new-hosts",
        "divert-toofull-osd263-existing-upmap-chain",
    ]

    def run_traced(self, fixture):
        """Return (result, the class max after each move, in turn order)."""
        trace = []
        commit = ut.Balancer.commit

        def traced(balancer, shard, state, target):
            commit(balancer, shard, state, target)
            osds = [*balancer.targets, *self.sources]
            trace.append(max(balancer.final.utilization(o) for o in osds))

        balance = ut.balance

        def capture(balancer, by_source, *a, **kw):
            self.sources = list(by_source)
            return balance(balancer, by_source, *a, **kw)

        with (
            mock.patch.object(ut.Balancer, "commit", traced),
            mock.patch.object(ut, "balance", capture),
        ):
            result = plan_from_state(ut, TEST_DATA / fixture)
        return result, trace

    def test_invariants(self):
        for fixture in self.FIXTURES:
            with self.subTest(fixture):
                result, trace = self.run_traced(fixture)
                self.check(fixture, result, trace)

    def check(self, fixture, result, trace):
        before = result.max_before[0]
        # The class max never rises.
        for prev, cur in pairwise([before, *trace]):
            self.assertLessEqual(cur, prev + 1e-9)
        self.assertLessEqual(result.max_after[0], before)

        sources = set(result.sources)
        pgs = {
            pg["pgid"]: pg
            for pg in shared.extract_pg_stats(
                json.loads((TEST_DATA / fixture / "pg_dump_pgs.json").read_text()),
                "pg dump",
            )
        }
        upmaps = shared.fetch_upmap_items(
            shared.SnapshotStore.from_args(
                parse_args(ut, [], load_state=str(TEST_DATA / fixture)),
                ut.SNAPSHOT_COMMANDS,
            )
        )
        new_up = {pgid: list(pg["up"]) for pgid, pg in pgs.items()}
        for m in result.moves:
            pg = pgs[m.pgid]
            self.assertIn(m.from_osd, sources)
            self.assertNotIn(m.target_osd, sources)
            self.assertNotIn(m.target_osd, pg["acting"])
            self.assertNotIn(m.target_osd, pg["up"])
            # No chains with the PG's existing pairs.
            self.assertNotIn(m.target_osd, {p["from"] for p in upmaps.get(m.pgid, [])})
            self.assertLess(m.target_projected, before)
            up = new_up[m.pgid]
            up[up.index(m.from_osd)] = m.target_osd
        for m in result.moves:
            hosts = [
                result.osd_host[o] for o in new_up[m.pgid] if shared.is_real_osd(o)
            ]
            self.assertEqual(len(hosts), len(set(hosts)), m.pgid)

    def test_every_move_lowers_the_max_on_a_busy_cluster(self):
        # Sources ranked by final projection: nothing but a source holds the max.
        _, trace = self.run_traced("ceph1-resumed-backfills-exact-progress")
        self.assertGreater(len(trace), 100)
        drops = sum(cur < prev for prev, cur in pairwise(trace))
        self.assertEqual(drops, len(trace) - 1)


if __name__ == "__main__":
    unittest.main()
