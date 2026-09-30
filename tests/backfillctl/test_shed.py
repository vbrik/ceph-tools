"""Unit tests for backfillctl's shed module, the planner behind drain and balance.

Tests run shed() on a small synthetic cluster (_support.SyntheticCluster),
naming the sources and the level directly; test_drain and test_balance cover
how each command chooses them, and what it prints.

What drives the tests: an upmap that is invalid (two shards of a PG on one
host, an OSD twice, a source or an acting OSD as target) or that re-wedges
(a target over the cap, a PG held in backfill_toofull by another shard)
still reads as a plausible row. With a level, a move that takes its target
to the level or above its source would do as much harm as good, and would
look plausible too. The two projections must not be confused: the
--max-target-util cap counts arrivals only (Ceph checks at reservation),
while ranking, levels and the source guard use the final state, with
departures credited.
"""

import contextlib
import io
import json
import unittest
from unittest import mock

from _support import (
    NONE,
    PCT,
    FakeStore,
    SyntheticCluster,
    flat,
    messages,
    placement,
    real_query_backfill_positions,
    shared,
    stderr_of,
)
from _support import shed as sh


class Cluster(SyntheticCluster):
    def shed(
        self, *sources, level=None, level_class=None, max_target_util=None
    ) -> sh.ShedResult:
        """Run shed() on this cluster, off sources, down to level."""
        with self.store() as store:
            cluster = sh.Cluster(store, max_target_util)
            return sh.shed(cluster, set(sources), level, level_class)


def pairs(result: sh.ShedResult) -> list[tuple[str, object, int, int]]:
    return [(m.pgid, m.shard, m.up_osd, m.target_osd) for m in result.moves]


class TargetTest(unittest.TestCase):
    """Which OSD a shard goes to, emptying the sources (no level)."""

    def test_resident_ec_shard_goes_to_least_utilized_legal_osd(self):
        c = Cluster()
        c.util[31] = 10.0  # emptiest, on a host the PG does not use
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.moves[0].acting_osd, 0)

    def test_source_is_never_a_target_even_if_emptiest(self):
        c = Cluster()
        c.util[0] = 1.0
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_hosts_of_the_pgs_other_shards_are_excluded(self):
        c = Cluster()
        c.util[11] = c.util[21] = 1.0  # emptiest, but on the PG's other hosts
        c.util[41] = 10.0
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_same_host_as_the_source_is_allowed(self):
        c = Cluster()
        c.util[1] = 1.0  # osd.0's host sibling
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])

    def test_shard_still_arriving_on_a_source_is_redirected(self):
        c = Cluster()
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20], [41, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.moves[0].acting_osd, 41)

    def test_acting_osd_is_not_a_target(self):
        # osd.41 is emptiest, and its copy of shard 0 is leaving: a move
        # back there would be a pin.
        c = Cluster()
        c.util[41] = 1.0
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20], [41, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_shard_already_leaving_is_counted_not_moved(self):
        c = Cluster()
        result = c.pg("1.0", [41, 10, 20], [0, 10, 20]).shed(0)
        self.assertEqual(result.moves, [])
        self.assertEqual((result.mapped_count, result.leaving_count), (0, 1))

    def test_replicated_replica_is_moved(self):
        c = Cluster()
        c.util[31] = 10.0
        result = c.pg("2.0", [10, 0, 20]).shed(0)
        self.assertEqual(pairs(result), [("2.0", "-", 0, 31)])

    def test_two_sources_of_one_pg_get_distinct_hosts(self):
        c = Cluster()
        c.util[31] = c.util[30] = 10.0  # same host: only one may be used
        result = c.pg("1.0", [0, 10, 20]).shed(0, 10)
        targets = [m.target_osd for m in result.moves]
        self.assertEqual(len(targets), 2)
        self.assertEqual(len({Cluster.host(t) for t in targets} | {2}), 3)
        self.assertFalse({0, 10} & set(targets))

    def test_osd_in_the_raw_crush_mapping_is_not_a_target(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 20.0
        # CRUSH chose 31 for shard 2; an existing upmap sends it to 20.
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 31, "to": 20}]})
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_target_whose_pair_would_chain_is_skipped(self):
        # A stale pair 31->40 (neither in 'up') leaves 31 out of the raw
        # mapping, but 0->31 would chain with it: pgremapper cannot apply that.
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 31, "to": 40}]})
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_osd_with_data_leaving_is_preferred_where_it_ends_up_emptiest(self):
        # osd.41 is fuller now, but ends at 5% once its 25% leaves.
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 30.0
        c.pg("1.9", [51, 11, 21], [41, 11, 21], shard_pct=25)
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])
        self.assertEqual(result.moves[0].target_projected, 6.0)

    def test_data_arriving_counts_against_a_target(self):
        # osd.41 is emptier now, but ends at 15% once its 10% lands.
        c = Cluster()
        c.util[31] = 12.0
        c.util[41] = 5.0
        c.pg("1.9", [41, 11, 21], [51, 11, 21], shard_pct=10)
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.moves[0].target_projected, 13.0)

    def test_other_device_classes_are_not_targets(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 1.0
        c.classes[41] = "ssd"
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])


class CapacityTest(unittest.TestCase):
    def test_target_projected_over_the_cap_is_skipped(self):
        c = Cluster(default_util=95.0)
        c.util[41] = 85.0  # a 5% shard would take it to 90%, over 89%
        result = c.pg("1.0", [0, 10, 20], shard_pct=5).shed(0)
        self.assertEqual(result.moves, [])
        self.assertEqual(len(result.unplaceable), 1)

    def test_cap_is_inclusive(self):
        c = Cluster(default_util=95.0)
        c.util[31] = 84.0
        result = c.pg("1.0", [0, 10, 20], shard_pct=5).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_cap_counts_arrivals_not_departures(self):
        # osd.31 ends at 45% once its 40% leaves, but Ceph reserves the
        # backfill against 85% + 5% > 89%.
        c = Cluster(default_util=95.0)
        c.util[31] = 85.0
        c.util[41] = 70.0
        c.pg("1.9", [51, 11, 21], [31, 11, 21], shard_pct=40)
        result = c.pg("1.0", [0, 10, 20], shard_pct=5).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_arriving_shards_count_towards_the_target(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 12.0
        # 5% already on its way to osd.31 from another PG.
        c.pg("1.1", [31, 11, 21], [30, 11, 21], shard_pct=5)
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_explicit_max_target_util(self):
        c = Cluster()
        result = c.pg("1.0", [0, 10, 20]).shed(0, max_target_util=40)
        self.assertEqual(result.moves, [])
        self.assertEqual(len(result.unplaceable), 1)

    def test_max_target_util_above_backfillfull_is_an_error(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().pg("1.0", [0, 10, 20]).shed(0, max_target_util=91)
        self.assertIn("backfillfull_ratio", str(cm.exception))

    def test_many_shards_may_go_to_one_target(self):
        c = Cluster()
        c.util[31] = 10.0
        for i in range(3):
            c.pg(f"1.{i}", [0, 10, 20])
        result = c.shed(0)
        self.assertEqual([m.target_osd for m in result.moves], [31, 31, 31])

    def test_unplaceable_are_in_pg_then_numeric_shard_order(self):
        # An 11-slot up set with sources in shards 2 and 10 ("10" sorts
        # before "2" as a string). Not a real layout for pool 1, but the
        # planner only needs the slots.
        up = [NONE] * 11
        up[2], up[10] = 0, 10
        result = Cluster(default_util=95.0).pg("1.0", up).shed(0, 10)
        self.assertEqual([e.shard for e in result.unplaceable], [2, 10])

    def test_largest_shard_is_placed_first(self):
        c = Cluster(default_util=95.0)
        c.util[31] = 84.0  # room for 5%, then not for 1% more
        c.pg("1.0", [0, 10, 20], shard_pct=1)
        c.pg("1.1", [0, 10, 20], shard_pct=5)
        result = c.shed(0)
        self.assertEqual(pairs(result), [("1.1", 0, 0, 31)])
        self.assertEqual([e.pgid for e in result.unplaceable], ["1.0"])

    def test_rows_of_an_osd_all_show_its_projection_once_everything_is_placed(self):
        c = Cluster(default_util=95.0)
        c.util[31] = 70.0
        c.pg("1.0", [0, 10, 20], shard_pct=5)
        c.pg("1.1", [0, 10, 20], shard_pct=10)
        result = c.shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31), ("1.1", 0, 0, 31)])
        self.assertEqual([m.target_projected for m in result.moves], [85.0, 85.0])
        self.assertEqual([m.up_projected for m in result.moves], [80.0, 80.0])


class PartlyCopiedTest(unittest.TestCase):
    """1.0's 10% shard 0 is arriving on source osd.0 from osd.41, half copied:
    osd.0's 55% holds 5 points of it."""

    def cluster(self) -> Cluster:
        c = Cluster()
        c.util[0] = 55.0
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [41, 10, 20], shard_pct=10)
        c.positions = {"1.0": {"0(0)": "08000000"}}
        return c

    def test_redirect_then_uncommit(self):
        with self.cluster().store() as store:
            cluster = sh.Cluster(store, None)
            planner = sh.Planner(cluster, {0}, None, None, relieve=False)
            state = cluster.pg_state(cluster.pgs[0])
            shard = placement.MappedShard("1.0", 0, 0, 41, 10 * PCT)

            def projections():
                """(reservation, final) of osd.0, osd.31 and osd.41."""
                return [
                    (
                        round(cluster.reservation.utilization_after(o, 0), 6),
                        round(cluster.final.utilization(o), 6),
                    )
                    for o in (0, 31, 41)
                ]

            before = projections()
            planner.commit(state, shard, 31)
            redirected = projections()
            planner.uncommit(state, shard, 31)
            self.assertEqual(projections(), before)
        self.assertEqual(before, [(60.0, 60.0), (10.0, 10.0), (50.0, 40.0)])
        # osd.0 frees what it copied only once the PG settles: the
        # reservation keeps it.
        self.assertEqual(redirected, [(55.0, 50.0), (20.0, 20.0), (50.0, 40.0)])

    def test_a_failed_query_says_the_projections_count_the_backfill_in_full(self):
        failed = shared.TimedPositions({}, {}, ["1.0"])
        store = FakeStore(self.cluster().snapshots(), load_dir=None)
        with (
            mock.patch.object(
                shared, "query_backfill_positions", real_query_backfill_positions
            ),
            mock.patch.object(shared, "_query_positions", return_value=failed),
        ):
            cluster = sh.Cluster(store, None)
            err = stderr_of(lambda: cluster.final)
        self.assertIn("failed for 1 of 1 PG(s) (1.0)", err)
        self.assertIn(flat(messages.PROJECTION_QUERY_EFFECT), err)
        self.assertAlmostEqual(cluster.final.utilization(0), 65.0)

    def test_the_copied_part_counts_once_against_the_level(self):
        # osd.0 ends at 60% with the rest of the shard, below a 62% level:
        # nothing to move. Counted twice, it would read 65%.
        result = self.cluster().shed(0, level=62)
        self.assertEqual(result.moves, [])


class OrderTest(unittest.TestCase):
    """The fullest source goes first, not the first PG or the largest shard."""

    def cluster(self, osd0: float, osd10: float) -> Cluster:
        c = Cluster(default_util=95.0)
        c.util[0], c.util[10] = osd0, osd10
        c.util[31] = 84.0  # room for one 5% shard
        c.pg("1.0", [0, 20, 40], shard_pct=5)
        c.pg("1.1", [10, 20, 40], shard_pct=5)
        return c

    def test_fullest_source_takes_the_room(self):
        for osd0, osd10, moved in ((96.0, 95.0, "1.0"), (95.0, 96.0, "1.1")):
            with self.subTest(osd0=osd0, osd10=osd10):
                result = self.cluster(osd0, osd10).shed(0, 10)
                self.assertEqual([m.pgid for m in result.moves], [moved])
                self.assertEqual(len(result.unplaceable), 1)

    def test_ties_go_to_the_lower_id(self):
        result = self.cluster(95.0, 95.0).shed(0, 10)
        self.assertEqual([m.pgid for m in result.moves], ["1.0"])

    def test_largest_shard_of_a_less_full_source_waits(self):
        c = self.cluster(96.0, 95.0)
        c.pgs[1]["stat_sum"]["num_bytes"] //= 5  # 1.1's shard: 1%
        c.pg("1.2", [10, 21, 41], shard_pct=4)
        result = c.shed(0, 10)
        self.assertEqual([m.pgid for m in result.moves], ["1.0"])

    def test_source_with_nothing_placeable_does_not_stop_the_others(self):
        # osd.0's 30% shard fits nowhere below the level and osd.0.
        c = Cluster(default_util=40.0)
        c.util[0], c.util[10] = 90.0, 70.0
        c.pg("1.0", [0, 20, 30], shard_pct=30).pg("1.1", [10, 20, 30])
        result = c.shed(0, 10, level=60)
        self.assertEqual(pairs(result), [("1.1", 0, 10, 1)])
        self.assertEqual([e.pgid for e in result.unplaceable], ["1.0"])
        self.assertEqual(result.still_above, [(0, 90.0), (10, 69.0)])


class LevelTest(unittest.TestCase):
    """A source sheds while its final projection, departures credited, is at
    or above the level; a target must end up below the level and its source."""

    def test_source_stops_shedding_once_below_the_level(self):
        # 60 - 5 = 55% is not below 55, so the 4% shard goes too (51%); the
        # 3% shard stays.
        c = Cluster(default_util=40.0)
        c.util[0] = 60.0
        c.pg("1.0", [0, 10, 20], shard_pct=3)
        c.pg("1.1", [0, 10, 20], shard_pct=5)
        c.pg("1.2", [0, 10, 20], shard_pct=4)
        result = c.shed(0, level=55)
        self.assertEqual([m.pgid for m in result.moves], ["1.1", "1.2"])
        self.assertEqual((result.mapped_count, result.kept_count), (3, 1))
        self.assertEqual((result.unplaceable, result.still_above), ([], []))
        self.assertEqual(result.final_util, {0: 51.0})

    def test_source_may_not_drop_below_a_target_it_sent_data_to(self):
        # 1.0's 10% shard goes to osd.51 (45% -> 55%; h4 is 1.0's), leaving
        # osd.0 at 60%. 1.1's could go to osd.41 (20% -> 30%), but would
        # leave osd.0 at 50%, below osd.51. The rest have no room.
        c = Cluster(default_util=88.0)
        c.util |= {0: 70.0, 41: 20.0, 51: 45.0}
        c.pg("1.0", [0, 40, 20], shard_pct=10).pg("1.1", [0, 10, 30], shard_pct=10)
        result = c.shed(0, level=40)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 51)])
        self.assertEqual([s.pgid for s in result.unplaceable], ["1.1"])

    def test_source_already_below_the_level_sheds_nothing(self):
        result = Cluster().pg("1.0", [0, 10, 20]).shed(0, level=60)
        self.assertEqual(result.moves, [])
        self.assertEqual((result.mapped_count, result.kept_count), (1, 1))

    def test_each_source_stops_on_its_own(self):
        c = Cluster(default_util=40.0)
        c.util[0], c.util[10] = 60.0, 50.0
        c.pg("1.0", [0, 20, 30]).pg("1.1", [10, 21, 31])
        result = c.shed(0, 10, level=55)
        self.assertEqual([m.up_osd for m in result.moves], [0])
        self.assertEqual(result.kept_count, 1)

    def test_target_of_the_level_class_must_end_below_the_level(self):
        c = Cluster(default_util=56.0)
        c.util[0] = 80.0
        c.pg("1.0", [0, 10, 20], shard_pct=5)
        # Without a level class (drain), 61% is fine: below osd.0's 75%.
        self.assertEqual(len(c.shed(0, level=60).moves), 1)
        result = c.shed(0, level=60, level_class="hdd")
        self.assertEqual(result.moves, [])
        self.assertEqual(len(result.unplaceable), 1)
        self.assertEqual(len(c.shed(0, level=60, level_class="ssd").moves), 1)
        c.util[31] = 54.9  # 59.9%
        self.assertEqual(
            pairs(c.shed(0, level=60, level_class="hdd")), [("1.0", 0, 0, 31)]
        )

    def test_target_must_end_below_its_source(self):
        # osd.0 ends at 57%: a target ending there too is refused.
        c = Cluster(default_util=52.0)
        c.util[0] = 62.0
        c.pg("1.0", [0, 10, 20], shard_pct=5)
        self.assertEqual(c.shed(0, level=60).moves, [])
        c.util[31] = 51.0
        self.assertEqual(pairs(c.shed(0, level=60)), [("1.0", 0, 0, 31)])

    def test_shard_that_fits_nowhere_on_a_source_that_gets_below_is_kept(self):
        # 62 - 10 = 52%: every target would end at 60%. The 3% shard then
        # takes osd.0 below 60.
        c = Cluster()
        c.util[0] = 62.0
        c.pg("1.0", [0, 10, 20], shard_pct=10).pg("1.1", [0, 11, 21], shard_pct=3)
        result = c.shed(0, level=60)
        self.assertEqual([m.pgid for m in result.moves], ["1.1"])
        self.assertEqual((result.unplaceable, result.kept_count), ([], 1))

    def test_replicated_shard_is_credited_too(self):
        c = Cluster(default_util=40.0)
        c.util[0] = 56.0
        c.pg("2.0", [0, 10, 20], shard_pct=2).pg("2.1", [0, 11, 21], shard_pct=1)
        result = c.shed(0, level=55)
        self.assertEqual(pairs(result)[0][:3], ("2.0", "-", 0))
        self.assertEqual(len(result.moves), 1)

    def test_data_already_leaving_is_credited(self):
        c = Cluster()
        c.util[0] = 60.0
        c.pg("1.9", [41, 11, 21], [0, 11, 21], shard_pct=6)  # 54% once done
        c.pg("1.0", [0, 10, 20])
        result = c.shed(0, level=55)
        self.assertEqual(result.moves, [])
        self.assertEqual(result.kept_count, 1)

    def test_shard_arriving_on_a_source_counts_towards_it(self):
        c = Cluster(default_util=40.0)
        c.util[0] = 54.5  # + 1% arriving: 55.5%, at or above 55
        c.pg("1.0", [0, 10, 20], [41, 10, 20])
        self.assertEqual(len(c.shed(0, level=55).moves), 1)
        c.util[0] = 53.5  # 54.5% with it: below, so it keeps arriving
        result = c.shed(0, level=55)
        self.assertEqual((result.moves, result.kept_count), ([], 1))

    def test_blocker_arriving_on_a_source_below_the_level_is_diverted(self):
        # osd.0 ends at 80.5% once 1.9 leaves, below 85, but reserving
        # shard 0's backfill sees 90.5%, over backfillfull_ratio: it would
        # hold up shard 1, moved off osd.10.
        c = Cluster()
        c.util[0], c.util[10] = 89.5, 90.0
        c.pg("1.9", [41, 11, 21], [0, 11, 21], shard_pct=10)
        c.pg("1.0", [0, 10, 20], [31, 10, 20])
        result = c.shed(0, 10, level=85)
        self.assertEqual(pairs(result), [("1.0", 1, 10, 11), ("1.0", 0, 0, 1)])
        self.assertEqual(
            result.moves[1].note,
            "diverted: osd.0 projected at 90.5%, at or over backfillfull_ratio, "
            "which would stall the PG, holding up shard 1 leaving osd.10",
        )
        self.assertEqual((result.diverted_count, result.kept_count), (1, 0))

    def test_blocker_arriving_on_a_source_holding_up_nothing_is_left(self):
        # As above, but no other shard of 1.0 moves: its stall predates the
        # run, which leaves backfills in motion alone.
        c = Cluster()
        c.util[0] = 89.5
        c.pg("1.9", [41, 11, 21], [0, 11, 21], shard_pct=10)
        c.pg("1.0", [0, 10, 20], [31, 10, 20])
        result = c.shed(0, level=85)
        self.assertEqual((result.moves, result.kept_count), ([], 1))
        self.assertEqual(result.stalled_pgs, ["1.0"])

    def test_source_a_pin_takes_below_the_level_keeps_its_unplaceable_shards(self):
        # 1.1's shard fits nowhere (h1 is its PG's), osd.0 being at 90%. Then
        # 1.0's shard 0, arriving on osd.0, is pinned back as a blocker of
        # shard 1's move off osd.10: osd.0 ends at 50%, so 1.1's shard is
        # left in place, not unplaceable.
        c = Cluster(default_util=95.0)
        c.util[0], c.util[11] = 50.0, 10.0
        c.pg("1.0", [0, 10, 20], [41, 10, 20], shard_pct=40)
        c.pg("1.1", [0, 11, 21])
        result = c.shed(0, 10, level=60)
        self.assertEqual(pairs(result), [("1.0", 1, 10, 11), ("1.0", 0, 0, 41)])
        self.assertEqual((result.unplaceable, result.kept_count), ([], 1))
        self.assertEqual(result.still_above, [])

    def test_unplaceable_shard_pinned_as_a_blocker_is_not_reported(self):
        # Shard 1, arriving on source osd.10 (96%), has no target, and would
        # hold up shard 0's move: it is pinned back to osd.41.
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        result = c.pg("1.0", [0, 10, 20], [0, 41, 20]).shed(0, 10)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1), ("1.0", 1, 10, 41)])
        self.assertEqual((result.unplaceable, result.pinned_count), ([], 1))

    def test_resident_shard_is_kept_however_full_its_osd(self):
        # No backfill onto osd.0 to refuse: the data is already there.
        c = Cluster()
        c.util[0] = 95.0
        c.pg("1.9", [41, 11, 21], [0, 11, 21], shard_pct=20)
        c.pg("1.0", [0, 10, 20])
        self.assertEqual(c.shed(0, level=85).kept_count, 1)

    def test_kept_shard_pinned_as_a_companion_is_not_counted_kept(self):
        # As in BlockerTest's companion case: pinning blocker shard 1 back to
        # osd.21 needs shard 2, kept on source osd.20, pinned back to osd.11.
        c = Cluster(default_util=95.0)
        c.util[0] = 70.0
        c.util[1] = 10.0
        c.util[20] = 50.0
        result = c.pg("1.0", [0, 10, 20], [0, 21, 11]).shed(0, 20, level=60)
        self.assertIn(("1.0", 2, 20, 11), pairs(result))
        self.assertEqual(result.kept_count, 0)

    def test_pin_back_onto_a_source_below_the_level_is_allowed(self):
        # As in BlockerTest: shard 1 is leaving source osd.20 for full
        # osd.10. osd.20 ends at 49%, 50% with the shard pinned back.
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        c.util[20] = 50.0
        c.pg("1.0", [0, 10, 30], [0, 20, 30])
        result = c.shed(0, 20, level=60)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1), ("1.0", 1, 10, 20)])
        self.assertEqual(result.stuck_pgs, [])

    def test_pin_back_that_would_reach_the_level_is_refused(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        c.util[20] = 59.5  # 58.5% once the shard leaves, 59.5% if pinned
        c.pg("1.0", [0, 10, 30], [0, 20, 30])
        result = c.shed(0, 20, level=59)
        self.assertEqual(result.stuck_pgs, ["1.0"])
        self.assertIn("source osd.20, taking it to the 59% level", result.moves[0].note)
        self.assertEqual(c.shed(0, 20, level=60).stuck_pgs, [])

    def test_source_left_above_the_level_is_reported(self):
        c = Cluster(default_util=95.0)  # no room anywhere
        c.util[0] = 70.0
        result = c.pg("1.0", [0, 10, 20]).shed(0, level=60)
        self.assertEqual(len(result.unplaceable), 1)
        self.assertEqual(result.still_above, [(0, 70.0)])

    def test_without_a_level_everything_moves(self):
        c = Cluster()
        c.pg("1.0", [0, 10, 20]).pg("1.1", [0, 11, 21])
        result = c.shed(0)
        self.assertEqual(len(result.moves), 2)
        self.assertEqual(
            (result.level, result.kept_count, result.still_above), (None, 0, [])
        )

    def test_source_without_a_size_is_drained_in_full(self):
        c = Cluster().pg("1.0", [0, 10, 20])
        snapshots = c.snapshots()
        for node in snapshots["osd_df"]["nodes"]:
            if node["id"] == 0:
                node["kb"] = 0
        result = sh.shed(sh.Cluster(FakeStore(snapshots), None), {0}, 60)
        self.assertEqual((len(result.moves), result.kept_count), (1, 0))
        self.assertEqual(sh.unsized_sources(result), [0])


class LeftAloneTest(unittest.TestCase):
    def test_unsettled_pg_is_left_alone(self):
        c = Cluster()
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], state="active+undersized+degraded")
        result = c.pg("1.1", [0, 11, 21]).shed(0)
        self.assertEqual(pairs(result), [("1.1", 0, 0, 31)])
        self.assertEqual((result.unsettled_pgs, result.mapped_count), (1, 1))

    def test_pg_whose_upmap_pairs_chain_is_left_alone(self):
        c = Cluster()
        c.upmaps.append(
            {
                "pgid": "1.0",
                "mappings": [{"from": 31, "to": 30}, {"from": 30, "to": 20}],
            }
        )
        result = c.pg("1.0", [0, 10, 20]).shed(0)
        self.assertEqual((result.moves, result.chained_pgs), ([], 1))

    def test_is_settled(self):
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
                self.assertEqual(sh.is_settled({"state": state}), settled)

    def test_unknown_failure_domain_is_checked_only_for_pgs_to_move(self):
        c = Cluster()
        c.rule = {"rule_id": 0, "steps": [{"op": "chooseleaf_indep", "type": "rack"}]}
        c.pg("1.0", [0, 10, 20])
        with self.assertRaises(SystemExit) as cm:
            c.shed(0)
        self.assertIn("rack", str(cm.exception))
        self.assertEqual(c.shed(51).moves, [])


class BlockerTest(unittest.TestCase):
    """A sibling arriving on an OSD projected at or over backfillfull_ratio
    (90% here) holds the PG in toofull."""

    def test_blocker_is_diverted(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.util[41] = 20.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31), ("1.0", 1, 10, 41)])
        self.assertEqual(
            result.moves[1].note,
            "diverted: osd.10 projected at 96.0%, at or over backfillfull_ratio, "
            "which would stall the PG, holding up shard 0 leaving osd.0",
        )
        self.assertEqual(result.diverted_count, 1)
        self.assertEqual(result.stuck_pgs, [])

    def test_blocker_threshold_is_backfillfull_not_max_target_util(self):
        # Ceph's rule, as in cancel-backfill: --max-target-util caps targets,
        # it does not decide what Ceph refuses.
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.util[10] = 89.5  # + 0.1% shard: 89.6%, over the 89% cap, under 90%
        c.pg("1.0", [0, 10, 20], [0, 11, 20], shard_pct=0.1)
        self.assertEqual(c.shed(0).diverted_count, 0)
        c.util[10] = 89.9  # + 0.1% shard: 90.0%, at backfillfull
        self.assertEqual(c.shed(0).diverted_count, 1)
        self.assertEqual(c.shed(0, max_target_util=80).diverted_count, 1)

    def test_sibling_under_the_cap_is_left_alone(self):
        c = Cluster()
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_blocker_is_pinned_when_it_cannot_be_diverted(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0  # the only OSD with room: taken by the source's shard
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1), ("1.0", 1, 10, 11)])
        pin = result.moves[1]
        self.assertEqual(
            pin.note,
            "pinned, no room to divert: osd.10 projected at 96.0%, at or over "
            "backfillfull_ratio, which would stall the PG, holding up shard 0 "
            "leaving osd.0",
        )
        self.assertIsNone(pin.target_projected)
        self.assertEqual(pin.up_projected, 95.0)
        self.assertEqual(result.pinned_count, 1)

    def test_blocker_already_pinned_as_a_companion_is_not_revisited(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        # Shards 1 and 2 swap hosts h1/h2, both onto full OSDs. Pinning
        # shard 1 back to osd.21 clashes with shard 2 arriving on osd.20, so
        # shard 2 is pinned too, as its companion: the PG is then unblocked.
        result = c.pg("1.0", [0, 10, 20], [0, 21, 11]).shed(0)
        self.assertEqual(
            pairs(result),
            [("1.0", 0, 0, 1), ("1.0", 1, 10, 21), ("1.0", 2, 20, 11)],
        )
        self.assertEqual(result.moves[2].note, "companion of blocker shard 1")
        self.assertEqual(result.moves[0].note, "")
        self.assertEqual(
            [m.role for m in result.moves],
            [shared.ROLE_REQUESTED, shared.ROLE_BLOCKER, shared.ROLE_BLOCKER],
        )
        self.assertEqual((result.pinned_count, result.stuck_pgs), (1, []))

    def test_replicated_blocker_is_pinned(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        result = c.pg("2.0", [0, 10, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("2.0", "-", 0, 1), ("2.0", "-", 10, 11)])
        self.assertTrue(
            result.moves[1].note.endswith("holding up the replica leaving osd.0")
        )

    def test_blocker_note_lists_every_move_it_holds_up(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.util[51] = 30.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0, 20)
        self.assertTrue(
            result.moves[-1].note.endswith(
                "holding up shard 0 leaving osd.0 and shard 2 leaving osd.20"
            ),
            result.moves[-1].note,
        )

    def test_unpinnable_blocker_keeps_the_move_with_a_note(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        result = c.pg("1.0", [0, 10, 20], [0, NONE, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])
        self.assertIn("PG stays toofull: shard 1 -> osd.10", result.moves[0].note)
        self.assertIn("no acting OSD", result.moves[0].note)
        self.assertEqual(result.stuck_pgs, ["1.0"])

    def test_pin_back_onto_a_source_is_refused(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        # Shard 1 is leaving source osd.20 for full osd.10.
        result = c.pg("1.0", [0, 10, 30], [0, 20, 30]).shed(0, 20)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])
        self.assertIn("pin data back onto source osd.20", result.moves[0].note)

    def test_toofull_pg_blocker_at_nearfull_is_diverted(self):
        c = Cluster()
        c.util[10] = 86.0  # under backfillfull, but at nearfull (85%)
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_toofull")
        result = c.shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31), ("1.0", 1, 10, 41)])
        self.assertEqual(
            result.moves[1].note,
            "diverted: osd.10 now 86.0% >= nearfull_ratio 85% and PG is "
            "backfill_toofull, which would stall the PG, holding up shard 0 "
            "leaving osd.0",
        )

    def test_toofull_pg_blocker_over_both_thresholds_cites_backfillfull(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_toofull")
        self.assertIn(
            "osd.10 projected at 96.0%, at or over backfillfull_ratio,",
            c.shed(0).moves[1].note,
        )

    def test_nearfull_sibling_of_a_pg_not_toofull_is_left_alone(self):
        c = Cluster()
        c.util[10] = 86.0
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_wait")
        self.assertEqual(pairs(c.shed(0)), [("1.0", 0, 0, 31)])

    def test_unpinnable_nearfull_blocker_note_shows_why_it_blocks(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        c.util[10] = 86.0
        c.pg("1.0", [0, 10, 20], [0, NONE, 20], state="active+backfill_toofull")
        self.assertIn(
            "shard 1 -> osd.10 now 86.0% >= nearfull_ratio 85% and PG is "
            "backfill_toofull (cannot pin: no acting OSD)",
            c.shed(0).moves[0].note,
        )

    def test_toofull_pg_with_no_identified_blocker_is_flagged(self):
        c = Cluster()
        c.util[10] = 84.0  # under nearfull and the cap: not a suspect
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_toofull")
        result = c.shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertIn("blocker unidentified", result.moves[0].note)
        self.assertEqual((result.unexplained_pgs, result.stuck_pgs), (["1.0"], []))

    def test_toofull_pg_whose_moved_shard_was_arriving_is_not_flagged(self):
        c = Cluster()
        c.util[31] = 10.0
        # The shard itself was backfilling onto osd.0: redirecting it may be
        # exactly what unwedges the PG.
        c.pg("1.0", [0, 10, 20], [41, 10, 20], state="active+backfill_toofull")
        result = c.shed(0)
        self.assertEqual(result.moves[0].note, "")
        self.assertEqual(result.unexplained_pgs, [])

    def test_diverted_blocker_of_the_level_class_obeys_the_level(self):
        # A blocker's new target must end up below the level too; osd.31's
        # host is the PG's now, and every other OSD would end at 63%.
        c = Cluster(default_util=62.0)
        c.util[0] = 70.0
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20])
        result = c.shed(0, level=60, level_class="hdd")
        self.assertEqual(pairs(result)[0], ("1.0", 0, 0, 31))
        self.assertEqual(result.pinned_count, 1)
        self.assertEqual(c.shed(0, level=60).diverted_count, 1)

    def test_blocker_of_another_class_is_not_held_to_the_level(self):
        # An ssd shard of a hybrid PG: the hdd level does not apply to it.
        c = Cluster()
        c.util[0] = 70.0
        c.classes[10] = c.classes[11] = c.classes[41] = "ssd"
        c.util[10], c.util[41] = 95.0, 70.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20])
        result = c.shed(0, level=60, level_class="hdd")
        self.assertEqual(pairs(result)[1], ("1.0", 1, 10, 41))
        self.assertEqual(result.diverted_count, 1)

    def test_move_clashing_with_a_blockers_pin_is_placed_elsewhere(self):
        # Shard 0 first goes to osd.10, on h1 with blocker shard 1's data
        # (osd.11): pinning shard 1 back there would clash. Its ssd class has
        # no room to divert it, so shard 0 goes to osd.40 instead, and shard 1
        # is pinned.
        c = Cluster()
        for osd in (11, 30, 31, 41):
            c.classes[osd] = "ssd"
            c.util[osd] = 95.0
        c.util[10], c.util[40] = 10.0, 20.0
        result = c.pg("1.0", [0, 30, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 40), ("1.0", 1, 30, 11)])
        self.assertEqual((result.pinned_count, result.stuck_pgs), (1, []))
        self.assertEqual(result.moves[0].note, "")

    def test_move_re_placed_for_a_pin_that_still_fails_is_restored(self):
        # Shard 0 first goes to osd.10, on h1 with shard 1's data (osd.11).
        # Re-placed on osd.40, shard 1's pin is still refused (osd.11 is a
        # source), so shard 0 goes back to osd.10, and osd.40 is free again:
        # blocker shard 2 is diverted there.
        c = Cluster(default_util=95.0)
        for osd in (11, 30, 31):
            c.classes[osd] = "ssd"
        c.util[10], c.util[40] = 10.0, 20.0
        result = c.pg("1.0", [0, 30, 50], [0, 11, 21]).shed(0, 11)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 10), ("1.0", 2, 50, 40)])
        self.assertEqual(result.stuck_pgs, ["1.0"])
        self.assertIn(
            "cannot pin: it would pin data back onto source osd.11",
            result.moves[0].note,
        )

    def test_move_with_nowhere_else_to_go_keeps_its_target(self):
        # As above, but osd.10 is the only hdd OSD with room: the PG is
        # reported stuck, and the move stays.
        c = Cluster(default_util=95.0)
        for osd in (11, 30, 31, 41):
            c.classes[osd] = "ssd"
        c.util[10] = 10.0
        result = c.pg("1.0", [0, 30, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 10)])
        self.assertEqual(result.stuck_pgs, ["1.0"])
        self.assertEqual(result.final_util, {0: 94.0})

    def test_pin_chaining_with_an_existing_pair_is_refused(self):
        # CRUSH now puts osd.30 in slot 2, where an existing pair sends it
        # on to osd.50. Pinning blocker slot 1 back to osd.30 would add
        # 11->30 to 30->50: a chain pgremapper cannot apply.
        c = Cluster(default_util=95.0)
        c.util[40] = 10.0
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 30, "to": 50}]})
        result = c.pg("1.0", [0, 11, 50], [0, 30, 50]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 40)])
        self.assertEqual(result.stuck_pgs, ["1.0"])
        self.assertIn("the PG's pairs would chain", result.moves[0].note)

    def test_pin_folding_into_an_existing_pair_is_allowed(self):
        # An existing pair 21->10 put osd.10 in 'up': pinning 10->11 folds
        # into 21->11, which does not chain.
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 21, "to": 10}]})
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1), ("1.0", 1, 10, 11)])
        self.assertEqual(result.stuck_pgs, [])


class OutputTest(unittest.TestCase):
    def result(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        return c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)

    def test_row_matches_columns(self):
        result = self.result()
        for move in result.moves:
            row = sh.format_row(move, result.osd_host, result.osd_df)
            self.assertEqual(len(row), len(sh.COLUMNS))
        row = sh.format_row(result.moves[0], result.osd_host, result.osd_df)
        labels = [label for _, label in sh.COLUMNS]
        self.assertEqual(row[:3], ["1.0", "0", "9.8 MiB"])
        self.assertEqual(row[6], "0")  # UP OSD: the upmap's 'from'
        self.assertEqual(labels[6], "OSD")
        self.assertEqual(row[8], "49.0%")  # UP PROJ
        self.assertEqual(row[10], "31")  # TARGET OSD: its 'to'
        self.assertEqual(row[12], "11.0%")  # TARGET PROJ

    def test_pin_row_has_no_target_projection(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)
        row = sh.format_row(result.moves[1], result.osd_host, result.osd_df)
        self.assertEqual(row[12], shared.NOT_APPLICABLE)

    def test_pgremapper_mappings_carry_each_rows_shard_role_and_note(self):
        moves = self.result().moves
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sh.print_pgremapper_mappings(moves)
        entries = json.loads(out.getvalue())
        self.assertEqual(
            [e["mapping"] for e in entries],
            [{"from": 0, "to": 31}, {"from": 10, "to": 1}],
        )
        self.assertEqual(
            [(e["shard"], e["role"], e["note"]) for e in entries],
            [(m.shard, m.role, m.note) for m in moves],
        )
        self.assertEqual(
            [m.role for m in moves], [shared.ROLE_REQUESTED, shared.ROLE_BLOCKER]
        )

    def test_pgremapper_mappings_empty(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sh.print_pgremapper_mappings([])
        self.assertEqual(out.getvalue(), "[]\n")

    def test_unplaceable_are_listed_when_emptying_counted_with_a_level(self):
        c = Cluster(default_util=95.0).pg("1.0", [0, 10, 20]).pg("1.1", [0, 11, 21])
        text = stderr_of(sh.print_notes, c.shed(0), "source(s)")
        self.assertIn("cannot place 1.0 shard 0 off osd.0: no legal target", text)
        self.assertIn("ran out of room (--max-target-util), or", text)
        # An unreachable level can leave thousands: one line, not one each.
        text = stderr_of(sh.print_notes, c.shed(0, level=60), "source(s)")
        self.assertNotIn("cannot place", text)
        self.assertIn(
            "NOTE: 2 shard(s) found no target at or below --max-target-util "
            "that would end up below their source.",
            text,
        )
        result = c.shed(0, level=60, level_class="hdd")
        self.assertIn(
            "below the level and their source.",
            stderr_of(sh.print_notes, result, "source(s)"),
        )
        self.assertEqual(stderr_of(sh.print_notes, Cluster().shed(0), "x"), "")

    def test_notes_name_the_sources_left_above_and_stalled_pgs(self):
        c = Cluster(default_util=95.0)
        c.util[0] = 70.0
        text = stderr_of(
            sh.print_notes, c.pg("1.0", [0, 10, 20]).shed(0, level=60), "source(s)"
        )
        self.assertIn(
            "1 source(s) projected to stay at or above the 60% level: osd.0 (70.0%).",
            text,
        )
        c = Cluster()
        c.util[0] = 89.5
        c.pg("1.9", [41, 11, 21], [0, 11, 21], shard_pct=10)
        c.pg("1.0", [0, 10, 20], [31, 10, 20])
        text = stderr_of(sh.print_notes, c.shed(0, level=85), "source(s)")
        self.assertIn("NOTE: 1 PG(s) with no proposed move have a shard", text)

    def test_outcome_counts_moves_pins_and_bytes(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0  # room for the source's shard only: the blocker is pinned
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0)
        text = stderr_of(sh.print_outcome, result, "the drained OSDs")
        self.assertIn(
            "Proposed 1 move(s) off the drained OSDs, 9.8 MiB, 0 unplaceable;", text
        )
        self.assertIn("0 blocking shard(s) diverted, 1 pinned back.", text)

    def test_outcome_names_stuck_pgs_and_where_to_read_why(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        result = c.pg("1.0", [0, 10, 20], [0, NONE, 20]).shed(0)
        text = stderr_of(sh.print_outcome, result, "the drained OSDs")
        self.assertIn("stay backfill_toofull: 1 (1.0);", text)
        self.assertIn("Their NOTE (JSON: 'note') says why.", text)
        text = stderr_of(sh.print_outcome, self.result(), "the drained OSDs")
        self.assertIn(
            "backfill_toofull: 0; backfill_toofull now, blocker unidentified: 0.", text
        )
        self.assertNotIn("NOTE (JSON", text)

    def test_outcome_counts_blocker_moves_off_a_source_apart(self):
        # 1.0's shard 0, pinned back as a blocker, is not a move off osd.0
        # in the count, as it is not in the bytes.
        c = Cluster(default_util=95.0)
        c.util[0], c.util[11] = 50.0, 10.0
        c.pg("1.0", [0, 10, 20], [41, 10, 20], shard_pct=40)
        result = c.shed(0, 10, level=60)
        text = stderr_of(sh.print_outcome, result, "the sources")
        self.assertIn("Proposed 1 move(s) off the sources, 390.6 MiB,", text)
        self.assertIn("0 blocking shard(s) diverted, 1 pinned back.", text)


if __name__ == "__main__":
    unittest.main()
