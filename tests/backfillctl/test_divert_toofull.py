"""Unit tests for backfillctl's divert-toofull subcommand.

Two kinds of bug drive what is tested here, both of which read as
plausible output rather than as an obvious failure.

Which shards are diverted: --toofull-util keeps shards that were never
blocked from being diverted (backfill_toofull is a property of the PG, not
of each shard arriving on it). Degraded PGs are diverted too (an out OSD
leaves them so); PGs whose existing upmap pairs chain are not.

Where they go: targets are chosen by shed.Planner, as in drain and balance
(its rules are tested in test_shed.py), with the relief rule on: a target
must end up below the OSD it relieves. --max-target-util caps a target's
reservation projection, which counts every backfill in motion, and may not
exceed backfillfull_ratio. Both thresholds default to the cluster's own
ratios, so the tests check the defaults are read from the capture, and the
cluster-sized fixture is checked by invariant. The order shards are served
in (fullest acting OSD first) is divert's own, tested on divert() alone.
"""

import contextlib
import io
import json
import math
import pathlib
import random
import shutil
import tempfile
import unittest
from collections import Counter, defaultdict
from types import SimpleNamespace
from typing import ClassVar

from _support import (
    KB,
    NONE,
    PCT,
    TEST_DATA,
    FakeStore,
    SyntheticCluster,
    check_one_shard_per_host,
    check_pairs_apply,
    check_reservation_cap,
    flat,
    messages,
    osd_df_of,
    parse_args,
    placement,
    plan_from_state,
    run_command,
    shared,
    stderr_of,
    upmap_pairs,
)

from backfillctl import divert_toofull as dt

TOOFULL = "active+remapped+backfill_toofull"


class FindArrivingShardsTest(unittest.TestCase):
    def test_ec_pairs_up_and_acting_by_position(self):
        pg = {"pgid": "19.2", "up": [882, 111], "acting": [406, 111]}
        (shard,) = placement.find_arriving_shards(pg, is_ec=True)
        self.assertEqual((shard.shard, shard.up_osd, shard.acting_osd), (0, 882, 406))

    def test_ec_empty_acting_slot_yields_unknown_acting_osd(self):
        pg = {"pgid": "19.2", "up": [882, 111], "acting": [NONE, 111]}
        (shard,) = placement.find_arriving_shards(pg, is_ec=True)
        self.assertEqual(shard.up_osd, 882)
        self.assertIsNone(shard.acting_osd)

    def test_replicated_names_acting_osd_when_pairing_is_unambiguous(self):
        pg = {"pgid": "5.1", "up": [882, 111], "acting": [406, 111]}
        (shard,) = placement.find_arriving_shards(pg, is_ec=False)
        self.assertEqual((shard.shard, shard.up_osd, shard.acting_osd), ("-", 882, 406))

    def test_replicated_leaves_acting_osd_unknown_when_ambiguous(self):
        # Two replicas arriving and two leaving: no way to say which came
        # from which, so neither row claims an acting OSD.
        pg = {"pgid": "5.1", "up": [882, 883, 111], "acting": [406, 407, 111]}
        shards = placement.find_arriving_shards(pg, is_ec=False)
        self.assertEqual([s.up_osd for s in shards], [882, 883])
        self.assertEqual([s.acting_osd for s in shards], [None, None])

    def test_every_shard_of_the_pg_carries_its_size(self):
        pg = {"pgid": "19.2", "up": [882, 883, 111], "acting": [406, 407, 111]}
        shards = placement.find_arriving_shards(pg, is_ec=True, size_bytes=1234)
        self.assertEqual([s.size_bytes for s in shards], [1234, 1234])

    def test_replicated_acting_osd_is_not_named_on_every_arriving_replica(self):
        # One replica leaving and two arriving: naming the departing OSD on
        # both would claim it holds two replicas.
        pg = {"pgid": "5.1", "up": [882, 883, 111], "acting": [406, 111]}
        shards = placement.find_arriving_shards(pg, is_ec=False)
        self.assertEqual([s.up_osd for s in shards], [882, 883])
        self.assertEqual([s.acting_osd for s in shards], [None, None])

    def test_replicated_acting_osd_is_named_for_a_one_for_one_swap(self):
        pg = {"pgid": "5.1", "up": [882, 111], "acting": [406, 111]}
        (shard,) = placement.find_arriving_shards(pg, is_ec=False)
        self.assertEqual(shard.acting_osd, 406)

    def test_reordered_replicated_set_is_not_movement(self):
        pg = {"pgid": "5.1", "up": [111, 882], "acting": [882, 111]}
        self.assertEqual(placement.find_arriving_shards(pg, is_ec=False), [])


class FilterToofullPgsTest(unittest.TestCase):
    """filter_toofull_pgs: the --pgs restriction, kept apart from argparse/IO."""

    def pgs(self, *pgids):
        return [{"pgid": p} for p in pgids]

    def test_only_wanted_pgs_are_kept(self):
        kept, matched = dt.filter_toofull_pgs(
            self.pgs("19.1", "19.2", "19.3"), {"19.2"}
        )
        self.assertEqual([pg["pgid"] for pg in kept], ["19.2"])
        self.assertEqual(matched, {"19.2"})

    def test_input_order_is_preserved(self):
        kept, _ = dt.filter_toofull_pgs(
            self.pgs("19.3", "19.1", "19.2"), {"19.1", "19.3"}
        )
        self.assertEqual([pg["pgid"] for pg in kept], ["19.3", "19.1"])

    def test_wanted_id_that_matches_nothing_is_left_out_of_matched(self):
        kept, matched = dt.filter_toofull_pgs(self.pgs("19.1"), {"19.1", "19.zzz"})
        self.assertEqual([pg["pgid"] for pg in kept], ["19.1"])
        self.assertEqual(matched, {"19.1"})

    def test_empty_wanted_set_keeps_nothing(self):
        kept, matched = dt.filter_toofull_pgs(self.pgs("19.1", "19.2"), set())
        self.assertEqual((kept, matched), ([], set()))


class FullRatiosTest(unittest.TestCase):
    """The thresholds both defaults derive from."""

    def _ratios(self, dump):
        return placement.fetch_full_ratios(FakeStore({"osd_dump": dump}))

    def test_ratios_are_converted_to_percent(self):
        # Ceph reports fractions; the flags and 'ceph osd df' are in percent.
        r = self._ratios({"nearfull_ratio": 0.85, "backfillfull_ratio": 0.91})
        self.assertEqual((r.nearfull, r.backfillfull), (85.0, 91.0))

    def test_missing_ratios_fall_back_to_cephs_defaults(self):
        # Falling back to "no threshold" would be the unsafe direction, so a
        # dump without the keys still yields usable numbers.
        r = self._ratios({})
        self.assertEqual(
            (r.nearfull, r.backfillfull),
            (
                placement.DEFAULT_NEARFULL_RATIO * 100,
                placement.DEFAULT_BACKFILLFULL_RATIO * 100,
            ),
        )


# Arriving OSDs spanning the --toofull-util decision: well over the
# threshold, exactly on it, plainly below it, and one 'ceph osd df' has no
# figure for.
SOURCE_DF = {
    1: {"id": 1, "utilization": 92.0},
    2: {"id": 2, "utilization": 85.0},
    3: {"id": 3, "utilization": 70.0},
    4: {"id": 4},
}


class SelectStuckShardsTest(unittest.TestCase):
    """backfill_toofull is a PG property, so arriving shards get filtered."""

    def shard(self, up_osd):
        return placement.ArrivingShard("19.1", "-", up_osd, None, [up_osd])

    def test_shard_on_a_full_osd_is_kept(self):
        stuck, skipped = dt.select_stuck_shards([self.shard(1)], SOURCE_DF, 85.0)
        self.assertEqual(([s.up_osd for s in stuck], skipped), ([1], []))

    def test_threshold_is_inclusive(self):
        # An OSD exactly at nearfull_ratio is still a plausible blocker.
        stuck, skipped = dt.select_stuck_shards([self.shard(2)], SOURCE_DF, 85.0)
        self.assertEqual(([s.up_osd for s in stuck], skipped), ([2], []))

    def test_shard_arriving_on_an_empty_osd_is_left_alone(self):
        # The case that wasted targets before: a healthy shard of a PG that
        # is in backfill_toofull because some *other* shard is wedged.
        stuck, skipped = dt.select_stuck_shards([self.shard(3)], SOURCE_DF, 85.0)
        self.assertEqual((stuck, [s.up_osd for s in skipped]), ([], [3]))

    def test_unknown_utilization_is_kept_not_dropped(self):
        # Cannot be ruled out as the blocker, so it must not vanish silently.
        stuck, skipped = dt.select_stuck_shards([self.shard(4)], SOURCE_DF, 85.0)
        self.assertEqual(([s.up_osd for s in stuck], skipped), ([4], []))

    def test_zero_threshold_keeps_everything(self):
        shards = [self.shard(o) for o in (1, 2, 3, 4)]
        stuck, skipped = dt.select_stuck_shards(shards, SOURCE_DF, 0)
        self.assertEqual((len(stuck), skipped), (4, []))

    def test_input_order_is_preserved_in_both_halves(self):
        shards = [self.shard(o) for o in (3, 1, 3, 2)]
        stuck, skipped = dt.select_stuck_shards(shards, SOURCE_DF, 85.0)
        self.assertEqual([s.up_osd for s in stuck], [1, 2])
        self.assertEqual([s.up_osd for s in skipped], [3, 3])


class BuildCandidateOsdsTest(unittest.TestCase):
    def df(self, **overrides):
        node = {
            "id": 1,
            "status": "up",
            "reweight": 1.0,
            "crush_weight": 1.0,
            "device_class": "hdd",
            "utilization": 50.0,
            "kb": KB,
        }
        return {1: node | overrides}

    def test_usable_osd_is_offered_under_its_class(self):
        self.assertEqual(placement.build_candidate_osds(self.df()), {"hdd": [1]})

    def test_a_full_osd_is_still_a_candidate(self):
        # The cap applies to the projection, per shard, not to the shortlist.
        self.assertEqual(
            placement.build_candidate_osds(self.df(utilization=99.0)), {"hdd": [1]}
        )

    def test_osd_without_a_utilization_figure_is_excluded(self):
        # It cannot be ranked, and treating a missing value as 0% would make
        # it sort ahead of every real candidate.
        df = self.df()
        del df[1]["utilization"]
        self.assertEqual(placement.build_candidate_osds(df), {})

    def test_osd_without_a_capacity_figure_is_excluded(self):
        # The projections cannot track it, so offering it would crash the
        # planner.
        self.assertEqual(placement.build_candidate_osds(self.df(kb=0)), {})
        df = self.df()
        del df[1]["kb"]
        self.assertEqual(placement.build_candidate_osds(df), {})

    def test_down_and_out_osds_are_excluded(self):
        self.assertEqual(placement.build_candidate_osds(self.df(status="down")), {})
        self.assertEqual(placement.build_candidate_osds(self.df(reweight=0)), {})
        self.assertEqual(placement.build_candidate_osds(self.df(crush_weight=0)), {})

    def test_candidates_are_sorted_by_utilization_then_id(self):
        base = self.df()[1]
        df = {
            10: base | {"id": 10, "utilization": 60.0},
            11: base | {"id": 11, "utilization": 50.0},
            12: base | {"id": 12, "utilization": 50.0},
        }
        self.assertEqual(placement.build_candidate_osds(df), {"hdd": [11, 12, 10]})


def stuck(pgid, up_osd=1, up_set=None, size_pct=0, shard=0, acting=None):
    """A diverted shard arriving on up_osd whose size is size_pct percent of an OSD."""
    return placement.ArrivingShard(
        pgid, shard, up_osd, acting, up_set or [up_osd], size_pct * PCT
    )


class ProjectedUsageTest(unittest.TestCase):
    def test_projection_starts_from_current_usage(self):
        proj = placement.ProjectedUsage(osd_df_of({2: 50.0}), [])
        self.assertEqual(proj.utilization_after(2, 0), 50.0)

    def test_extra_bytes_are_added_without_being_recorded(self):
        proj = placement.ProjectedUsage(osd_df_of({2: 50.0}), [])
        self.assertEqual(proj.utilization_after(2, 25 * PCT), 75.0)
        self.assertEqual(proj.utilization_after(2, 0), 50.0)

    def test_redirect_accumulates_on_the_target(self):
        proj = placement.ProjectedUsage(osd_df_of({2: 50.0}), [])
        proj.redirect(stuck("1.0", up_osd=1, size_pct=10), 2)
        proj.redirect(stuck("1.1", up_osd=1, size_pct=15), 2)
        self.assertEqual(proj.utilization_after(2, 0), 75.0)

    def test_redirect_takes_the_shard_off_the_osd_it_was_headed_for(self):
        shard = stuck("1.0", up_osd=1, size_pct=20)
        proj = placement.ProjectedUsage(osd_df_of({1: 60.0, 2: 50.0}), [shard])
        self.assertEqual(proj.utilization_after(1, 0), 80.0)
        proj.redirect(shard, 2)
        self.assertEqual(proj.utilization_after(1, 0), 60.0)
        self.assertEqual(proj.utilization_after(2, 0), 70.0)

    def test_redirecting_off_an_osd_missing_from_osd_df_still_credits_the_target(self):
        proj = placement.ProjectedUsage(osd_df_of({2: 50.0}), [])
        proj.redirect(stuck("1.0", up_osd=7, size_pct=10), 2)
        self.assertEqual(proj.utilization_after(2, 0), 60.0)

    def test_arriving_shards_count_towards_their_osd(self):
        proj = placement.ProjectedUsage(
            osd_df_of({2: 50.0}), [stuck("9.9", up_osd=2, size_pct=25)]
        )
        self.assertEqual(proj.utilization_after(2, 0), 75.0)

    def test_arriving_shard_on_an_osd_missing_from_osd_df_is_ignored(self):
        proj = placement.ProjectedUsage(
            osd_df_of({2: 50.0}), [stuck("9.9", up_osd=7, size_pct=25)]
        )
        self.assertEqual(proj.utilization_after(2, 0), 50.0)

    def test_osd_without_capacity_is_not_tracked(self):
        df = osd_df_of({2: 50.0}) | {3: {"id": 3, "kb": 0, "kb_used": 0}}
        proj = placement.ProjectedUsage(df, [])
        with self.assertRaises(KeyError):
            proj.utilization_after(3, 0)


class SourcePressureTest(unittest.TestCase):
    def pressure(self, utils):
        return dt.SourcePressure(osd_df_of(utils))

    def test_utilization_is_the_acting_osds(self):
        pressure = self.pressure({6: 90.0})
        self.assertEqual(pressure.utilization(stuck("1.0", acting=6)), 90.0)

    def test_unknown_acting_osd_ranks_behind_every_known_one(self):
        pressure = self.pressure({6: 0.0})
        self.assertLess(
            pressure.utilization(stuck("1.0")),
            pressure.utilization(stuck("1.1", acting=6)),
        )

    def test_acting_osd_missing_from_osd_df_is_unknown(self):
        pressure = self.pressure({6: 90.0})
        self.assertEqual(pressure.utilization(stuck("1.0", acting=7)), -math.inf)

    def test_relieve_lowers_the_acting_osd_by_the_shard_size(self):
        pressure = self.pressure({6: 90.0})
        shard = stuck("1.0", acting=6, size_pct=10)
        pressure.relieve(shard)
        self.assertEqual(pressure.utilization(shard), 80.0)

    def test_relieving_a_shard_with_no_known_acting_osd_is_harmless(self):
        pressure = self.pressure({6: 90.0})
        pressure.relieve(stuck("1.0", size_pct=10))
        pressure.relieve(stuck("1.1", acting=7, size_pct=10))
        self.assertEqual(pressure.utilization(stuck("1.2", acting=6)), 90.0)


class ShardSizeTest(unittest.TestCase):
    PROFILES: ClassVar[dict[str, dict]] = {"k8m2": {"k": "8", "m": "2"}}
    EC_POOL: ClassVar[dict] = {
        "pool_id": 1,
        "type": shared.POOL_TYPE_ERASURE,
        "erasure_code_profile": "k8m2",
    }

    @staticmethod
    def pg(num_bytes):
        return {"pgid": "1.0", "stat_sum": {"num_bytes": num_bytes}}

    def size(self, num_bytes, pool=None):
        return placement.shard_size_bytes(
            self.pg(num_bytes), pool or self.EC_POOL, self.PROFILES
        )

    def test_ec_shard_is_one_kth_of_the_pg(self):
        self.assertEqual(self.size(800), 100)

    def test_ec_shard_size_rounds_up(self):
        self.assertEqual(self.size(17), 3)

    def test_empty_ec_pg_has_empty_shards(self):
        self.assertEqual(self.size(0), 0)

    def test_replica_is_the_whole_pg(self):
        pool = {"pool_id": 1, "type": 1, "erasure_code_profile": ""}
        self.assertEqual(self.size(800, pool), 800)

    def test_unknown_profile_is_refused_rather_than_guessed(self):
        pool = self.EC_POOL | {"erasure_code_profile": "gone"}
        with self.assertRaises(SystemExit) as cm:
            self.size(800, pool)
        self.assertIn("'gone'", str(cm.exception))


# ---------------------------------------------------------------------------
# divert(): the order shards are served in
# ---------------------------------------------------------------------------


class RecordingPlanner:
    """Stands in for shed.Planner in divert(): records the order shards are placed.

    Every shard gets a target but those of PGs in refuse, and those after
    the first room (if given) shards placed.
    """

    def __init__(self, acting_utils, refuse=(), room=None):
        self.cluster = SimpleNamespace(osd_df=osd_df_of(acting_utils))
        self.refuse = set(refuse)
        self.room = room
        self.placed: list[str] = []  # PG ids, in the order placed

    def place(self, state, shard):
        full = self.room is not None and len(self.placed) >= self.room
        return None if full or shard.pgid in self.refuse else (0.0, 99)

    def commit(self, state, shard, target):
        self.placed.append(shard.pgid)


def divert_order(shards, acting_utils, **kwargs):
    """Run divert() with a RecordingPlanner; return (placed PG ids, unplaceable ids)."""
    planner = RecordingPlanner(acting_utils, **kwargs)
    states = defaultdict(lambda: SimpleNamespace(add_move=lambda shard, target: None))
    unplaceable = dt.divert(planner, states, shards)
    return planner.placed, [s.pgid for s in unplaceable]


class DivertOrderTest(unittest.TestCase):
    def test_the_fullest_acting_osd_is_served_first(self):
        shards = [stuck("1.0", acting=6), stuck("1.1", acting=7)]
        placed, unplaceable = divert_order(shards, {6: 80.0, 7: 90.0}, room=1)
        self.assertEqual((placed, unplaceable), (["1.1"], ["1.0"]))

    def test_priority_rotates_as_a_placed_shard_relieves_its_acting_osd(self):
        # osd.6 (90%) holds two shards, osd.7 (85%) one, each 10%. Placing one
        # of osd.6's drops it to 80%, below osd.7, so the order is 6, 7, 6 and
        # not 6, 6, 7.
        shards = [
            stuck("1.0", acting=6, size_pct=10),
            stuck("1.1", acting=6, size_pct=10),
            stuck("1.2", acting=7, size_pct=10),
        ]
        placed, _ = divert_order(shards, {6: 90.0, 7: 85.0})
        self.assertEqual(placed, ["1.0", "1.2", "1.1"])

    def test_an_unplaced_shard_does_not_relieve_its_acting_osd(self):
        # 1.0 finds no target, so osd.6 stays at 90% and 1.1 still goes
        # before osd.7's.
        shards = [
            stuck("1.0", acting=6, size_pct=10),
            stuck("1.1", acting=6, size_pct=10),
            stuck("1.2", acting=7, size_pct=10),
        ]
        placed, unplaceable = divert_order(shards, {6: 90.0, 7: 85.0}, refuse={"1.0"})
        self.assertEqual((placed, unplaceable), (["1.1", "1.2"], ["1.0"]))

    def test_a_shard_with_no_known_acting_osd_goes_last(self):
        shards = [stuck("1.0"), stuck("1.1", acting=6)]
        placed, _ = divert_order(shards, {6: 60.0}, room=1)
        self.assertEqual(placed, ["1.1"])

    def test_ties_keep_the_order_given(self):
        shards = [stuck("1.0", acting=6), stuck("1.1", acting=7)]
        placed, _ = divert_order(shards, {6: 90.0, 7: 90.0}, room=1)
        self.assertEqual(placed, ["1.0"])

    def test_unplaceable_come_back_in_the_order_given(self):
        shards = [stuck(f"1.{i}", acting=a) for i, a in enumerate((6, 7, 8))]
        _, unplaceable = divert_order(
            shards, {6: 80.0, 7: 90.0, 8: 85.0}, refuse={"1.0", "1.1", "1.2"}
        )
        self.assertEqual(unplaceable, ["1.0", "1.1", "1.2"])

    def test_the_queue_per_acting_osd_ranks_like_picking_the_best_shard_overall(self):
        # divert() ranks queues of shards per acting OSD in a heap. That must
        # be the same as, each turn, taking the best of all shards: check it
        # against that naive definition on random input full of ties.
        rng = random.Random(20260920)
        for trial in range(200):
            n = rng.randint(1, 40)
            acting_utils = {
                a: rng.choice([70.0, 80.0, 80.0, 90.0]) for a in range(10, 16)
            }
            shards = [
                stuck(
                    f"1.{i}",
                    acting=rng.choice([None, *acting_utils]),
                    size_pct=rng.randint(1, 3),
                )
                for i in range(n)
            ]

            used = dict(acting_utils)
            pending = list(range(n))
            expected = []
            while pending:
                i = min(
                    pending,
                    key=lambda i: (-used.get(shards[i].acting_osd, -math.inf), i),
                )
                pending.remove(i)
                expected.append(f"1.{i}")
                if shards[i].acting_osd is not None:
                    used[shards[i].acting_osd] -= shards[i].size_bytes / PCT

            placed, unplaceable = divert_order(shards, acting_utils)
            self.assertEqual(unplaceable, [], trial)
            self.assertEqual(placed, expected, trial)


# ---------------------------------------------------------------------------
# plan(): which shards, and where, on a synthetic cluster
# ---------------------------------------------------------------------------


class Cluster(SyntheticCluster):
    """SyntheticCluster (six hosts h0..h5, OSDs host*10 + j; nearfull 85%,
    backfillfull 90%, so --max-target-util 89%) with backfill_toofull PGs."""

    def toofull(self, pgid, up, acting, **kwargs):
        """Add a backfill_toofull PG."""
        return self.pg(pgid, up, acting, state=TOOFULL, **kwargs)

    def divert(self, *argv) -> dt.DivertResult:
        return self.plan_with(dt, *argv)


def targets(result):
    """The (pgid, shard, up OSD, target OSD) of each move."""
    return [(m.pgid, m.shard, m.up_osd, m.target_osd) for m in result.moves]


class PlanTest(unittest.TestCase):
    """A stuck EC shard 0 of PG 1.0 arriving on osd.0 (h0), its siblings on
    h1 and h2."""

    def cluster(self, up_util=88.0, **utils):
        c = Cluster()
        c.util[0] = up_util
        for osd, util in utils.items():
            c.util[int(osd.removeprefix("osd"))] = util
        return c

    def test_stuck_shard_goes_to_the_least_utilized_legal_osd(self):
        c = self.cluster(osd31=40.0)
        result = c.toofull("1.0", [0, 10, 20], [1, 10, 20]).divert()
        self.assertEqual(targets(result), [("1.0", 0, 0, 31)])
        self.assertEqual((result.stuck_count, result.unplaceable), (1, []))

    def test_shard_arriving_below_toofull_util_is_left_alone(self):
        c = self.cluster(up_util=80.0)
        result = c.toofull("1.0", [0, 10, 20], [1, 10, 20]).divert()
        self.assertEqual((result.moves, result.left_alone_count), ([], 1))
        lowered = c.divert("--toofull-util", "80")
        self.assertEqual(len(lowered.moves), 1)

    def test_degraded_pg_is_diverted(self):
        # The out-OSD case: the shard's acting slot is empty, the PG degraded.
        c = self.cluster(osd31=40.0)
        c.pg("1.0", [0, 10, 20], [NONE, 10, 20], state=TOOFULL + "+degraded")
        (move,) = c.divert().moves
        self.assertEqual((move.acting_osd, move.target_osd), (None, 31))

    def test_the_up_osds_own_host_is_allowed(self):
        # osd.1 shares h0 with osd.0, which the shard leaves.
        c = self.cluster(osd1=40.0)
        result = c.toofull("1.0", [0, 10, 20], [NONE, 10, 20]).divert()
        self.assertEqual(targets(result), [("1.0", 0, 0, 1)])

    def test_hosts_of_the_pgs_other_shards_are_excluded(self):
        c = self.cluster(osd11=10.0, osd21=10.0, osd31=40.0)
        result = c.toofull("1.0", [0, 10, 20], [1, 10, 20]).divert()
        self.assertEqual(targets(result), [("1.0", 0, 0, 31)])

    def test_acting_osds_are_not_targets(self):
        # osd.1 holds the shard's data: moving there would be a pin.
        c = self.cluster(osd1=10.0, osd31=40.0)
        result = c.toofull("1.0", [0, 10, 20], [1, 10, 20]).divert()
        self.assertEqual(targets(result), [("1.0", 0, 0, 31)])

    def test_two_stuck_shards_of_a_pg_get_distinct_hosts(self):
        # osd.30 and osd.31 share h3: only one of the two shards may go there.
        c = self.cluster(osd10=88.0, osd30=30.0, osd31=31.0)
        result = c.toofull("1.0", [0, 10, 20], [1, 11, 20]).divert()
        hosts = {Cluster.host(m.target_osd) for m in result.moves}
        self.assertEqual((len(result.moves), len(hosts)), (2, 2))

    def test_target_must_end_below_the_osd_it_relieves(self):
        # osd.0 at 86% ends at 86% without the 2% shard. Every other OSD is
        # at 85%, so would end at 87%: under the 89% cap, but above osd.0.
        c = Cluster(default_util=85.0)
        c.util[0] = 86.0
        c.toofull("1.0", [0, 10, 20], [1, 10, 20], shard_pct=2.0)
        self.assertEqual(c.divert().moves, [])
        c.util[31] = 83.0  # ends at 85%
        self.assertEqual(targets(c.divert()), [("1.0", 0, 0, 31)])

    def test_cap_counts_every_backfill_in_motion(self):
        # osd.31 ends up emptiest (40%: 30% arriving, 30% leaving), but a
        # 20% shard would reserve it at 90%, over the 89% cap. The PGs moving
        # data on and off it are not backfill_toofull.
        c = self.cluster(osd31=40.0)
        c.pg("1.1", [31, 40, 50], [NONE, 40, 50], shard_pct=30.0)
        c.pg("1.2", [21, 40, 50], [31, 40, 50], shard_pct=30.0)
        c.toofull("1.0", [0, 10, 20], [NONE, 10, 20], shard_pct=20.0)
        self.assertEqual(targets(c.divert()), [("1.0", 0, 0, 1)])
        c.pgs = [pg for pg in c.pgs if pg["pgid"] != "1.1"]
        self.assertEqual(targets(c.divert()), [("1.0", 0, 0, 31)])

    def test_pg_whose_upmap_pairs_chain_is_left_alone(self):
        c = self.cluster(osd31=40.0)
        c.upmaps.append(
            {
                "pgid": "1.0",
                "mappings": [{"from": 41, "to": 40}, {"from": 40, "to": 20}],
            }
        )
        c.toofull("1.0", [0, 10, 20], [1, 10, 20])
        result = c.toofull("1.1", [0, 11, 21], [1, 11, 21]).divert()
        self.assertEqual(targets(result), [("1.1", 0, 0, 31)])
        self.assertEqual(result.chained_pgs, 1)

    def test_unplaceable_shard_whose_osd_stays_over_backfillfull(self):
        # No room anywhere, and osd.0 is reserved at 93% with the shard.
        c = Cluster(default_util=89.0)
        c.util[0] = 92.0
        result = c.toofull("1.0", [0, 10, 20], [1, 10, 20]).divert()
        self.assertEqual(
            ([s.pgid for s in result.unplaceable], result.fitting), (["1.0"], [])
        )

    def test_shard_whose_osd_has_room_once_its_siblings_go_is_left(self):
        # osd.0 (86%) has two 2% shards arriving: 90%, at backfillfull_ratio.
        # Once one is diverted to osd.31 (83% -> 85%), osd.0 is reserved at
        # 88%, and would end at 86% without the other, below any target.
        c = Cluster(default_util=85.0)
        c.util[0], c.util[31] = 86.0, 83.0
        c.toofull("1.0", [0, 10, 20], [1, 10, 20], shard_pct=2.0)
        c.toofull("1.1", [0, 11, 21], [1, 11, 21], shard_pct=2.0)
        result = c.divert()
        self.assertEqual(len(result.moves), 1)
        self.assertEqual(result.unplaceable, [])
        ((shard, util),) = result.fitting
        self.assertEqual(shard.up_osd, 0)
        self.assertAlmostEqual(util, 88.0)

    def test_shard_whose_osd_no_move_relieves_is_unplaceable(self):
        # osd.0 is reserved at 88% with the shard, below backfillfull_ratio,
        # but Ceph refuses it now and no move changes that.
        c = Cluster(default_util=89.0)
        c.util[0] = 87.0
        result = c.toofull("1.0", [0, 10, 20], [1, 10, 20]).divert()
        self.assertEqual(result.moves, [])
        self.assertEqual(
            ([s.pgid for s in result.unplaceable], result.fitting), (["1.0"], [])
        )

    def test_no_stuck_shard_projects_nothing(self):
        # A PG in motion in a pool 'pool ls detail' does not list would make
        # the projections exit; without a stuck shard they are not needed.
        c = self.cluster(up_util=80.0)
        c.toofull("1.0", [0, 10, 20], [1, 10, 20])
        c.pg("9.0", [30, 40, 50], [31, 40, 50])
        result = c.divert()
        self.assertEqual((result.stuck_count, result.moves), (0, []))
        with self.assertRaises(SystemExit):
            c.divert("--toofull-util", "80")

    def test_a_target_may_not_rise_above_an_osd_it_relieved(self):
        # osd.51 (72%) takes 1.0's 5% shard off osd.0 (85% -> 80%): 77%. 1.1's
        # shard off osd.10 (93% -> 88%) would take osd.51 to 82%: below
        # osd.10, but above osd.0. Every other OSD is at 88%, so over the cap,
        # but osd.0, on a host of 1.1's.
        c = Cluster(default_util=88.0)
        c.util |= {0: 80.0, 1: 95.0, 11: 90.0, 51: 72.0}
        c.toofull("1.0", [0, 20, 30], [1, 20, 30], shard_pct=5.0)
        c.toofull("1.1", [10, 1, 30], [11, 1, 30], shard_pct=5.0)
        result = c.divert("--toofull-util", "80")
        self.assertEqual(targets(result), [("1.0", 0, 0, 51)])
        self.assertEqual([s.pgid for s in result.unplaceable], ["1.1"])
        c.util[51] = 60.0  # 65%, then 70%: below both
        self.assertEqual(len(c.divert("--toofull-util", "80").moves), 2)

    def test_an_osd_may_not_drop_below_a_target_it_sent_data_to(self):
        # osd.0 (86%) has two 5% shards arriving: 96%. 1.0's goes to osd.51
        # (84% -> 89%; osd.41 is on a host of 1.0's), leaving osd.0 at 91%.
        # 1.1's could go to osd.41 (80% -> 85%), but would leave osd.0 at 86%,
        # below osd.51.
        c = Cluster(default_util=88.0)
        c.util |= {0: 86.0, 1: 95.0, 41: 80.0, 51: 84.0}
        c.toofull("1.0", [0, 40, 20], [1, 40, 20], shard_pct=5.0)
        c.toofull("1.1", [0, 10, 30], [1, 10, 30], shard_pct=5.0)
        result = c.divert()
        self.assertEqual(targets(result), [("1.0", 0, 0, 51)])
        self.assertEqual([s.pgid for s in result.unplaceable], ["1.1"])

    def test_moves_are_in_pg_then_shard_order(self):
        # Shard 1's acting OSD is fuller, so it is placed first.
        c = self.cluster(osd10=88.0, osd11=95.0, osd30=30.0, osd40=40.0)
        c.toofull("1.1", [30, 41, 51], [30, 40, 51])
        result = c.toofull("1.0", [0, 10, 20], [1, 11, 20]).divert()
        self.assertEqual(
            [(m.pgid, m.shard) for m in result.moves], [("1.0", 0), ("1.0", 1)]
        )

    def test_rows_of_a_target_show_its_projection_once_all_are_placed(self):
        # osd.51 (10%) takes both 1% shards: 12% in both rows.
        c = self.cluster(osd10=88.0, osd51=10.0)
        c.toofull("1.0", [0, 20, 30], [1, 20, 30])
        result = c.toofull("1.1", [10, 20, 30], [11, 20, 30]).divert()
        self.assertEqual([m.target_osd for m in result.moves], [51, 51])
        self.assertEqual({round(m.target_projected, 6) for m in result.moves}, {12.0})


class RenderTest(unittest.TestCase):
    def test_summary_names_the_rules_and_the_chained_pgs(self):
        c = Cluster()
        c.util[0], c.util[31] = 88.0, 40.0
        c.upmaps.append(
            {
                "pgid": "1.1",
                "mappings": [{"from": 41, "to": 40}, {"from": 40, "to": 21}],
            }
        )
        c.toofull("1.0", [0, 10, 20], [1, 10, 20])
        c.toofull("1.1", [0, 11, 21], [1, 11, 21])
        _, err = c.rendered_with(dt)
        self.assertIn(
            "PGs left alone: " + messages.chained_text(1) + ". Targets: ", err
        )
        self.assertIn(messages.RELIEVE_CLAUSE.lstrip(", "), err)
        self.assertIn("Proposed 1 move(s); 0 shard(s) could not be placed.", err)

    def test_left_shards_are_named_apart_from_the_unplaceable(self):
        c = Cluster(default_util=85.0)
        c.util[0], c.util[31] = 86.0, 83.0
        c.toofull("1.0", [0, 10, 20], [1, 10, 20], shard_pct=2.0)
        c.toofull("1.1", [0, 11, 21], [1, 11, 21], shard_pct=2.0)
        _, err = c.rendered_with(dt)
        self.assertIn("0 shard(s) could not be placed.", err)
        self.assertIn(
            "1 more got no target, but the moves off the OSD each is headed for "
            "take it below backfillfull_ratio 90%, so Ceph may take them as they "
            "are: 1.1 shard 0 (osd.0 88.0%).",
            err,
        )
        self.assertNotIn("greedy placement", err)

    def test_unplaceable_note_names_the_relief_rule(self):
        c = Cluster(default_util=89.0)
        c.util[0] = 92.0
        c.toofull("1.0", [0, 10, 20], [1, 10, 20])
        _, err = c.rendered_with(dt)
        self.assertIn("cannot place 1.0 shard 0 (headed for osd.0)", err)
        self.assertIn(flat(messages.unplaceable_note("the OSD it was headed for")), err)


# ---------------------------------------------------------------------------
# Captures
# ---------------------------------------------------------------------------


def proposals_from_readme(fixture):
    """Return the 'Expected proposals' block of a fixture README as tuples.

    Each is (pgid, shard, acting OSD, up OSD, target OSD), as strings, the
    acting OSD 'none' when unknown. Pulling the expected proposals out of the
    README, rather than duplicating them here, is what keeps the two from
    drifting apart.
    """
    lines = (TEST_DATA / fixture / "README.txt").read_text().splitlines()
    # The heading wraps onto further lines before the indented block.
    start = next(i for i, ln in enumerate(lines) if ln.startswith("Expected proposals"))
    block = []
    for line in lines[start:]:
        if line.startswith("  "):
            block.append(tuple(line.split()))
        elif block:
            break
    return block


def move_tuple(m):
    """m in the form proposals_from_readme returns."""
    acting = "none" if m.acting_osd is None else str(m.acting_osd)
    return (m.pgid, str(m.shard), acting, str(m.up_osd), str(m.target_osd))


def fixture_plan(fixture, *argv):
    """Return plan()'s DivertResult for a fixture under test-data."""
    return plan_from_state(dt, TEST_DATA / fixture, *argv)


def copy_without_pool(fixture: str, pool_id: int, tmp: str) -> pathlib.Path:
    """Copy fixture into directory tmp minus pool_id's 'pool ls detail' entry.

    Returns the copy's path.
    """
    dst = pathlib.Path(tmp) / "fixture"
    shutil.copytree(TEST_DATA / fixture, dst)
    path = dst / "pool_ls_detail.json"
    pools = json.loads(path.read_text())
    path.write_text(json.dumps([p for p in pools if p["pool_id"] != pool_id]))
    return dst


def remap_triples(moves):
    """The (pgid, up OSD, target OSD) of each move, as '<pgid> <up> <target>'."""
    return [f"{m.pgid} {m.up_osd} {m.target_osd}" for m in moves]


OSD457 = "divert-toofull-osd457-down"
OSD263 = "divert-toofull-osd263-existing-upmap-chain"
NOMINAL = "divert-toofull-nominal-synthetic"
CHAINED = "ceph2-cancel-backfill-chained-pairs-upmap"
CEPH2_FIXTURE = "divert-toofull-ceph2-util-emergency-2-new-hosts"

# What the ceph2 fixture's README.txt documents: 1513 arriving shards, 976
# of them plausibly blocked, 40 placed, fullest acting OSD first. The rest
# are unplaceable, or headed for an OSD the 40 take below
# backfillfull_ratio. Asserted as counts and invariants rather than an exact
# 40-row table, which would be unreadable in a README.
CEPH2_ARRIVING = 1513
CEPH2_STUCK = 976
CEPH2_CANDIDATES = 900
CEPH2_PROPOSED = 40
CEPH2_UNPLACEABLE = 934
CEPH2_FITTING = 2
CEPH2_NEARFULL = 85.0
CEPH2_BACKFILLFULL = 91.0
# Ceph refuses on a target's projected usage, so the default cap keeps one
# point of margin below backfillfull_ratio.
CEPH2_MAX_TARGET_UTIL = CEPH2_BACKFILLFULL - 1
# Proposals when the cap is instead set to backfillfull_ratio itself: targets
# may be projected right up to the ratio, with no margin.
CEPH2_NO_MARGIN_PROPOSED = 114
# With both thresholds at their loosest (--toofull-util 0, cap at
# backfillfull_ratio): still no target past backfillfull_ratio.
CEPH2_UNCAPPED_PROPOSED = 117


class PrintOutcomeTest(unittest.TestCase):
    def outcome(self, unplaceable=(), fitting=()):
        result = SimpleNamespace(
            moves=[None] * 5,
            unplaceable=list(unplaceable),
            fitting=list(fitting),
            ratios=placement.FullRatios(85.0, 90.0),
        )
        return stderr_of(dt.print_outcome, result)

    def test_names_each_unplaceable_shard_then_the_caveat(self):
        text = self.outcome(
            [
                placement.ArrivingShard("19.1", 3, 31, 7, [31]),
                placement.ArrivingShard("7.2", "-", 40, None, [40]),
            ]
        )
        self.assertIn("Proposed 5 move(s); 2 shard(s) could not be placed.", text)
        items, caveat = text.split(" NOTE: ")
        self.assertIn("the OSD it was headed for", caveat)
        self.assertEqual(
            items.split(" cannot place ")[1:],
            [
                "19.1 shard 3 (headed for osd.31): no legal target",
                "7.2 shard - (headed for osd.40): no legal target",
            ],
        )

    def test_no_caveat_or_list_when_everything_is_placed(self):
        text = self.outcome()
        self.assertIn("0 shard(s) could not be placed.", text)
        self.assertNotIn("NOTE:", text)
        self.assertNotIn("cannot place", text)
        self.assertNotIn("got no target", text)

    def test_left_shards_are_named_with_their_osds_projection(self):
        text = self.outcome(fitting=[(stuck("19.1", up_osd=31, shard=3), 88.44)])
        self.assertIn("1 more got no target", text)
        self.assertTrue(text.endswith(": 19.1 shard 3 (osd.31 88.4%)."), text)


class FixturePlanTest(unittest.TestCase):
    """plan() on the small fixtures, checked against their READMEs."""

    def test_osd457_down_proposals_match_readme(self):
        result = fixture_plan(OSD457)
        self.assertEqual(
            [move_tuple(m) for m in result.moves], proposals_from_readme(OSD457)
        )
        self.assertEqual((result.toofull_pg_count, result.arriving_count), (1, 1))

    def test_existing_upmap_chain_proposals_match_readme(self):
        result = fixture_plan(OSD263)
        self.assertEqual(
            [move_tuple(m) for m in result.moves], proposals_from_readme(OSD263)
        )
        self.assertEqual(result.unplaceable, [])

    def test_the_last_shard_to_osd263_is_left_as_it_has_room(self):
        # See the fixture's README: once the other five are diverted.
        ((shard, util),) = fixture_plan(OSD263).fitting
        self.assertEqual((shard.pgid, shard.shard, shard.up_osd), ("19.d85", 9, 263))
        self.assertLess(util, 90.0)

    def test_no_backfill_toofull_pgs_proposes_nothing(self):
        result = fixture_plan(NOMINAL)
        self.assertEqual((result.moves, result.unplaceable), ([], []))

    def test_pgs_whose_existing_pairs_chain_are_left_alone(self):
        # 19.3a4 (890->110, 110->753) and 19.5fd (889->19, 19->669).
        result = fixture_plan(CHAINED)
        self.assertEqual(result.chained_pgs, 2)
        self.assertFalse({m.pgid for m in result.moves} & {"19.3a4", "19.5fd"})
        self.assertTrue(result.moves)

    def test_default_thresholds_come_from_the_clusters_own_ratios(self):
        # osd457-down has backfillfull_ratio 0.90, ceph2 has it raised to
        # 0.91: the caps must track the capture, not a constant. The target
        # cap is backfillfull_ratio minus one point.
        for fixture, nearfull, max_target in [
            (OSD457, 85, 89),
            (CEPH2_FIXTURE, 85, 90),
        ]:
            with self.subTest(fixture=fixture):
                # Ceph keeps the ratios as float32, so 0.85 reads back as
                # 85.0000024%.
                result = fixture_plan(fixture)
                self.assertAlmostEqual(result.toofull_util, nearfull, places=3)
                self.assertAlmostEqual(result.max_target_util, max_target, places=3)

    def test_every_capture_keeps_the_placement_invariants(self):
        # Every capture with PGs: pgremapper can apply the pairs, no PG uses
        # a host twice, and no target is reserved over the cap.
        for fixture in sorted(TEST_DATA.iterdir()):
            if not (fixture / "pg_dump_pgs.json").exists():
                continue
            with self.subTest(fixture=fixture.name):
                result = plan_from_state(dt, fixture)
                check_pairs_apply(self, fixture, result)
                check_one_shard_per_host(self, fixture, result)
                check_reservation_cap(self, fixture, result)
                for m in result.moves:
                    self.assertLess(m.target_projected, m.up_projected, m)


class FixtureReplayTest(unittest.TestCase):
    """End-to-end --load-state runs: how run() prints what plan() decides."""

    def run_proc(self, fixture, *extra):
        return run_command(dt, *extra, load_state=TEST_DATA / fixture, check=True)

    def run_script(self, fixture, *extra):
        return self.run_proc(fixture, *extra).stdout.rstrip("\n")

    def test_table_has_shed_s_columns_and_shows_an_out_acting_osd_as_none(self):
        header, labels, row = self.run_script(OSD457).splitlines()
        self.assertEqual(header.split()[1::3], ["ACTING", "UP", "TARGET"])
        self.assertEqual(labels.split()[:3], ["PGID", "SHARD", "SIZE"])
        self.assertEqual(labels.split()[-1], "NOTE")
        self.assertEqual(row.split()[4:7], ["none", "-", "-"])

    def test_pgremapper_mappings_lists_existing_upmap_rows_like_any_other(self):
        # 19.bd5's existing pair is 625->263, so 263 is a 'to': the entry must
        # still map 'from' 263 to the target, for 'pgremapper import-mappings'
        # to rewrite that pair's 'to'.
        proc = self.run_proc(OSD263, "--pgremapper-mappings")
        entries = upmap_pairs(proc.stdout)
        self.assertEqual(len(entries), 5)
        self.assertIn({"pgid": "19.bd5", "mapping": {"from": 263, "to": 842}}, entries)
        # The balancer's, and that the capture has no backfill positions; no more.
        self.assertEqual(proc.stderr.count("NOTE"), 2)
        self.assertIn("has no backfill_positions.json", proc.stderr)

    def test_pgremapper_mappings_emits_up_osd_not_acting_osd(self):
        # 'pgremapper import-mappings' takes the upmap's 'from', which is the
        # UP OSD. Emitting the ACTING OSD here would remap the wrong OSD, and
        # the table would still look right.
        (entry,) = json.loads(self.run_script(OSD457, "--pgremapper-mappings"))
        self.assertEqual(
            entry,
            {
                "pgid": "19.21f",
                "mapping": {"from": 625, "to": 849},
                "shard": 7,
                "role": shared.ROLE_REQUESTED,
                "note": "",
            },
        )

    def test_no_backfill_toofull_pgs_prints_nothing_on_stdout(self):
        self.assertEqual(self.run_script(NOMINAL), "")

    def test_no_backfill_toofull_pgs_prints_an_empty_json_array(self):
        self.assertEqual(self.run_script(NOMINAL, "--pgremapper-mappings"), "[]")

    def test_balancer_note_closes_a_run_with_proposals(self):
        note = stderr_of(messages.print_balancer_note)
        for extra in ((), ("--pgremapper-mappings",)):
            with self.subTest(extra=extra):
                err = flat(self.run_proc(OSD457, *extra).stderr)
                self.assertTrue(err.endswith(note), err)

    def test_no_balancer_note_without_proposals(self):
        for fixture, extra in (
            (NOMINAL, ()),
            # No room anywhere: the one stuck shard is unplaceable.
            (OSD457, ("--max-target-util", "2")),
        ):
            with self.subTest(fixture=fixture):
                err = self.run_proc(fixture, *extra).stderr
                self.assertNotIn("balancer", err)

    def test_default_thresholds_are_reported_on_stderr(self):
        err = flat(self.run_proc(OSD457).stderr)
        self.assertIn("--toofull-util 85%", err)
        self.assertIn("--max-target-util 89%", err)


class PgsFlagTest(unittest.TestCase):
    """--pgs restricts the run to shards of the named PG(s) only."""

    def plan(self, *argv):
        return fixture_plan(OSD263, *argv)

    def test_only_the_named_pg_is_proposed(self):
        result = self.plan("--pgs", "19.bd5")
        self.assertEqual(remap_triples(result.moves), ["19.bd5 263 842"])
        self.assertEqual(result.pgs_filter, shared.PgidFilter(1, 1, []))
        self.assertEqual(result.toofull_pg_count, 1)

    def test_several_named_pgs_are_all_kept(self):
        # Targets are not pinned here: with only two of the six PGs in play,
        # room is contended differently than in the full run. Only which PGs
        # got a proposal is guaranteed.
        result = self.plan("--pgs", "19.bd5", "19.7be")
        self.assertEqual({m.pgid for m in result.moves}, {"19.bd5", "19.7be"})
        self.assertEqual(result.toofull_pg_count, 2)

    def test_id_that_matches_nothing_is_reported_and_yields_no_proposals(self):
        result = self.plan("--pgs", "19.zzz")
        self.assertEqual(result.moves, [])
        self.assertEqual(result.pgs_filter, shared.PgidFilter(1, 0, ["19.zzz"]))
        self.assertEqual(result.toofull_pg_count, 0)

    def test_a_mix_of_matching_and_unmatched_ids_reports_both(self):
        result = self.plan("--pgs", "19.bd5", "19.zzz")
        self.assertEqual(remap_triples(result.moves), ["19.bd5 263 842"])
        self.assertEqual(result.pgs_filter, shared.PgidFilter(2, 1, ["19.zzz"]))

    def test_no_pgs_flag_considers_every_pg(self):
        result = self.plan()
        self.assertEqual(len(result.moves) + len(result.fitting), 6)
        self.assertIsNone(result.pgs_filter)


class NothingToDivertTest(unittest.TestCase):
    def test_no_toofull_pg_among_pgs_says_so_and_prints_no_table(self):
        for extra, out_text in (([], ""), (["--pgremapper-mappings"], "[]\n")):
            with self.subTest(extra=extra):
                proc = run_ceph2("--pgs", "1.0", *extra)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(proc.stdout, out_text)
                self.assertIn("No backfill_toofull PGs among --pgs.", proc.stderr)
                self.assertNotIn("Targets:", proc.stderr)


class PrintPgsFilterTest(unittest.TestCase):
    """divert-toofull's --pgs note (the shared print_pgid_filter)."""

    def test_printed_even_when_planning_then_exits(self):
        # A typo in --pgs can be what trips a later error, so the note naming
        # it must not wait for render(), which an exit never reaches.
        with tempfile.TemporaryDirectory() as tmp:
            dst = copy_without_pool(OSD457, 19, tmp)
            err = io.StringIO()
            with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as ctx:
                plan_from_state(dt, dst, "--pgs", "19.21f", "19.zzz")
        self.assertIn("pool id(s) 19", str(ctx.exception))
        self.assertIn(
            "are backfill_toofull and will be the only ones considered; 1 matched "
            "nothing (not backfill_toofull, or a typo): 19.zzz.",
            flat(err.getvalue()),
        )

    def test_printed_only_with_pgs(self):
        # The note goes to stderr, ahead of the summary, only under --pgs.
        for extra, shown in [((), False), (("--pgs", "19.zzz"), True)]:
            with self.subTest(extra=extra):
                proc = run_command(
                    dt, *extra, load_state=TEST_DATA / OSD263, check=True
                )
                self.assertEqual("--pgs:" in proc.stderr, shown)


def run_ceph2(*extra, check=True):
    """Run divert-toofull on the cluster-sized fixture (see run_command)."""
    return run_command(dt, *extra, load_state=TEST_DATA / CEPH2_FIXTURE, check=check)


class Ceph2FixtureInvariantTest(unittest.TestCase):
    """The cluster-sized capture's plan, checked by invariant rather than by list.

    This is the fixture that exposed both threshold bugs: a general
    utilization emergency with two newly-added, still-empty hosts. Before
    the thresholds existed it proposed 822 remaps, 342 of them onto OSDs
    already past backfillfull_ratio, while diverting 537 shards that were
    arriving on perfectly healthy OSDs.
    """

    @classmethod
    def setUpClass(cls):
        cls.result = fixture_plan(CEPH2_FIXTURE)
        cls.moves = cls.result.moves

    def util(self, osd_id):
        return self.result.osd_df[osd_id]["utilization"]

    def test_move_and_leftover_counts_match_the_readme(self):
        self.assertEqual(len(self.moves), CEPH2_PROPOSED)
        self.assertEqual(len(self.result.unplaceable), CEPH2_UNPLACEABLE)
        self.assertEqual(len(self.result.fitting), CEPH2_FITTING)

    def test_shard_counts_match_the_readme(self):
        self.assertEqual(self.result.arriving_count, CEPH2_ARRIVING)
        self.assertEqual(self.result.stuck_count, CEPH2_STUCK)
        self.assertEqual(self.result.left_alone_count, CEPH2_ARRIVING - CEPH2_STUCK)
        leftovers = len(self.result.unplaceable) + len(self.result.fitting)
        self.assertEqual(len(self.moves) + leftovers, CEPH2_STUCK)

    def test_every_usable_osd_is_a_candidate(self):
        self.assertEqual(
            sum(map(len, self.result.candidates.values())), CEPH2_CANDIDATES
        )

    def test_no_target_is_reserved_above_the_default_cap(self):
        # The headline invariant: every one of these remaps can actually
        # complete. Before --max-target-util defaulted, 342 could not.
        check_reservation_cap(self, TEST_DATA / CEPH2_FIXTURE, self.result)
        self.assertAlmostEqual(self.result.max_target_util, CEPH2_MAX_TARGET_UTIL, 3)

    def test_default_cap_keeps_a_margin_below_backfillfull(self):
        # Capping at backfillfull_ratio itself lets targets be projected
        # into the last point below it. Pin that the margin is what keeps
        # them out, not merely that the cap is below the ratio.
        moves = fixture_plan(
            CEPH2_FIXTURE, "--max-target-util", f"{CEPH2_BACKFILLFULL:g}"
        ).moves
        self.assertEqual(len(moves), CEPH2_NO_MARGIN_PROPOSED)
        in_margin = [m for m in moves if CEPH2_MAX_TARGET_UTIL < m.target_projected]
        self.assertTrue(in_margin)

    def test_no_shard_is_diverted_off_a_healthy_osd(self):
        # The two new hosts sit around 70% and are absorbing shards, not
        # blocking them; diverting off them wasted targets.
        under = [m for m in self.moves if self.util(m.up_osd) < CEPH2_NEARFULL]
        self.assertEqual(under, [])

    def test_the_new_empty_hosts_receive_shards_instead_of_losing_them(self):
        targets = {self.result.osd_host[m.target_osd] for m in self.moves}
        self.assertTrue({"host50", "host51"} <= targets)

    def test_every_target_ends_up_below_the_osd_it_relieves(self):
        for m in self.moves:
            self.assertLess(m.target_projected, m.up_projected, m)

    def test_no_target_takes_more_than_five(self):
        # What --max-target-uses 5 used to cap; the relief rule now does.
        uses = Counter(m.target_osd for m in self.moves)
        self.assertLessEqual(max(uses.values()), 5)

    def test_targets_are_reused(self):
        targets = {m.target_osd for m in self.moves}
        self.assertLess(len(targets), len(self.moves))

    def test_projection_is_one_figure_per_target(self):
        # Moves are in PG order, not the order shards were placed in, so
        # the rows of an OSD cannot show its projection growing: all of them
        # show the final one, above its current utilization.
        projections = {}
        for m in self.moves:
            self.assertGreater(m.target_projected, self.util(m.target_osd))
            projections.setdefault(m.target_osd, set()).add(m.target_projected)
        for osd, values in projections.items():
            with self.subTest(osd=osd):
                self.assertEqual(len(values), 1)

    def test_moves_are_in_pg_order_whatever_order_shards_were_placed_in(self):
        keys = [(shared.pgid_sort_key(m.pgid), m.shard) for m in self.moves]
        self.assertEqual(keys, sorted(keys))

    def test_only_shards_on_the_fullest_acting_osds_get_the_scarce_room(self):
        # 578 of the 976 stuck shards have an acting OSD at or above
        # backfillfull_ratio and there is room for only 52, so every placed
        # shard should come from one. (In PG order far fewer did.)
        below = [m for m in self.moves if self.util(m.acting_osd) < CEPH2_BACKFILLFULL]
        self.assertEqual(below, [])

    def test_pairs_apply_and_no_pg_uses_a_host_twice(self):
        check_pairs_apply(self, TEST_DATA / CEPH2_FIXTURE, self.result)
        check_one_shard_per_host(self, TEST_DATA / CEPH2_FIXTURE, self.result)

    def test_loosest_thresholds_still_never_target_past_backfillfull(self):
        # The loosest cap allowed is backfillfull_ratio itself, so no target
        # re-wedges on arrival.
        result = fixture_plan(
            CEPH2_FIXTURE,
            "--toofull-util",
            "0",
            "--max-target-util",
            f"{CEPH2_BACKFILLFULL:g}",
        )
        self.assertEqual(len(result.moves), CEPH2_UNCAPPED_PROPOSED)
        for m in result.moves:
            self.assertLess(self.util(m.target_osd), CEPH2_BACKFILLFULL)
            self.assertLessEqual(m.target_projected, CEPH2_BACKFILLFULL)


class Ceph2FixtureOutputTest(unittest.TestCase):
    """The cluster-sized capture run end to end: CLI checks and output."""

    @classmethod
    def setUpClass(cls):
        cls.proc = run_ceph2()
        cls.moves = fixture_plan(CEPH2_FIXTURE).moves

    def test_the_counts_are_reported_on_stderr(self):
        err = flat(self.proc.stderr)
        self.assertIn(
            f"Proposed {CEPH2_PROPOSED} move(s); {CEPH2_UNPLACEABLE} shard(s) "
            "could not be placed",
            err,
        )
        self.assertIn(f"{CEPH2_FITTING} more got no target", err)
        self.assertIn(
            f"{CEPH2_ARRIVING} arriving shard(s), {CEPH2_STUCK} of them on an OSD", err
        )
        skipped = CEPH2_ARRIVING - CEPH2_STUCK
        self.assertIn(f"the other {skipped}, taken not to be the refused ones", err)

    def test_the_table_has_a_row_per_move(self):
        group_and_label_lines = 2
        self.assertEqual(
            len(self.proc.stdout.splitlines()), group_and_label_lines + CEPH2_PROPOSED
        )

    def test_the_cap_and_ratio_are_reported_on_stderr(self):
        self.assertIn(
            f"--max-target-util {CEPH2_MAX_TARGET_UTIL:g}% (backfillfull_ratio 91%)",
            flat(self.proc.stderr),
        )

    def test_max_target_uses_is_gone(self):
        proc = run_ceph2("--max-target-uses", "5", check=False)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("unrecognized arguments: --max-target-uses", proc.stderr)

    def test_a_cap_above_backfillfull_is_an_error(self):
        # 100 used to mean "no cap"; it must now fail rather than be honored.
        for value in ("91.1", "100"):
            with self.subTest(value=value):
                proc = run_ceph2("--max-target-util", value, check=False)
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(proc.stdout, "")
                self.assertIn("ERROR: max target utilization", proc.stderr)
                self.assertIn("backfillfull_ratio (91%)", proc.stderr)

    def test_a_non_positive_cap_is_an_error(self):
        # 0 parses, then fails the range check against backfillfull_ratio;
        # a negative value, a ratio or garbage is refused by argparse.
        for value, message in (
            ("0", "ERROR: max target utilization"),
            ("-5", "must be from 0 to 100, got -5"),
            ("0.89", "not a ratio like 0.85, got 0.89"),
        ):
            with self.subTest(value=value):
                proc = run_ceph2(f"--max-target-util={value}", check=False)
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(proc.stdout, "")
                self.assertIn(message, " ".join(proc.stderr.split()))

    def test_every_percent_option_refuses_a_ratio(self):
        for argv in (["--toofull-util", "0.85"], ["--max-target-util", "0.9"]):
            with self.subTest(argv=argv):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
                    parse_args(dt, argv)
                self.assertIn("not a ratio", err.getvalue())

    def test_pgremapper_mappings_mode_prints_the_planned_moves(self):
        entries = upmap_pairs(run_ceph2("--pgremapper-mappings").stdout)
        expected = [
            {"pgid": m.pgid, "mapping": {"from": m.up_osd, "to": m.target_osd}}
            for m in self.moves
        ]
        self.assertEqual(entries, expected)

    def test_unplaceable_shards_are_only_counted(self):
        err = flat(self.proc.stderr)
        self.assertEqual(err.count("could not be placed"), 1)
        self.assertIn(f"{CEPH2_UNPLACEABLE} shard(s) could not be placed.", err)
        self.assertIn(flat(messages.unplaceable_note("the OSD it was headed for")), err)
        # No per-shard '<pgid>:<shard>' list, however long the tail is.
        self.assertNotRegex(self.proc.stderr, r"\d+\.\w+:[\d-]+, ")

    def test_pgremapper_mappings_mode_reports_the_unplaceable_count_on_stderr(self):
        proc = run_ceph2("--pgremapper-mappings")
        self.assertIn(
            f"{CEPH2_UNPLACEABLE} shard(s) could not be placed", flat(proc.stderr)
        )
        # Stdout stays parseable: only the JSON array.
        self.assertNotIn("could not be placed", proc.stdout)


class UnknownPoolTest(unittest.TestCase):
    """A stuck PG whose pool is missing from 'ceph osd pool ls detail'."""

    def test_unknown_pool_is_refused_rather_than_analyzed_wrongly(self):
        # Silently skipping it would bypass the failure-domain check and
        # diff the pool's EC shards as interchangeable replicas — both
        # failures produce plausible-looking rows.
        with tempfile.TemporaryDirectory() as tmp:
            dst = copy_without_pool(OSD457, 19, tmp)
            proc = run_command(dt, load_state=dst)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("pool id(s) 19", proc.stderr)
        self.assertIn("does not list", proc.stderr)


if __name__ == "__main__":
    unittest.main()
