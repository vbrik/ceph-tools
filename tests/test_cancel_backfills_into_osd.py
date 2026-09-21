"""Unit tests for cancel-backfills-into-osd.py.

The risky parts are deciding which acting OSD a shard can be pinned back to
(EC by position, replicated by set difference), and refusing to propose a pin
that Ceph would silently drop (acting OSD already elsewhere in 'up') or that
has nothing to pin to (empty acting slot). Those rules are tested on
hand-built PG dicts; one end-to-end test runs main() against canned 'ceph'
output.
"""

import argparse
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from typing import ClassVar
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(REPO_ROOT, "cancel-backfills-into-osd.py")
spec = importlib.util.spec_from_file_location("cancel_backfills_into_osd", SCRIPT)
cb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cb)

NONE = cb.CRUSH_ITEM_NONE
OSD = 682
EC_POOL = {
    "pool_id": 19,
    "type": 3,
    "size": 6,
    "erasure_code_profile": "p",
    "crush_rule": 0,
}
REP_POOL = {"pool_id": 7, "type": 1, "size": 3, "crush_rule": 0}
HOST_RULE = {
    "rule_id": 0,
    "steps": [{"op": "take"}, {"op": "chooseleaf_indep", "type": "host"}],
}
RULES = {0: HOST_RULE}
EC_PROFILES = {"p": {"k": "4", "m": "2"}}


def pg(
    pgid: str,
    up: list,
    acting: list,
    state: str = "active+remapped+backfill_wait",
    num_objects: int = 100,
    num_bytes: int = 4_000,
    misplaced: int = 0,
    degraded: int = 0,
) -> dict:
    return {
        "pgid": pgid,
        "up": up,
        "acting": acting,
        "state": state,
        "stat_sum": {
            "num_objects": num_objects,
            "num_bytes": num_bytes,
            "num_objects_misplaced": misplaced,
            "num_objects_degraded": degraded,
        },
    }


class EcArrivalsTest(unittest.TestCase):
    def find(self, up, acting):
        return cb.find_arrivals(up, acting, OSD, is_ec=True)

    def test_shard_arriving_is_pinned_to_its_acting_osd(self):
        pins, skipped = self.find([1, 2, OSD, 4], [1, 2, 9, 4])
        self.assertEqual(pins, [(2, 9)])
        self.assertEqual(skipped, [])

    def test_osd_not_in_up_yields_nothing(self):
        self.assertEqual(self.find([1, 2, 3, 4], [1, 2, 9, 4]), ([], []))

    def test_shard_already_in_place_is_not_a_move(self):
        self.assertEqual(self.find([1, 2, OSD, 4], [1, 2, OSD, 4]), ([], []))

    def test_other_shards_moving_are_ignored(self):
        pins, _ = self.find([5, 2, OSD, 4], [1, 2, 9, 4])
        self.assertEqual(pins, [(2, 9)])

    def test_empty_acting_slot_cannot_be_pinned(self):
        pins, skipped = self.find([1, 2, OSD, 4], [1, 2, NONE, 4])
        self.assertEqual(pins, [])
        self.assertEqual([shard for shard, _ in skipped], [2])

    def test_acting_slot_missing_from_short_acting_list(self):
        pins, skipped = self.find([1, 2, OSD], [1, 2])
        self.assertEqual(pins, [])
        self.assertEqual([shard for shard, _ in skipped], [2])

    def test_acting_osd_elsewhere_in_up_is_left_to_the_resolver(self):
        # Pinning shard 2 back to 9 would put 9 in the mapping twice;
        # pin_with_companions deals with that, not find_arrivals.
        pins, skipped = self.find([1, 2, OSD, 9], [1, 2, 9, 4])
        self.assertEqual(pins, [(2, 9)])
        self.assertEqual(skipped, [])

    def test_osd_holds_another_shard_now_is_still_pinnable(self):
        # Shard 0 arrives on OSD from osd.1 while the OSD's current shard 2
        # is moving on to osd.3: the pin concerns shard 0 only.
        pins, skipped = self.find([OSD, 2, 3], [1, 2, OSD])
        self.assertEqual(pins, [(0, 1)])
        self.assertEqual(skipped, [])


class ReplicatedArrivalsTest(unittest.TestCase):
    def find(self, up, acting):
        return cb.find_arrivals(up, acting, OSD, is_ec=False)

    def test_one_arriving_one_departing_is_paired(self):
        pins, skipped = self.find([1, 2, OSD], [1, 2, 9])
        self.assertEqual(pins, [("-", 9)])
        self.assertEqual(skipped, [])

    def test_reorder_of_same_set_is_not_movement(self):
        self.assertEqual(self.find([OSD, 1, 2], [1, 2, OSD]), ([], []))

    def test_several_replicas_moving_is_ambiguous(self):
        pins, skipped = self.find([1, OSD, 700], [1, 9, 10])
        self.assertEqual(pins, [])
        self.assertEqual([shard for shard, _ in skipped], ["-"])

    def test_missing_replica_has_no_acting_osd_to_pin_to(self):
        pins, skipped = self.find([1, 2, OSD], [1, 2, NONE])
        self.assertEqual(pins, [])
        self.assertEqual([shard for shard, _ in skipped], ["-"])

    def test_osd_not_arriving(self):
        self.assertEqual(self.find([1, 2, 3], [1, 2, 9]), ([], []))


class CopiesMovingTest(unittest.TestCase):
    def test_ec_counts_moves_and_unassigned_slots(self):
        # slot 0 moves, slot 1 stays, slot 2 has no OSD anywhere, slot 3 moves
        up = [5, 2, NONE, 7]
        acting = [1, 2, NONE, 4]
        self.assertEqual(cb.copies_moving(up, acting, True, 4), 3)

    def test_replicated_counts_arrivals_and_missing_replicas(self):
        # one arrives; size 3 with 2 acting and 1 arriving leaves nothing missing
        self.assertEqual(cb.copies_moving([1, 2, 5], [1, 2], False, 3), 1)
        # one arrives and one more replica is still unassigned
        self.assertEqual(cb.copies_moving([1, 5], [1], False, 3), 2)


class ShardSizeTest(unittest.TestCase):
    def test_replicated_is_whole_pg(self):
        self.assertEqual(cb.shard_size_bytes(pg("7.1", [], []), REP_POOL, {}), 4_000)

    def test_ec_is_ceil_of_one_kth(self):
        p = pg("19.1", [], [], num_bytes=4_001)
        self.assertEqual(cb.shard_size_bytes(p, EC_POOL, EC_PROFILES), 1_001)

    def test_unknown_ec_profile_gives_none(self):
        self.assertIsNone(cb.shard_size_bytes(pg("19.1", [], []), EC_POOL, {}))


class PinWithCompanionsTest(unittest.TestCase):
    """A pin is valid only if the result has no two shards on one host/OSD."""

    def pin(self, up, acting, slot, hosts):
        return cb.pin_with_companions(up, acting, slot, hosts)

    def test_no_clash_pins_only_the_requested_shard(self):
        pins, why = self.pin([OSD, 2], [9, 2], 0, {9: "a", 2: "b", OSD: "c"})
        self.assertEqual((pins, why), ({0: 9}, None))

    def test_unknown_hosts_never_clash(self):
        self.assertEqual(self.pin([OSD, 2], [9, 2], 0, {}), ({0: 9}, None))

    def test_clashing_shard_that_is_moving_is_pinned_too(self):
        # 19.7e9's shape: shard 0 goes back to 627 on H, where shard 3 is
        # about to land (149), so shard 3 must go back to 497 as well.
        up, acting = [OSD, 300, 626, 149], [627, 300, 626, 497]
        hosts = {OSD: "x", 627: "H", 149: "H", 497: "z", 300: "b", 626: "c"}
        pins, why = self.pin(up, acting, 0, hosts)
        self.assertEqual((pins, why), ({0: 627, 3: 497}, None))
        self.assertEqual(list(pins), [0, 3])  # requested shard first

    def test_companions_chain(self):
        up, acting = [OSD, 20, 30, 40], [10, 21, 31, 41]
        hosts = {OSD: "x", 10: "A", 20: "A", 21: "B", 30: "B", 31: "C", 40: "D"}
        pins, why = self.pin(up, acting, 0, hosts)
        self.assertEqual((pins, why), ({0: 10, 1: 21, 2: 31}, None))

    def test_same_osd_twice_is_a_clash_even_without_host_info(self):
        pins, why = self.pin([1, 2, OSD, 9], [1, 2, 9, 4], 2, {})
        self.assertEqual((pins, why), ({2: 9, 3: 4}, None))

    def test_clashing_shard_that_is_not_moving_blocks_the_pin(self):
        pins, why = self.pin([OSD, 2, 3], [9, 2, 3], 0, {9: "H", 2: "H"})
        self.assertEqual(pins, {})
        self.assertIn("shard 1", why)
        self.assertIn("not moving", why)

    def test_clashing_shard_without_acting_osd_blocks_the_pin(self):
        pins, why = self.pin([OSD, 2, 149], [627, 2, NONE], 0, {627: "H", 149: "H"})
        self.assertEqual(pins, {})
        self.assertIn("no acting OSD", why)

    def test_nothing_partial_is_returned_when_a_chain_breaks(self):
        # shard 1 clashes and could be pinned, but then clashes with the
        # non-moving shard 2: the whole pin is refused.
        up, acting = [OSD, 20, 30], [10, 21, 30]
        hosts = {10: "A", 20: "A", 21: "B", 30: "B"}
        pins, why = self.pin(up, acting, 0, hosts)
        self.assertEqual(pins, {})
        self.assertIsNotNone(why)


class PinReplicaTest(unittest.TestCase):
    def test_no_clash(self):
        self.assertIsNone(cb.pin_replica([1, 2, OSD], OSD, 9, {9: "a", 2: "b"}))
        self.assertIsNone(cb.pin_replica([1, 2, OSD], OSD, 9, {}))

    def test_clash_with_another_replica(self):
        why = cb.pin_replica([1, 2, OSD], OSD, 9, {9: "H", 2: "H"})
        self.assertIn("shares a host", why)

    def test_the_replaced_replica_itself_is_not_a_clash(self):
        # osd and acting_osd may share a host: the swap removes one of them.
        self.assertIsNone(cb.pin_replica([1, OSD], OSD, 9, {9: "H", OSD: "H"}))


def run_plan(pgs, pools=None, osd_host=None, rules=None):
    pools = pools or {19: EC_POOL, 7: REP_POOL}
    return cb.plan_cancellations(
        pgs, pools, EC_PROFILES, OSD, osd_host or {}, RULES if rules is None else rules
    )


class PlanTest(unittest.TestCase):
    plan = staticmethod(run_plan)

    def test_sorted_numerically_by_pool_then_hex_pg_then_shard(self):
        pgs = [
            pg("19.16fc", [1, OSD, 3, 4], [1, 9, 3, 4]),
            pg("19.2a", [1, 2, OSD, 4], [1, 2, 9, 4]),
            pg("7.ff", [1, 2, OSD], [1, 2, 9]),
            pg("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4]),
        ]
        cancellations, _ = self.plan(pgs)
        self.assertEqual(
            [(c.pgid, c.shard) for c in cancellations],
            [("7.ff", "-"), ("19.9", 0), ("19.2a", 2), ("19.16fc", 1)],
        )

    def test_carries_acting_osd_state_size_and_progress(self):
        state = "active+remapped+backfilling"
        p = pg("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4], state, misplaced=25)
        (c,), skipped = self.plan([p])
        self.assertEqual(skipped, [])
        self.assertEqual((c.acting_osd, c.state, c.size_bytes), (8, state, 1_000))
        self.assertEqual(c.progress_pct, 75.0)

    def test_skipped_are_reported_with_pgid(self):
        p = pg("19.9", [OSD, 2, 3, 4], [NONE, 2, 3, 4])
        cancellations, skipped = self.plan([p])
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.9", 0)])

    def test_pg_of_unlisted_pool_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.plan([pg("99.1", [OSD], [9])])

    def test_pgs_not_involving_osd_are_ignored(self):
        self.assertEqual(self.plan([pg("19.9", [1, 2, 3, 4], [1, 2, 3, 4])]), ([], []))


RATIO = 91.0


class ProjectedUtilizationTest(unittest.TestCase):
    def test_adds_the_shard_to_current_usage(self):
        df = {5: osd_df_node(5, 90.0, kb=1000)}  # 1000 KiB, 900 KiB used
        self.assertAlmostEqual(cb.projected_utilization(df, 5, 0), 90.0)
        self.assertAlmostEqual(cb.projected_utilization(df, 5, 10 * 1024), 91.0)

    def test_unknown_size_counts_as_nothing(self):
        df = {5: osd_df_node(5, 90.0)}
        self.assertAlmostEqual(cb.projected_utilization(df, 5, None), 90.0)

    def test_unknown_osd_or_capacity_gives_none(self):
        self.assertIsNone(cb.projected_utilization({}, 5, 1))
        self.assertIsNone(cb.projected_utilization({5: {"kb": 0, "kb_used": 0}}, 5, 1))


class FindBlockersTest(unittest.TestCase):
    def find(self, up, acting, df, pinned=None):
        return cb.find_blockers(up, acting, pinned or {0: acting[0]}, df, RATIO, 1_000)

    def test_lists_moving_shards_whose_target_reaches_the_ratio(self):
        up, acting = [OSD, 2, 77, 88], [8, 2, 66, 99]
        df = {77: osd_df_node(77, 92.0), 88: osd_df_node(88, 80.0)}
        self.assertEqual(self.find(up, acting, df), [2])

    def test_at_the_ratio_counts(self):
        df = {77: osd_df_node(77, 91.0)}
        self.assertEqual(self.find([OSD, 77], [8, 66], df), [1])

    def test_a_shard_that_is_not_moving_is_not_a_blocker(self):
        df = {77: osd_df_node(77, 95.0)}
        self.assertEqual(self.find([OSD, 77], [8, 77], df), [])

    def test_a_shard_with_no_acting_osd_cannot_be_pinned_so_is_not_listed(self):
        df = {77: osd_df_node(77, 95.0)}
        self.assertEqual(self.find([OSD, 77], [8, NONE], df), [])

    def test_already_pinned_shards_are_not_listed(self):
        df = {77: osd_df_node(77, 95.0)}
        self.assertEqual(self.find([OSD, 77], [8, 66], df, pinned={0: 8, 1: 66}), [])

    def test_an_osd_missing_from_osd_df_is_not_a_blocker(self):
        self.assertEqual(self.find([OSD, 77], [8, 66], {}), [])

    def test_the_size_of_the_shard_can_tip_it_over(self):
        # 90.9% used is under the ratio, but a 0.2% shard takes it to 91.1%.
        df = {77: osd_df_node(77, 90.9)}
        up, acting = [OSD, 77], [8, 66]
        small = cb.find_blockers(up, acting, {0: 8}, df, RATIO, 1_000)
        big = cb.find_blockers(up, acting, {0: 8}, df, RATIO, 3 * 1024 * 1024)
        self.assertEqual((small, big), ([], [1]))


class PlanBlockersTest(unittest.TestCase):
    """plan_cancellations with the OSD utilizations and backfillfull_ratio given."""

    def plan(self, pgs, df, osd_host=None, ratio=RATIO):
        pools = {19: EC_POOL, 7: REP_POOL}
        return cb.plan_cancellations(
            pgs, pools, EC_PROFILES, OSD, osd_host or {}, RULES, df, ratio
        )

    def test_blocker_is_pinned_and_marked_with_its_projected_utilization(self):
        p = pg("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        cancellations, skipped = self.plan([p], {77: osd_df_node(77, 92.0)})
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.up_osd, c.acting_osd, c.companion_of) for c in cancellations],
            [(0, OSD, 8, None), (3, 77, 66, 0)],
        )
        self.assertIsNone(cancellations[0].blocker_util)
        self.assertAlmostEqual(cancellations[1].blocker_util, 92.0, places=2)

    def test_the_requested_shard_is_never_marked_a_blocker(self):
        p = pg("19.5", [OSD, 2], [8, 2])
        (c,), _ = self.plan([p], {OSD: osd_df_node(OSD, 99.0)})
        self.assertIsNone(c.blocker_util)

    def test_pgs_without_a_shard_into_the_osd_are_left_alone(self):
        p = pg("19.5", [1, 2, 3, 77], [1, 2, 3, 66])
        self.assertEqual(self.plan([p], {77: osd_df_node(77, 99.0)}), ([], []))

    def test_no_blockers_without_utilizations_or_ratio(self):
        p = pg("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        pools = {19: EC_POOL}
        for df, ratio in ((None, RATIO), ({77: osd_df_node(77, 99.0)}, None)):
            cancellations, _ = cb.plan_cancellations(
                [p], pools, EC_PROFILES, OSD, {}, RULES, df, ratio
            )
            self.assertEqual([c.shard for c in cancellations], [0])

    def test_a_blocker_that_cannot_be_pinned_is_reported_and_the_pin_is_kept(self):
        # pinning shard 3 back to osd.66 would share host H with shard 1
        # (osd.2), which is not moving, so the blocker is skipped.
        p = pg("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        cancellations, skipped = self.plan(
            [p], {77: osd_df_node(77, 92.0)}, osd_host={66: "H", 2: "H"}
        )
        self.assertEqual([c.shard for c in cancellations], [0])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.5", 3)])
        self.assertTrue(skipped[0].reason.startswith("blocker:"))

    def test_pinning_a_blocker_can_pull_in_a_companion(self):
        # shard 2 (77) is the blocker; its acting osd.66 shares a host with
        # shard 3's target osd.88, so shard 3 comes along as a plain companion.
        p = pg("19.5", [OSD, 2, 77, 88], [8, 2, 66, 99])
        df = {77: osd_df_node(77, 92.0), 88: osd_df_node(88, 50.0)}
        cancellations, skipped = self.plan([p], df, osd_host={66: "H", 88: "H"})
        self.assertEqual(skipped, [])
        self.assertEqual([c.shard for c in cancellations], [0, 2, 3])
        self.assertIsNotNone(cancellations[1].blocker_util)
        self.assertIsNone(cancellations[2].blocker_util)  # companion, not blocker
        self.assertEqual(cancellations[2].companion_of, 0)

    def test_a_companion_whose_target_is_over_the_ratio_counts_as_a_blocker(self):
        p = pg("19.5", [OSD, 149], [627, 497])
        cancellations, _ = self.plan(
            [p], {149: osd_df_node(149, 92.0)}, osd_host={627: "H", 149: "H"}
        )
        self.assertEqual([c.shard for c in cancellations], [0, 1])
        self.assertAlmostEqual(cancellations[1].blocker_util, 92.0, places=2)

    def test_several_blockers_come_in_shard_order(self):
        p = pg("19.5", [OSD, 77, 3, 88], [8, 66, 3, 99])
        df = {77: osd_df_node(77, 92.0), 88: osd_df_node(88, 93.0)}
        cancellations, _ = self.plan([p], df)
        self.assertEqual([c.shard for c in cancellations], [0, 1, 3])

    def test_replicated_pgs_get_no_blockers(self):
        p = pg("7.1", [1, 2, OSD], [1, 2, 9])
        cancellations, _ = self.plan([p], {2: osd_df_node(2, 99.0)})
        self.assertEqual(
            [(c.shard, c.blocker_util) for c in cancellations], [("-", None)]
        )


class OrderMovesTest(unittest.TestCase):
    """Ceph applies an entry's pairs in order and skips one whose target is
    still in the mapping, so a pair must follow the pair that moves its target
    away."""

    def test_independent_moves_keep_shard_order(self):
        moves = [(3, 30, 31), (0, 10, 11), (1, 20, 21)]
        self.assertEqual(
            cb.order_moves(moves), ([(0, 10, 11), (1, 20, 21), (3, 30, 31)], None)
        )

    def test_a_chain_puts_the_pair_that_frees_the_osd_first(self):
        # 19.1299's shape: shard 1 goes 891->579 but 579 is still shard 8's OSD
        moves = [(1, 891, 579), (8, 579, 825)]
        self.assertEqual(cb.order_moves(moves), ([(8, 579, 825), (1, 891, 579)], None))

    def test_a_chain_of_three(self):
        moves = [(0, 1, 2), (1, 2, 3), (2, 3, 4)]
        ordered, why = cb.order_moves(moves)
        self.assertIsNone(why)
        self.assertEqual([m[0] for m in ordered], [2, 1, 0])

    def test_unrelated_moves_do_not_get_reordered_by_a_chain(self):
        moves = [(0, 1, 2), (1, 2, 3), (2, 50, 51)]
        ordered, _ = cb.order_moves(moves)
        self.assertEqual([m[0] for m in ordered], [1, 0, 2])

    def test_a_ring_cannot_be_expressed(self):
        ordered, why = cb.order_moves([(0, 1, 2), (1, 2, 1)])
        self.assertEqual(ordered, [])
        self.assertIn("cycle", why)

    def test_a_ring_inside_a_longer_list_is_still_found(self):
        ordered, why = cb.order_moves([(0, 10, 11), (1, 1, 2), (2, 2, 3), (3, 3, 1)])
        self.assertEqual(ordered, [])
        self.assertIn("cycle", why)

    def test_empty_and_single(self):
        self.assertEqual(cb.order_moves([]), ([], None))
        self.assertEqual(cb.order_moves([(0, 1, 2)]), ([(0, 1, 2)], None))


class PlanChainTest(unittest.TestCase):
    plan = staticmethod(run_plan)

    def test_a_chained_pin_comes_out_in_the_valid_order(self):
        # shard 0 moves to osd.20 while osd.20 still holds shard 1, which is
        # itself going to osd.30: (20->30) has to be applied before (OSD->20)
        p = pg("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        cancellations, skipped = self.plan([p])
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.up_osd, c.acting_osd) for c in cancellations],
            [(1, 20, 30), (0, OSD, 20)],
        )

    def test_a_ring_skips_the_pin_and_says_why(self):
        # osd.20 and OSD would swap places between shards 0 and 1
        p = pg("19.f", [OSD, 20, 3, 4], [20, OSD, 3, 4])
        cancellations, skipped = self.plan([p])
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.f", 0)])
        self.assertIn("cycle", skipped[0].reason)

    def test_the_order_survives_sorting_across_pgs(self):
        chain = pg("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        plain = pg("19.2", [OSD, 2, 3, 4], [8, 2, 3, 4])
        cancellations, _ = self.plan([chain, plain])
        self.assertEqual(
            [(c.pgid, c.shard) for c in cancellations],
            [("19.2", 0), ("19.f", 1), ("19.f", 0)],
        )

    def test_a_blocker_that_would_make_a_ring_is_skipped_but_the_pin_stays(self):
        # shard 3 (77->66) is a blocker (osd.77 is over the ratio) but pinning
        # it back would ring with shard 2 (66 <-> 77 swap): skip it only.
        p = pg("19.5", [OSD, 2, 66, 77], [8, 2, 77, 66])
        cancellations, skipped = run_plan_with_df(
            [p], {77: osd_df_node(77, 92.0), 66: osd_df_node(66, 50.0)}
        )
        self.assertEqual([c.shard for c in cancellations], [0])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.5", 3)])
        self.assertIn("cycle", skipped[0].reason)


def run_plan_with_df(pgs, df, ratio=RATIO):
    return cb.plan_cancellations(
        pgs, {19: EC_POOL, 7: REP_POOL}, EC_PROFILES, OSD, {}, RULES, df, ratio
    )


class ChainedPgsTest(unittest.TestCase):
    def c(self, pgid, shard, up, acting):
        return cb.Cancellation(pgid, shard, up, acting, None, "s", None)

    def test_finds_only_pgs_with_chained_pairs_in_order(self):
        cs = [
            self.c("19.1", 0, 1, 2),
            self.c("19.1", 1, 5, 6),  # independent
            self.c("19.9", 0, 20, 30),
            self.c("19.9", 1, 682, 20),  # chains with the pair above
        ]
        chained = cb.chained_pgs(cs)
        self.assertEqual(list(chained), ["19.9"])
        self.assertEqual(len(chained["19.9"]), 2)

    def test_none_when_nothing_chains(self):
        self.assertEqual(cb.chained_pgs([self.c("19.1", 0, 1, 2)]), {})

    def test_warning_gives_the_commands_in_apply_order(self):
        chained = {"19.9": [self.c("19.9", 1, 20, 30), self.c("19.9", 0, 682, 20)]}
        for left_out in (True, False):
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                cb.warn_chained_pgs(chained, left_out=left_out)
            text = err.getvalue()
            self.assertIn("ceph osd pg-upmap-items 19.9 20 30 682 20", text)
            self.assertIn("panic", text)
            self.assertEqual("left out of this output" in text, left_out)


class NoteTest(unittest.TestCase):
    def note(self, **kw):
        c = cb.Cancellation("19.1", 3, 77, 66, 1_000, "s", None, **kw)
        return cb.format_note(c)

    def test_requested_shards_have_no_note(self):
        self.assertEqual(self.note(), "")

    def test_companion(self):
        self.assertEqual(self.note(companion_of=0), "companion of shard 0")

    def test_blocker_names_the_shard_it_blocks_and_the_projection(self):
        self.assertEqual(
            self.note(companion_of=0, blocker_util=92.04),
            "blocks shard 0: target osd.77 would be at 92.0%, over backfillfull",
        )


class PlanCompanionsTest(unittest.TestCase):
    """Planning that involves same-host clashes."""

    plan = staticmethod(run_plan)

    HOSTS: ClassVar[dict[int, str]] = {OSD: "x", 627: "H", 149: "H", 497: "z"}

    def test_companion_is_emitted_and_marked(self):
        p = pg("19.7e9", [OSD, 300, 626, 149], [627, 300, 626, 497])
        cancellations, skipped = self.plan([p], osd_host=self.HOSTS)
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.up_osd, c.acting_osd, c.companion_of) for c in cancellations],
            [(0, OSD, 627, None), (3, 149, 497, 0)],
        )
        # the companion shares the PG's state, size and progress
        self.assertEqual({c.state for c in cancellations}, {p["state"]})
        self.assertEqual({c.size_bytes for c in cancellations}, {1_000})

    def test_unresolvable_clash_is_skipped_without_partial_output(self):
        p = pg("19.7e9", [OSD, 300, 149], [627, 300, 149])
        cancellations, skipped = self.plan([p], osd_host=self.HOSTS)
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.7e9", 0)])

    def test_replicated_clash_is_skipped(self):
        p = pg("7.1", [1, 2, OSD], [1, 2, 9])
        cancellations, skipped = self.plan([p], osd_host={9: "H", 2: "H"})
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("7.1", "-")])

    def test_replicated_without_clash_is_pinned(self):
        p = pg("7.1", [1, 2, OSD], [1, 2, 9])
        (c,), _ = self.plan([p], osd_host={9: "a", 2: "b"})
        self.assertEqual((c.shard, c.up_osd, c.acting_osd), ("-", OSD, 9))

    def test_companions_sort_with_their_pg(self):
        a = pg("19.2", [OSD, 300, 626, 149], [627, 300, 626, 497])
        b = pg("19.1", [OSD, 2, 3, 4], [8, 2, 3, 4])
        cancellations, _ = self.plan([a, b], osd_host=self.HOSTS)
        self.assertEqual(
            [(c.pgid, c.shard) for c in cancellations],
            [("19.1", 0), ("19.2", 0), ("19.2", 3)],
        )

    def test_non_host_failure_domain_is_an_error(self):
        rack = {
            0: {"rule_id": 0, "steps": [{"op": "chooseleaf_indep", "type": "rack"}]}
        }
        with self.assertRaises(SystemExit) as ctx:
            self.plan([pg("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])], rules=rack)
        self.assertIn("rack", str(ctx.exception))

    def test_missing_crush_rule_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.plan([pg("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])], rules={})

    def test_failure_domain_is_not_checked_for_uninvolved_pgs(self):
        self.assertEqual(
            self.plan([pg("19.9", [1, 2, 3, 4], [1, 2, 3, 4])], rules={}), ([], [])
        )


class OutputTest(unittest.TestCase):
    def cancellation(self, companion_of=None):
        return cb.Cancellation("19.7e9", 3, 149, 497, 1_000, "s", 50.0, companion_of)

    def test_pgremapper_uses_each_lines_own_up_osd(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cb.print_pgremapper([self.cancellation(), self.cancellation(0)])
        self.assertEqual(out.getvalue(), "19.7e9 149 497\n19.7e9 149 497\n")

    def test_row_marks_companions_in_the_note_column(self):
        cells = dict(zip(cb.COLUMNS, cb.format_row(self.cancellation(0), {}, {})))
        self.assertEqual(cells["UP OSD"], "osd.149")
        self.assertEqual((cells["UP UTIL"], cells["UP HOST"]), ("?", "?"))
        self.assertEqual(cells["NOTE"], "companion of shard 0")
        plain = dict(zip(cb.COLUMNS, cb.format_row(self.cancellation(), {}, {})))
        self.assertEqual(plain["NOTE"], "")


class UpAndActingColumnsTest(unittest.TestCase):
    def test_each_osd_gets_its_own_utilization_and_host(self):
        c = cb.Cancellation("19.1", 0, 5, 7, 1_000, "s", None)
        osd_df = {5: {"utilization": 91.25}, 7: {"utilization": 80.0}}
        cells = dict(zip(cb.COLUMNS, cb.format_row(c, osd_df, {5: "hu", 7: "ha"})))
        self.assertEqual(
            (cells["UP OSD"], cells["UP UTIL"], cells["UP HOST"]),
            ("osd.5", "91.2%", "hu"),
        )
        self.assertEqual(
            (cells["ACTING OSD"], cells["ACTING UTIL"], cells["ACTING HOST"]),
            ("osd.7", "80.0%", "ha"),
        )

    def test_missing_figures_show_a_question_mark(self):
        c = cb.Cancellation("19.1", 0, 5, 7, 1_000, "s", None)
        cells = dict(zip(cb.COLUMNS, cb.format_row(c, {}, {})))
        self.assertEqual(
            [cells[k] for k in ("UP UTIL", "UP HOST", "ACTING UTIL", "ACTING HOST")],
            ["?"] * 4,
        )


class ImportMappingsOutputTest(unittest.TestCase):
    def cancellations(self):
        return [
            cb.Cancellation("19.14cd", 5, 232, 337, 1_000, "s", None, 8),
            cb.Cancellation("19.14cd", 8, 896, 614, 1_000, "s", None),
            cb.Cancellation("7.1", "-", 5, 6, 1_000, "s", None),
        ]

    def printed(self, cancellations):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cb.print_import_mappings(cancellations)
        return out.getvalue()

    def test_is_a_json_array_of_pgid_and_mapping_entries(self):
        self.assertEqual(
            json.loads(self.printed(self.cancellations())),
            [
                {"pgid": "19.14cd", "mapping": {"from": 232, "to": 337}},
                {"pgid": "19.14cd", "mapping": {"from": 896, "to": 614}},
                {"pgid": "7.1", "mapping": {"from": 5, "to": 6}},
            ],
        )

    def test_one_entry_per_line_so_it_is_easy_to_read_and_prune(self):
        lines = self.printed(self.cancellations()).splitlines()
        self.assertEqual(lines[0], "[")
        self.assertEqual(lines[-1], "]")
        self.assertEqual(len(lines), 2 + 3)
        self.assertTrue(lines[1].endswith(","))
        self.assertFalse(lines[-2].endswith(","))  # valid JSON: no trailing comma

    def test_a_single_entry_has_no_comma(self):
        self.assertEqual(
            json.loads(self.printed(self.cancellations()[:1])),
            [{"pgid": "19.14cd", "mapping": {"from": 232, "to": 337}}],
        )

    def test_same_pairs_as_the_pgremapper_lines(self):
        cancellations = self.cancellations()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cb.print_pgremapper(cancellations)
        from_lines = [tuple(line.split()) for line in out.getvalue().splitlines()]
        from_json = [
            (e["pgid"], str(e["mapping"]["from"]), str(e["mapping"]["to"]))
            for e in json.loads(self.printed(cancellations))
        ]
        self.assertEqual(from_lines, from_json)


class SeveralPinsTest(unittest.TestCase):
    def c(self, pgid, shard):
        return cb.Cancellation(pgid, shard, 1, 2, None, "s", None)

    def test_lists_only_pgs_with_more_than_one_pin_in_pg_order(self):
        cs = [self.c("19.16fc", 1), self.c("19.16fc", 2), self.c("19.9", 0)]
        cs += [self.c("7.a", 0), self.c("7.a", 1), self.c("7.b", 0)]
        self.assertEqual(cb.pgs_needing_several_pins(cs), ["7.a", "19.16fc"])

    def test_none_when_every_pg_has_one(self):
        self.assertEqual(cb.pgs_needing_several_pins([self.c("19.9", 0)]), [])

    def test_warning_names_the_pgs_and_the_remedy(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            cb.warn_separate_remaps(["19.1", "19.2"])
        self.assertIn("19.1, 19.2", err.getvalue())
        self.assertIn("--import-mappings", err.getvalue())

    def test_a_long_list_of_pgs_is_shortened(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            cb.warn_separate_remaps([f"19.{i:x}" for i in range(20)])
        self.assertIn("20 in all", err.getvalue())
        self.assertNotIn("19.13", err.getvalue())


class PrintTableTest(unittest.TestCase):
    def test_no_rows_prints_just_the_header(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cb.print_table([])
        self.assertEqual(
            out.getvalue().split(), [w for c in cb.COLUMNS for w in c.split()]
        )


class ParseOsdTest(unittest.TestCase):
    def test_bare_and_prefixed(self):
        self.assertEqual(cb.parse_osd("682"), 682)
        self.assertEqual(cb.parse_osd("osd.682"), 682)

    def test_rejects_garbage_and_negatives(self):
        for text in ("", "osd.", "x", "-1", "6.8"):
            with self.subTest(text=text), self.assertRaises(argparse.ArgumentTypeError):
                cb.parse_osd(text)


def osd_df_node(osd_id, util, kb=1_000_000):
    """A 'ceph osd df' node at util percent of kb KiB."""
    return {
        "id": osd_id,
        "utilization": util,
        "kb": kb,
        "kb_used": int(kb * util / 100),
    }


def canned_snapshots(pg_stats, extra_osds=()):
    """The six snapshots (cb.SNAPSHOT_COMMANDS keys) of a tiny cluster."""
    return {
        "pg_ls_remapped": {"pg_ready": True, "pg_stats": pg_stats},
        "pool_ls_detail": [{**EC_POOL, "pool_name": "ec"}, REP_POOL],
        "osd_df": {
            "nodes": [
                {"id": OSD, "utilization": 88.0, "kb": 1000, "kb_used": 880},
                {"id": 8, "utilization": 90.5, "kb": 1000, "kb_used": 905},
                *extra_osds,
            ]
        },
        "osd_tree": {
            "nodes": [
                {"id": -1, "type": "host", "name": "h1.example", "children": [OSD]},
                {"id": -2, "type": "host", "name": "h2.example", "children": [8, 9]},
                {"id": OSD, "type": "osd"},
                {"id": 8, "type": "osd"},
                {"id": 9, "type": "osd"},
            ]
        },
        "crush_rule_dump": [{**HOST_RULE, "rule_name": "secret-rule"}],
        "osd_dump": {
            "fsid": "11111111-2222-3333-4444-555555555555",
            "full_ratio": 0.95,
            "backfillfull_ratio": 0.91,
            "nearfull_ratio": 0.85,
            "erasure_code_profiles": EC_PROFILES,
            "osds": [{"osd": 8, "public_addr": "10.1.2.3:6800/1", "uuid": "abc"}],
        },
    }


def canned_ceph(pg_stats, extra_osds=(), drop=()):
    """Return a stand-in for _ceph_json serving a tiny cluster.

    drop names 'osd dump' keys to leave out.
    """
    data = canned_snapshots(pg_stats, extra_osds)
    for key in drop:
        del data["osd_dump"][key]

    def fake(key):
        return json.loads(json.dumps(data[key]))

    return fake


class MainTest(unittest.TestCase):
    PGS: ClassVar[list[dict]] = [
        pg("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4], "active+remapped+backfilling"),
        pg("19.a", [OSD, 2, 3, 4], [NONE, 2, 3, 4]),
        pg("19.b", [1, 2, 3, 4], [1, 2, 3, 4], "active+clean"),
        # osd.8 and osd.9 share host h2: pinning shard 0 back to 8 clashes with
        # shard 3 arriving on 9, so 9->4 is pinned back as a companion.
        pg("19.d", [OSD, 2, 3, 9], [8, 2, 3, 4]),
    ]

    def run_main(self, *argv, pgs=None, extra_osds=(), drop=()):
        out, err = io.StringIO(), io.StringIO()
        fake = canned_ceph(self.PGS if pgs is None else pgs, extra_osds, drop)
        with (
            mock.patch.object(cb, "_ceph_json", fake),
            mock.patch("sys.argv", ["cancel-backfills-into-osd.py", *argv]),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            cb.main()
        return out.getvalue(), err.getvalue()

    def test_pgremapper_output_is_bare_lines_only(self):
        out, err = self.run_main("--pgremapper", "osd.682")
        # 19.d's companion line has its own up OSD, not 682
        self.assertEqual(out, "19.9 682 8\n19.d 682 8\n19.d 9 4\n")
        # the unpinnable shard is reported, not silently dropped
        self.assertIn("19.a", err)
        self.assertIn("1 more shard(s)", err)

    def test_table_shows_acting_osd_state_and_host(self):
        out, _ = self.run_main("682")
        lines = out.splitlines()
        self.assertIn("PGID", lines[0])
        self.assertEqual(len(lines), 4)
        for cell in ("19.9", "osd.682", "osd.8", "90.5%", "h2", "backfilling"):
            self.assertIn(cell, lines[1])
        # the UP OSD (osd.682) gets its own utilization and host too
        self.assertRegex(lines[1], r"osd\.682\s+88\.0%\s+h1\s+osd\.8\s+90\.5%\s+h2")
        self.assertIn("companion of shard 0", lines[3])

    def test_import_mappings_is_json_with_every_pair_and_no_warning(self):
        out, err = self.run_main("--import-mappings", "682")
        self.assertEqual(
            json.loads(out),
            [
                {"pgid": "19.9", "mapping": {"from": 682, "to": 8}},
                {"pgid": "19.d", "mapping": {"from": 682, "to": 8}},
                {"pgid": "19.d", "mapping": {"from": 9, "to": 4}},
            ],
        )
        self.assertNotIn("WARNING", err)
        self.assertIn("19.a", err)  # unpinnable shards are still reported

    def test_pgremapper_warns_when_a_pg_needs_several_remaps(self):
        _, err = self.run_main("--pgremapper", "682")
        self.assertIn("WARNING", err)
        self.assertIn("19.d", err)  # the PG with the companion
        self.assertNotIn("19.9,", err)  # a single-line PG is not named
        self.assertIn("--import-mappings", err)

    def test_pgremapper_does_not_warn_when_every_pg_has_one_remap(self):
        out, err = self.run_main("--pgremapper", "682", pgs=[self.PGS[0]])
        self.assertEqual(out, "19.9 682 8\n")
        self.assertNotIn("WARNING", err)

    def test_table_output_does_not_warn(self):
        _, err = self.run_main("682")
        self.assertNotIn("WARNING", err)

    def test_blocker_in_the_same_pg_is_proposed_and_explained(self):
        # osd.77 is at 92%, over the 91% backfillfull_ratio, so shard 3 (66->77)
        # would hold 19.e in backfill_toofull and must be pinned back too.
        pgs = [pg("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        out, err = self.run_main(
            "--pgremapper", "682", pgs=pgs, extra_osds=[osd_df_node(77, 92.0)]
        )
        self.assertEqual(out, "19.e 682 8\n19.e 77 66\n")
        self.assertIn("1 more shard(s)", err)
        self.assertIn("1 because their target would be over backfillfull_ratio", err)

    def test_table_says_which_shard_a_blocker_blocks(self):
        pgs = [pg("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        out, _ = self.run_main("682", pgs=pgs, extra_osds=[osd_df_node(77, 92.0)])
        rows = out.splitlines()
        self.assertEqual(len(rows), 3)
        self.assertIn("blocks shard 0: target osd.77 would be at 92.0%", rows[2])
        self.assertNotIn("blocks", rows[1])

    def test_a_target_below_the_ratio_is_not_a_blocker(self):
        pgs = [pg("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        out, err = self.run_main(
            "--pgremapper", "682", pgs=pgs, extra_osds=[osd_df_node(77, 80.0)]
        )
        self.assertEqual(out, "19.e 682 8\n")
        self.assertNotIn("more shard(s)", err)

    def test_without_a_backfillfull_ratio_no_blockers_are_looked_for(self):
        pgs = [pg("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        out, err = self.run_main(
            "--pgremapper",
            "682",
            pgs=pgs,
            extra_osds=[osd_df_node(77, 92.0)],
            drop=["backfillfull_ratio"],
        )
        self.assertEqual(out, "19.e 682 8\n")
        self.assertIn("no backfillfull_ratio", err)

    def test_chained_pgs_are_left_out_of_the_machine_formats(self):
        chain = pg("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        plain = pg("19.2", [OSD, 2, 3, 4], [8, 2, 3, 4])
        out, err = self.run_main("--import-mappings", "682", pgs=[chain, plain])
        self.assertEqual(
            json.loads(out), [{"pgid": "19.2", "mapping": {"from": 682, "to": 8}}]
        )
        self.assertIn("ceph osd pg-upmap-items 19.f 20 30 682 20", err)
        out, err = self.run_main("--pgremapper", "682", pgs=[chain, plain])
        self.assertEqual(out, "19.2 682 8\n")
        self.assertIn("ceph osd pg-upmap-items 19.f 20 30 682 20", err)
        self.assertNotIn("need more than one remap", err)  # 19.2 has one line

    def test_the_table_still_shows_a_chained_pg_in_apply_order(self):
        chain = pg("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        out, err = self.run_main("682", pgs=[chain])
        rows = out.splitlines()
        self.assertEqual(
            [r.split()[:2] for r in rows[1:]], [["19.f", "1"], ["19.f", "0"]]
        )
        self.assertIn("ceph osd pg-upmap-items 19.f 20 30 682 20", err)
        self.assertNotIn("left out", err)

    def test_import_mappings_is_valid_json_even_with_nothing_to_apply(self):
        # nothing arriving at all
        out, err = self.run_main("--import-mappings", "682", pgs=[self.PGS[2]])
        self.assertEqual(json.loads(out), [])
        self.assertIn("No backfills", err)
        # only an unpinnable shard (no acting OSD)
        out, err = self.run_main("--import-mappings", "682", pgs=[self.PGS[1]])
        self.assertEqual(json.loads(out), [])
        self.assertIn("19.a", err)

    def test_no_backfills_prints_nothing_on_stdout(self):
        out, err = self.run_main("--pgremapper", "682", pgs=[self.PGS[2]])
        self.assertEqual(out, "")
        self.assertIn("No backfills", err)

    def test_unknown_osd_is_an_error(self):
        with self.assertRaises(SystemExit) as ctx:
            self.run_main("5000")
        self.assertIn("5000", str(ctx.exception))


class AnonymizeTest(unittest.TestCase):
    def snapshots(self):
        return canned_snapshots([pg("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])])

    def test_hosts_pools_and_rules_are_renamed(self):
        snaps = self.snapshots()
        cb.anonymize_snapshots(snaps)
        hosts = [n["name"] for n in snaps["osd_tree"]["nodes"] if n["type"] == "host"]
        self.assertTrue(all("example" not in h for h in hosts), hosts)
        self.assertEqual(len(set(hosts)), 2)
        self.assertEqual(snaps["pool_ls_detail"][0]["pool_name"], "pool19")
        self.assertEqual(snaps["crush_rule_dump"][0]["rule_name"], "rule0")

    def test_hostname_with_trailing_number_maps_to_that_number(self):
        self.assertEqual(cb._fake_hostname("ceph2-7"), "host07")
        self.assertEqual(cb._fake_hostname("ceph2-51"), "host51")

    def test_hostname_without_trailing_number_is_hashed_deterministically(self):
        fake = cb._fake_hostname("nodigits.example")
        self.assertRegex(fake, r"host-[0-9a-f]{8}")
        self.assertEqual(fake, cb._fake_hostname("nodigits.example"))
        self.assertNotEqual(fake, cb._fake_hostname("other.example"))

    def test_fake_names_map_to_themselves(self):
        for name in ("host51", cb._fake_hostname("nodigits.example")):
            self.assertEqual(cb._fake_hostname(name), name)

    def test_osd_dump_is_reduced_to_what_the_analysis_reads(self):
        snaps = self.snapshots()
        cb.anonymize_snapshots(snaps)
        self.assertEqual(
            snaps["osd_dump"],
            {
                "erasure_code_profiles": EC_PROFILES,
                "full_ratio": 0.95,
                "backfillfull_ratio": 0.91,
                "nearfull_ratio": 0.85,
            },
        )

    def test_osd_dump_without_ratios_stays_valid(self):
        snaps = self.snapshots()
        del snaps["osd_dump"]["backfillfull_ratio"]
        cb.anonymize_snapshots(snaps)
        self.assertNotIn("backfillfull_ratio", snaps["osd_dump"])

    def test_analysis_inputs_are_left_alone(self):
        before = self.snapshots()
        after = self.snapshots()
        cb.anonymize_snapshots(after)
        for key in ("pg_ls_remapped", "osd_df"):
            self.assertEqual(after[key], before[key])

    def test_idempotent(self):
        once = self.snapshots()
        cb.anonymize_snapshots(once)
        twice = json.loads(json.dumps(once))
        cb.anonymize_snapshots(twice)
        self.assertEqual(once, twice)

    def test_write_saves_all_keys_and_leaves_the_input_untouched(self):
        snaps = self.snapshots()
        with tempfile.TemporaryDirectory() as tmp:
            cb.write_anonymized_state(pathlib.Path(tmp), snaps)
            self.assertEqual(
                {f.stem for f in pathlib.Path(tmp).glob("*.json")},
                set(cb.SNAPSHOT_COMMANDS),
            )
            saved = (pathlib.Path(tmp) / "osd_tree.json").read_text()
        self.assertNotIn("example", saved)
        self.assertEqual(snaps, self.snapshots())  # run's own data not anonymized


class HostnameCollisionTest(unittest.TestCase):
    """Two hosts must never anonymize to one: the analysis depends on sharing."""

    def test_same_trailing_number_gets_distinct_names(self):
        fakes = cb._fake_hostnames({"ceph1-5", "ceph2-5", "ceph2-6"})
        self.assertEqual(len(set(fakes.values())), 3)
        self.assertEqual(fakes["ceph2-6"], "host06")  # a unique number keeps its name
        self.assertRegex(fakes["ceph1-5"], r"host-[0-9a-f]{8}")

    def test_unique_names_keep_the_numbered_form(self):
        self.assertEqual(
            cb._fake_hostnames({"ceph2-1", "ceph2-2"}),
            {"ceph2-1": "host01", "ceph2-2": "host02"},
        )

    def test_two_hosts_stay_two_hosts_in_the_saved_tree(self):
        snaps = {
            "osd_tree": {
                "nodes": [
                    {"id": -1, "type": "host", "name": "ceph1-5", "children": [1]},
                    {"id": -2, "type": "host", "name": "ceph2-5", "children": [2]},
                    {"id": 1, "type": "osd"},
                    {"id": 2, "type": "osd"},
                ]
            },
            "osd_dump": {},
            "pool_ls_detail": [],
            "crush_rule_dump": [],
        }
        cb.anonymize_snapshots(snaps)
        names = [n["name"] for n in snaps["osd_tree"]["nodes"] if n["type"] == "host"]
        self.assertEqual(len(set(names)), 2)

    def test_still_idempotent_when_names_collided(self):
        def tree():
            return {
                "osd_tree": {
                    "nodes": [
                        {"id": -1, "type": "host", "name": "a-5", "children": []},
                        {"id": -2, "type": "host", "name": "b-5", "children": []},
                    ]
                },
                "osd_dump": {},
                "pool_ls_detail": [],
                "crush_rule_dump": [],
            }

        once = tree()
        cb.anonymize_snapshots(once)
        twice = json.loads(json.dumps(once))
        cb.anonymize_snapshots(twice)
        self.assertEqual(once, twice)


class NoPgsTest(unittest.TestCase):
    def test_pg_ls_that_is_not_ready_is_an_error_not_no_pgs(self):
        with (
            mock.patch.object(cb, "_ceph_json", lambda key: {"pg_ready": False}),
            self.assertRaises(SystemExit) as ctx,
        ):
            cb.fetch_pg_stats()
        self.assertIn("not ready", str(ctx.exception))

    def test_pg_ready_true_with_no_pgs_is_still_empty(self):
        with mock.patch.object(cb, "_ceph_json", lambda key: {"pg_ready": True}):
            self.assertEqual(cb.fetch_pg_stats(), [])

    def test_pg_ls_with_no_matching_pgs_returns_only_pg_ready(self):
        with mock.patch.object(cb, "_ceph_json", lambda key: {"pg_ready": True}):
            self.assertEqual(cb.fetch_pg_stats(), [])

    def test_unrecognised_json_is_an_error(self):
        with (
            mock.patch.object(cb, "_ceph_json", lambda key: {"what": 1}),
            self.assertRaises(SystemExit),
        ):
            cb.fetch_pg_stats()


def run_cli(*argv, path=None):
    """Run the script as a subprocess; path replaces PATH when given."""
    env = {**os.environ, "PATH": path} if path is not None else None
    return subprocess.run(
        [sys.executable, SCRIPT, *argv],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


class StateOptionsCliTest(unittest.TestCase):
    """--save-state / --load-state, end to end through a fake 'ceph' on PATH."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        snaps = canned_snapshots(MainTest.PGS)
        table = {
            " ".join(cmd[1:]): snaps[key] for key, cmd in cb.SNAPSHOT_COMMANDS.items()
        }
        fake = self.bin / "ceph"
        fake.write_text(
            f"#!{sys.executable}\nimport json, sys\n"
            f"print(json.dumps({table!r}[' '.join(sys.argv[1:])]))\n"
        )
        fake.chmod(0o755)
        self.with_ceph = f"{self.bin}{os.pathsep}{os.environ['PATH']}"
        self.without_ceph = str(self.root / "empty")  # nothing runnable

    def test_save_then_load_reproduces_the_output_without_ceph(self):
        state = self.root / "state"
        live = run_cli(
            "--save-state", str(state), "--pgremapper", "682", path=self.with_ceph
        )
        self.assertEqual(live.returncode, 0, live.stderr)
        self.assertEqual(live.stdout, "19.9 682 8\n19.d 682 8\n19.d 9 4\n")
        self.assertEqual(
            {f.stem for f in state.glob("*.json")}, set(cb.SNAPSHOT_COMMANDS)
        )

        replay = run_cli(
            "--load-state", str(state), "--pgremapper", "682", path=self.without_ceph
        )
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(replay.stdout, live.stdout)

    def test_saved_state_is_anonymized_but_the_run_reports_real_names(self):
        state = self.root / "state"
        live = run_cli("--save-state", str(state), "682", path=self.with_ceph)
        self.assertIn("h2", live.stdout)  # the live table shows the real host
        saved = "".join(f.read_text() for f in state.glob("*.json"))
        for secret in ("h1.example", "secret-rule", "10.1.2.3", "11111111-2222"):
            self.assertNotIn(secret, saved)

    def test_save_still_happens_when_the_analysis_exits_with_an_error(self):
        state = self.root / "state"
        result = run_cli("--save-state", str(state), "5000", path=self.with_ceph)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("5000", result.stderr)
        self.assertEqual(
            {f.stem for f in state.glob("*.json")}, set(cb.SNAPSHOT_COMMANDS)
        )

    def test_save_refuses_a_non_empty_directory(self):
        state = self.root / "state"
        state.mkdir()
        (state / "x").write_text("")
        result = run_cli("--save-state", str(state), "682", path=self.with_ceph)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not empty", result.stderr)

    def test_save_accepts_an_existing_empty_directory(self):
        state = self.root / "state"
        state.mkdir()
        self.assertEqual(
            run_cli("--save-state", str(state), "682", path=self.with_ceph).returncode,
            0,
        )

    def test_load_reports_a_missing_directory_and_a_missing_file(self):
        result = run_cli("--load-state", str(self.root / "nope"), "682")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not found", result.stderr)

        partial = self.root / "partial"
        partial.mkdir()
        result = run_cli("--load-state", str(partial), "682")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)

    def test_pgremapper_and_import_mappings_are_mutually_exclusive(self):
        result = run_cli("--pgremapper", "--import-mappings", "682")
        self.assertEqual(result.returncode, 2)
        self.assertIn("not allowed with", result.stderr)

    def test_the_two_options_are_mutually_exclusive(self):
        result = run_cli("--load-state", "a", "--save-state", "b", "682")
        self.assertEqual(result.returncode, 2)
        self.assertIn("not allowed with", result.stderr)


FIXTURE = (
    pathlib.Path(REPO_ROOT)
    / "tests"
    / "test-data"
    / ("cancel-backfills-into-osd-ceph2-osd896-host-clash-companions")
)

FIXTURE_BLOCKER = (
    FIXTURE.parent / "cancel-backfills-into-osd-ceph2-osd896-blocker-in-same-pg"
)

# Documented in the fixture's README.txt. The pins that are neither into 896 nor
# needed for host validity alone are the blockers (targets over backfillfull).
EXPECTED_896 = """\
19.7e9 896 627
19.7e9 149 497
19.92e 896 231
19.92e 337 99
19.94c 896 522
19.94c 716 266
19.14cd 314 347
19.14cd 232 337
19.14cd 896 614
19.1b16 896 591
19.1b16 524 578
19.1fed 896 5
19.1fed 12 207
"""


class FixtureReplayTest(unittest.TestCase):
    """Replay the real-cluster snapshot in tests/test-data (see its README.txt)."""

    def replay(self, osd, *flags):
        result = run_cli("--load-state", str(FIXTURE), *flags, str(osd))
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_osd_896_pgremapper_output(self):
        self.assertEqual(self.replay(896, "--pgremapper").stdout, EXPECTED_896)

    def test_osd_896_import_mappings_matches_the_pgremapper_lines(self):
        result = self.replay(896, "--import-mappings")
        entries = json.loads(result.stdout)
        self.assertEqual(
            [
                f"{e['pgid']} {e['mapping']['from']} {e['mapping']['to']}"
                for e in entries
            ],
            EXPECTED_896.splitlines(),
        )
        self.assertNotIn("WARNING", result.stderr)

    def test_osd_896_pgremapper_lines_carry_the_warning(self):
        err = self.replay(896, "--pgremapper").stderr
        self.assertIn("WARNING: 6 PG(s) need more than one remap", err)
        for pgid in ("19.7e9", "19.92e", "19.94c", "19.14cd", "19.1b16", "19.1fed"):
            self.assertIn(pgid, err)

    def test_osd_74_pgremapper_lines_need_no_warning(self):
        self.assertNotIn("WARNING", self.replay(74, "--pgremapper").stderr)

    def test_osd_896_table_and_summary(self):
        result = self.replay(896)
        rows = result.stdout.splitlines()
        self.assertEqual(len(rows), 1 + 13)  # header + the 13 pins
        self.assertEqual(sum("blocks shard" in r for r in rows), 7)
        self.assertIn("6 arriving shard(s)", result.stderr)
        self.assertIn("7 more shard(s)", result.stderr)
        self.assertIn("(7 because their target would be over", result.stderr)
        self.assertIn("0 cannot be pinned", result.stderr)

    def test_osd_74_needs_no_companions(self):
        out = self.replay(74, "--pgremapper").stdout
        self.assertEqual(out, "19.16fc 74 183\n19.1eb3 74 512\n")

    def test_osd_682_pairs_its_shard_with_the_one_on_the_same_host(self):
        # osd.231 (the acting OSD of 19.16fc shard 7) shares a host with osd.74,
        # where shard 6 of the same PG is arriving.
        out = self.replay(682, "--pgremapper").stdout
        self.assertEqual(out, "19.16fc 74 183\n19.16fc 682 231\n")

    def test_an_osd_with_no_backfills_prints_nothing(self):
        result = self.replay(231, "--pgremapper")
        self.assertEqual(result.stdout, "")
        self.assertIn("No backfills", result.stderr)

    def test_proposals_leave_every_pg_with_valid_placement(self):
        """Apply the pins to 'up' and check no host or OSD repeats (independent
        of the script's own clash logic)."""
        snap = {p.stem: json.loads(p.read_text()) for p in FIXTURE.glob("*.json")}
        host = {
            c: n["name"]
            for n in snap["osd_tree"]["nodes"]
            if n["type"] == "host"
            for c in n["children"]
        }
        pgs = {p["pgid"]: p for p in snap["pg_ls_remapped"]["pg_stats"]}
        for osd in (896, 74, 682):
            pins: dict[str, list[tuple[int, int]]] = {}
            for line in self.replay(osd, "--pgremapper").stdout.splitlines():
                pgid, from_osd, to_osd = line.split()
                pins.setdefault(pgid, []).append((int(from_osd), int(to_osd)))
            for pgid, pairs in pins.items():
                up = list(pgs[pgid]["up"])
                for from_osd, to_osd in pairs:
                    slot = up.index(from_osd)
                    # a pin always sends the shard back to where it is now
                    self.assertEqual(pgs[pgid]["acting"][slot], to_osd, (osd, pgid))
                    up[slot] = to_osd
                self.assertEqual(len(set(up)), len(up), (osd, pgid, "OSD twice"))
                hosts = [host[o] for o in up]
                self.assertEqual(len(set(hosts)), len(hosts), (osd, pgid, hosts))

    def test_fixtures_hold_no_real_hostnames_addresses_or_uuids(self):
        for fixture in (FIXTURE, FIXTURE_BLOCKER):
            text = "".join(f.read_text() for f in fixture.glob("*.json"))
            self.assertNotIn("ceph2", text, fixture.name)
            self.assertNotRegex(text, r"\b\d{1,3}(\.\d{1,3}){3}\b", fixture.name)
            self.assertNotRegex(
                text, r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-", fixture.name
            )

    def test_the_older_fixture_only_gained_the_ratios_it_lacked(self):
        dump = json.loads((FIXTURE / "osd_dump.json").read_text())
        self.assertEqual(dump["backfillfull_ratio"], 0.91)


class ChainFixtureReplayTest(unittest.TestCase):
    """Real chains: 19.1299 (asked about osd.891) has shard 1 going to osd.579
    while osd.579 still holds shard 8, which is going to osd.825."""

    def replay(self, *flags):
        result = run_cli("--load-state", str(FIXTURE), *flags, "891")
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_the_table_lists_the_pair_that_frees_osd_579_first(self):
        rows = [r.split() for r in self.replay().stdout.splitlines() if "19.1299" in r]
        self.assertEqual(
            [(r[1], r[2], r[5]) for r in rows],
            [("8", "osd.579", "osd.825"), ("1", "osd.891", "osd.579")],
        )

    def test_machine_formats_leave_it_out_and_the_warning_gives_the_command(self):
        for flag in ("--import-mappings", "--pgremapper"):
            result = self.replay(flag)
            self.assertNotIn("19.1299", result.stdout, flag)
            self.assertIn(
                "ceph osd pg-upmap-items 19.1299 579 825 891 579", result.stderr, flag
            )

    def test_every_chained_pg_in_the_cluster_is_ordered_or_reported(self):
        """Across every OSD that is a target of a remapped shard: no output ever
        lists a pair before the pair that frees its target OSD."""
        snap = {p.stem: json.loads(p.read_text()) for p in FIXTURE.glob("*.json")}
        with mock.patch.object(cb, "_ceph_json", lambda key: snap[key]):
            osd_df, osd_host = cb.fetch_osd_df(), cb.fetch_osd_hosts()
            pgs, pools = cb.fetch_pg_stats(), cb.fetch_pools()
            ecp, rules = cb.fetch_ec_profiles(), cb.fetch_crush_rules()
            pct = cb.fetch_backfillfull_pct()
        targets = {
            o
            for p in pgs
            for i, o in enumerate(p["up"])
            if o != NONE and o != p["acting"][i]
        }
        chained = 0
        for osd in sorted(targets):
            cancellations, _ = cb.plan_cancellations(
                pgs, pools, ecp, osd, osd_host, rules, osd_df, pct
            )
            by_pg = {}
            for c in cancellations:
                by_pg.setdefault(c.pgid, []).append(c)
            for pgid, cs in by_pg.items():
                for i, c in enumerate(cs):
                    later_sources = {o.up_osd for o in cs[i + 1 :]}
                    self.assertNotIn(c.acting_osd, later_sources, (osd, pgid))
                chained += pgid in cb.chained_pgs(cancellations)
        self.assertGreaterEqual(chained, 13)  # the real chains this test is about


class BlockerFixtureReplayTest(unittest.TestCase):
    """The state that prompted blockers: osd.896 has ONE arriving shard
    (19.92e shard 4, from osd.231) yet its PG stays in backfill_toofull, because
    shard 6 of the same PG is going to osd.337, which is over backfillfull."""

    def replay(self, *flags):
        result = run_cli("--load-state", str(FIXTURE_BLOCKER), *flags, "896")
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_the_blocking_shard_is_proposed_next_to_the_requested_one(self):
        self.assertEqual(
            self.replay("--pgremapper").stdout, "19.92e 896 231\n19.92e 337 99\n"
        )

    def test_import_mappings_output(self):
        self.assertEqual(
            json.loads(self.replay("--import-mappings").stdout),
            [
                {"pgid": "19.92e", "mapping": {"from": 896, "to": 231}},
                {"pgid": "19.92e", "mapping": {"from": 337, "to": 99}},
            ],
        )

    def test_the_table_explains_the_second_line(self):
        rows = self.replay().stdout.splitlines()
        self.assertEqual(len(rows), 3)
        self.assertIn("blocks shard 4: target osd.337 would be at 93.4%", rows[2])
        self.assertNotIn("blocks", rows[1])

    def test_summary_says_it_is_one_arriving_shard_plus_one_blocker(self):
        err = self.replay().stderr
        self.assertIn("1 arriving shard(s)", err)
        self.assertIn("1 more shard(s)", err)
        self.assertIn("1 because their target would be over backfillfull_ratio", err)

    def test_keeping_the_wanted_backfill_means_dropping_only_its_own_entry(self):
        # the user's case: keep 231->896, so drop that entry and keep the blocker
        entries = json.loads(self.replay("--import-mappings").stdout)
        kept = [e for e in entries if e["mapping"]["from"] != 896]
        self.assertEqual(kept, [{"pgid": "19.92e", "mapping": {"from": 337, "to": 99}}])


if __name__ == "__main__":
    unittest.main()
