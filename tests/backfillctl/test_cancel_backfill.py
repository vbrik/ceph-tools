"""Unit tests for backfillctl's cancel-backfill subcommand.

The risky parts are deciding which acting OSD a shard can be pinned back to
(EC by position, replicated by set difference), and refusing to propose a pin
that Ceph would silently drop (acting OSD already elsewhere in 'up') or that
has nothing to pin to (empty acting slot). Those rules are tested on
hand-built PG dicts, and through plan() on canned 'ceph' output and the
real-cluster fixtures; run() tests check how render() prints the result.
"""

import contextlib
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

from _support import (
    EC_POOL_DETAIL,
    EC_PROFILES,
    HOST_RULE,
    NONE,
    REP_POOL_DETAIL,
    REPO_ROOT,
    RULES,
    TEST_DATA,
    FakeStore,
    flat,
    parse_args,
    pg_stat,
    placement,
    plan_from_state,
    shared,
    upmap_pairs,
)

from backfillctl import cancel_backfill as cb

OSD = 682


class EcArrivalsTest(unittest.TestCase):
    def find(self, up, acting):
        return cb.find_arrivals(up, acting, {OSD}, is_ec=True)

    def test_shard_arriving_is_pinned_to_its_acting_osd(self):
        pins, skipped = self.find([1, 2, OSD, 4], [1, 2, 9, 4])
        self.assertEqual(pins, [(2, OSD, 9)])
        self.assertEqual(skipped, [])

    def test_osd_not_in_up_yields_nothing(self):
        self.assertEqual(self.find([1, 2, 3, 4], [1, 2, 9, 4]), ([], []))

    def test_shard_already_in_place_is_not_a_move(self):
        self.assertEqual(self.find([1, 2, OSD, 4], [1, 2, OSD, 4]), ([], []))

    def test_other_shards_moving_are_ignored(self):
        pins, _ = self.find([5, 2, OSD, 4], [1, 2, 9, 4])
        self.assertEqual(pins, [(2, OSD, 9)])

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
        # close_pins deals with that, not find_arrivals.
        pins, skipped = self.find([1, 2, OSD, 9], [1, 2, 9, 4])
        self.assertEqual(pins, [(2, OSD, 9)])
        self.assertEqual(skipped, [])

    def test_osd_holds_another_shard_now_is_still_pinnable(self):
        # Shard 0 arrives on OSD from osd.1 while the OSD's current shard 2
        # is moving on to osd.3: the pin concerns shard 0 only.
        pins, skipped = self.find([OSD, 2, 3], [1, 2, OSD])
        self.assertEqual(pins, [(0, OSD, 1)])
        self.assertEqual(skipped, [])


class ReplicatedArrivalsTest(unittest.TestCase):
    def find(self, up, acting):
        return cb.find_arrivals(up, acting, {OSD}, is_ec=False)

    def test_one_arriving_one_departing_is_paired(self):
        pins, skipped = self.find([1, 2, OSD], [1, 2, 9])
        self.assertEqual(pins, [("-", OSD, 9)])
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


class PinWithCompanionsTest(unittest.TestCase):
    """A pin is valid only if the result has no two shards on one host/OSD."""

    def pin(self, up, acting, slot, hosts):
        return shared.close_pins(up, acting, {slot: acting[slot]}, hosts)

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


def run_plan(pgs, pools=None, osd_host=None, rules=None, exclude_pgs=None):
    pools = pools or {19: EC_POOL_DETAIL, 7: REP_POOL_DETAIL}
    return cb.plan_cancellations(
        pgs,
        pools,
        EC_PROFILES,
        {OSD},
        osd_host or {},
        RULES if rules is None else rules,
        exclude_pgs=exclude_pgs or frozenset(),
    )


class PlanTest(unittest.TestCase):
    plan = staticmethod(run_plan)

    def test_sorted_numerically_by_pool_then_hex_pg_then_shard(self):
        pgs = [
            pg_stat("19.16fc", [1, OSD, 3, 4], [1, 9, 3, 4]),
            pg_stat("19.2a", [1, 2, OSD, 4], [1, 2, 9, 4]),
            pg_stat("7.ff", [1, 2, OSD], [1, 2, 9]),
            pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4]),
        ]
        cancellations, _ = self.plan(pgs)
        self.assertEqual(
            [(c.pgid, c.shard) for c in cancellations],
            [("7.ff", "-"), ("19.9", 0), ("19.2a", 2), ("19.16fc", 1)],
        )

    def test_carries_acting_osd_state_size_and_progress(self):
        state = "active+remapped+backfilling"
        p = pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4], state, misplaced=25)
        (c,), skipped = self.plan([p])
        self.assertEqual(skipped, [])
        self.assertEqual((c.acting_osd, c.state, c.size_bytes), (8, state, 1_000))
        self.assertEqual(c.progress_pct, 75.0)

    def test_skipped_are_reported_with_pgid(self):
        p = pg_stat("19.9", [OSD, 2, 3, 4], [NONE, 2, 3, 4])
        cancellations, skipped = self.plan([p])
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.9", 0)])

    def test_pg_of_unlisted_pool_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.plan([pg_stat("99.1", [OSD], [9])])

    def test_pgs_not_involving_osd_are_ignored(self):
        self.assertEqual(
            self.plan([pg_stat("19.9", [1, 2, 3, 4], [1, 2, 3, 4])]), ([], [])
        )


RATIO = 91.0


class ArrivalProjectionTest(unittest.TestCase):
    """arrival_projection and blocker_projection: Ceph's backfillfull check."""

    # 1000 KiB OSDs; a 40 KiB EC 4+2 PG has 10 KiB (1%) shards.
    def setUp(self):
        self.df = {77: osd_df_node(77, 89.0, kb=1000), 5: osd_df_node(5, 50.0, kb=1000)}

    def projection(self, pgs, pools=None):
        return cb.arrival_projection(
            pgs, pools or {19: EC_POOL_DETAIL}, EC_PROFILES, self.df
        )

    def test_every_shard_arriving_counts(self):
        # Two PGs send a shard to osd.77: 89% + 1% + 1%.
        pgs = [
            pg_stat("19.1", [77, 2], [8, 2], num_bytes=40 * 1024),
            pg_stat("19.2", [3, 77], [3, 9], num_bytes=40 * 1024),
        ]
        self.assertAlmostEqual(self.projection(pgs).utilization_after(77, 0), 91.0)

    def test_pgs_of_unknown_pools_add_nothing(self):
        pgs = [pg_stat("42.1", [77, 2], [8, 2], num_bytes=40 * 1024)]
        self.assertAlmostEqual(self.projection(pgs).utilization_after(77, 0), 89.0)

    def test_blocks_at_or_over_the_ratio_only(self):
        pgs = [pg_stat(f"19.{i}", [77, 2], [8, 2], num_bytes=40 * 1024) for i in (1, 2)]
        at_ratio = self.projection(pgs)  # 91%
        self.assertAlmostEqual(cb.blocker_projection(at_ratio, 77, RATIO), 91.0)
        self.assertIsNone(cb.blocker_projection(self.projection(pgs[:1]), 77, RATIO))
        self.assertIsNone(cb.blocker_projection(at_ratio, 5, RATIO))

    def test_unknown_capacity_never_blocks(self):
        self.assertIsNone(cb.blocker_projection(self.projection([]), 1234, RATIO))


class FindBlockersTest(unittest.TestCase):
    def find(self, up, acting, df, pinned=None, arriving=()):
        projection = placement.ProjectedUsage(df, arriving)
        return cb.find_blockers(up, acting, pinned or {0: acting[0]}, projection, RATIO)

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

    def test_shards_arriving_from_other_pgs_can_tip_it_over(self):
        # 90.9% used is under the ratio; a 0.2% shard arriving from another
        # PG takes it to 91.1%, which Ceph counts too.
        df = {77: osd_df_node(77, 90.9)}
        other = placement.ArrivingShard("19.f", 0, 77, 1, [77], 2 * 1024 * 1024)
        self.assertEqual(self.find([OSD, 77], [8, 66], df), [])
        self.assertEqual(self.find([OSD, 77], [8, 66], df, arriving=[other]), [1])


class PlanBlockersTest(unittest.TestCase):
    """plan_cancellations with pin_blockers=True and the OSD utilizations and
    backfillfull_ratio given: what --pin-blockers finds."""

    def plan(self, pgs, df, osd_host=None, ratio=RATIO, pin_blockers=True):
        pools = {19: EC_POOL_DETAIL, 7: REP_POOL_DETAIL}
        return cb.plan_cancellations(
            pgs,
            pools,
            EC_PROFILES,
            {OSD},
            osd_host or {},
            RULES,
            df,
            ratio,
            pin_blockers,
        )

    def test_blocker_is_pinned_and_marked_with_its_projected_utilization(self):
        p = pg_stat("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        cancellations, skipped = self.plan([p], {77: osd_df_node(77, 92.0)})
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.up_osd, c.acting_osd, c.companion_of) for c in cancellations],
            [(0, OSD, 8, None), (3, 77, 66, 0)],
        )
        self.assertIsNone(cancellations[0].blocker_util)
        self.assertAlmostEqual(cancellations[1].blocker_util, 92.0, places=2)

    def test_the_requested_shard_is_never_marked_a_blocker(self):
        p = pg_stat("19.5", [OSD, 2], [8, 2])
        (c,), _ = self.plan([p], {OSD: osd_df_node(OSD, 99.0)})
        self.assertIsNone(c.blocker_util)

    def test_pgs_without_a_shard_into_the_osd_are_left_alone(self):
        p = pg_stat("19.5", [1, 2, 3, 77], [1, 2, 3, 66])
        self.assertEqual(self.plan([p], {77: osd_df_node(77, 99.0)}), ([], []))

    def test_no_blockers_without_utilizations_or_ratio(self):
        # even with pin_blockers=True, no osd_df or no backfillfull_pct means
        # there is nothing to look blockers up in.
        p = pg_stat("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        pools = {19: EC_POOL_DETAIL}
        for df, ratio in ((None, RATIO), ({77: osd_df_node(77, 99.0)}, None)):
            cancellations, _ = cb.plan_cancellations(
                [p], pools, EC_PROFILES, {OSD}, {}, RULES, df, ratio, True
            )
            self.assertEqual([c.shard for c in cancellations], [0])

    def test_no_blockers_without_pin_blockers_even_with_utilization_and_ratio(self):
        # pin_blockers defaults to False: osd.77 is over the ratio, but the
        # blocker search never runs unless the flag says to.
        p = pg_stat("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        cancellations, _ = self.plan(
            [p], {77: osd_df_node(77, 92.0)}, pin_blockers=False
        )
        self.assertEqual([c.shard for c in cancellations], [0])

    def test_a_blocker_that_cannot_be_pinned_is_reported_and_the_pin_is_kept(self):
        # pinning shard 3 back to osd.66 would share host H with shard 1
        # (osd.2), which is not moving, so the blocker is skipped.
        p = pg_stat("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        cancellations, skipped = self.plan(
            [p], {77: osd_df_node(77, 92.0)}, osd_host={66: "H", 2: "H"}
        )
        self.assertEqual([c.shard for c in cancellations], [0])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.5", 3)])
        self.assertTrue(skipped[0].reason.startswith("blocker:"))

    def test_pinning_a_blocker_can_pull_in_a_companion(self):
        # shard 2 (77) is the blocker; its acting osd.66 shares a host with
        # shard 3's target osd.88, so shard 3 comes along as the blocker's
        # companion: it goes with the blocker, not with requested shard 0.
        p = pg_stat("19.5", [OSD, 2, 77, 88], [8, 2, 66, 99])
        df = {77: osd_df_node(77, 92.0), 88: osd_df_node(88, 50.0)}
        cancellations, skipped = self.plan([p], df, osd_host={66: "H", 88: "H"})
        self.assertEqual(skipped, [])
        self.assertEqual([c.shard for c in cancellations], [0, 2, 3])
        self.assertIsNotNone(cancellations[1].blocker_util)
        self.assertIsNone(cancellations[2].blocker_util)  # companion, not blocker
        self.assertEqual(cancellations[2].companion_of, 2)
        self.assertEqual(
            [c.role for c in cancellations],
            [shared.ROLE_REQUESTED, shared.ROLE_BLOCKER, shared.ROLE_BLOCKER],
        )
        self.assertEqual(
            shared.format_note(cancellations[2]), "companion of blocker shard 2"
        )

    def test_a_requested_pins_companion_is_not_a_blocker(self):
        # shard 1's target shares a host with shard 0's acting osd.8.
        p = pg_stat("19.5", [OSD, 88], [8, 99])
        cancellations, _ = self.plan(
            [p], {88: osd_df_node(88, 50.0)}, osd_host={8: "H", 88: "H"}
        )
        self.assertEqual(
            [(c.shard, c.role, c.companion_of) for c in cancellations],
            [(0, shared.ROLE_REQUESTED, None), (1, shared.ROLE_COMPANION, 0)],
        )

    def test_a_companion_whose_target_is_over_the_ratio_counts_as_a_blocker(self):
        p = pg_stat("19.5", [OSD, 149], [627, 497])
        cancellations, _ = self.plan(
            [p], {149: osd_df_node(149, 92.0)}, osd_host={627: "H", 149: "H"}
        )
        self.assertEqual([c.shard for c in cancellations], [0, 1])
        self.assertAlmostEqual(cancellations[1].blocker_util, 92.0, places=2)

    def test_without_pin_blockers_that_same_companion_is_not_labeled_a_blocker(self):
        # the companion pin itself is unconditional (host clash), but with the
        # flag off it must not be mislabeled "blocks shard N": that would
        # contradict the run's own note that blockers were not looked for.
        p = pg_stat("19.5", [OSD, 149], [627, 497])
        cancellations, _ = self.plan(
            [p],
            {149: osd_df_node(149, 92.0)},
            osd_host={627: "H", 149: "H"},
            pin_blockers=False,
        )
        self.assertEqual([c.shard for c in cancellations], [0, 1])
        self.assertIsNone(cancellations[1].blocker_util)
        self.assertEqual(cancellations[1].companion_of, 0)

    def test_shards_arriving_on_several_given_osds_are_all_requested(self):
        p = pg_stat("19.5", [OSD, 77, 3, 4], [8, 66, 3, 4])
        cancellations, skipped = cb.plan_cancellations(
            [p], {19: EC_POOL_DETAIL}, EC_PROFILES, {OSD, 77}, {}, RULES
        )
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.role) for c in cancellations],
            [(0, shared.ROLE_REQUESTED), (1, shared.ROLE_REQUESTED)],
        )

    def test_a_pg_with_several_requested_shards_is_pinned_whole_or_not_at_all(self):
        # Pinning shard 1 back to osd.66 clashes with shard 2 (osd.3, same
        # host), which is not moving: shard 0's pin goes too.
        p = pg_stat("19.5", [OSD, 77, 3, 4], [8, 66, 3, 4])
        cancellations, skipped = cb.plan_cancellations(
            [p], {19: EC_POOL_DETAIL}, EC_PROFILES, {OSD, 77}, {66: "H", 3: "H"}, RULES
        )
        self.assertEqual(cancellations, [])
        self.assertEqual([s.shard for s in skipped], [0, 1])

    def test_several_blockers_come_in_shard_order(self):
        p = pg_stat("19.5", [OSD, 77, 3, 88], [8, 66, 3, 99])
        df = {77: osd_df_node(77, 92.0), 88: osd_df_node(88, 93.0)}
        cancellations, _ = self.plan([p], df)
        self.assertEqual([c.shard for c in cancellations], [0, 1, 3])

    def test_replicated_pgs_get_no_blockers(self):
        p = pg_stat("7.1", [1, 2, OSD], [1, 2, 9])
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
        p = pg_stat("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        cancellations, skipped = self.plan([p])
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.up_osd, c.acting_osd) for c in cancellations],
            [(1, 20, 30), (0, OSD, 20)],
        )

    def test_a_ring_skips_the_pin_and_says_why(self):
        # osd.20 and OSD would swap places between shards 0 and 1
        p = pg_stat("19.f", [OSD, 20, 3, 4], [20, OSD, 3, 4])
        cancellations, skipped = self.plan([p])
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.f", 0)])
        self.assertIn("cycle", skipped[0].reason)

    def test_the_order_survives_sorting_across_pgs(self):
        chain = pg_stat("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        plain = pg_stat("19.2", [OSD, 2, 3, 4], [8, 2, 3, 4])
        cancellations, _ = self.plan([chain, plain])
        self.assertEqual(
            [(c.pgid, c.shard) for c in cancellations],
            [("19.2", 0), ("19.f", 1), ("19.f", 0)],
        )

    def test_a_blocker_that_would_make_a_ring_is_skipped_but_the_pin_stays(self):
        # shard 3 (77->66) is a blocker (osd.77 is over the ratio) but pinning
        # it back would ring with shard 2 (66 <-> 77 swap): skip it only.
        p = pg_stat("19.5", [OSD, 2, 66, 77], [8, 2, 77, 66])
        cancellations, skipped = run_plan_with_df(
            [p], {77: osd_df_node(77, 92.0), 66: osd_df_node(66, 50.0)}
        )
        self.assertEqual([c.shard for c in cancellations], [0])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.5", 3)])
        self.assertIn("cycle", skipped[0].reason)


def run_plan_with_df(pgs, df, ratio=RATIO, pin_blockers=True, exclude_pgs=None):
    return cb.plan_cancellations(
        pgs,
        {19: EC_POOL_DETAIL, 7: REP_POOL_DETAIL},
        EC_PROFILES,
        {OSD},
        {},
        RULES,
        df,
        ratio,
        pin_blockers,
        exclude_pgs or frozenset(),
    )


def up_pg(pgid: str, up: list) -> dict:
    """The part of a PG's stats avoid_chains reads."""
    return {"pgid": pgid, "up": up}


class AvoidChainsTest(unittest.TestCase):
    """shared.avoid_chains: what pgremapper can apply, per PG."""

    def c(self, pgid, shard, up, acting, companion_of=None, blocker_util=None):
        return cb.Cancellation(
            pgid, shard, up, acting, None, "s", None, companion_of, blocker_util
        )

    def avoid(self, cs, ups, upmaps=None, osd_host=None, partial=True):
        return shared.avoid_chains(
            cs,
            {
                pgid: [{"from": f, "to": t} for f, t in pairs]
                for pgid, pairs in (upmaps or {}).items()
            },
            [up_pg(pgid, up) for pgid, up in ups.items()],
            osd_host or {},
            partial,
        )

    # 19.9: shard 1 moves 20->30 while shard 0 moves onto osd.20
    CHAIN_UP: ClassVar[dict] = {"19.9": [682, 20, 3, 4]}

    def chain(self, **kw):
        return [self.c("19.9", 1, 20, 30), self.c("19.9", 0, 682, 20, **kw)]

    def test_pgs_without_chains_pass_through_in_order(self):
        cs = [self.c("19.1", 0, 1, 2), self.c("19.1", 1, 5, 6)]
        result = self.avoid(cs, {"19.1": [1, 5, 3]})
        self.assertEqual(result, (cs, [], {}))

    def test_partial_keeps_the_last_pin_and_leaves_the_other_running(self):
        tail, head = self.chain()
        result = self.avoid([tail, head], self.CHAIN_UP)
        self.assertEqual(result.cancellations, [tail])
        ((pgid, shard, reason),) = result.skipped
        self.assertEqual((pgid, shard), ("19.9", 0))
        self.assertIn("682->20, 20->30 would chain", reason)
        self.assertEqual(result.chained, {"19.9": [(20, 30), (682, 20)]})

    def test_only_the_last_pin_of_a_longer_chain_stays(self):
        # 682->20, 20->30, 30->40: only 30->40 is valid on its own
        cs = [
            self.c("19.9", 2, 30, 40),
            self.c("19.9", 1, 20, 30),
            self.c("19.9", 0, 682, 20),
        ]
        result = self.avoid(cs, {"19.9": [682, 20, 30, 4]})
        self.assertEqual(result.cancellations, cs[:1])
        self.assertEqual([s.shard for s in result.skipped], [1, 0])

    def test_without_partial_a_requested_head_leaves_the_pg_out(self):
        tail, head = self.chain()
        tail = tail._replace(companion_of=0)
        result = self.avoid([tail, head], self.CHAIN_UP, partial=False)
        self.assertEqual(result.cancellations, [])
        # only the requested shard is reported, as for other refusals
        self.assertEqual([(s.pgid, s.shard) for s in result.skipped], [("19.9", 0)])
        self.assertIn("19.9", result.chained)

    def test_a_blocker_head_is_dropped_and_the_requested_pin_stays(self):
        # requested shard 0 (5->6) chains with nothing; blocker shard 2
        # (682->20) chains into blocker shard 1 (20->30)
        requested = self.c("19.9", 0, 5, 6)
        tail = self.c("19.9", 1, 20, 30, companion_of=0, blocker_util=95.0)
        head = self.c("19.9", 2, 682, 20, companion_of=0, blocker_util=95.0)
        result = self.avoid(
            [requested, tail, head], {"19.9": [5, 20, 682, 4]}, partial=False
        )
        self.assertEqual(result.cancellations, [requested, tail])
        ((_, shard, reason),) = result.skipped
        self.assertEqual(shard, 2)
        self.assertTrue(reason.startswith("blocker: "), reason)

    def test_dropping_a_companion_head_breaks_host_validity_so_the_pg_is_left_out(self):
        # requested shard 1 goes back to osd.30 on host H; companion shard 0
        # was pinned because its target osd.31 is on H too. Leaving shard 0
        # running would put osd.30 and osd.31 on H.
        requested = self.c("19.9", 1, 20, 30)
        companion = self.c("19.9", 0, 31, 20, companion_of=1)
        result = self.avoid(
            [requested, companion],
            {"19.9": [31, 20, 3]},
            osd_host={30: "H", 31: "H"},
            partial=False,
        )
        self.assertEqual(result.cancellations, [])
        ((_, shard, reason),) = result.skipped
        self.assertEqual(shard, 1)
        self.assertIn("osd.30 would share a host with osd.31", reason)

    def test_partial_leaves_the_pg_out_when_the_rest_would_clash(self):
        tail, head = self.chain()
        result = self.avoid([tail, head], self.CHAIN_UP, osd_host={30: "H", 682: "H"})
        self.assertEqual(result.cancellations, [])
        self.assertEqual([s.shard for s in result.skipped], [1, 0])

    def test_a_pin_chaining_into_an_existing_pair_is_dropped(self):
        # 19.1128 in the chained-pairs fixture: existing 545->94, pin 888->545
        pin = self.c("19.1128", 4, 888, 545)
        other = self.c("19.1128", 1, 471, 866)
        result = self.avoid(
            [other, pin],
            {"19.1128": [148, 471, 648, 328, 888, 70, 143, 352, 94, 61]},
            upmaps={"19.1128": [(274, 148), (545, 94)]},
        )
        self.assertEqual(result.cancellations, [other])
        self.assertEqual([s.shard for s in result.skipped], [4])
        self.assertEqual(
            result.chained["19.1128"],
            [(274, 148), (545, 94), (471, 866), (888, 545)],
        )

    def test_a_pin_on_an_existing_pairs_target_folds_and_is_kept(self):
        # existing 7->20 put osd.20 in 'up'; pinning 20->30 makes it 7->30,
        # as pgremapper does: no chain
        cs = [self.c("19.9", 1, 20, 30)]
        result = self.avoid(cs, {"19.9": [1, 20, 3]}, upmaps={"19.9": [(7, 20)]})
        self.assertEqual(result, (cs, [], {}))

    def test_a_pin_undoing_an_existing_pair_is_kept(self):
        cs = [self.c("19.9", 1, 20, 7)]
        result = self.avoid(cs, {"19.9": [1, 20, 3]}, upmaps={"19.9": [(7, 20)]})
        self.assertEqual(result, (cs, [], {}))

    def test_a_stale_existing_pair_does_not_hide_a_chain(self):
        # 24.20 in ceph1-backfills-stuck-at-100-pct: existing 652->636 is
        # stale (652 still in 'up'), so pin 636->373 must not fold into it;
        # pins 652->636 and 636->373 chain
        tail, head = self.c("24.20", 12, 636, 373), self.c("24.20", 11, 652, 636)
        result = self.avoid(
            [tail, head],
            {"24.20": [95, 86, 318, 188, 440, 525, 222, 529, 628, 85, 527, 652, 636]},
            upmaps={"24.20": [(652, 636), (128, 188)]},
        )
        self.assertEqual(result.cancellations, [tail])
        # the stale pair stays out of the full entry too
        self.assertEqual(result.chained["24.20"], [(128, 188), (636, 373), (652, 636)])

    def test_existing_chained_pairs_leave_the_pg_out(self):
        # pgremapper would drop 110->753 as stale when changing the PG
        cs = [self.c("19.3a4", 1, 256, 816)]
        result = self.avoid(
            cs,
            {"19.3a4": [723, 256, 448, 110, 753]},
            upmaps={"19.3a4": [(302, 723), (110, 753), (890, 110)]},
        )
        self.assertEqual(result.cancellations, [])
        ((_, _, reason),) = result.skipped
        self.assertIn("existing upmap pairs chain (890->110, 110->753)", reason)
        self.assertEqual(
            result.chained["19.3a4"], [(302, 723), (110, 753), (890, 110), (256, 816)]
        )

    def test_a_replicated_chain_through_an_existing_pair(self):
        # existing 9->5; pinning 4->9 (osd.9 departing) chains with it
        cs = [self.c("7.1", "-", 4, 9)]
        result = self.avoid(cs, {"7.1": [1, 5, 4]}, upmaps={"7.1": [(9, 5)]})
        self.assertEqual(result.cancellations, [])
        self.assertEqual(result.chained, {"7.1": [(9, 5), (4, 9)]})


class NoteTest(unittest.TestCase):
    def note(self, **kw):
        c = cb.Cancellation("19.1", 3, 77, 66, 1_000, "s", None, **kw)
        return shared.format_note(c)

    def test_requested_shards_have_no_note(self):
        self.assertEqual(self.note(), "")

    def test_companion(self):
        self.assertEqual(self.note(companion_of=0), "companion of shard 0")

    def test_blocker_names_the_shard_it_blocks_and_the_projection(self):
        self.assertEqual(
            self.note(companion_of=0, blocker_util=92.04),
            "blocks shard 0: target osd.77 projected at 92.0%, at or over "
            "backfillfull_ratio",
        )


class PlanCompanionsTest(unittest.TestCase):
    """Planning that involves same-host clashes."""

    plan = staticmethod(run_plan)

    HOSTS: ClassVar[dict[int, str]] = {OSD: "x", 627: "H", 149: "H", 497: "z"}

    def test_companion_is_emitted_and_marked(self):
        p = pg_stat("19.7e9", [OSD, 300, 626, 149], [627, 300, 626, 497])
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
        p = pg_stat("19.7e9", [OSD, 300, 149], [627, 300, 149])
        cancellations, skipped = self.plan([p], osd_host=self.HOSTS)
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("19.7e9", 0)])

    def test_replicated_clash_is_skipped(self):
        p = pg_stat("7.1", [1, 2, OSD], [1, 2, 9])
        cancellations, skipped = self.plan([p], osd_host={9: "H", 2: "H"})
        self.assertEqual(cancellations, [])
        self.assertEqual([(s.pgid, s.shard) for s in skipped], [("7.1", "-")])

    def test_replicated_without_clash_is_pinned(self):
        p = pg_stat("7.1", [1, 2, OSD], [1, 2, 9])
        (c,), _ = self.plan([p], osd_host={9: "a", 2: "b"})
        self.assertEqual((c.shard, c.up_osd, c.acting_osd), ("-", OSD, 9))

    def test_companions_sort_with_their_pg(self):
        a = pg_stat("19.2", [OSD, 300, 626, 149], [627, 300, 626, 497])
        b = pg_stat("19.1", [OSD, 2, 3, 4], [8, 2, 3, 4])
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
            self.plan([pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])], rules=rack)
        self.assertIn("failure domain rack", str(ctx.exception))
        self.assertIn("PGs with a backfill into osd.682", str(ctx.exception))

    def test_missing_crush_rule_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.plan([pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])], rules={})

    def test_failure_domain_is_not_checked_for_uninvolved_pgs(self):
        self.assertEqual(
            self.plan([pg_stat("19.9", [1, 2, 3, 4], [1, 2, 3, 4])], rules={}), ([], [])
        )


class PlanExcludeTest(unittest.TestCase):
    """exclude_pgs removes a PG from consideration entirely."""

    plan = staticmethod(run_plan)

    def test_excluded_pg_produces_no_cancellation(self):
        p = pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])
        self.assertEqual(self.plan([p], exclude_pgs={"19.9"}), ([], []))

    def test_excluded_pg_is_not_reported_as_skipped_either(self):
        # normally an empty acting slot is reported on stderr as unpinnable;
        # once the PG is excluded it must not be mentioned at all.
        p = pg_stat("19.9", [OSD, 2, 3, 4], [NONE, 2, 3, 4])
        self.assertEqual(self.plan([p], exclude_pgs={"19.9"}), ([], []))

    def test_excluding_one_pg_does_not_touch_others(self):
        excluded = pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])
        kept = pg_stat("19.10", [OSD, 2, 3, 4], [8, 2, 3, 4])
        cancellations, _ = self.plan([excluded, kept], exclude_pgs={"19.9"})
        self.assertEqual([c.pgid for c in cancellations], ["19.10"])

    def test_excluding_a_pg_also_drops_its_companions(self):
        # shard 0 (->627, host H) would otherwise force shard 3's companion
        # (149->497, also host H); excluding the PG must drop both.
        p = pg_stat("19.7e9", [OSD, 300, 626, 149], [627, 300, 626, 497])
        hosts = {OSD: "x", 627: "H", 149: "H", 497: "z"}
        cancellations, skipped = self.plan([p], osd_host=hosts, exclude_pgs={"19.7e9"})
        self.assertEqual((cancellations, skipped), ([], []))

    def test_excluding_a_pg_also_drops_its_blockers(self):
        p = pg_stat("19.5", [OSD, 2, 3, 77], [8, 2, 3, 66])
        cancellations, skipped = run_plan_with_df(
            [p], {77: osd_df_node(77, 92.0)}, exclude_pgs={"19.5"}
        )
        self.assertEqual((cancellations, skipped), ([], []))


def column_names() -> list[str]:
    """The table columns as one name each, e.g. 'ACTING OSD'; 'SIZE' has no group."""
    return [f"{group} {label}".strip() for group, label in cb.COLUMNS]


class OutputTest(unittest.TestCase):
    def cancellation(self, companion_of=None):
        return cb.Cancellation("19.7e9", 3, 149, 497, 1_000, "s", 50.0, companion_of)

    def test_row_marks_companions_in_the_note_column(self):
        cells = dict(zip(column_names(), cb.format_row(self.cancellation(0), {}, {})))
        self.assertEqual(cells["UP OSD"], "149")
        self.assertEqual((cells["UP UTIL"], cells["UP HOST"]), ("?", "?"))
        self.assertEqual(cells["NOTE"], "companion of shard 0")
        plain = dict(zip(column_names(), cb.format_row(self.cancellation(), {}, {})))
        self.assertEqual(plain["NOTE"], "")


class RenderTest(unittest.TestCase):
    """render() on results built by hand: no planning involved."""

    # 19.f's pins chain (shard 1 frees osd.20 for shard 0), so avoid_chains
    # left 19.f out; 19.2's pin is plain.
    PLAIN = cb.Cancellation("19.2", 0, 682, 8, None, "s", None)
    CHAINED: ClassVar[dict] = {"19.f": [(20, 30), (682, 20)]}

    def render(self, *argv, **fields):
        result = cb.StopResult(
            **{
                "osds": [682],
                "cancellations": [self.PLAIN],
                "skipped": [shared.Skipped("19.f", 0, "would chain")],
                "chained": self.CHAINED,
                "backfillfull_pct": 91.0,
                "exclude_filter": None,
                "osd_df": {682: {"utilization": 88.0, "kb": 1000}},
                "osd_host": {},
            }
            | fields
        )
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            cb.render(result, parse_args(cb, [*argv, "--osds", "682"]))
        return out.getvalue(), err.getvalue()

    def test_both_formats_print_the_same_pins_and_the_chain_warning(self):
        out, err = self.render("--pgremapper-mappings")
        self.assertEqual(
            upmap_pairs(out), [{"pgid": "19.2", "mapping": {"from": 682, "to": 8}}]
        )
        self.assertIn("ceph osd pg-upmap-items 19.f 20 30 682 20", err)
        out, err = self.render()
        self.assertEqual(len(out.splitlines()), 2 + 1)  # two header lines + 1 pin
        self.assertIn("ceph osd pg-upmap-items 19.f 20 30 682 20", err)
        self.assertIn("cannot pin 19.f shard 0: would chain", err)

    def test_no_warning_without_chains(self):
        _, err = self.render(chained={}, skipped=[])
        self.assertNotIn("WARNING", err)

    def test_planning_notes_are_printed_even_when_planning_then_exits(self):
        # A typo in --exclude-pgs is worth knowing about even when a PG then
        # cannot be analyzed (here: its CRUSH rule is missing), so the notes
        # must not wait for render(), which an exit never reaches.
        snaps = canned_snapshots([pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4])])
        snaps["crush_rule_dump"] = []
        del snaps["osd_dump"]["backfillfull_ratio"]
        store = FakeStore(snaps, load_dir=None)
        argv = ["--pin-blockers", "--osds", "682", "--exclude-pgs", "19.zzz"]
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
            cb.plan(parse_args(cb, argv), store)
        paras = flat(err.getvalue())
        self.assertIn("no backfillfull_ratio", paras)
        self.assertIn(
            "1 matched nothing (not remapped, not involving osd.682, or a typo): "
            "19.zzz",
            paras,
        )
        self.assertLess(
            paras.index("no backfillfull_ratio"), paras.index("--exclude-pgs:")
        )

    def test_nothing_to_pin_says_so(self):
        out, err = self.render(
            "--pgremapper-mappings",
            cancellations=[],
            skipped=[],
            chained={},
            osd_df={682: {}},
        )
        self.assertEqual(json.loads(out), [])
        self.assertIn("No backfills into osd.682.", err)


class UpAndActingColumnsTest(unittest.TestCase):
    def test_each_osd_gets_its_own_utilization_and_host(self):
        c = cb.Cancellation("19.1", 0, 5, 7, 1_000, "s", None)
        osd_df = {5: {"utilization": 91.25}, 7: {"utilization": 80.0}}
        cells = dict(zip(column_names(), cb.format_row(c, osd_df, {5: "hu", 7: "ha"})))
        self.assertEqual(
            (cells["UP OSD"], cells["UP UTIL"], cells["UP HOST"]),
            ("5", "91.2%", "hu"),
        )
        self.assertEqual(
            (cells["ACTING OSD"], cells["ACTING UTIL"], cells["ACTING HOST"]),
            ("7", "80.0%", "ha"),
        )

    def test_acting_columns_come_before_up_columns(self):
        self.assertEqual(
            column_names()[2:8],
            [
                "ACTING OSD",
                "ACTING UTIL",
                "ACTING HOST",
                "UP OSD",
                "UP UTIL",
                "UP HOST",
            ],
        )

    def test_state_is_abbreviated(self):
        c = cb.Cancellation(
            "19.1", 0, 5, 7, 1_000, "active+remapped+backfill_wait", None
        )
        cells = dict(zip(column_names(), cb.format_row(c, {}, {})))
        self.assertEqual(cells["STATE"], "act+remap+bkfl_wt")

    def test_missing_figures_show_a_question_mark(self):
        c = cb.Cancellation("19.1", 0, 5, 7, 1_000, "s", None)
        cells = dict(zip(column_names(), cb.format_row(c, {}, {})))
        self.assertEqual(
            [cells[k] for k in ("UP UTIL", "UP HOST", "ACTING UTIL", "ACTING HOST")],
            ["?"] * 4,
        )


class PgremapperMappingsOutputTest(unittest.TestCase):
    def cancellations(self):
        return [
            cb.Cancellation("19.14cd", 5, 232, 337, 1_000, "s", None, 8),
            cb.Cancellation("19.14cd", 8, 896, 614, 1_000, "s", None),
            cb.Cancellation("7.1", "-", 5, 6, 1_000, "s", None),
        ]

    def printed(self, cancellations):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cb.print_pgremapper_mappings(cancellations)
        return out.getvalue()

    def test_is_a_json_array_of_pgid_and_mapping_entries(self):
        self.assertEqual(
            upmap_pairs(self.printed(self.cancellations())),
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

    def test_each_entry_carries_its_rows_shard_role_and_note(self):
        entries = json.loads(self.printed(self.cancellations()))
        self.assertEqual(
            [(e["shard"], e["role"], e["note"]) for e in entries],
            [
                (5, shared.ROLE_COMPANION, "companion of shard 8"),
                (8, shared.ROLE_REQUESTED, ""),
                ("-", shared.ROLE_REQUESTED, ""),
            ],
        )

    def test_a_single_entry_has_no_comma(self):
        self.assertEqual(
            upmap_pairs(self.printed(self.cancellations()[:1])),
            [{"pgid": "19.14cd", "mapping": {"from": 232, "to": 337}}],
        )


class PrintTableTest(unittest.TestCase):
    def printed(self, rows):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            shared.print_table(cb.COLUMNS, rows)
        return out.getvalue().splitlines()

    def test_no_rows_prints_just_the_two_header_lines(self):
        group_line, label_line = self.printed([])
        self.assertEqual(label_line.split(), [label for _, label in cb.COLUMNS])
        self.assertEqual(group_line.replace("-", " ").split(), ["ACTING", "UP"])

    def test_group_names_are_centered_in_dashes_over_their_columns(self):
        row = ["19.1", "0", "77", "80.0%", "host07", "55", "91.2%", "host05"]
        row += ["1.0 GiB", "50%", "active+remapped+backfilling", ""]
        group_line, label_line, data_line = self.printed([row])
        # each group's name sits centered in dashes over its OSD..HOST columns
        # (as wide as their widest cells), and nothing spans the ungrouped ones
        acting_start = label_line.index("OSD")
        acting_end = data_line.index("host07") + len("host07")
        span = group_line[acting_start:acting_end]
        self.assertEqual(span.strip("- "), "ACTING")
        self.assertTrue(span.startswith("-") and span.endswith("-"))
        up_start = label_line.index("OSD", acting_end)
        up_end = data_line.index("host05") + len("host05")
        span = group_line[up_start:up_end]
        self.assertEqual(span.strip("- "), "UP")
        self.assertTrue(span.startswith("-") and span.endswith("-"))
        self.assertEqual(group_line[:acting_start].strip(), "")
        self.assertEqual(len(group_line.rstrip()), up_end)
        # each cell sits under its own label
        self.assertEqual(data_line.index("77"), acting_start)
        self.assertEqual(data_line.index("55"), up_start)

    def test_groups_are_set_apart_by_wider_gaps_than_columns(self):
        _, label_line = self.printed([])
        self.assertIn("OSD" + shared.COLUMN_SEP + "UTIL", label_line)
        self.assertIn("HOST" + shared.GROUP_SEP + "OSD", label_line)


class ParseArgsCliTest(unittest.TestCase):
    def parse(self, *argv):
        return parse_args(cb, argv)

    def test_osds_is_a_flag_not_positional(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.parse("682")  # no --osds: bare id is unrecognized
        self.assertEqual(self.parse("--osds", "682").osds, [682])
        self.assertEqual(self.parse("--osds", "osd.682", "74").osds, [682, 74])

    def test_osds_is_optional(self):
        self.assertEqual(self.parse().osds, [])

    def test_exclude_pgs_defaults_to_empty(self):
        self.assertEqual(self.parse("--osds", "682").exclude_pgs, [])

    def test_exclude_pgs_takes_a_space_separated_list(self):
        args = self.parse("--osds", "682", "--exclude-pgs", "19.9", "19.a")
        self.assertEqual(args.exclude_pgs, ["19.9", "19.a"])


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
        "pool_ls_detail": [{**EC_POOL_DETAIL, "pool_name": "ec"}, REP_POOL_DETAIL],
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


def pins(cancellations) -> list[str]:
    """Each cancellation as the '<pgid> <up OSD> <acting OSD>' pair it pins."""
    return [f"{c.pgid} {c.up_osd} {c.acting_osd}" for c in cancellations]


def canned_ceph(pg_stats, extra_osds=(), drop=()):
    """Return a stand-in for SnapshotStore.json serving a tiny cluster.

    drop names 'osd dump' keys to leave out.
    """
    data = canned_snapshots(pg_stats, extra_osds)
    for key in drop:
        del data["osd_dump"][key]

    def fake(store, key):
        return json.loads(json.dumps(data[key]))

    return fake


class MainTest(unittest.TestCase):
    PGS: ClassVar[list[dict]] = [
        pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4], "active+remapped+backfilling"),
        pg_stat("19.a", [OSD, 2, 3, 4], [NONE, 2, 3, 4]),
        pg_stat("19.b", [1, 2, 3, 4], [1, 2, 3, 4], "active+clean"),
        # osd.8 and osd.9 share host h2: pinning shard 0 back to 8 clashes with
        # shard 3 arriving on 9, so 9->4 is pinned back as a companion.
        pg_stat("19.d", [OSD, 2, 3, 9], [8, 2, 3, 4]),
    ]

    def run_main(self, *argv, pgs=None, extra_osds=(), drop=()):
        out, err = io.StringIO(), io.StringIO()
        fake = canned_ceph(self.PGS if pgs is None else pgs, extra_osds, drop)
        args = parse_args(cb, argv)
        with (
            mock.patch.object(shared.SnapshotStore, "json", fake),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            cb.run(args)
        return out.getvalue(), err.getvalue()

    def plan(self, *argv, pgs=None, extra_osds=(), drop=()):
        """Return plan()'s StopResult on the same canned cluster run_main uses."""
        fake = canned_ceph(self.PGS if pgs is None else pgs, extra_osds, drop)
        store = FakeStore(
            {k: fake(None, k) for k in cb.SNAPSHOT_COMMANDS if k != "pg_dump_pgs"},
            load_dir=None,
        )
        return cb.plan(parse_args(cb, argv), store)

    def test_plan_pins_each_arrival_and_its_companion(self):
        result = self.plan("--osds", "682")
        self.assertEqual(
            pins(result.cancellations), ["19.9 682 8", "19.d 682 8", "19.d 9 4"]
        )
        self.assertEqual(result.cancellations[2].companion_of, 0)
        # the unpinnable shard is reported, not silently dropped
        self.assertEqual([(s.pgid, s.shard) for s in result.skipped], [("19.a", 0)])

    def test_table_shows_acting_osd_state_and_host(self):
        out, _ = self.run_main("--osds", "682")
        lines = out.splitlines()
        self.assertIn("PGID", lines[1])
        self.assertEqual(len(lines), 5)  # two header lines + the 3 pins
        for cell in ("19.9", "682", "8", "90.5%", "h2", "bkfl"):
            self.assertIn(cell, lines[2])
        # the acting OSD (8) comes first, then the UP OSD (682) with its own
        # utilization and host; both are bare ids
        self.assertRegex(lines[2], r"\s8\s+90\.5%\s+h2\s+682\s+88\.0%\s+h1")
        self.assertIn("companion of shard 0", lines[4])

    def test_approx_note_appears_when_progress_comes_from_counters(self):
        # No backfill positions (the tests' stubbed live query returns none),
        # so every row's PROGRESS is from Ceph's counters.
        out, err = self.run_main("--osds", "682")
        self.assertIn("~100%", out)
        self.assertIn("NOTE: ~ marks PROGRESS from Ceph's misplaced/degraded", err)

    def test_progress_from_backfill_positions(self):
        # The counters say 100%; the target's position says it hasn't started.
        pgs = [
            pg_stat(
                "19.9", [OSD, 2, 3, 4], [8, 2, 3, 4], "active+remapped+backfilling"
            ),
        ]
        positions = mock.Mock(return_value={"19.9": {f"{OSD}(0)": "MIN"}})
        with mock.patch.object(shared, "query_backfill_positions", positions):
            out, err = self.run_main("--osds", "682", pgs=pgs)
        positions.assert_called_once()
        self.assertEqual({"19.9"}, set(positions.call_args.args[0]))
        self.assertIn(" 0% ", out)
        self.assertNotIn("~", out.split("\n\n")[0])
        self.assertNotIn("~ marks PROGRESS", err)

    def test_pgremapper_mappings_is_json_with_every_pair_and_no_warning(self):
        out, err = self.run_main("--pgremapper-mappings", "--osds", "682")
        self.assertEqual(
            upmap_pairs(out),
            [
                {"pgid": "19.9", "mapping": {"from": 682, "to": 8}},
                {"pgid": "19.d", "mapping": {"from": 682, "to": 8}},
                {"pgid": "19.d", "mapping": {"from": 9, "to": 4}},
            ],
        )
        self.assertNotIn("WARNING", err)
        self.assertIn("19.a", err)  # unpinnable shards are still reported

    def test_table_output_does_not_warn(self):
        _, err = self.run_main("--osds", "682")
        self.assertNotIn("WARNING", err)

    def test_blockers_are_not_pinned_by_default(self):
        # osd.77 is at 92%, over the 91% backfillfull_ratio, so shard 3 (66->77)
        # would hold 19.e in backfill_toofull, but without --pin-blockers the
        # tool never looks for it: only the requested pin comes out, plus a
        # note pointing at the flag.
        pgs = [pg_stat("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        kwargs = {"pgs": pgs, "extra_osds": [osd_df_node(77, 92.0)]}
        result = self.plan("--osds", "682", **kwargs)
        self.assertEqual(pins(result.cancellations), ["19.e 682 8"])
        _, err = self.run_main("--osds", "682", **kwargs)
        self.assertIn("--pin-blockers was not given", err)

    def test_blocker_in_the_same_pg_is_proposed_and_explained(self):
        # osd.77 is at 92%, over the 91% backfillfull_ratio, so shard 3 (66->77)
        # would hold 19.e in backfill_toofull and, with --pin-blockers, must be
        # pinned back too.
        pgs = [pg_stat("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        kwargs = {"pgs": pgs, "extra_osds": [osd_df_node(77, 92.0)]}
        result = self.plan("--pin-blockers", "--osds", "682", **kwargs)
        self.assertEqual(pins(result.cancellations), ["19.e 682 8", "19.e 77 66"])
        blocker = result.cancellations[1]
        self.assertEqual(blocker.companion_of, 0)
        self.assertIsNotNone(blocker.blocker_util)
        _, err = self.run_main("--pin-blockers", "--osds", "682", **kwargs)
        self.assertIn("1 more shard(s)", err)
        self.assertIn("(companions; blockers: 1)", flat(err))
        self.assertNotIn("--pin-blockers was not given", err)

    def test_table_says_which_shard_a_blocker_blocks(self):
        pgs = [pg_stat("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        out, _ = self.run_main(
            "--pin-blockers",
            "--osds",
            "682",
            pgs=pgs,
            extra_osds=[osd_df_node(77, 92.0)],
        )
        rows = out.splitlines()
        self.assertEqual(len(rows), 4)  # two header lines + the 2 pins
        self.assertIn("blocks shard 0: target osd.77 projected at 92.0%", rows[3])
        self.assertNotIn("blocks", rows[2])

    def test_a_target_below_the_ratio_is_not_a_blocker(self):
        pgs = [pg_stat("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        result = self.plan(
            "--pin-blockers",
            "--osds",
            "682",
            pgs=pgs,
            extra_osds=[osd_df_node(77, 80.0)],
        )
        self.assertEqual(pins(result.cancellations), ["19.e 682 8"])

    def test_without_a_backfillfull_ratio_pin_blockers_has_nothing_to_work_from(self):
        pgs = [pg_stat("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        kwargs = {
            "pgs": pgs,
            "extra_osds": [osd_df_node(77, 92.0)],
            "drop": ["backfillfull_ratio"],
        }
        result = self.plan("--pin-blockers", "--osds", "682", **kwargs)
        self.assertEqual(pins(result.cancellations), ["19.e 682 8"])
        self.assertIsNone(result.backfillfull_pct)
        _, err = self.run_main("--pin-blockers", "--osds", "682", **kwargs)
        self.assertIn("no backfillfull_ratio", err)
        self.assertNotIn("--pin-blockers was not given", err)

    def test_without_a_backfillfull_ratio_and_without_the_flag_only_one_note(self):
        # nothing useful to say about a flag whose search would find nothing
        # anyway: neither note is worth printing.
        pgs = [pg_stat("19.e", [OSD, 2, 3, 77], [8, 2, 3, 66])]
        out, err = self.run_main(
            "--pgremapper-mappings",
            "--osds",
            "682",
            pgs=pgs,
            extra_osds=[osd_df_node(77, 92.0)],
            drop=["backfillfull_ratio"],
        )
        self.assertEqual(
            upmap_pairs(out), [{"pgid": "19.e", "mapping": {"from": 682, "to": 8}}]
        )
        self.assertNotIn("no backfillfull_ratio", err)
        self.assertNotIn("--pin-blockers was not given", err)

    def test_a_requested_pin_that_chains_leaves_its_pg_out(self):
        # shard 0 comes back to osd.20 only once shard 1 (20->30) is pinned:
        # a chain pgremapper cannot apply, and the requested pin is its head
        chain = pg_stat("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        plain = pg_stat("19.2", [OSD, 2, 3, 4], [8, 2, 3, 4])
        result = self.plan("--osds", "682", pgs=[chain, plain])
        self.assertEqual(pins(result.cancellations), ["19.2 682 8"])
        self.assertEqual([(s.pgid, s.shard) for s in result.skipped], [("19.f", 0)])
        self.assertEqual(result.chained, {"19.f": [(20, 30), (OSD, 20)]})
        for flags in ((), ("--pgremapper-mappings",)):
            out, err = self.run_main(*flags, "--osds", "682", pgs=[chain, plain])
            self.assertNotIn("19.f", out)
            self.assertIn("ceph osd pg-upmap-items 19.f 20 30 682 20", err)

    def test_existing_upmap_pairs_are_checked_for_chains(self):
        # the pin's target, osd.8, is the source of an existing pair
        fake = canned_ceph([pg_stat("19.2", [OSD, 2, 3, 4], [8, 2, 3, 4])])
        snaps = {k: fake(None, k) for k in cb.SNAPSHOT_COMMANDS if k != "pg_dump_pgs"}
        snaps["osd_dump"]["pg_upmap_items"] = [
            {"pgid": "19.2", "mappings": [{"from": 8, "to": 9}]}
        ]
        result = cb.plan(
            parse_args(cb, ["--osds", "682"]), FakeStore(snaps, load_dir=None)
        )
        self.assertEqual(result.cancellations, [])
        self.assertEqual(result.chained, {"19.2": [(8, 9), (OSD, 8)]})

    def test_without_osd_a_chain_keeps_its_last_pin(self):
        chain = pg_stat("19.f", [OSD, 20, 3, 4], [20, 30, 3, 4])
        result = self.plan(pgs=[chain])
        self.assertEqual(pins(result.cancellations), ["19.f 20 30"])
        self.assertEqual([(s.pgid, s.shard) for s in result.skipped], [("19.f", 0)])

    def test_pgremapper_mappings_is_valid_json_even_with_nothing_to_apply(self):
        # nothing arriving at all
        out, err = self.run_main(
            "--pgremapper-mappings", "--osds", "682", pgs=[self.PGS[2]]
        )
        self.assertEqual(json.loads(out), [])
        self.assertIn("No backfills", err)
        # only an unpinnable shard (no acting OSD)
        out, err = self.run_main(
            "--pgremapper-mappings", "--osds", "682", pgs=[self.PGS[1]]
        )
        self.assertEqual(json.loads(out), [])
        self.assertIn("19.a", err)

    def test_no_backfills_prints_nothing_on_stdout(self):
        out, err = self.run_main("--osds", "682", pgs=[self.PGS[2]])
        self.assertEqual(out, "")
        self.assertIn("No backfills", err)

    def test_unknown_osd_is_an_error(self):
        with self.assertRaises(SystemExit) as ctx:
            self.plan("--osds", "5000")
        self.assertEqual(
            str(ctx.exception), "ERROR: --osds: not in 'ceph osd df': osd.5000"
        )

    def test_exclude_pgs_removes_the_named_pg_only(self):
        result = self.plan("--osds", "682", "--exclude-pgs", "19.9")
        # 19.9 (a plain arrival) is gone; 19.d's companion pin remains
        self.assertEqual(pins(result.cancellations), ["19.d 682 8", "19.d 9 4"])
        self.assertEqual(result.exclude_filter, shared.PgidFilter(1, 1, []))

    def test_exclude_pgs_reports_an_entry_that_matched_nothing(self):
        result = self.plan("--osds", "682", "--exclude-pgs", "19.9", "19.zzz")
        self.assertEqual(pins(result.cancellations), ["19.d 682 8", "19.d 9 4"])
        self.assertEqual(result.exclude_filter, shared.PgidFilter(2, 1, ["19.zzz"]))
        _, err = self.run_main("--osds", "682", "--exclude-pgs", "19.9", "19.zzz")
        self.assertIn("1 of 2 given PG id(s) matched", flat(err))
        self.assertIn(
            "1 matched nothing (not remapped, not involving osd.682, or a typo): "
            "19.zzz",
            flat(err),
        )

    def test_exclude_pgs_all_entries_matched_nothing(self):
        # none of PGS involves osd.5000 at all: matched must read as zero,
        # not silently omit the count.
        result = self.plan("--osds", "682", "--exclude-pgs", "19.zzz")
        self.assertEqual(
            pins(result.cancellations), ["19.9 682 8", "19.d 682 8", "19.d 9 4"]
        )
        self.assertEqual(result.exclude_filter, shared.PgidFilter(1, 0, ["19.zzz"]))
        _, err = self.run_main("--osds", "682", "--exclude-pgs", "19.zzz")
        self.assertIn("0 of 1 given PG id(s) matched", err)

    def test_no_exclude_pgs_note_when_the_flag_is_not_given(self):
        _, err = self.run_main("--osds", "682")
        self.assertNotIn("--exclude-pgs", err)

    def test_exclude_pgs_note_does_not_claim_a_backfill_that_never_existed(self):
        # osd.682 is stable here (shard 0); only shard 4 is remapped, and not
        # onto osd.682. The note must not claim a backfill was skipped, only
        # that the PG (which does list osd.682 in 'up') matched.
        stable = pg_stat("19.99", [OSD, 2, 3, 4, 20, 6], [OSD, 2, 3, 4, 9, 6])
        out, err = self.run_main(
            "--osds", "682", "--exclude-pgs", "19.99", pgs=[stable]
        )
        self.assertEqual(out, "")
        self.assertIn("No backfills into osd.682", err)
        self.assertIn("1 of 1 given PG id(s) matched", err)
        self.assertNotIn("had a backfill", err)


class HostnameCollisionTest(unittest.TestCase):
    """Two hosts must never anonymize to one: the analysis depends on sharing."""

    def test_same_trailing_number_gets_distinct_names(self):
        fakes = shared._fake_hostnames({"ceph1-5", "ceph2-5", "ceph2-6"})
        self.assertEqual(len(set(fakes.values())), 3)
        self.assertEqual(fakes["ceph2-6"], "host06")  # a unique number keeps its name
        self.assertRegex(fakes["ceph1-5"], r"host-[0-9a-f]{8}")

    def test_unique_names_keep_the_numbered_form(self):
        self.assertEqual(
            shared._fake_hostnames({"ceph2-1", "ceph2-2"}),
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
        shared.anonymize_snapshots(snaps)
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
        shared.anonymize_snapshots(once)
        twice = json.loads(json.dumps(once))
        shared.anonymize_snapshots(twice)
        self.assertEqual(once, twice)


class NoPgsTest(unittest.TestCase):
    def fetch(self, raw):
        store = FakeStore({"pg_ls_remapped": raw})
        return shared.fetch_pg_stats(store, "pg_ls_remapped")

    def test_pg_ls_that_is_not_ready_is_an_error_not_no_pgs(self):
        with self.assertRaises(SystemExit) as ctx:
            self.fetch({"pg_ready": False})
        self.assertIn("not ready", str(ctx.exception))

    def test_pg_ready_true_with_no_pgs_is_still_empty(self):
        self.assertEqual(self.fetch({"pg_ready": True}), [])

    def test_pg_ls_with_no_matching_pgs_returns_only_pg_ready(self):
        self.assertEqual(self.fetch({"pg_ready": True}), [])

    def test_unrecognised_json_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.fetch({"what": 1})


def run_cli(*argv, load_state=None, path=None, no_rados=None):
    """Run the subcommand as a subprocess.

    load_state, when given, is passed as the global --load-state; path
    replaces PATH when given; no_rados, a directory, becomes PYTHONPATH (see
    LoadStateCliTest.setUp).
    """
    env = None
    if path is not None:
        env = {**os.environ, "PATH": path}
    if no_rados is not None:
        env = {**(env or os.environ), "PYTHONPATH": str(no_rados)}
    global_argv = ["--load-state", load_state] if load_state is not None else []
    return subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "backfillctl"),
            *global_argv,
            "cancel-backfill",
            *argv,
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


class LoadStateCliTest(unittest.TestCase):
    """--load-state, end to end: a directory (what 'backfillctl save-state'
    produces) replaces the live cluster. A fake 'ceph' on PATH gives a live
    run to compare against; the on-disk state is built by hand from the same
    canned data, keyed as save-state would write it (pg_dump_pgs.json, not
    pg_ls_remapped.json -- see cb.fetch_remapped_pg_stats)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        snaps = canned_snapshots(MainTest.PGS)
        table = {
            " ".join(cmd[1:]): snaps[key]
            for key, cmd in cb.SNAPSHOT_COMMANDS.items()
            if key != "pg_dump_pgs"  # never requested live, see fetch_remapped_pg_stats
        }
        fake = self.bin / "ceph"
        # 'pg <pgid> query' (the backfill positions) answers {}: no targets,
        # so PROGRESS comes from the counters, the same as the replay's.
        fake.write_text(
            f"#!{sys.executable}\nimport json, sys\n"
            "cmd = ' '.join(sys.argv[1:])\n"
            f"print(json.dumps({table!r}.get(cmd, {{}})))\n"
        )
        fake.chmod(0o755)
        self.with_ceph = f"{self.bin}{os.pathsep}{os.environ['PATH']}"
        # Keep the live run from reaching a real cluster over librados
        # (rados would be importable on a ceph admin host): a 'rados' module
        # that fails to import sends it to the fake ceph CLI instead.
        self.no_rados = self.root / "no-rados"
        self.no_rados.mkdir()
        (self.no_rados / "rados.py").write_text(
            "raise ImportError('blocked in tests')\n"
        )

        self.state = self.root / "state"
        self.state.mkdir()
        for key in (
            "osd_tree",
            "osd_df",
            "osd_dump",
            "pool_ls_detail",
            "crush_rule_dump",
        ):
            (self.state / f"{key}.json").write_text(json.dumps(snaps[key]))
        (self.state / "pg_dump_pgs.json").write_text(
            json.dumps(snaps["pg_ls_remapped"])
        )

    def test_load_state_reproduces_the_live_output(self):
        live = run_cli(
            "--pgremapper-mappings",
            "--osds",
            "682",
            path=self.with_ceph,
            no_rados=self.no_rados,
        )
        self.assertEqual(live.returncode, 0, live.stderr)
        self.assertEqual(
            upmap_pairs(live.stdout),
            [
                {"pgid": "19.9", "mapping": {"from": 682, "to": 8}},
                {"pgid": "19.d", "mapping": {"from": 682, "to": 8}},
                {"pgid": "19.d", "mapping": {"from": 9, "to": 4}},
            ],
        )

        replay = run_cli(
            "--pgremapper-mappings", "--osds", "682", load_state=str(self.state)
        )
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(replay.stdout, live.stdout)

    def test_load_state_reproduces_the_live_table(self):
        # The table shows PROGRESS, so the live run queries each proposed PG's
        # backfill position through the fake ceph (see setUp).
        live = run_cli("--osds", "682", path=self.with_ceph, no_rados=self.no_rados)
        self.assertEqual(live.returncode, 0, live.stderr)
        self.assertIn("~", live.stdout)
        replay = run_cli("--osds", "682", load_state=str(self.state))
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(replay.stdout, live.stdout)
        self.assertEqual(replay.stderr, live.stderr)

    def test_load_reports_a_missing_directory_and_a_missing_file(self):
        result = run_cli("--osds", "682", load_state=str(self.root / "nope"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not found", result.stderr)

        (self.state / "pg_dump_pgs.json").unlink()
        result = run_cli("--osds", "682", load_state=str(self.state))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)


FIXTURE = TEST_DATA / "cancel-backfill-ceph2-osd896-host-clash-companions"

FIXTURE_BLOCKER = TEST_DATA / "cancel-backfill-ceph2-osd896-blocker-in-same-pg"

# Documented in the fixture's README.txt. The pins that are neither into 896 nor
# needed for host validity alone are the blockers (targets over backfillfull),
# only found with --pin-blockers.
EXPECTED_896_WITH_BLOCKERS = """\
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

# Without --pin-blockers: the two pure-blocker lines (19.92e's "337 99" and
# 19.14cd's "314 347") are missing; everything else (the companion pins) is
# unconditional and unchanged.
EXPECTED_896_DEFAULT = """\
19.7e9 896 627
19.7e9 149 497
19.92e 896 231
19.94c 896 522
19.94c 716 266
19.14cd 232 337
19.14cd 896 614
19.1b16 896 591
19.1b16 524 578
19.1fed 896 5
19.1fed 12 207
"""


def fixture_plan(fixture, osd, *flags):
    """Return plan()'s StopResult for osd on a fixture directory."""
    return plan_from_state(cb, fixture, *flags, "--osds", str(osd))


class FixtureReplayTest(unittest.TestCase):
    """Replay the real-cluster snapshot in tests/backfillctl/test-data (see its README.txt)."""

    def replay(self, osd, *flags):
        result = run_cli(*flags, "--osds", str(osd), load_state=str(FIXTURE))
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def plan(self, osd, *flags):
        return fixture_plan(FIXTURE, osd, *flags)

    def test_osd_896_pins_default(self):
        # --pin-blockers not given: the two pure-blocker pins are missing.
        self.assertEqual(
            pins(self.plan(896).cancellations), EXPECTED_896_DEFAULT.splitlines()
        )

    def test_osd_896_pins_with_pin_blockers(self):
        self.assertEqual(
            pins(self.plan(896, "--pin-blockers").cancellations),
            EXPECTED_896_WITH_BLOCKERS.splitlines(),
        )

    def test_osd_896_pgremapper_mappings_matches_the_planned_pins(self):
        for flags, expected in (
            ((), EXPECTED_896_DEFAULT),
            (("--pin-blockers",), EXPECTED_896_WITH_BLOCKERS),
        ):
            with self.subTest(flags=flags):
                result = self.replay(896, *flags, "--pgremapper-mappings")
                entries = json.loads(result.stdout)
                self.assertEqual(
                    [
                        f"{e['pgid']} {e['mapping']['from']} {e['mapping']['to']}"
                        for e in entries
                    ],
                    expected.splitlines(),
                )
                self.assertNotIn("WARNING", result.stderr)

    def summary_counts(self, result):
        """(requested pins, other pins, blockers, skipped): what the summary reports."""
        requested = sum(c.companion_of is None for c in result.cancellations)
        return (
            requested,
            len(result.cancellations) - requested,
            sum(c.blocker_util is not None for c in result.cancellations),
            len(result.skipped),
        )

    def test_osd_896_summary_counts_default(self):
        self.assertEqual(self.summary_counts(self.plan(896)), (6, 5, 0, 0))

    def test_osd_896_summary_counts_with_pin_blockers(self):
        self.assertEqual(
            self.summary_counts(self.plan(896, "--pin-blockers")), (6, 7, 7, 0)
        )

    def test_osd_896_summary_on_stderr(self):
        err = flat(self.replay(896).stderr)
        self.assertIn("6 arriving shard(s)", err)
        self.assertIn("5 more shard(s)", err)
        self.assertNotIn("blockers:", err)
        self.assertIn("0 cannot be pinned", err)
        self.assertIn("--pin-blockers was not given", err)

        err = self.replay(896, "--pin-blockers").stderr
        self.assertIn("(companions; blockers: 7)", flat(err))
        self.assertNotIn("--pin-blockers was not given", err)

    def test_several_osds_pin_the_union_of_their_backfills(self):
        one = pins(self.plan(896).cancellations) + pins(self.plan(74).cancellations)
        both = plan_from_state(cb, FIXTURE, "--osds", "896", "74")
        self.assertEqual(sorted(pins(both.cancellations)), sorted(one))
        result = run_cli("--osds", "896", "74", load_state=str(FIXTURE))
        self.assertEqual(result.returncode, 0, result.stderr)
        err = flat(result.stderr)
        self.assertIn("osd.74 (host14) is at 90.7%, osd.896 (host51) at 81.1%.", err)
        self.assertIn("of their capacity", err)

    def test_osd_74_needs_no_companions(self):
        self.assertEqual(
            pins(self.plan(74).cancellations),
            ["19.16fc 74 183", "19.1eb3 74 512"],
        )

    def test_osd_682_pairs_its_shard_with_the_one_on_the_same_host(self):
        # osd.231 (the acting OSD of 19.16fc shard 7) shares a host with osd.74,
        # where shard 6 of the same PG is arriving. This pin is unconditional
        # (a companion), so it is there with or without --pin-blockers. The
        # flag adds shards 4 and 5: osd.898 and osd.885 are only 66.5% and 78%
        # full, but the shards queued for them project 91.8% and 99.5%.
        self.assertEqual(
            pins(self.plan(682).cancellations),
            ["19.16fc 74 183", "19.16fc 682 231"],
        )
        self.assertEqual(
            pins(self.plan(682, "--pin-blockers").cancellations),
            ["19.16fc 898 334", "19.16fc 885 260", "19.16fc 74 183", "19.16fc 682 231"],
        )

    def test_osd_682_companion_becomes_a_blocker_with_the_flag(self):
        # osd.74 (shard 6's target) is also projected over backfillfull_ratio,
        # so --pin-blockers changes only how that pin is explained, not whether
        # it is there.
        companion = self.plan(682).cancellations[0]
        self.assertEqual((companion.shard, companion.companion_of), (6, 7))
        self.assertIsNone(companion.blocker_util)
        (blocker,) = [
            c for c in self.plan(682, "--pin-blockers").cancellations if c.shard == 6
        ]
        self.assertEqual(blocker.companion_of, 7)
        self.assertAlmostEqual(blocker.blocker_util, 92.5, places=1)

    def test_an_osd_with_no_backfills_prints_nothing(self):
        result = self.replay(231)
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
        pgs = {p["pgid"]: p for p in snap["pg_dump_pgs"]["pg_stats"]}
        for osd in (896, 74, 682):
            for pin_blockers in (False, True):
                flags = ("--pin-blockers",) if pin_blockers else ()
                by_pg: dict[str, list[tuple[int, int]]] = {}
                for c in self.plan(osd, *flags).cancellations:
                    by_pg.setdefault(c.pgid, []).append((c.up_osd, c.acting_osd))
                for pgid, pairs in by_pg.items():
                    up = list(pgs[pgid]["up"])
                    for from_osd, to_osd in pairs:
                        slot = up.index(from_osd)
                        # a pin always sends the shard back to where it is now
                        self.assertEqual(
                            pgs[pgid]["acting"][slot], to_osd, (osd, pin_blockers, pgid)
                        )
                        up[slot] = to_osd
                    self.assertEqual(
                        len(set(up)), len(up), (osd, pin_blockers, pgid, "OSD twice")
                    )
                    hosts = [host[o] for o in up]
                    self.assertEqual(
                        len(set(hosts)), len(hosts), (osd, pin_blockers, pgid, hosts)
                    )

    def test_the_older_fixture_only_gained_the_ratios_it_lacked(self):
        dump = json.loads((FIXTURE / "osd_dump.json").read_text())
        self.assertEqual(dump["backfillfull_ratio"], 0.91)


class ChainFixtureReplayTest(unittest.TestCase):
    """Real chains: 19.1299 (asked about osd.891) has shard 1 going to osd.579
    while osd.579 still holds shard 8, which is going to osd.825."""

    def replay(self, *flags):
        result = run_cli(*flags, "--osds", "891", load_state=str(FIXTURE))
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_it_is_left_out_and_the_warning_has_the_pairs_in_apply_order(self):
        # the requested pin (891->579) is the chain's head, so no pin helps
        result = fixture_plan(FIXTURE, 891)
        self.assertEqual(result.chained["19.1299"], [(579, 825), (891, 579)])
        self.assertNotIn("19.1299", {c.pgid for c in result.cancellations})
        self.assertIn(("19.1299", 1), {(s.pgid, s.shard) for s in result.skipped})

    def test_machine_format_leaves_it_out_and_the_warning_gives_the_command(self):
        result = self.replay("--pgremapper-mappings")
        self.assertNotIn("19.1299", result.stdout)
        self.assertIn("ceph osd pg-upmap-items 19.1299 579 825 891 579", result.stderr)

    def test_no_output_in_the_cluster_chains(self):
        """Across every OSD that is a target of a remapped shard: no PG's pins
        chain once avoid_chains is done."""
        snap = {p.stem: json.loads(p.read_text()) for p in FIXTURE.glob("*.json")}
        store = FakeStore(snap)
        osd_df, osd_host = cb.fetch_osd_df(store), cb.fetch_osd_hosts(store)
        pgs, pools = shared.fetch_pg_stats(store, "pg_dump_pgs"), cb.fetch_pools(store)
        ecp, rules = cb.fetch_ec_profiles(store), cb.fetch_crush_rules(store)
        pct = cb.fetch_backfillfull_pct(store)
        targets = {
            o
            for p in pgs
            for i, o in enumerate(p["up"])
            if o != NONE and o != p["acting"][i]
        }
        chained = 0
        for osd in sorted(targets):
            cancellations, _ = cb.plan_cancellations(
                pgs, pools, ecp, {osd}, osd_host, rules, osd_df, pct, True
            )
            resolved = shared.avoid_chains(cancellations, {}, pgs, osd_host, False)
            by_pg = {}
            for c in resolved.cancellations:
                by_pg.setdefault(c.pgid, []).append(c)
            for pgid, cs in by_pg.items():
                self.assertEqual(shared.chain_heads([], cs), [], (osd, pgid))
            chained += len(resolved.chained)
        self.assertGreaterEqual(chained, 13)  # the real chains this test is about


FIXTURE_CHAINS = TEST_DATA / "ceph2-cancel-backfill-chained-pairs-upmap"


class ChainedPairsFixtureTest(unittest.TestCase):
    """Chains among pins, into existing pairs, and among existing pairs (see
    the fixture's README.txt), cancelled without --osds."""

    @classmethod
    def setUpClass(cls):
        cls.result = plan_from_state(cb, FIXTURE_CHAINS)
        store = FakeStore(
            {p.stem: json.loads(p.read_text()) for p in FIXTURE_CHAINS.glob("*.json")}
        )
        cls.upmaps = {
            pgid: [(m["from"], m["to"]) for m in pairs]
            for pgid, pairs in shared.fetch_upmap_items(store).items()
        }

    def test_each_chain_leaves_one_backfill_running(self):
        self.assertEqual(
            [(s.pgid, s.shard) for s in self.result.skipped],
            [
                ("19.3a4", 1),
                ("19.5fd", 8),
                ("19.1128", 4),
                ("19.1399", 9),
                ("19.146e", 3),
                ("19.1a6f", 4),
                ("19.1d52", 7),
            ],
        )

    def test_the_last_pin_of_each_chain_stays(self):
        self.assertEqual(
            pins(self.result.cancellations),
            [
                "19.1128 471 866",
                "19.1128 328 588",
                "19.1399 414 341",
                "19.146e 246 161",
                "19.146e 563 610",
                "19.1a6f 357 59",
                "19.1d52 838 82",
            ],
        )

    def test_no_pgs_pairs_chain_with_existing_ones(self):
        by_pg: dict[str, list] = {}
        for c in self.result.cancellations:
            by_pg.setdefault(c.pgid, []).append(c)
        for pgid, cs in by_pg.items():
            existing = self.upmaps.get(pgid, [])
            self.assertIsNone(shared.chain_link(existing), pgid)
            self.assertEqual(shared.chain_heads(existing, cs), [], pgid)

    def test_the_warning_commands_keep_the_existing_pairs(self):
        self.assertEqual(
            self.result.chained["19.1128"],
            [(274, 148), (545, 94), (471, 866), (328, 588), (888, 545)],
        )
        self.assertEqual(
            self.result.chained["19.3a4"],
            [(302, 723), (110, 753), (890, 110), (256, 816)],
        )


class BlockerFixtureReplayTest(unittest.TestCase):
    """The state that prompted blockers: osd.896 has ONE arriving shard
    (19.92e shard 4, from osd.231) yet its PG stays in backfill_toofull, because
    shard 6 of the same PG is going to osd.337, which is over backfillfull.
    That second shard is a pure blocker (no host clash), so it is only found
    with --pin-blockers -- this fixture is the reason the flag exists."""

    def replay(self, *flags):
        result = run_cli(*flags, "--osds", "896", load_state=str(FIXTURE_BLOCKER))
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def plan(self, *flags):
        return fixture_plan(FIXTURE_BLOCKER, 896, *flags)

    def test_without_pin_blockers_only_the_stuck_pin_comes_out(self):
        self.assertEqual(pins(self.plan().cancellations), ["19.92e 896 231"])
        self.assertIn("--pin-blockers was not given", self.replay().stderr)

    def test_the_blocking_shard_is_proposed_next_to_the_requested_one(self):
        self.assertEqual(
            pins(self.plan("--pin-blockers").cancellations),
            ["19.92e 896 231", "19.92e 337 99"],
        )

    def test_pgremapper_mappings_output(self):
        self.assertEqual(
            upmap_pairs(self.replay("--pin-blockers", "--pgremapper-mappings").stdout),
            [
                {"pgid": "19.92e", "mapping": {"from": 896, "to": 231}},
                {"pgid": "19.92e", "mapping": {"from": 337, "to": 99}},
            ],
        )

    def test_the_second_pin_is_a_blocker_of_the_first(self):
        requested, blocker = self.plan("--pin-blockers").cancellations
        self.assertEqual((requested.companion_of, requested.blocker_util), (None, None))
        self.assertEqual((blocker.shard, blocker.companion_of), (6, 4))
        self.assertAlmostEqual(blocker.blocker_util, 93.4, places=1)

    def test_the_table_explains_the_second_line(self):
        rows = self.replay("--pin-blockers").stdout.splitlines()
        self.assertIn(
            "blocks shard 4: target osd.337 projected at 93.4%, at or over "
            "backfillfull_ratio",
            rows[-1],
        )

    def test_summary_says_it_is_one_arriving_shard_plus_one_blocker(self):
        err = self.replay("--pin-blockers").stderr
        self.assertIn("1 arriving shard(s)", err)
        self.assertIn("1 more shard(s)", err)
        self.assertIn("(companions; blockers: 1)", flat(err))

    def test_the_help_recipe_keeps_the_wanted_backfill_and_its_blocker(self):
        # the user's case: keep 231->896. The help's jq filter keeps a PG's
        # entries only if they are blockers: select(.pgid != X or .role == "blocker").
        entries = json.loads(
            self.replay("--pin-blockers", "--pgremapper-mappings").stdout
        )
        kept = [e for e in entries if e["pgid"] != "19.92e" or e["role"] == "blocker"]
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["mapping"], {"from": 337, "to": 99})
        self.assertEqual(kept[0]["shard"], 6)
        self.assertIn("blocks shard 4: target osd.337", kept[0]["note"])


class CancelWholePgTest(unittest.TestCase):
    """cancel_whole_pg: every moving shard of one PG, as used without --osds."""

    def cancel(self, up, acting, is_ec=True, osd_host=None):
        return cb.cancel_whole_pg(up, acting, is_ec, osd_host or {})

    def test_every_moving_ec_shard_is_pinned_in_shard_order(self):
        moves, skipped = self.cancel([10, 2, 30, 4], [1, 2, 3, 4])
        self.assertEqual(moves, [(0, 10, 1), (2, 30, 3)])
        self.assertEqual(skipped, [])

    def test_nothing_moving_yields_nothing(self):
        self.assertEqual(self.cancel([1, 2, 3], [1, 2, 3]), ([], []))

    def test_an_empty_up_slot_is_not_a_backfill(self):
        # CRUSH found no OSD for shard 1: nothing arrives, so nothing to cancel
        self.assertEqual(self.cancel([10, NONE, 3], [1, 2, 3]), ([(0, 10, 1)], []))

    def test_a_degraded_shard_is_skipped_and_the_others_still_pinned(self):
        moves, skipped = self.cancel([10, 20, 3], [1, NONE, 3])
        self.assertEqual(moves, [(0, 10, 1)])
        self.assertEqual([s for s, _ in skipped], [1])
        self.assertIn("degraded", skipped[0][1])

    def test_a_clash_with_a_degraded_shard_skips_the_whole_pg(self):
        # shard 0 goes back to osd.1 on host H, but degraded shard 1 is still
        # headed for osd.20, also on H, and has nothing to be pinned back to
        moves, skipped = self.cancel(
            [10, 20, 3], [1, NONE, 3], osd_host={1: "H", 20: "H"}
        )
        self.assertEqual(moves, [])
        self.assertEqual([s for s, _ in skipped], [1, 0])
        self.assertIn("shares a host with shard 1", skipped[1][1])

    def test_a_chain_comes_out_in_apply_order(self):
        # shard 0 goes back to osd.20, which 'up' still has for shard 1, so
        # the pair moving osd.20 away (20->5) has to come first
        moves, _ = self.cancel([30, 20, 3], [20, 5, 3])
        self.assertEqual(moves, [(1, 20, 5), (0, 30, 20)])

    def test_a_ring_skips_every_pin_of_the_pg(self):
        moves, skipped = self.cancel([2, 1, 3], [1, 2, 3])
        self.assertEqual(moves, [])
        self.assertEqual([s for s, _ in skipped], [0, 1])
        self.assertIn("cycle", skipped[0][1])

    def test_several_replicas_are_paired_in_sorted_order(self):
        # find_arrivals calls this ambiguous; cancelling all of them is not
        moves, skipped = self.cancel([1, 40, 30], [1, 3, 4], is_ec=False)
        self.assertEqual(moves, [("-", 30, 3), ("-", 40, 4)])
        self.assertEqual(skipped, [])

    def test_a_missing_replica_is_skipped_and_the_rest_pinned(self):
        moves, skipped = self.cancel([1, 30, 40], [1, 3, NONE], is_ec=False)
        self.assertEqual(moves, [("-", 30, 3)])
        self.assertEqual(len(skipped), 1)
        self.assertIn("osd.40", skipped[0][1])
        self.assertIn("missing replica", skipped[0][1])

    def test_a_replicated_clash_skips_the_whole_pg(self):
        # osd.40 has no departing partner and stays in 'up', on osd.3's host
        moves, skipped = self.cancel(
            [1, 30, 40], [1, 3, NONE], is_ec=False, osd_host={3: "H", 40: "H"}
        )
        self.assertEqual(moves, [])
        self.assertEqual(len(skipped), 2)
        self.assertIn("acting osd.3 shares a host with replica osd.40", skipped[1][1])


def run_plan_all(pgs, **kwargs):
    """plan_cancellations without --osds, on the same tiny cluster as run_plan."""
    return cb.plan_cancellations(
        pgs,
        {19: EC_POOL_DETAIL, 7: REP_POOL_DETAIL},
        EC_PROFILES,
        set(),
        {},
        RULES,
        **kwargs,
    )


class PlanAllTest(unittest.TestCase):
    def test_every_remapped_pg_is_cancelled_whatever_its_osds(self):
        pgs = [
            pg_stat("19.9", [10, 2, 30, 4], [1, 2, 3, 4]),
            pg_stat("7.ff", [1, 2, 50], [1, 2, 5]),
            pg_stat("19.b", [1, 2, 3, 4], [1, 2, 3, 4], "active+clean"),
        ]
        cancellations, skipped = run_plan_all(pgs)
        self.assertEqual(pins(cancellations), ["7.ff 50 5", "19.9 10 1", "19.9 30 3"])
        self.assertEqual(skipped, [])

    def test_no_pin_is_a_companion(self):
        # with --osds 682, 9->4 would be a companion of shard 0 (see MainTest)
        cancellations, _ = run_plan_all([pg_stat("19.d", [OSD, 2, 3, 9], [8, 2, 3, 4])])
        self.assertEqual(pins(cancellations), ["19.d 682 8", "19.d 9 4"])
        self.assertEqual({c.companion_of for c in cancellations}, {None})

    def test_carries_state_size_and_progress(self):
        state = "active+remapped+backfilling"
        p = pg_stat("19.9", [OSD, 2, 3, 4], [8, 2, 3, 4], state, misplaced=25)
        (c,), _ = run_plan_all([p])
        self.assertEqual((c.state, c.size_bytes, c.progress_pct), (state, 1_000, 75.0))

    def test_excluded_pgs_are_left_alone(self):
        pgs = [pg_stat("19.9", [10, 2, 3, 4], [1, 2, 3, 4]), pg_stat("19.a", [10], [1])]
        cancellations, _ = run_plan_all(pgs, exclude_pgs={"19.9"})
        self.assertEqual(pins(cancellations), ["19.a 10 1"])

    def test_failure_domain_is_checked_for_every_remapped_pg(self):
        with self.assertRaises(SystemExit):
            cb.plan_cancellations(
                [pg_stat("19.9", [10, 2, 3, 4], [1, 2, 3, 4])],
                {19: EC_POOL_DETAIL},
                EC_PROFILES,
                set(),
                {},
                {},
            )


class MainAllTest(unittest.TestCase):
    """plan() and run() without --osds, on MainTest's canned cluster."""

    run_main = MainTest.run_main
    plan = MainTest.plan
    PGS = MainTest.PGS

    def test_plan_pins_every_moving_shard(self):
        result = self.plan()
        self.assertEqual(result.osds, [])
        self.assertEqual(
            pins(result.cancellations), ["19.9 682 8", "19.d 682 8", "19.d 9 4"]
        )
        self.assertEqual([(s.pgid, s.shard) for s in result.skipped], [("19.a", 0)])

    def test_pin_blockers_requires_osd(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as cm:
            self.plan("--pin-blockers")
        self.assertIn("--pin-blockers requires --osds", str(cm.exception))

    def test_summary_counts_shards_and_pgs_and_no_blocker_note(self):
        _, err = self.run_main()
        err = flat(err)
        self.assertIn("3 moving shard(s) in 2 PG(s) can be pinned back", err)
        self.assertIn("1 cannot be pinned", err)
        self.assertNotIn("--pin-blockers", err)
        self.assertNotIn("osd.682 (", err)  # no single OSD to report on

    def test_exclude_pgs_note_does_not_name_an_osd(self):
        _, err = self.run_main("--exclude-pgs", "19.9", "19.zzz")
        err = flat(err)
        self.assertIn("1 of 2 given PG id(s) matched a remapped PG and", err)
        self.assertIn("matched nothing (not remapped, or a typo): 19.zzz", err)

    def test_nothing_to_pin_says_so(self):
        out, err = self.run_main("--pgremapper-mappings", pgs=[])
        self.assertEqual(json.loads(out), [])
        self.assertIn("No backfills.", err)


class FixtureAllTest(unittest.TestCase):
    """cancel-backfill without --osds on the real-cluster snapshot."""

    @classmethod
    def setUpClass(cls):
        cls.result = plan_from_state(cb, FIXTURE)

    def test_every_remapped_pg_is_covered_and_only_chains_are_unpinnable(self):
        # 688 remapped PGs, see the fixture's README.txt
        covered = {c.pgid for c in self.result.cancellations}
        covered |= {s.pgid for s in self.result.skipped}
        self.assertEqual(len(covered), 688)
        self.assertGreater(len(self.result.skipped), 0)
        for s in self.result.skipped:
            self.assertIn("would chain", s.reason)
        self.assertEqual(
            set(self.result.chained), {s.pgid for s in self.result.skipped}
        )

    def test_contains_every_pin_the_osd_896_run_proposes(self):
        # --osds 896's pins, companions included, all cancel moving shards
        everything = set(pins(self.result.cancellations))
        self.assertLessEqual(set(EXPECTED_896_DEFAULT.splitlines()), everything)

    def test_every_pin_turns_up_into_acting(self):
        # after the pins, each PG's 'up' is its 'acting' (no degraded shards
        # here), except for shards left running because their pin would chain
        pgs = {
            p["pgid"]: p
            for p in shared.extract_pg_stats(
                json.loads((FIXTURE / "pg_dump_pgs.json").read_text()), "pg_dump_pgs"
            )
        }
        left_running = {(s.pgid, s.shard) for s in self.result.skipped}
        by_pg: dict[str, list] = {}
        for c in self.result.cancellations:
            by_pg.setdefault(c.pgid, []).append(c)
        for pgid, cs in by_pg.items():
            up = list(pgs[pgid]["up"])
            for c in cs:  # in apply order, as Ceph applies pg_upmap_items pairs
                up[up.index(c.up_osd)] = c.acting_osd
            with self.subTest(pgid=pgid):
                if cs[0].shard == "-":
                    self.assertEqual(sorted(up), sorted(pgs[pgid]["acting"]))
                    continue
                expected = [
                    osd if (pgid, i) in left_running else pgs[pgid]["acting"][i]
                    for i, osd in enumerate(pgs[pgid]["up"])
                ]
                self.assertEqual(up, expected)


if __name__ == "__main__":
    unittest.main()
