"""Unit tests for backfillctl's cancel-uphill subcommand.

The risky part specific to this subcommand is find_uphill_shards: deciding
which moving shards go from a less-utilized OSD to a more-utilized one, and
refusing to guess when it cannot (unknown utilization, ambiguous replica
pairing). Everything downstream of that selection (companions, chains,
output) is exercised in test_cancel_backfill.py, since it is the same code
in shared.py; here it is only spot-checked through plan_cancellations and
render() to prove cancel_uphill.py wires it up correctly.
"""

import contextlib
import io
import json
import subprocess
import sys
import unittest

from _support import (
    EC_POOL_DETAIL,
    EC_PROFILES,
    NONE,
    REP_POOL_DETAIL,
    REPO_ROOT,
    RULES,
    TEST_DATA,
    parse_args,
    pg_stat,
    plan_from_state,
    shared,
)

from backfillctl import cancel_uphill as cu

FIXTURE = TEST_DATA / "ceph1-backfills-stuck-at-100-pct"
FIXTURE_RESUMED = TEST_DATA / "ceph1-resumed-backfills-exact-progress"


def util(pct: float) -> dict:
    return {"utilization": pct}


class EcUphillShardsTest(unittest.TestCase):
    def find(self, up, acting, osd_df, min_delta=1.0):
        return cu.find_uphill_shards(up, acting, True, osd_df, min_delta)

    def test_shard_moving_to_a_more_utilized_osd_is_a_candidate(self):
        candidates, skipped = self.find(
            [1, 2, 20, 4], [1, 2, 9, 4], {9: util(30.0), 20: util(80.0)}
        )
        self.assertEqual(candidates, [(2, 9, 20)])
        self.assertEqual(skipped, [])

    def test_shard_moving_to_a_less_utilized_osd_is_not_a_candidate(self):
        candidates, skipped = self.find(
            [1, 2, 20, 4], [1, 2, 9, 4], {9: util(80.0), 20: util(30.0)}
        )
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_equal_utilization_is_not_uphill(self):
        candidates, skipped = self.find(
            [1, 2, 20, 4], [1, 2, 9, 4], {9: util(50.0), 20: util(50.0)}
        )
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_delta_below_the_default_min_delta_is_not_uphill(self):
        candidates, skipped = self.find(
            [1, 2, 20, 4], [1, 2, 9, 4], {9: util(50.0), 20: util(50.5)}
        )
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_delta_exactly_at_min_delta_is_uphill(self):
        candidates, _skipped = self.find(
            [1, 2, 20, 4], [1, 2, 9, 4], {9: util(50.0), 20: util(51.0)}
        )
        self.assertEqual(candidates, [(2, 9, 20)])

    def test_custom_min_delta_raises_the_bar(self):
        osd_df = {9: util(50.0), 20: util(53.0)}
        candidates, _ = self.find([1, 2, 20, 4], [1, 2, 9, 4], osd_df, min_delta=5.0)
        self.assertEqual(candidates, [])
        candidates, _ = self.find([1, 2, 20, 4], [1, 2, 9, 4], osd_df, min_delta=3.0)
        self.assertEqual(candidates, [(2, 9, 20)])

    def test_unknown_utilization_is_skipped_not_guessed(self):
        candidates, skipped = self.find(
            [1, 2, 20, 4],
            [1, 2, 9, 4],
            {20: util(80.0)},  # 9 missing
        )
        self.assertEqual(candidates, [])
        self.assertEqual(len(skipped), 1)
        shard, reason = skipped[0]
        self.assertEqual(shard, 2)
        self.assertIn("utilization unknown", reason)

    def test_degraded_shard_with_no_acting_osd_is_skipped(self):
        candidates, skipped = self.find(
            [1, 2, 20, 4], [1, 2, NONE, 4], {20: util(80.0)}
        )
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [(2, "no acting OSD for this shard (degraded)")])

    def test_shard_not_moving_is_ignored(self):
        candidates, skipped = self.find([1, 2, 9, 4], [1, 2, 9, 4], {9: util(10.0)})
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_several_uphill_shards_in_one_pg_are_all_returned(self):
        candidates, skipped = self.find(
            [10, 20, 30, 4],
            [1, 2, 3, 4],
            {
                1: util(10),
                10: util(90),
                2: util(10),
                20: util(90),
                3: util(90),
                30: util(10),
            },
        )
        self.assertEqual(candidates, [(0, 1, 10), (1, 2, 20)])
        self.assertEqual(skipped, [])


class ReplicatedUphillShardsTest(unittest.TestCase):
    def find(self, up, acting, osd_df):
        return cu.find_uphill_shards(up, acting, False, osd_df)

    def test_replica_moving_to_a_more_utilized_osd_is_a_candidate(self):
        candidates, skipped = self.find(
            [1, 2, 30], [1, 2, 9], {9: util(20.0), 30: util(70.0)}
        )
        self.assertEqual(candidates, [("-", 9, 30)])
        self.assertEqual(skipped, [])

    def test_replica_moving_to_a_less_utilized_osd_is_not_a_candidate(self):
        candidates, skipped = self.find(
            [1, 2, 30], [1, 2, 9], {9: util(70.0), 30: util(20.0)}
        )
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_several_replicas_moving_at_once_is_ambiguous(self):
        candidates, skipped = self.find(
            [1, 30, 40],
            [1, 9, 8],
            {8: util(10), 9: util(10), 30: util(90), 40: util(90)},
        )
        self.assertEqual(candidates, [])
        self.assertEqual(
            skipped, [("-", "several replicas moving, pairing is ambiguous")]
        )

    def test_clean_pg_is_not_reported(self):
        candidates, skipped = self.find([1, 2, 3], [1, 2, 3], {})
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_replica_arriving_with_none_departing_is_a_missing_replica(self):
        # Undersized PG gaining a replica: nothing is leaving to compare
        # against, so this is not a move and not "ambiguous pairing" either.
        candidates, skipped = self.find([1, 2, 30], [1, 2], {30: util(90.0)})
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [("-", "no acting OSD to pin to (missing replica)")])

    def test_replica_departing_with_none_arriving_is_not_reported(self):
        # PG shrinking: a replica is leaving but nothing is backfilling in,
        # so there is no move to judge as uphill or otherwise.
        candidates, skipped = self.find([1, 2], [1, 2, 9], {9: util(90.0)})
        self.assertEqual(candidates, [])
        self.assertEqual(skipped, [])

    def test_unknown_utilization_is_skipped(self):
        candidates, skipped = self.find([1, 2, 30], [1, 2, 9], {30: util(70.0)})
        self.assertEqual(candidates, [])
        self.assertEqual(len(skipped), 1)
        self.assertIn("utilization unknown", skipped[0][1])


class PlanCancellationsTest(unittest.TestCase):
    """plan_cancellations wiring: selection -> pin -> Cancellation, per PG."""

    def plan(
        self, pg_stats, osd_df, pools=None, exclude_pgs=frozenset(), min_delta=1.0
    ):
        return cu.plan_cancellations(
            pg_stats,
            pools or {19: EC_POOL_DETAIL, 7: REP_POOL_DETAIL},
            EC_PROFILES,
            {},
            RULES,
            osd_df,
            exclude_pgs,
            min_delta,
        )

    def test_min_delta_is_passed_through(self):
        p = pg_stat("19.1", [1, 2, 20, 4, 5, 6], [1, 2, 9, 4, 5, 6])
        osd_df = {9: util(50.0), 20: util(53.0)}
        cancellations, skipped = self.plan(
            [p], osd_df, exclude_pgs=frozenset(), min_delta=5.0
        )
        self.assertEqual(cancellations, [])
        self.assertEqual(skipped, [])
        cancellations, _ = self.plan([p], osd_df, min_delta=3.0)
        self.assertEqual(len(cancellations), 1)

    def test_single_uphill_ec_shard_is_cancelled_with_no_companion_note(self):
        cancellations, skipped = self.plan(
            [pg_stat("19.1", [1, 2, 20, 4, 5, 6], [1, 2, 9, 4, 5, 6])],
            {9: util(10.0), 20: util(90.0)},
        )
        self.assertEqual(len(cancellations), 1)
        c = cancellations[0]
        self.assertEqual((c.shard, c.up_osd, c.acting_osd), (2, 20, 9))
        self.assertIsNone(c.companion_of)
        self.assertEqual(skipped, [])

    def test_no_uphill_shards_yields_nothing(self):
        cancellations, skipped = self.plan(
            [pg_stat("19.1", [1, 2, 20, 4, 5, 6], [1, 2, 9, 4, 5, 6])],
            {9: util(90.0), 20: util(10.0)},
        )
        self.assertEqual(cancellations, [])
        self.assertEqual(skipped, [])

    def test_replicated_uphill_shard_is_cancelled(self):
        cancellations, _skipped = self.plan(
            [pg_stat("7.1", [1, 2, 30], [1, 2, 9])],
            {9: util(10.0), 30: util(90.0)},
        )
        self.assertEqual(len(cancellations), 1)
        c = cancellations[0]
        self.assertEqual((c.shard, c.up_osd, c.acting_osd), ("-", 30, 9))

    def test_excluded_pg_is_left_alone(self):
        cancellations, skipped = self.plan(
            [pg_stat("19.1", [1, 2, 20, 4, 5, 6], [1, 2, 9, 4, 5, 6])],
            {9: util(10.0), 20: util(90.0)},
            exclude_pgs={"19.1"},
        )
        self.assertEqual(cancellations, [])
        self.assertEqual(skipped, [])

    def test_companion_pulled_in_by_an_uphill_shard_is_marked(self):
        # Same host layout as test_cancel_backfill.py's PlanCompanionsTest:
        # pinning shard 0 back to 627 (host H) clashes with shard 3, which is
        # still headed for 149 (also host H), so shard 3 must be pinned too.
        # Shard 3's own utilization is NOT uphill, so it must only appear as
        # a companion, never as a candidate in its own right.
        p = pg_stat("19.7e9", [682, 300, 626, 149], [627, 300, 626, 497])
        osd_host = {682: "x", 627: "H", 149: "H", 497: "z"}
        candidates, _ = cu.find_uphill_shards(
            p["up"],
            p["acting"],
            True,
            {627: util(10), 682: util(90), 497: util(90), 149: util(10)},
        )
        self.assertEqual(candidates, [(0, 627, 682)])
        cancellations, skipped = cu.plan_cancellations(
            [p],
            {19: EC_POOL_DETAIL},
            EC_PROFILES,
            osd_host,
            RULES,
            {627: util(10), 682: util(90), 497: util(90), 149: util(10)},
        )
        self.assertEqual(skipped, [])
        self.assertEqual(
            [(c.shard, c.up_osd, c.acting_osd, c.companion_of) for c in cancellations],
            [(0, 682, 627, None), (3, 149, 497, 0)],
        )

    def test_two_independent_uphill_shards_in_one_pg_both_appear(self):
        cancellations, _skipped = self.plan(
            [pg_stat("19.1", [10, 20, 30, 4, 5, 6], [1, 2, 3, 4, 5, 6])],
            {
                1: util(10),
                10: util(90),
                2: util(10),
                20: util(90),
                3: util(90),
                30: util(10),
            },
        )
        by_shard = {c.shard: c for c in cancellations}
        self.assertEqual(set(by_shard), {0, 1})
        self.assertIsNone(by_shard[0].companion_of)
        self.assertIsNone(by_shard[1].companion_of)

    def test_unresolvable_clash_skips_every_uphill_shard_of_the_pg(self):
        # Shard 0 is uphill and clashes unresolvably (same layout as
        # test_cancel_backfill.py's test_unresolvable_clash_is_skipped_
        # without_partial_output: shard 2 shares its host and is not
        # moving). Shard 3 is a second, independent uphill shard with no
        # clash of its own. Both are requested together (one close_pins
        # call per PG), so both must be skipped -- not just shard 0 -- since
        # nothing partial is proposed for a PG.
        p = pg_stat("19.1", [682, 300, 149, 20, 5, 6], [627, 300, 149, 10, 5, 6])
        osd_host = {682: "x", 627: "H", 149: "H", 10: "p", 20: "q"}
        osd_df = {627: util(10), 682: util(90), 10: util(5), 20: util(95)}
        candidates, _ = cu.find_uphill_shards(p["up"], p["acting"], True, osd_df)
        self.assertEqual(candidates, [(0, 627, 682), (3, 10, 20)])
        cancellations, skipped = cu.plan_cancellations(
            [p], {19: EC_POOL_DETAIL}, EC_PROFILES, osd_host, RULES, osd_df
        )
        self.assertEqual(cancellations, [])
        self.assertEqual({s.shard for s in skipped}, {0, 3})

    def test_companion_needed_by_two_uphill_shards_is_marked_with_both(self):
        # Shards 0 and 1 are both uphill and, once pinned back to 627/628
        # (both host H), each independently clashes with shard 2, which is
        # still headed for 200 (also host H) but is itself not uphill.
        p = pg_stat("19.1", [682, 800, 200, 4, 5, 6], [627, 628, 149, 4, 5, 6])
        osd_host = {682: "A", 627: "H", 800: "B", 628: "H", 200: "H", 149: "Z"}
        osd_df = {
            627: util(10),
            682: util(90),
            628: util(10),
            800: util(90),
            149: util(90),
            200: util(10),
        }
        candidates, _ = cu.find_uphill_shards(p["up"], p["acting"], True, osd_df)
        self.assertEqual(candidates, [(0, 627, 682), (1, 628, 800)])
        cancellations, skipped = cu.plan_cancellations(
            [p], {19: EC_POOL_DETAIL}, EC_PROFILES, osd_host, RULES, osd_df
        )
        self.assertEqual(skipped, [])
        by_shard = {c.shard: c for c in cancellations}
        self.assertEqual(set(by_shard), {0, 1, 2})
        self.assertIsNone(by_shard[0].companion_of)
        self.assertIsNone(by_shard[1].companion_of)
        self.assertEqual(by_shard[2].companion_of, "0, 1")
        self.assertEqual(shared.format_note(by_shard[2]), "companion of shard 0, 1")


class BuildParserTest(unittest.TestCase):
    def test_registers_cancel_uphill(self):
        args = parse_args(cu, [])
        self.assertEqual(args.command, "cancel-uphill")
        self.assertEqual(args.exclude_pgs, [])
        self.assertFalse(args.pgremapper_mappings)
        self.assertEqual(args.min_delta, 1.0)

    def test_min_delta_is_settable(self):
        args = parse_args(cu, ["--min-delta", "5"])
        self.assertEqual(args.min_delta, 5.0)

    def test_min_delta_takes_fractions_of_a_point_but_not_negatives(self):
        self.assertEqual(parse_args(cu, ["--min-delta", "0.5"]).min_delta, 0.5)
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
            parse_args(cu, ["--min-delta", "-1"])
        self.assertIn("must be from 0 to 100", err.getvalue())


class RenderTest(unittest.TestCase):
    def render(self, *argv, **fields):
        result = cu.UphillResult(
            **{
                "cancellations": [],
                "skipped": [],
                "chained": {},
                "exclude_filter": None,
                "osd_df": {},
                "osd_host": {},
            }
            | fields
        )
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            cu.render(result, parse_args(cu, list(argv)))
        return out.getvalue(), err.getvalue()

    def test_no_cancellations_reports_none_on_stderr(self):
        out, err = self.render()
        self.assertEqual(out, "")
        self.assertIn("No uphill backfills.", err)

    def test_summary_follows_the_table(self):
        # As in the other remapping commands: proposals first, then the outcome.
        c = shared.Cancellation("19.1", 2, 20, 9, 1_000, "s", 50.0)
        both = io.StringIO()
        with contextlib.redirect_stdout(both), contextlib.redirect_stderr(both):
            cu.render(cu.UphillResult([c], [], {}, None, {}, {}), parse_args(cu, []))
        text = both.getvalue()
        self.assertLess(text.index("PGID"), text.index("can be pinned back"))

    def test_pgremapper_mappings_with_nothing_prints_empty_array(self):
        out, _ = self.render("--pgremapper-mappings")
        self.assertEqual(out.strip(), "[]")

    def test_chains_are_warned_about_in_both_formats(self):
        for argv in ((), ("--pgremapper-mappings",)):
            _, err = self.render(
                *argv,
                skipped=[shared.Skipped("19.9", 0, "would chain")],
                chained={"19.9": [(20, 30), (682, 20)]},
            )
            self.assertIn("ceph osd pg-upmap-items 19.9 20 30 682 20", err)

    def test_one_cancellation_is_printed_as_a_table(self):
        c = shared.Cancellation("19.1", 2, 20, 9, 1_000, "s", 50.0)
        out, _err = self.render(cancellations=[c])
        self.assertIn("19.1", out)
        self.assertIn("PGID", out)


class FixtureReplayTest(unittest.TestCase):
    """Replay the real-cluster snapshot cancel-backfill's own tests use.

    Not a fixture check of exact rows (there is no cancel-uphill-specific
    fixture with a documented "expected proposals" list): these are
    invariants that must hold of any plan against real cluster data.
    """

    def setUp(self):
        self.result = plan_from_state(cu, FIXTURE)

    def test_directly_selected_shards_are_actually_uphill(self):
        for c in self.result.cancellations:
            if c.companion_of is not None:
                continue
            acting_util = self.result.osd_df[c.acting_osd]["utilization"]
            up_util = self.result.osd_df[c.up_osd]["utilization"]
            self.assertLess(acting_util, up_util, (c.pgid, c.shard))

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
        by_pg: dict[str, list[tuple[int, int]]] = {}
        for c in self.result.cancellations:
            by_pg.setdefault(c.pgid, []).append((c.up_osd, c.acting_osd))
        for pgid, pairs in by_pg.items():
            up = list(pgs[pgid]["up"])
            for from_osd, to_osd in pairs:
                slot = up.index(from_osd)
                self.assertEqual(pgs[pgid]["acting"][slot], to_osd, pgid)
                up[slot] = to_osd
            self.assertEqual(len(set(up)), len(up), (pgid, "OSD twice"))
            hosts = [host[o] for o in up]
            self.assertEqual(len(set(hosts)), len(hosts), (pgid, hosts))

    def test_chained_pgs_are_left_out_whole(self):
        # Confirms shared.avoid_chains is reached against real data. Without
        # partial, a PG whose pins chain gets none: its uphill shards go
        # together or not at all.
        self.assertGreater(len(self.result.chained), 0)
        pinned = {c.pgid for c in self.result.cancellations}
        self.assertFalse(pinned & set(self.result.chained))
        by_pg: dict[str, list] = {}
        for c in self.result.cancellations:
            by_pg.setdefault(c.pgid, []).append(c)
        dump = json.loads((FIXTURE / "osd_dump.json").read_text())
        upmaps = {
            e["pgid"]: [(m["from"], m["to"]) for m in e["mappings"]]
            for e in dump["pg_upmap_items"]
        }
        for pgid, cs in by_pg.items():
            self.assertEqual(shared.chain_heads(upmaps.get(pgid, []), cs), [], pgid)


class ExactProgressTest(unittest.TestCase):
    """Replay a capture with backfill positions (see its README.txt)."""

    def test_every_proposal_gets_progress_from_positions(self):
        cancellations = plan_from_state(cu, FIXTURE_RESUMED).cancellations
        self.assertGreater(len(cancellations), 0)
        self.assertTrue(all(c.progress_exact for c in cancellations))
        # 18.1d's counters read 100%; its position 98%.
        (pct,) = {c.progress_pct for c in cancellations if c.pgid == "18.1d"}
        self.assertAlmostEqual(98.2, pct, 1)

    def test_no_approx_note(self):
        err = io.StringIO()
        args = parse_args(cu, [], load_state=str(FIXTURE_RESUMED))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            cu.run(args)
        self.assertNotIn("~ marks PROGRESS", err.getvalue())

    def test_capture_without_positions_is_marked(self):
        err, out = io.StringIO(), io.StringIO()
        args = parse_args(cu, [], load_state=str(FIXTURE))
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            cu.run(args)
        self.assertIn("~", out.getvalue())
        self.assertIn("~ marks PROGRESS", err.getvalue())


class MainRegistrationTest(unittest.TestCase):
    """cancel-uphill through the real backfillctl entry point, not parse_args'
    own throwaway parser: proves it is actually wired into __main__.py."""

    def test_runs_end_to_end_through_the_dispatcher(self):
        result = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "backfillctl"),
                "--load-state",
                str(FIXTURE),
                "cancel-uphill",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PGID", result.stdout)


if __name__ == "__main__":
    unittest.main()
