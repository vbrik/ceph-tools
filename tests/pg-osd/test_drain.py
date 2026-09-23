"""Unit tests for backfillctl's drain subcommand.

Most tests run plan() on a small synthetic cluster (see Cluster): six hosts
h0..h5 with two hdd OSDs each, numbered host*10 + j (0, 1, 10, 11, ... 51),
every OSD of 1,000,000 KiB so that utilizations and shard sizes are exact
percentages. Pool 1 is EC k=2 m=1 (size 3), pool 2 replicated size 3, both
with a host failure domain. backfillfull_ratio is 90%, so --max-target-util
defaults to 89%.

What drives the tests: an upmap that is invalid (two shards of a PG on one
host, an OSD twice, a drained OSD as target) or that re-wedges (a target
over the cap, a PG held in backfill_toofull by another shard) still reads as
a plausible row, so those invariants are checked both on hand-built corner
cases and, by invariant, on a cluster-sized capture.
"""

import contextlib
import io
import json
import subprocess
import sys
import unittest
from collections import Counter

from _support import REPO_ROOT, FakeStore, parse_args, shared

from backfillctl import drain as ut

KB = 1_000_000  # every synthetic OSD's capacity, in KiB
PCT = KB * shared.KIB // 100  # bytes in 1% of an OSD
FIXTURE = (
    REPO_ROOT / "tests" / "pg-osd" / "test-data" / "ceph1-backfills-stuck-at-100-pct"
)

EC_POOL, REP_POOL = 1, 2
HOST_RULE = {
    "rule_id": 0,
    "steps": [{"op": "take"}, {"op": "chooseleaf_indep", "type": "host"}],
}


class Cluster:
    """Builder for a synthetic cluster's snapshots (see module docstring)."""

    def __init__(self, default_util: float = 50.0):
        self.util = {h * 10 + j: default_util for h in range(6) for j in range(2)}
        self.pgs: list[dict] = []
        self.upmaps: list[dict] = []
        self.rule = HOST_RULE

    def pg(self, pgid, up, acting=None, *, shard_pct=1.0, state="active+clean"):
        """Add a PG whose shards are each shard_pct of an OSD."""
        is_ec = pgid.startswith(f"{EC_POOL}.")
        num_bytes = int(shard_pct * PCT) * (2 if is_ec else 1)
        acting = up if acting is None else acting
        if up != acting and "remapped" not in state:
            state += "+remapped"
        self.pgs.append(
            {
                "pgid": pgid,
                "state": state,
                "up": up,
                "acting": acting,
                "stat_sum": {"num_bytes": num_bytes},
            }
        )
        return self

    def snapshots(self) -> dict:
        hosts = [
            {
                "id": -1 - h,
                "type": "host",
                "name": f"h{h}",
                "children": [h * 10, h * 10 + 1],
            }
            for h in range(6)
        ]
        osds = [
            {
                "id": o,
                "type": "osd",
                "device_class": "hdd",
                "utilization": u,
                "kb": KB,
                "kb_used": int(u * KB / 100),
                "status": "up",
                "reweight": 1.0,
                "crush_weight": 1.0,
            }
            for o, u in self.util.items()
        ]
        return {
            "osd_tree": {"nodes": hosts + osds},
            "osd_df": {"nodes": osds},
            "osd_dump": {
                "nearfull_ratio": 0.85,
                "backfillfull_ratio": 0.90,
                "erasure_code_profiles": {"p": {"k": "2", "m": "1"}},
                "pg_upmap_items": self.upmaps,
            },
            "pool_ls_detail": [
                {
                    "pool_id": EC_POOL,
                    "pool_name": "ec",
                    "type": 3,
                    "crush_rule": 0,
                    "erasure_code_profile": "p",
                },
                {"pool_id": REP_POOL, "pool_name": "rep", "type": 1, "crush_rule": 0},
            ],
            "crush_rule_dump": [self.rule],
            "pg_dump_pgs": self.pgs,
        }

    def plan(self, *argv) -> ut.DrainResult:
        """Run plan() on this cluster; leading bare OSD ids go to --osds."""
        argv = [str(a) for a in argv]
        if argv and not argv[0].startswith("--"):
            argv.insert(0, "--osds")
        args = parse_args(ut, argv)
        return ut.plan(args, FakeStore(self.snapshots()))


def pairs(result: ut.DrainResult) -> list[tuple[str, object, int, int]]:
    return [(m.pgid, m.shard, m.up_osd, m.target_osd) for m in result.moves]


def host(osd: int) -> int:
    return osd // 10


class EvacueeSelectionTest(unittest.TestCase):
    def test_resident_ec_shard_goes_to_least_utilized_legal_osd(self):
        c = Cluster()
        c.util[31] = 10.0  # emptiest, on a host the PG does not use
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.moves[0].acting_osd, 0)

    def test_drained_osd_is_never_a_target_even_if_emptiest(self):
        c = Cluster()
        c.util[0] = 1.0
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_hosts_of_the_pgs_other_shards_are_excluded(self):
        c = Cluster()
        c.util[11] = c.util[21] = 1.0  # emptiest, but on the PG's other hosts
        c.util[41] = 10.0
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_same_host_as_the_evacuee_is_allowed(self):
        c = Cluster()
        c.util[1] = 1.0  # osd.0's host sibling
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])

    def test_shard_still_arriving_on_the_drained_osd_is_redirected(self):
        c = Cluster()
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20], [41, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertEqual(result.moves[0].acting_osd, 41)

    def test_shard_already_leaving_is_counted_not_moved(self):
        c = Cluster()
        result = c.pg("1.0", [41, 10, 20], [0, 10, 20]).plan(0)
        self.assertEqual(result.moves, [])
        self.assertEqual((result.evacuee_count, result.leaving_count), (0, 1))

    def test_replicated_replica_is_moved(self):
        c = Cluster()
        c.util[31] = 10.0
        result = c.pg("2.0", [10, 0, 20]).plan("osd.0")
        self.assertEqual(pairs(result), [("2.0", "-", 0, 31)])

    def test_two_drained_osds_of_one_pg_get_distinct_hosts(self):
        c = Cluster()
        c.util[31] = c.util[30] = 10.0  # same host: only one may be used
        result = c.pg("1.0", [0, 10, 20]).plan(0, 10)
        targets = [m.target_osd for m in result.moves]
        self.assertEqual(len(targets), 2)
        self.assertEqual(len({host(t) for t in targets} | {2}), 3)
        self.assertFalse({0, 10} & set(targets))

    def test_osd_in_the_raw_crush_mapping_is_not_a_target(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 20.0
        # CRUSH chose 31 for shard 2; an existing upmap sends it to 20.
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 31, "to": 20}]})
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])


class CapacityTest(unittest.TestCase):
    def test_target_projected_over_the_cap_is_skipped(self):
        c = Cluster(default_util=95.0)
        c.util[41] = 85.0  # a 5% shard would take it to 90%, over 89%
        result = c.pg("1.0", [0, 10, 20], shard_pct=5).plan(0)
        self.assertEqual(result.moves, [])
        self.assertEqual(len(result.unplaceable), 1)

    def test_cap_is_inclusive(self):
        c = Cluster(default_util=95.0)
        c.util[31] = 84.0
        result = c.pg("1.0", [0, 10, 20], shard_pct=5).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_no_room_leaves_the_evacuee_unplaceable(self):
        c = Cluster(default_util=95.0)
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(result.moves, [])
        self.assertEqual([e.pgid for e in result.unplaceable], ["1.0"])

    def test_explicit_max_target_util(self):
        c = Cluster()
        result = c.pg("1.0", [0, 10, 20]).plan(0, "--max-target-util", 40)
        self.assertEqual(result.moves, [])
        self.assertEqual(len(result.unplaceable), 1)

    def test_max_target_util_above_backfillfull_is_an_error(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().pg("1.0", [0, 10, 20]).plan(0, "--max-target-util", 91)
        self.assertIn("backfillfull_ratio", str(cm.exception))

    def test_arriving_shards_count_towards_the_target(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 12.0
        # 5% already on its way to osd.31 from another PG.
        c.pg("1.1", [31, 11, 21], [30, 11, 21], shard_pct=5)
        result = c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])

    def test_max_target_uses(self):
        c = Cluster()
        c.util[31] = 10.0
        for i in range(3):
            c.pg(f"1.{i}", [0, 10, 20])
        result = c.plan(0, "--max-target-uses", 2)
        uses = Counter(m.target_osd for m in result.moves)
        self.assertEqual(uses[31], 2)
        self.assertEqual(len(result.moves), 3)

    def test_unplaceable_are_in_pg_then_numeric_shard_order(self):
        # An 11-slot up set with drained OSDs in shards 2 and 10 ("10" sorts
        # before "2" as a string). Not a real layout for pool 1, but the
        # planner only needs the slots.
        none = shared.CRUSH_ITEM_NONE
        up = [none] * 11
        up[2], up[10] = 0, 10
        c = Cluster(default_util=95.0).pg("1.0", up)
        result = c.plan(0, 10)
        self.assertEqual([e.shard for e in result.unplaceable], [2, 10])

    def test_largest_shard_is_placed_first(self):
        c = Cluster(default_util=95.0)
        c.util[31] = 80.0  # room for one of the two
        c.pg("1.0", [0, 10, 20], shard_pct=1)
        c.pg("1.1", [0, 10, 20], shard_pct=5)
        result = c.plan(0, "--max-target-uses", 1)
        self.assertEqual(pairs(result), [("1.1", 0, 0, 31)])
        self.assertEqual([e.pgid for e in result.unplaceable], ["1.0"])


class BlockerTest(unittest.TestCase):
    """A sibling arriving on an OSD over the cap holds the PG in toofull."""

    def test_blocker_is_diverted(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.util[41] = 20.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31), ("1.0", 1, 10, 41)])
        self.assertEqual(result.moves[1].note, "diverted: unblocks osd.0")
        self.assertEqual(result.diverted_count, 1)
        self.assertEqual(result.stuck_pgs, [])

    def test_blocker_threshold_is_max_target_util_not_backfillfull(self):
        c = Cluster()
        c.util[10] = 89.5  # under backfillfull (90%), over the 89% cap
        c.util[31] = 10.0
        c.util[41] = 20.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20], shard_pct=0.1).plan(0)
        self.assertEqual(result.diverted_count, 1)
        # With a looser cap, the same sibling is no longer a blocker.
        result = c.plan(0, "--max-target-util", 90)
        self.assertEqual(result.diverted_count, 0)

    def test_sibling_under_the_cap_is_left_alone(self):
        c = Cluster()
        c.util[31] = 10.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_blocker_is_pinned_when_it_cannot_be_diverted(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0  # the only OSD with room: taken by the evacuee
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1), ("1.0", 1, 10, 11)])
        pin = result.moves[1]
        self.assertEqual(pin.note, "pinned: unblocks osd.0")
        self.assertIsNone(pin.projected)
        self.assertEqual(result.pinned_count, 1)

    def test_blocker_already_pinned_as_a_companion_is_not_revisited(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        # Shards 1 and 2 swap hosts h1/h2, both onto full OSDs. Pinning
        # shard 1 back to osd.21 clashes with shard 2 arriving on osd.20, so
        # shard 2 is pinned too, as its companion: the PG is then unblocked.
        result = c.pg("1.0", [0, 10, 20], [0, 21, 11]).plan(0)
        self.assertEqual(
            pairs(result),
            [("1.0", 0, 0, 1), ("1.0", 1, 10, 21), ("1.0", 2, 20, 11)],
        )
        self.assertEqual(result.moves[2].note, "companion of shard 1")
        self.assertEqual(result.moves[0].note, "")
        self.assertEqual((result.pinned_count, result.stuck_pgs), (1, []))

    def test_replicated_blocker_is_pinned(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        result = c.pg("2.0", [0, 10, 20], [0, 11, 20]).plan(0)
        self.assertEqual(pairs(result), [("2.0", "-", 0, 1), ("2.0", "-", 10, 11)])

    def test_unpinnable_blocker_keeps_the_evacuee_with_a_note(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        none = shared.CRUSH_ITEM_NONE
        result = c.pg("1.0", [0, 10, 20], [0, none, 20]).plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])
        self.assertIn("PG stays toofull: shard 1 -> osd.10", result.moves[0].note)
        self.assertIn("no acting OSD", result.moves[0].note)
        self.assertEqual(result.stuck_pgs, ["1.0"])

    def test_pin_back_onto_a_drained_osd_is_refused(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        # Shard 1 is leaving drained osd.20 for full osd.10.
        result = c.pg("1.0", [0, 10, 30], [0, 20, 30]).plan(0, 20)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 1)])
        self.assertIn("drained osd.20", result.moves[0].note)

    def test_toofull_pg_blocker_at_nearfull_is_diverted(self):
        c = Cluster()
        c.util[10] = 86.0  # under the cap, but at nearfull (85%)
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_toofull")
        result = c.plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31), ("1.0", 1, 10, 41)])
        self.assertEqual(result.moves[1].note, "diverted: unblocks osd.0")
        self.assertEqual(result.min_up_util, 85.0)

    def test_nearfull_sibling_of_a_pg_not_toofull_is_left_alone(self):
        c = Cluster()
        c.util[10] = 86.0
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_wait")
        result = c.plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])

    def test_min_up_util_overrides_nearfull(self):
        c = Cluster()
        c.util[10] = 86.0
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_toofull")
        result = c.plan(0, "--min-up-util", 87)
        self.assertEqual(result.diverted_count, 0)

    def test_unpinnable_nearfull_blocker_note_shows_now_and_projected(self):
        c = Cluster(default_util=95.0)
        c.util[1] = 10.0
        c.util[10] = 86.0
        none = shared.CRUSH_ITEM_NONE
        c.pg("1.0", [0, 10, 20], [0, none, 20], state="active+backfill_toofull")
        note = c.plan(0).moves[0].note
        self.assertIn("shard 1 -> osd.10 (now 86.0%, projected 87.0%;", note)

    def test_toofull_pg_with_no_identified_blocker_is_flagged(self):
        c = Cluster()
        c.util[10] = 84.0  # under nearfull and the cap: not a suspect
        c.util[31] = 10.0
        c.pg("1.0", [0, 10, 20], [0, 11, 20], state="active+backfill_toofull")
        result = c.plan(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 31)])
        self.assertIn("blocker unidentified", result.moves[0].note)
        self.assertEqual((result.unexplained_pgs, result.stuck_pgs), (["1.0"], []))

    def test_toofull_pg_whose_evacuee_was_arriving_is_not_flagged(self):
        c = Cluster()
        c.util[31] = 10.0
        # The evacuee itself was backfilling onto osd.0: redirecting it may
        # be exactly what unwedges the PG.
        c.pg("1.0", [0, 10, 20], [41, 10, 20], state="active+backfill_toofull")
        result = c.plan(0)
        self.assertEqual(result.moves[0].note, "")
        self.assertEqual(result.unexplained_pgs, [])

    def test_unknown_osd_is_an_error(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan(99)
        self.assertIn("osd.99", str(cm.exception))

    def test_non_host_failure_domain_is_an_error(self):
        c = Cluster()
        c.rule = {"rule_id": 0, "steps": [{"op": "chooseleaf_indep", "type": "rack"}]}
        with self.assertRaises(SystemExit) as cm:
            c.pg("1.0", [0, 10, 20]).plan(0)
        self.assertIn("rack", str(cm.exception))


class HostsTest(unittest.TestCase):
    def test_every_osd_of_the_host_is_drained(self):
        c = Cluster()
        c.util[31] = 10.0
        c.util[41] = 20.0
        c.pg("1.0", [0, 10, 20]).pg("1.1", [11, 21, 30])
        result = c.plan("--hosts", "h1")
        self.assertEqual(result.osds, [10, 11])
        self.assertEqual(result.hosts, ["h1"])
        self.assertEqual({m.up_osd for m in result.moves}, {10, 11})
        self.assertFalse({host(m.target_osd) for m in result.moves} & {1})

    def test_fully_qualified_name_matches_the_short_one(self):
        result = Cluster().pg("1.0", [0, 10, 20]).plan("--hosts", "h1.example.org")
        self.assertEqual((result.osds, result.hosts), ([10, 11], ["h1"]))

    def test_several_hosts(self):
        result = Cluster().pg("1.0", [0, 10, 20]).plan("--hosts", "h0", "h1")
        self.assertEqual(result.osds, [0, 1, 10, 11])

    def test_unknown_host_is_an_error_naming_it(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--hosts", "h1", "nosuch")
        self.assertIn("nosuch", str(cm.exception))
        self.assertNotIn("h1", str(cm.exception).split(":")[-1])

    def test_osds_and_hosts_are_mutually_exclusive_and_one_is_required(self):
        for argv in (["--osds", "0", "--hosts", "h1"], []):
            with (
                self.subTest(argv=argv),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as cm,
            ):
                parse_args(ut, argv)
            self.assertEqual(cm.exception.code, 2)


class OutputTest(unittest.TestCase):
    def result(self):
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        return c.pg("1.0", [0, 10, 20], [0, 11, 20]).plan(0)

    def test_row_matches_columns(self):
        result = self.result()
        for move in result.moves:
            row = ut.format_row(move, result.osd_host, result.osd_df)
            self.assertEqual(len(row), len(ut.COLUMNS))
        row = ut.format_row(result.moves[0], result.osd_host, result.osd_df)
        self.assertEqual(row[:2], ["1.0", "0"])
        self.assertEqual(row[5], "osd.0")  # UP OSD: the upmap's 'from'
        self.assertEqual(row[8], "osd.31")  # TARGET OSD: its 'to'

    def test_pgremapper_mappings_is_valid_json_of_from_to_pairs(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ut.print_pgremapper_mappings(self.result().moves)
        self.assertEqual(
            json.loads(out.getvalue()),
            [
                {"pgid": "1.0", "mapping": {"from": 0, "to": 31}},
                {"pgid": "1.0", "mapping": {"from": 10, "to": 1}},
            ],
        )

    def test_pgremapper_mappings_empty(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ut.print_pgremapper_mappings([])
        self.assertEqual(out.getvalue(), "[]\n")


class LiveFetchTest(unittest.TestCase):
    def test_ls_by_osd_results_are_merged_and_deduplicated(self):
        pg_a = {"pgid": "1.a", "up": [0], "acting": [0]}
        pg_b = {"pgid": "1.2", "up": [0, 10], "acting": [0, 10]}
        store = FakeStore(
            {
                ut.ls_by_osd_key(0): {"pg_stats": [pg_a, pg_b]},
                ut.ls_by_osd_key(10): {"pg_stats": [pg_b]},
            },
            load_dir=None,
        )
        pgs = ut.fetch_drained_pg_stats(store, {0, 10})
        self.assertEqual([pg["pgid"] for pg in pgs], ["1.2", "1.a"])

    def test_one_ls_by_osd_command_per_osd(self):
        cmds = ut.ls_by_osd_commands({3, 7})
        self.assertEqual(set(cmds), {ut.ls_by_osd_key(3), ut.ls_by_osd_key(7)})
        self.assertEqual(
            cmds[ut.ls_by_osd_key(7)][:4], ["ceph", "pg", "ls-by-osd", "osd.7"]
        )

    def test_plan_registers_the_drained_osds_commands_with_the_store(self):
        # With --hosts the OSDs are only known once 'osd tree' is read, so a
        # live run depends on plan() adding their commands to the store.
        c = Cluster().pg("1.0", [0, 10, 20])
        store = FakeStore(c.snapshots())
        ut.plan(parse_args(ut, ["--hosts", "h1"]), store)
        self.assertIn(ut.ls_by_osd_key(10), store.commands)
        self.assertIn(ut.ls_by_osd_key(11), store.commands)


class FixtureInvariants:
    """Drain part of the capture in FIXTURE_DIR and check every proposal is sane.

    A mixin: each subclass names a capture and the arguments (ARGV) that say
    what to drain.
    """

    FIXTURE_DIR = FIXTURE
    ARGV: tuple[str, ...] = ()

    @classmethod
    def setUpClass(cls):
        args = parse_args(ut, list(cls.ARGV), load_state=str(cls.FIXTURE_DIR))
        store = shared.SnapshotStore.from_args(args, dict(ut.SNAPSHOT_COMMANDS))
        cls.result = ut.plan(args, store)
        cls.OSDS = set(cls.result.osds)
        cls.pgs = {pg["pgid"]: pg for pg in shared.fetch_pg_stats(store, "pg_dump_pgs")}

    def by_pg(self) -> dict[str, list[ut.Move]]:
        by_pg: dict[str, list[ut.Move]] = {}
        for m in self.result.moves:
            by_pg.setdefault(m.pgid, []).append(m)
        return by_pg

    def test_every_evacuee_is_moved_or_unplaceable(self):
        r = self.result
        moved = sum(m.up_osd in self.OSDS for m in r.moves)
        self.assertGreater(moved, 0)
        self.assertEqual(moved + len(r.unplaceable), r.evacuee_count)

    def test_no_target_is_drained_or_over_the_cap(self):
        # Covers pins too: none may send data back onto a drained OSD.
        for m in self.result.moves:
            self.assertNotIn(m.target_osd, self.OSDS)
            if m.projected is not None:
                self.assertLessEqual(m.projected, self.result.max_target_util)

    def test_max_target_uses(self):
        uses = Counter(
            m.target_osd for m in self.result.moves if m.projected is not None
        )
        self.assertLessEqual(max(uses.values()), 5)

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
        for pgid, moves in self.by_pg().items():
            with self.subTest(pgid=pgid):
                froms = {m.up_osd for m in moves}
                self.assertFalse(froms & {m.target_osd for m in moves})


class Ceph1FixtureTest(FixtureInvariants, unittest.TestCase):
    """A calm cluster: nothing over the cap, so evacuees only."""

    ARGV = ("--osds", "418", "511")


class Ceph2EmergencyFixtureTest(FixtureInvariants, unittest.TestCase):
    """A cluster in a fullness emergency, where blockers abound."""

    FIXTURE_DIR = FIXTURE.parent / "divert-toofull-ceph2-util-emergency-2-new-hosts"
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
