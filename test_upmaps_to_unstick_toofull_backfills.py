"""Unit tests for upmaps-to-unstick-toofull-backfills.py.

Two kinds of bug drive what is tested here, both of which read as
plausible output rather than as an obvious failure.

The table's column layout: the columns are grouped under the PG set they
come from (ACTING/UP/TARGET) and ordered along the shard's path, so a row
that silently drifts out of that order still looks like a valid proposal.

The two safety thresholds: --min-up-util keeps shards that were never
blocked from being diverted (backfill_toofull is a property of the PG, not
of each shard arriving on it), and --max-target-util keeps proposals off
OSDs Ceph already refuses to backfill onto. Both default to the cluster's
own ratios, so the tests check the defaults are read from the capture, and
the cluster-sized fixture is checked by invariant -- no target at or above
backfillfull_ratio, no shard diverted off an OSD below nearfull_ratio.
"""

import contextlib
import importlib.util
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "upmaps-to-unstick-toofull-backfills.py",
)
TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-data")

spec = importlib.util.spec_from_file_location("upmaps_toofull", SCRIPT)
ut = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ut)


# A real row: the shard's data is on osd.406, the stalled backfill is aimed
# at osd.882 (whose host is too full), and osd.898 is the proposal.
OSD_HOST = {406: "host32", 882: "host50", 898: "host51"}
OSD_DF = {
    406: {"id": 406, "utilization": 89.9, "device_class": "hdd"},
    882: {"id": 882, "utilization": 69.1, "device_class": "hdd"},
    898: {"id": 898, "utilization": 61.7, "device_class": "hdd"},
}


def make_proposal(acting_osd=406):
    shard = ut.DivertedShard(
        pgid="19.2",
        shard=0,
        up_osd=882,
        acting_osd=acting_osd,
        up_set=[882, 111, 222],
    )
    return ut.Proposal(shard, 898, "host51", 61.7)


class FormatRowTest(unittest.TestCase):
    def test_row_matches_column_order(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        self.assertEqual(
            row,
            [
                "19.2",
                "0",
                "osd.406",
                "89.9%",
                "host32",
                "osd.882",
                "69.1%",
                "host50",
                "osd.898",
                "61.7%",
                "host51",
            ],
        )

    def test_row_length_tracks_columns(self):
        # Guards against a column being added to COLUMNS (or to the row)
        # without the other side following.
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        self.assertEqual(len(row), len(ut.COLUMNS))

    def test_unknown_acting_osd_renders_as_none_and_dashes(self):
        # The usual out-OSD case: the slot the shard is coming from reads as
        # CRUSH_ITEM_NONE, so neither its utilization nor its host exists.
        row = ut.format_row(make_proposal(acting_osd=None), OSD_HOST, OSD_DF)
        cells = dict(zip(ut.COLUMNS, row))
        self.assertEqual(cells[("ACTING", "OSD")], "none")
        self.assertEqual(cells[("ACTING", "UTIL")], ut.NOT_APPLICABLE)
        self.assertEqual(cells[("ACTING", "HOST")], ut.NOT_APPLICABLE)
        # The up side is still fully known — that is the whole premise.
        self.assertEqual(cells[("UP", "OSD")], "osd.882")
        self.assertEqual(cells[("UP", "UTIL")], "69.1%")


def table_lines(rows):
    """Return the lines print_table writes for rows."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ut.print_table(rows)
    return buf.getvalue().splitlines()


class PrintTableTest(unittest.TestCase):
    def test_header_is_two_lines_with_each_group_named_once(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        group_line, label_line, _ = table_lines([row])
        self.assertEqual(re.findall(r"[A-Z]+", group_line), ["ACTING", "UP", "TARGET"])
        self.assertEqual(
            label_line.split(),
            ["PGID", "SHARD"] + ["OSD", "UTIL", "HOST"] * 3,
        )

    def test_group_span_covers_exactly_its_three_columns(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        group_line, label_line, data_line = table_lines([row])
        spans = list(re.finditer(r"-+ [A-Z]+ -+", group_line))
        osd_starts = [m.start() for m in re.finditer("OSD", label_line)]
        self.assertEqual([m.start() for m in spans], osd_starts)
        # A span ends a group separator before the next group's first column,
        # and the last one at the end of the data row.
        gap = len(ut.GROUP_SEP)
        self.assertEqual(
            [m.end() for m in spans[:2]], [start - gap for start in osd_starts[1:]]
        )
        self.assertEqual(spans[2].end(), len(data_line))
        # The name is centered: dashes on both sides.
        for m in spans:
            self.assertTrue(m.group().startswith("-") and m.group().endswith("-"))

    def test_ungrouped_columns_have_a_blank_group_line(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        group_line, label_line, _ = table_lines([row])
        pgid_and_shard = label_line.index("OSD")
        self.assertEqual(group_line[:pgid_and_shard], " " * pgid_and_shard)

    def test_groups_are_set_apart_by_a_wider_gap_than_columns_within_one(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        _, _, data_line = table_lines([row])
        # Data cells fill their columns exactly, so the gaps read off directly:
        # 2 spaces within a group, 4 between groups (and after SHARD).
        self.assertRegex(data_line, r"osd\.406 {2}89\.9% {2}host32")
        self.assertRegex(data_line, r"host32 {4}osd\.882")
        self.assertRegex(data_line, r"host50 {4}osd\.898")
        # SHARD's cell '0' is padded to the 5-char label, then the 4-space gap.
        self.assertRegex(data_line, r" 0 {8}osd\.406")

    def test_no_trailing_whitespace(self):
        # Narrow cells (unknown acting OSD) and a blank final group cell
        # must not leave padding on any line.
        row = ut.format_row(make_proposal(acting_osd=None), OSD_HOST, OSD_DF)
        for line in table_lines([row]):
            self.assertEqual(line, line.rstrip())

    def test_columns_widen_to_fit_the_widest_cell(self):
        rows = [
            ut.format_row(make_proposal(), OSD_HOST, OSD_DF),
            ["19.1ce0"] + ut.format_row(make_proposal(), OSD_HOST, OSD_DF)[1:],
        ]
        lines = table_lines(rows)
        # The widest PGID ('19.1ce0') pushes every line's SHARD column right.
        self.assertTrue(lines[1].startswith("PGID     SHARD"))
        self.assertEqual({line.index("osd.406") for line in lines[2:]}, {18})

    def test_column_and_row_widths_agree(self):
        # Every cell of the group line and label line must be a column of
        # the same table as the data rows.
        rows = [ut.format_row(make_proposal(), OSD_HOST, OSD_DF)]
        _, label_line, data_line = table_lines(rows)
        self.assertEqual(len(ut.COLUMNS), len(rows[0]))
        self.assertEqual(
            len(label_line.split()), len(data_line.split()), (label_line, data_line)
        )


class FindDivertedShardsTest(unittest.TestCase):
    def test_ec_pairs_up_and_acting_by_position(self):
        pg = {"pgid": "19.2", "up": [882, 111], "acting": [406, 111]}
        (shard,) = ut.find_diverted_shards(pg, is_ec=True)
        self.assertEqual((shard.shard, shard.up_osd, shard.acting_osd), (0, 882, 406))

    def test_ec_empty_acting_slot_yields_unknown_acting_osd(self):
        pg = {"pgid": "19.2", "up": [882, 111], "acting": [ut.CRUSH_ITEM_NONE, 111]}
        (shard,) = ut.find_diverted_shards(pg, is_ec=True)
        self.assertEqual(shard.up_osd, 882)
        self.assertIsNone(shard.acting_osd)

    def test_replicated_names_acting_osd_when_pairing_is_unambiguous(self):
        pg = {"pgid": "5.1", "up": [882, 111], "acting": [406, 111]}
        (shard,) = ut.find_diverted_shards(pg, is_ec=False)
        self.assertEqual((shard.shard, shard.up_osd, shard.acting_osd), ("-", 882, 406))

    def test_replicated_leaves_acting_osd_unknown_when_ambiguous(self):
        # Two replicas arriving and two leaving: no way to say which came
        # from which, so neither row claims an acting OSD.
        pg = {"pgid": "5.1", "up": [882, 883, 111], "acting": [406, 407, 111]}
        shards = ut.find_diverted_shards(pg, is_ec=False)
        self.assertEqual([s.up_osd for s in shards], [882, 883])
        self.assertEqual([s.acting_osd for s in shards], [None, None])

    def test_reordered_replicated_set_is_not_movement(self):
        pg = {"pgid": "5.1", "up": [111, 882], "acting": [882, 111]}
        self.assertEqual(ut.find_diverted_shards(pg, is_ec=False), [])


class FullRatiosTest(unittest.TestCase):
    """The thresholds both defaults derive from."""

    def _ratios(self, dump):
        ut._SNAPSHOT_CACHE.clear()
        ut._SNAPSHOT_CACHE["osd_dump"] = dump
        try:
            return ut.fetch_full_ratios()
        finally:
            ut._SNAPSHOT_CACHE.clear()

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
            (ut.DEFAULT_NEARFULL_RATIO * 100, ut.DEFAULT_BACKFILLFULL_RATIO * 100),
        )


# Arriving OSDs spanning the --min-up-util decision: well over the
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
        return ut.DivertedShard("19.1", "-", up_osd, None, [up_osd])

    def test_shard_on_a_full_osd_is_kept(self):
        stuck, skipped = ut.select_stuck_shards([self.shard(1)], SOURCE_DF, 85.0)
        self.assertEqual(([s.up_osd for s in stuck], skipped), ([1], []))

    def test_threshold_is_inclusive(self):
        # An OSD exactly at nearfull_ratio is still a plausible blocker.
        stuck, skipped = ut.select_stuck_shards([self.shard(2)], SOURCE_DF, 85.0)
        self.assertEqual(([s.up_osd for s in stuck], skipped), ([2], []))

    def test_shard_arriving_on_an_empty_osd_is_left_alone(self):
        # The case that wasted targets before: a healthy shard of a PG that
        # is in backfill_toofull because some *other* shard is wedged.
        stuck, skipped = ut.select_stuck_shards([self.shard(3)], SOURCE_DF, 85.0)
        self.assertEqual((stuck, [s.up_osd for s in skipped]), ([], [3]))

    def test_unknown_utilization_is_kept_not_dropped(self):
        # Cannot be ruled out as the blocker, so it must not vanish silently.
        stuck, skipped = ut.select_stuck_shards([self.shard(4)], SOURCE_DF, 85.0)
        self.assertEqual(([s.up_osd for s in stuck], skipped), ([4], []))

    def test_zero_threshold_keeps_everything(self):
        shards = [self.shard(o) for o in (1, 2, 3, 4)]
        stuck, skipped = ut.select_stuck_shards(shards, SOURCE_DF, 0)
        self.assertEqual((len(stuck), skipped), (4, []))

    def test_input_order_is_preserved_in_both_halves(self):
        shards = [self.shard(o) for o in (3, 1, 3, 2)]
        stuck, skipped = ut.select_stuck_shards(shards, SOURCE_DF, 85.0)
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
        }
        return {1: node | overrides}

    def test_usable_osd_is_offered_under_its_class(self):
        self.assertEqual(ut.build_candidate_osds(self.df()), {"hdd": [1]})

    def test_max_util_is_inclusive(self):
        self.assertEqual(ut.build_candidate_osds(self.df(), 50.0), {"hdd": [1]})
        self.assertEqual(ut.build_candidate_osds(self.df(), 49.9), {})

    def test_osd_without_a_utilization_figure_is_excluded(self):
        # It cannot be ranked, and treating a missing value as 0% would make
        # it sort ahead of every real candidate.
        df = self.df()
        del df[1]["utilization"]
        self.assertEqual(ut.build_candidate_osds(df), {})

    def test_down_and_out_osds_are_excluded(self):
        self.assertEqual(ut.build_candidate_osds(self.df(status="down")), {})
        self.assertEqual(ut.build_candidate_osds(self.df(reweight=0)), {})
        self.assertEqual(ut.build_candidate_osds(self.df(crush_weight=0)), {})

    def test_candidates_are_sorted_by_utilization_then_id(self):
        base = self.df()[1]
        df = {
            10: base | {"id": 10, "utilization": 60.0},
            11: base | {"id": 11, "utilization": 50.0},
            12: base | {"id": 12, "utilization": 50.0},
        }
        self.assertEqual(ut.build_candidate_osds(df), {"hdd": [11, 12, 10]})


class PrintTableEdgeCaseTest(unittest.TestCase):
    def test_empty_rows_prints_just_the_header(self):
        # main() guards this today, but the star-args width form used to
        # raise TypeError here rather than degrade gracefully.
        group_line, label_line = table_lines([])
        self.assertEqual(re.findall(r"[A-Z]+", group_line), ["ACTING", "UP", "TARGET"])
        self.assertEqual(
            label_line.split(), ["PGID", "SHARD"] + ["OSD", "UTIL", "HOST"] * 3
        )


def table_from_readme(fixture):
    """Return the table block quoted after 'Table output:' in a fixture README.

    Pulling the expected output out of the README, rather than duplicating
    it here, is what keeps the two from drifting apart.
    """
    path = os.path.join(TEST_DATA, fixture, "README.txt")
    with open(path) as f:
        lines = f.read().splitlines()
    # The marker ends a wrapped sentence rather than standing alone.
    start = next(i for i, ln in enumerate(lines) if ln.endswith("Table output:")) + 1
    block = []
    for line in lines[start:]:
        if line.startswith("  "):
            block.append(line[2:])
        elif block:
            break
    return "\n".join(block)


CEPH2_FIXTURE = "upmaps-toofull-ceph2-util-emergency-2-new-hosts"

# What the fixture's README.txt documents, and what the thresholds buy:
# 1513 arriving shards, 976 of them plausibly blocked, 242 placeable at or
# below the default cap of backfillfull_ratio - 1. Asserted as counts and
# invariants rather than an exact 242-row table, which would be unreadable
# in a README.
CEPH2_ARRIVING = 1513
CEPH2_STUCK = 976
CEPH2_PROPOSED = 242
CEPH2_UNPLACEABLE = 734
CEPH2_NEARFULL = 85.0
CEPH2_BACKFILLFULL = 91.0
# Ceph refuses on a target's projected usage, so the default cap keeps one
# point of margin below backfillfull_ratio.
CEPH2_MAX_TARGET_UTIL = CEPH2_BACKFILLFULL - 1
# Proposals when the cap is instead set to backfillfull_ratio itself.
CEPH2_NO_MARGIN_PROPOSED = 480

# What the tool did on this capture before the thresholds existed, kept so
# the regression is pinned rather than merely described: every usable hdd
# OSD consumed, 342 of them already past backfillfull_ratio.
CEPH2_UNCAPPED_PROPOSED = 822
CEPH2_UNCAPPED_DOOMED = 342


def parse_table(stdout):
    """Return the table's data rows as dicts keyed by ut.COLUMNS."""
    lines = stdout.splitlines()[2:]  # group line, label line, then rows
    rows = []
    for line in lines:
        cells = line.split()
        assert len(cells) == len(ut.COLUMNS), line
        rows.append(dict(zip(ut.COLUMNS, cells)))
    return rows


def percent(cell):
    return float(cell.rstrip("%"))


class PrintUnplaceableTest(unittest.TestCase):
    def capture(self, count):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            ut.print_unplaceable(count)
        return err.getvalue().splitlines()

    def test_reports_only_the_count_under_a_heuristic_caveat(self):
        lines = self.capture(3)
        self.assertEqual(len(lines), 1)
        self.assertIn("3 shard(s) could not be placed", lines[0])
        self.assertIn("limitation of the heuristic", lines[0])


class FixtureReplayTest(unittest.TestCase):
    """End-to-end --load-state runs, checked against the fixtures' READMEs."""

    def run_proc(self, fixture, *extra):
        return subprocess.run(
            [sys.executable, SCRIPT, "--load-state", os.path.join(TEST_DATA, fixture)]
            + list(extra),
            capture_output=True,
            text=True,
            check=True,
        )

    def run_script(self, fixture, *extra):
        return self.run_proc(fixture, *extra).stdout.rstrip("\n")

    def test_osd457_down_table_matches_readme(self):
        fixture = "upmaps-toofull-osd457-down"
        self.assertEqual(self.run_script(fixture), table_from_readme(fixture))

    def test_existing_upmap_chain_table_matches_readme(self):
        fixture = "upmaps-toofull-osd263-existing-upmap-chain"
        self.assertEqual(self.run_script(fixture), table_from_readme(fixture))

    def test_existing_upmap_row_is_unmarked_and_has_no_upmap_column(self):
        # The UP OSD is a plain 'osd.N' even when it is the 'to' of an
        # existing pair; pgremapper handles that case itself.
        out = self.run_script("upmaps-toofull-osd263-existing-upmap-chain")
        self.assertNotIn("*", out)
        self.assertNotIn("EXISTING_UPMAPS", out)

    def test_pgremapper_lists_existing_upmap_rows_like_any_other(self):
        # 19.bd5's existing pair is 625->263, so 263 is a 'to': the line must
        # still be '<pgid> 263 <target>' for 'pgremapper remap' to rewrite
        # that pair's 'to'.
        proc = self.run_proc(
            "upmaps-toofull-osd263-existing-upmap-chain", "--pgremapper"
        )
        lines = proc.stdout.splitlines()
        self.assertEqual(len(lines), 6)
        self.assertIn("19.bd5 263 829", lines)
        self.assertEqual(proc.stderr.count("NOTE"), 0)

    def test_pgremapper_emits_up_osd_not_acting_osd(self):
        # 'pgremapper remap' takes the upmap's 'from', which is the UP OSD.
        # Emitting the ACTING OSD here would remap the wrong OSD, and the table
        # would still look right.
        out = self.run_script("upmaps-toofull-osd457-down", "--pgremapper")
        self.assertEqual(out, "19.21f 625 849")

    def test_no_backfill_toofull_pgs_prints_nothing_on_stdout(self):
        self.assertEqual(self.run_script("upmaps-toofull-nominal-synthetic"), "")

    def test_default_thresholds_come_from_the_clusters_own_ratios(self):
        # osd457-down has backfillfull_ratio 0.90, ceph2 has it raised to
        # 0.91: the reported caps must track the capture, not a constant.
        # The target cap is backfillfull_ratio minus one point.
        for fixture, nearfull, max_target in [
            ("upmaps-toofull-osd457-down", "85", "89"),
            (CEPH2_FIXTURE, "85", "90"),
        ]:
            with self.subTest(fixture=fixture):
                err = self.run_proc(fixture).stderr
                self.assertIn(f"--min-up-util {nearfull}%", err)
                self.assertIn(f"--max-target-util {max_target}%", err)


class Ceph2FixtureInvariantTest(unittest.TestCase):
    """The cluster-sized capture, checked by invariant rather than by table.

    This is the fixture that exposed both threshold bugs: a general
    utilization emergency with two newly-added, still-empty hosts. Before
    the thresholds existed it proposed 822 remaps, 342 of them onto OSDs
    already past backfillfull_ratio, while diverting 537 shards that were
    arriving on perfectly healthy OSDs.
    """

    @classmethod
    def setUpClass(cls):
        cls.proc = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--load-state",
                os.path.join(TEST_DATA, CEPH2_FIXTURE),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        cls.rows = parse_table(cls.proc.stdout)

    def test_proposal_and_unplaceable_counts_match_the_readme(self):
        self.assertEqual(len(self.rows), CEPH2_PROPOSED)
        self.assertIn(
            f"proposed {CEPH2_PROPOSED} remap(s), {CEPH2_UNPLACEABLE} unplaceable",
            self.proc.stderr,
        )

    def test_shard_counts_match_the_readme(self):
        skipped = CEPH2_ARRIVING - CEPH2_STUCK
        self.assertIn(
            f"{CEPH2_ARRIVING} arriving shard(s), of which {CEPH2_STUCK} on an OSD",
            self.proc.stderr,
        )
        self.assertIn(f"({skipped} left alone as not the blocker)", self.proc.stderr)

    def test_no_target_is_above_the_default_cap(self):
        # The headline invariant: every one of these remaps can actually
        # complete. Before --max-target-util defaulted, 342 could not.
        over = [
            r
            for r in self.rows
            if percent(r[("TARGET", "UTIL")]) > CEPH2_MAX_TARGET_UTIL
        ]
        self.assertEqual(over, [])

    def test_default_cap_keeps_a_margin_below_backfillfull(self):
        # Capping at backfillfull_ratio itself would admit OSDs within a
        # point of it, which pass today's check but may lack room for the
        # shard (Ceph refuses on projected usage). Pin that the margin is
        # what excludes them, not merely that the cap is below the ratio.
        proc = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--load-state",
                os.path.join(TEST_DATA, CEPH2_FIXTURE),
                "--max-target-util",
                f"{CEPH2_BACKFILLFULL:g}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        rows = parse_table(proc.stdout)
        self.assertEqual(len(rows), CEPH2_NO_MARGIN_PROPOSED)
        in_margin = [
            r for r in rows if CEPH2_MAX_TARGET_UTIL < percent(r[("TARGET", "UTIL")])
        ]
        self.assertTrue(in_margin)

    def test_no_shard_is_diverted_off_a_healthy_osd(self):
        # The two new hosts sit around 70% and are absorbing shards, not
        # blocking them; diverting off them wasted targets.
        under = [r for r in self.rows if percent(r[("UP", "UTIL")]) < CEPH2_NEARFULL]
        self.assertEqual(under, [])

    def test_the_new_empty_hosts_receive_shards_instead_of_losing_them(self):
        targets = {r[("TARGET", "HOST")] for r in self.rows}
        self.assertTrue({"host50", "host51"} <= targets)

    def test_each_target_osd_is_used_at_most_once(self):
        targets = [r[("TARGET", "OSD")] for r in self.rows]
        self.assertEqual(len(targets), len(set(targets)))

    def test_no_row_targets_a_host_already_in_its_pgs_up_set(self):
        for row in self.rows:
            self.assertNotEqual(row[("TARGET", "HOST")], row[("UP", "HOST")])

    def test_disabling_both_thresholds_restores_the_old_unsafe_behavior(self):
        # Guards the regression path: this is what the tool used to do by
        # default, and it must now be both opt-in and loudly flagged.
        proc = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--load-state",
                os.path.join(TEST_DATA, CEPH2_FIXTURE),
                "--min-up-util",
                "0",
                "--max-target-util",
                "100",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        rows = parse_table(proc.stdout)
        self.assertEqual(len(rows), CEPH2_UNCAPPED_PROPOSED)
        doomed = [
            r for r in rows if percent(r[("TARGET", "UTIL")]) >= CEPH2_BACKFILLFULL
        ]
        self.assertEqual(len(doomed), CEPH2_UNCAPPED_DOOMED)
        self.assertIn(
            f"WARNING: {CEPH2_UNCAPPED_DOOMED} of {CEPH2_UNCAPPED_PROPOSED} "
            "proposed target(s) are at or above the cluster's "
            "backfillfull_ratio (91%)",
            proc.stderr,
        )

    def test_pgremapper_mode_agrees_with_the_table(self):
        proc = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--load-state",
                os.path.join(TEST_DATA, CEPH2_FIXTURE),
                "--pgremapper",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = proc.stdout.splitlines()
        self.assertEqual(len(lines), CEPH2_PROPOSED)
        expected = [
            f"{r[('', 'PGID')]} {r[('UP', 'OSD')].removeprefix('osd.')} "
            f"{r[('TARGET', 'OSD')].removeprefix('osd.')}"
            for r in self.rows
        ]
        self.assertEqual(lines, expected)

    def test_unplaceable_shards_are_only_counted(self):
        lines = self.proc.stderr.splitlines()
        matching = [ln for ln in lines if "could not be placed" in ln]
        self.assertEqual(len(matching), 1)
        self.assertIn(
            f"{CEPH2_UNPLACEABLE} shard(s) could not be placed. This is a "
            "limitation of the heuristic",
            matching[0],
        )
        # No per-shard '<pgid>:<shard>' list, however long the tail is.
        self.assertNotRegex(self.proc.stderr, r"\d+\.\w+:[\d-]+, ")

    def test_pgremapper_mode_reports_the_unplaceable_count_on_stderr(self):
        proc = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--load-state",
                os.path.join(TEST_DATA, CEPH2_FIXTURE),
                "--pgremapper",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn(f"{CEPH2_UNPLACEABLE} shard(s) could not be placed", proc.stderr)
        self.assertNotRegex(proc.stderr, r"\d+\.\w+:[\d-]+, ")
        # Stdout stays parseable: only remap triples.
        self.assertNotIn("could not be placed", proc.stdout)


class UnknownPoolTest(unittest.TestCase):
    """A stuck PG whose pool is missing from 'ceph osd pool ls detail'."""

    def test_unknown_pool_is_refused_rather_than_analyzed_wrongly(self):
        # Silently skipping it would bypass the failure-domain check and
        # diff the pool's EC shards as interchangeable replicas — both
        # failures produce plausible-looking rows.
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(TEST_DATA, "upmaps-toofull-osd457-down")
            dst = os.path.join(tmp, "fixture")
            shutil.copytree(src, dst)
            path = os.path.join(dst, "pool_ls_detail.json")
            with open(path) as f:
                pools = json.load(f)
            with open(path, "w") as f:
                json.dump([p for p in pools if p["pool_id"] != 19], f)
            proc = subprocess.run(
                [sys.executable, SCRIPT, "--load-state", dst],
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(proc.returncode, 1)
        self.assertIn("pool id(s) 19", proc.stderr)
        self.assertIn("does not list", proc.stderr)


if __name__ == "__main__":
    unittest.main()
