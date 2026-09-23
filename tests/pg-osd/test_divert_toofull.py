"""Unit tests for backfillctl's divert-toofull subcommand.

Two kinds of bug drive what is tested here, both of which read as
plausible output rather than as an obvious failure.

The table's column layout: the columns are grouped under the PG set they
come from (ACTING/UP/TARGET) and ordered along the shard's path, so a row
that silently drifts out of that order still looks like a valid proposal.

The two safety thresholds: --min-up-util keeps shards that were never
blocked from being diverted (backfill_toofull is a property of the PG, not
of each shard arriving on it), and --max-target-util caps a target's
projected utilization (and may not exceed backfillfull_ratio, which Ceph
refuses to backfill past). Both default to the cluster's own ratios, so the
tests check the defaults are read from the capture, and the cluster-sized
fixture is checked by invariant -- no target projected above the cap, no
shard diverted off an OSD below nearfull_ratio.

Target reuse: an OSD may take several shards (--max-target-uses), each one
sized from its PG and projected onto the OSD until --max-target-util would
be exceeded. The unit tests use round numbers (an OSD of 1,000,000 KiB, so
that 25% is an exact byte count) so the projection arithmetic is exact and
the cap boundary can be pinned without float slack.
"""

import contextlib
import io
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from typing import ClassVar

from _support import REPO_ROOT, FakeStore, placement, plan_from_state, shared

from backfillctl import divert_toofull as ut

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


def cli(state_dir: str) -> list[str]:
    """Argv prefix that runs this subcommand as a subprocess on a saved state
    (directory-execution form, works from any cwd); subcommand args follow.
    """
    return [
        sys.executable,
        str(REPO_ROOT / "backfillctl"),
        "--load-state",
        state_dir,
        "divert-toofull",
    ]


TEST_DATA = os.path.join(TESTS_DIR, "test-data")


# The column labels in order, as print_table's label line splits: the OSD/UTIL/
# HOST triple of ACTING and UP, then TARGET's, which adds the projection.
LABELS = (
    ["PGID", "SHARD"] + ["OSD", "UTIL", "HOST"] * 2 + ["OSD", "UTIL", "PROJ", "HOST"]
)

# A real row: the shard's data is on osd.406, the stalled backfill is aimed
# at osd.882 (whose host is too full), and osd.898 is the proposal (62.9%
# once this shard has been added to it).
OSD_HOST = {406: "host32", 882: "host50", 898: "host51"}
OSD_DF = {
    406: {"id": 406, "utilization": 89.9, "device_class": "hdd"},
    882: {"id": 882, "utilization": 69.1, "device_class": "hdd"},
    898: {"id": 898, "utilization": 61.7, "device_class": "hdd"},
}


# Every OSD in the unit tests below has this capacity, chosen so that a
# percentage of it is a whole number of bytes: 1% is PCT bytes exactly.
KB = 1_000_000
PCT = KB * shared.KIB // 100


def make_proposal(acting_osd=406):
    shard = placement.ArrivingShard(
        pgid="19.2",
        shard=0,
        up_osd=882,
        acting_osd=acting_osd,
        up_set=[882, 111, 222],
    )
    return ut.Proposal(shard, 898, "host51", 61.7, 62.9)


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
                "62.9%",
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
        self.assertEqual(cells[("ACTING", "UTIL")], shared.NOT_APPLICABLE)
        self.assertEqual(cells[("ACTING", "HOST")], shared.NOT_APPLICABLE)
        # The up side is still fully known — that is the whole premise.
        self.assertEqual(cells[("UP", "OSD")], "osd.882")
        self.assertEqual(cells[("UP", "UTIL")], "69.1%")


def table_lines(rows):
    """Return the lines print_table writes for rows."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        shared.print_table(ut.COLUMNS, rows)
    return buf.getvalue().splitlines()


class PrintTableTest(unittest.TestCase):
    def test_header_is_two_lines_with_each_group_named_once(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        group_line, label_line, _ = table_lines([row])
        self.assertEqual(re.findall(r"[A-Z]+", group_line), ["ACTING", "UP", "TARGET"])
        self.assertEqual(label_line.split(), LABELS)

    def test_group_span_covers_exactly_its_own_columns(self):
        row = ut.format_row(make_proposal(), OSD_HOST, OSD_DF)
        group_line, label_line, data_line = table_lines([row])
        spans = list(re.finditer(r"-+ [A-Z]+ -+", group_line))
        osd_starts = [m.start() for m in re.finditer("OSD", label_line)]
        self.assertEqual([m.start() for m in spans], osd_starts)
        # A span ends a group separator before the next group's first column,
        # and the last one at the end of the data row.
        gap = len(shared.GROUP_SEP)
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


class FindArrivingShardsTest(unittest.TestCase):
    def test_ec_pairs_up_and_acting_by_position(self):
        pg = {"pgid": "19.2", "up": [882, 111], "acting": [406, 111]}
        (shard,) = placement.find_arriving_shards(pg, is_ec=True)
        self.assertEqual((shard.shard, shard.up_osd, shard.acting_osd), (0, 882, 406))

    def test_ec_empty_acting_slot_yields_unknown_acting_osd(self):
        pg = {"pgid": "19.2", "up": [882, 111], "acting": [shared.CRUSH_ITEM_NONE, 111]}
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
        kept, matched = ut.filter_toofull_pgs(
            self.pgs("19.1", "19.2", "19.3"), {"19.2"}
        )
        self.assertEqual([pg["pgid"] for pg in kept], ["19.2"])
        self.assertEqual(matched, {"19.2"})

    def test_input_order_is_preserved(self):
        kept, _ = ut.filter_toofull_pgs(
            self.pgs("19.3", "19.1", "19.2"), {"19.1", "19.3"}
        )
        self.assertEqual([pg["pgid"] for pg in kept], ["19.3", "19.1"])

    def test_wanted_id_that_matches_nothing_is_left_out_of_matched(self):
        kept, matched = ut.filter_toofull_pgs(self.pgs("19.1"), {"19.1", "19.zzz"})
        self.assertEqual([pg["pgid"] for pg in kept], ["19.1"])
        self.assertEqual(matched, {"19.1"})

    def test_empty_wanted_set_keeps_nothing(self):
        kept, matched = ut.filter_toofull_pgs(self.pgs("19.1", "19.2"), set())
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
        return placement.ArrivingShard("19.1", "-", up_osd, None, [up_osd])

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
        # ProjectedUsage cannot track it, so offering it would crash the
        # run in assign_targets.
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


class PrintTableEdgeCaseTest(unittest.TestCase):
    def test_empty_rows_prints_just_the_header(self):
        # render() guards this today, but the star-args width form used to
        # raise TypeError here rather than degrade gracefully.
        group_line, label_line = table_lines([])
        self.assertEqual(re.findall(r"[A-Z]+", group_line), ["ACTING", "UP", "TARGET"])
        self.assertEqual(label_line.split(), LABELS)


def proposals_from_readme(fixture):
    """Return the 'Expected proposals' block of a fixture README as tuples.

    Each is (pgid, shard, acting OSD, up OSD, target OSD), as strings, the
    acting OSD 'none' when unknown. Pulling the expected proposals out of the
    README, rather than duplicating them here, is what keeps the two from
    drifting apart.
    """
    path = os.path.join(TEST_DATA, fixture, "README.txt")
    with open(path) as f:
        lines = f.read().splitlines()
    # The heading wraps onto further lines before the indented block.
    start = next(i for i, ln in enumerate(lines) if ln.startswith("Expected proposals"))
    block = []
    for line in lines[start:]:
        if line.startswith("  "):
            block.append(tuple(line.split()))
        elif block:
            break
    return block


def proposal_tuple(p):
    """p in the form proposals_from_readme returns."""
    acting = "none" if p.shard.acting_osd is None else str(p.shard.acting_osd)
    return (
        p.shard.pgid,
        str(p.shard.shard),
        acting,
        str(p.shard.up_osd),
        str(p.target_osd),
    )


def fixture_plan(fixture, *argv):
    """Return plan()'s DivertResult for a fixture under test-data."""
    return plan_from_state(ut, os.path.join(TEST_DATA, fixture), *argv)


def remap_triples(proposals):
    """The (pgid, up OSD, target OSD) of each proposal, as '<pgid> <up> <target>'."""
    return [f"{p.shard.pgid} {p.shard.up_osd} {p.target_osd}" for p in proposals]


CEPH2_FIXTURE = "divert-toofull-ceph2-util-emergency-2-new-hosts"

# What the fixture's README.txt documents, and what the thresholds buy:
# 1513 arriving shards, 976 of them plausibly blocked. With the defaults each
# of the 900 up/in OSDs may take up to 5 shards while its projected
# utilization -- counting every shard already arriving on it, the stuck ones
# included until they are diverted -- stays at or below the default cap of
# backfillfull_ratio - 1, and shards are placed fullest acting OSD first,
# which places 52 of the 976. Asserted as counts and invariants
# rather than an exact 52-row table, which would be unreadable in a README.
CEPH2_ARRIVING = 1513
CEPH2_STUCK = 976
CEPH2_CANDIDATES = 900
CEPH2_MAX_USES = 5
CEPH2_PROPOSED = 52
CEPH2_UNPLACEABLE = 924
# --max-target-uses 1: one shard per OSD, but projected like the rest, so
# not one per candidate, as before the projection existed.
CEPH2_SINGLE_USE_PROPOSED = 26
CEPH2_SINGLE_USE_UNPLACEABLE = 950
CEPH2_NEARFULL = 85.0
CEPH2_BACKFILLFULL = 91.0
# Ceph refuses on a target's projected usage, so the default cap keeps one
# point of margin below backfillfull_ratio.
CEPH2_MAX_TARGET_UTIL = CEPH2_BACKFILLFULL - 1
# Proposals when the cap is instead set to backfillfull_ratio itself: targets
# may be projected right up to the ratio, with no margin.
CEPH2_NO_MARGIN_PROPOSED = 199

# With both thresholds at their loosest (--min-up-util 0, cap at
# backfillfull_ratio) the tool used to propose every usable hdd OSD (822),
# 342 of them already past backfillfull_ratio. The projection now keeps every
# target at or below the cap.
CEPH2_UNCAPPED_PROPOSED = 210


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


def proposal(pgid, up_osd, target_osd, shard=0):
    """A Proposal with just enough fields set for the pgremapper-mappings tests."""
    ds = placement.ArrivingShard(pgid, shard, up_osd, None, [up_osd])
    return ut.Proposal(ds, target_osd, "h", 50.0, 55.0)


class PgremapperMappingsOutputTest(unittest.TestCase):
    def proposals(self):
        return [
            proposal("19.14cd", 232, 337, shard=5),
            proposal("19.14cd", 896, 614, shard=8),
            proposal("7.1", 5, 6, shard="-"),
        ]

    def printed(self, proposals):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ut.print_pgremapper_mappings(proposals)
        return out.getvalue()

    def test_is_a_json_array_of_pgid_and_mapping_entries(self):
        self.assertEqual(
            json.loads(self.printed(self.proposals())),
            [
                {"pgid": "19.14cd", "mapping": {"from": 232, "to": 337}},
                {"pgid": "19.14cd", "mapping": {"from": 896, "to": 614}},
                {"pgid": "7.1", "mapping": {"from": 5, "to": 6}},
            ],
        )

    def test_empty_list_prints_a_valid_empty_json_array(self):
        self.assertEqual(self.printed([]), "[]\n")

    def test_one_entry_per_line_so_it_is_easy_to_read_and_prune(self):
        lines = self.printed(self.proposals()).splitlines()
        self.assertEqual(lines[0], "[")
        self.assertEqual(lines[-1], "]")
        self.assertEqual(len(lines), 2 + 3)
        self.assertTrue(lines[1].endswith(","))
        self.assertFalse(lines[-2].endswith(","))  # valid JSON: no trailing comma

    def test_a_single_entry_has_no_comma(self):
        self.assertEqual(
            json.loads(self.printed(self.proposals()[:1])),
            [{"pgid": "19.14cd", "mapping": {"from": 232, "to": 337}}],
        )


class FixturePlanTest(unittest.TestCase):
    """plan() on the small fixtures, checked against their READMEs."""

    def test_osd457_down_proposals_match_readme(self):
        fixture = "divert-toofull-osd457-down"
        result = fixture_plan(fixture)
        self.assertEqual(
            [proposal_tuple(p) for p in result.proposals],
            proposals_from_readme(fixture),
        )
        self.assertEqual((result.toofull_pg_count, result.arriving_count), (1, 1))

    def test_existing_upmap_chain_proposals_match_readme(self):
        fixture = "divert-toofull-osd263-existing-upmap-chain"
        result = fixture_plan(fixture)
        self.assertEqual(
            [proposal_tuple(p) for p in result.proposals],
            proposals_from_readme(fixture),
        )
        self.assertEqual(result.unplaceable, [])

    def test_no_backfill_toofull_pgs_proposes_nothing(self):
        result = fixture_plan("divert-toofull-nominal-synthetic")
        self.assertEqual((result.proposals, result.unplaceable), ([], []))

    def test_default_thresholds_come_from_the_clusters_own_ratios(self):
        # osd457-down has backfillfull_ratio 0.90, ceph2 has it raised to
        # 0.91: the caps must track the capture, not a constant. The target
        # cap is backfillfull_ratio minus one point.
        for fixture, nearfull, max_target in [
            ("divert-toofull-osd457-down", 85, 89),
            (CEPH2_FIXTURE, 85, 90),
        ]:
            with self.subTest(fixture=fixture):
                # Ceph keeps the ratios as float32, so 0.85 reads back as
                # 85.0000024%.
                result = fixture_plan(fixture)
                self.assertAlmostEqual(result.min_up_util, nearfull, places=3)
                self.assertAlmostEqual(result.max_target_util, max_target, places=3)


class FixtureReplayTest(unittest.TestCase):
    """End-to-end --load-state runs: how run() prints what plan() decides."""

    def run_proc(self, fixture, *extra):
        return subprocess.run(
            [*cli(os.path.join(TEST_DATA, fixture))] + list(extra),
            capture_output=True,
            text=True,
            check=True,
        )

    def run_script(self, fixture, *extra):
        return self.run_proc(fixture, *extra).stdout.rstrip("\n")

    def test_existing_upmap_row_is_unmarked_and_has_no_upmap_column(self):
        # The UP OSD is a plain 'osd.N' even when it is the 'to' of an
        # existing pair; pgremapper handles that case itself.
        out = self.run_script("divert-toofull-osd263-existing-upmap-chain")
        self.assertNotIn("*", out)
        self.assertNotIn("EXISTING_UPMAPS", out)

    def test_pgremapper_mappings_lists_existing_upmap_rows_like_any_other(self):
        # 19.bd5's existing pair is 625->263, so 263 is a 'to': the entry must
        # still map 'from' 263 to the target, for 'pgremapper import-mappings'
        # to rewrite that pair's 'to'.
        proc = self.run_proc(
            "divert-toofull-osd263-existing-upmap-chain", "--pgremapper-mappings"
        )
        entries = json.loads(proc.stdout)
        self.assertEqual(len(entries), 6)
        self.assertIn({"pgid": "19.bd5", "mapping": {"from": 263, "to": 842}}, entries)
        self.assertEqual(proc.stderr.count("NOTE"), 0)

    def test_pgremapper_mappings_emits_up_osd_not_acting_osd(self):
        # 'pgremapper import-mappings' takes the upmap's 'from', which is the
        # UP OSD. Emitting the ACTING OSD here would remap the wrong OSD, and
        # the table would still look right.
        fixture = "divert-toofull-osd457-down"
        out = self.run_script(fixture, "--pgremapper-mappings")
        self.assertEqual(
            json.loads(out), [{"pgid": "19.21f", "mapping": {"from": 625, "to": 849}}]
        )

    def test_no_backfill_toofull_pgs_prints_nothing_on_stdout(self):
        self.assertEqual(self.run_script("divert-toofull-nominal-synthetic"), "")

    def test_no_backfill_toofull_pgs_prints_an_empty_json_array(self):
        self.assertEqual(
            self.run_script(
                "divert-toofull-nominal-synthetic", "--pgremapper-mappings"
            ),
            "[]",
        )

    def test_default_thresholds_are_reported_on_stderr(self):
        err = self.run_proc("divert-toofull-osd457-down").stderr
        self.assertIn("--min-up-util 85%", err)
        self.assertIn("--max-target-util 89%", err)


class PgsFlagTest(unittest.TestCase):
    """--pgs restricts the run to shards of the named PG(s) only."""

    FIXTURE = "divert-toofull-osd263-existing-upmap-chain"

    def plan(self, *argv):
        return fixture_plan(self.FIXTURE, *argv)

    def test_only_the_named_pg_is_proposed(self):
        result = self.plan("--pgs", "19.bd5")
        self.assertEqual(remap_triples(result.proposals), ["19.bd5 263 842"])
        self.assertEqual(result.pgs_filter, shared.PgidFilter(1, 1, []))
        self.assertEqual(result.toofull_pg_count, 1)

    def test_several_named_pgs_are_all_kept(self):
        # Targets are not pinned here: with only two of the six PGs in play,
        # the greedy assignment (see assign_targets) can pick a different
        # target than the full run does, since room is contended
        # differently. Only which PGs got a proposal is guaranteed.
        result = self.plan("--pgs", "19.bd5", "19.7be")
        self.assertEqual({p.shard.pgid for p in result.proposals}, {"19.bd5", "19.7be"})
        self.assertEqual(result.toofull_pg_count, 2)

    def test_id_that_matches_nothing_is_reported_and_yields_no_proposals(self):
        result = self.plan("--pgs", "19.zzz")
        self.assertEqual(result.proposals, [])
        self.assertEqual(result.pgs_filter, shared.PgidFilter(1, 0, ["19.zzz"]))
        self.assertEqual(result.toofull_pg_count, 0)

    def test_a_mix_of_matching_and_unmatched_ids_reports_both(self):
        result = self.plan("--pgs", "19.bd5", "19.zzz")
        self.assertEqual(remap_triples(result.proposals), ["19.bd5 263 842"])
        self.assertEqual(result.pgs_filter, shared.PgidFilter(2, 1, ["19.zzz"]))

    def test_no_pgs_flag_considers_every_pg(self):
        result = self.plan()
        self.assertEqual(len(result.proposals), 6)
        self.assertIsNone(result.pgs_filter)


class PrintPgsFilterTest(unittest.TestCase):
    def capture(self, pgs_filter):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            ut.print_pgs_filter(pgs_filter)
        return err.getvalue()

    def test_all_matched(self):
        self.assertEqual(
            self.capture(shared.PgidFilter(1, 1, [])),
            "--pgs: 1 of 1 given PG id(s) are currently backfill_toofull "
            "and will be the only ones considered\n",
        )

    def test_unmatched_ids_are_named(self):
        self.assertIn(
            "--pgs: 1 of 3 given PG id(s) are currently backfill_toofull "
            "and will be the only ones considered; 2 matched nothing "
            "(check for typos): 19.yyy, 19.zzz",
            self.capture(shared.PgidFilter(3, 1, ["19.yyy", "19.zzz"])),
        )

    def test_printed_even_when_planning_then_exits(self):
        # A typo in --pgs can be what trips a later error, so the note naming
        # it must not wait for render(), which an exit never reaches.
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(TEST_DATA, "divert-toofull-osd457-down")
            dst = os.path.join(tmp, "fixture")
            shutil.copytree(src, dst)
            path = os.path.join(dst, "pool_ls_detail.json")
            with open(path) as f:
                pools = json.load(f)
            with open(path, "w") as f:
                json.dump([p for p in pools if p["pool_id"] != 19], f)
            err = io.StringIO()
            with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as ctx:
                plan_from_state(ut, dst, "--pgs", "19.21f", "19.zzz")
        self.assertIn("pool id(s) 19", str(ctx.exception))
        self.assertIn("1 matched nothing (check for typos): 19.zzz", err.getvalue())

    def test_printed_only_with_pgs(self):
        # The note goes to stderr, ahead of the summary, only under --pgs.
        fixture = os.path.join(TEST_DATA, "divert-toofull-osd263-existing-upmap-chain")
        for extra, shown in [((), False), (("--pgs", "19.zzz"), True)]:
            with self.subTest(extra=extra):
                proc = subprocess.run(
                    [*cli(fixture), *extra],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                self.assertEqual("--pgs:" in proc.stderr, shown)


def osd_df_of(utils):
    """Build a minimal 'ceph osd df' map from {osd: utilization}."""
    return {
        osd: {
            "id": osd,
            "utilization": util,
            "device_class": "hdd",
            "kb": KB,
            "kb_used": (util or 0) / 100 * KB,
        }
        for osd, util in utils.items()
    }


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
        return ut.SourcePressure(osd_df_of(utils))

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

    @staticmethod
    def pg(num_bytes):
        return {"pgid": "1.0", "stat_sum": {"num_bytes": num_bytes}}

    def test_ec_shard_is_one_kth_of_the_pg(self):
        pool = {
            "pool_id": 1,
            "type": shared.POOL_TYPE_ERASURE,
            "erasure_code_profile": "k8m2",
        }
        self.assertEqual(
            placement.shard_size_bytes(self.pg(800), pool, self.PROFILES), 100
        )

    def test_ec_shard_size_rounds_up(self):
        pool = {
            "pool_id": 1,
            "type": shared.POOL_TYPE_ERASURE,
            "erasure_code_profile": "k8m2",
        }
        self.assertEqual(
            placement.shard_size_bytes(self.pg(17), pool, self.PROFILES), 3
        )

    def test_empty_ec_pg_has_empty_shards(self):
        pool = {
            "pool_id": 1,
            "type": shared.POOL_TYPE_ERASURE,
            "erasure_code_profile": "k8m2",
        }
        self.assertEqual(placement.shard_size_bytes(self.pg(0), pool, self.PROFILES), 0)

    def test_replica_is_the_whole_pg(self):
        pool = {"pool_id": 1, "type": 1, "erasure_code_profile": ""}
        self.assertEqual(
            placement.shard_size_bytes(self.pg(800), pool, self.PROFILES), 800
        )

    def test_unknown_profile_is_refused_rather_than_guessed(self):
        pool = {
            "pool_id": 1,
            "type": shared.POOL_TYPE_ERASURE,
            "erasure_code_profile": "gone",
        }
        with self.assertRaises(SystemExit) as cm:
            placement.shard_size_bytes(self.pg(800), pool, self.PROFILES)
        self.assertIn("'gone'", str(cm.exception))


class PositiveIntTest(unittest.TestCase):
    def test_accepts_one_and_up(self):
        self.assertEqual(placement.positive_int("1"), 1)
        self.assertEqual(placement.positive_int("12"), 12)

    def test_rejects_zero_negative_and_non_numbers(self):
        import argparse

        for text in ("0", "-3"):
            with self.subTest(text=text), self.assertRaises(argparse.ArgumentTypeError):
                placement.positive_int(text)
        with self.assertRaises(ValueError):
            placement.positive_int("many")


class AssignTargetsTest(unittest.TestCase):
    """assign_targets: legality rules for a shard's target OSD."""

    HOSTS: ClassVar[dict[int, str]] = {1: "h1", 2: "h2", 3: "h3", 4: "h4", 5: "h2"}

    def assign(
        self,
        utils,
        candidates,
        shards=None,
        *,
        arriving=None,
        max_uses=5,
        max_target_util=91.0,
    ):
        shards = shards or [stuck("1.0")]
        # By default the shards being placed are all there is arriving.
        arriving = shards if arriving is None else arriving
        df = osd_df_of(utils)
        return ut.assign_targets(
            shards,
            {"hdd": candidates},
            self.HOSTS,
            df,
            {},
            projection=placement.ProjectedUsage(df, list(arriving)),
            max_uses=max_uses,
            max_target_util=max_target_util,
        )

    def targets(self, proposals):
        return [p.target_osd for p in proposals]

    def test_target_fuller_than_up_osd_is_rejected(self):
        proposals, unplaceable = self.assign({1: 86.0, 2: 87.0}, [2])
        self.assertEqual(proposals, [])
        self.assertEqual(len(unplaceable), 1)

    def test_target_with_equal_utilization_is_rejected(self):
        proposals, unplaceable = self.assign({1: 86.0, 2: 86.0}, [2])
        self.assertEqual(proposals, [])
        self.assertEqual(len(unplaceable), 1)

    def test_fuller_candidate_is_skipped_for_a_later_emptier_one(self):
        # Candidates are normally sorted ascending, but the rule must not
        # depend on that: skip the offender, don't give up on the shard.
        proposals, _ = self.assign({1: 86.0, 2: 88.0, 3: 85.0}, [2, 3])
        self.assertEqual(self.targets(proposals), [3])

    def test_unknown_up_utilization_imposes_no_limit(self):
        proposals, _ = self.assign({1: None, 2: 89.0}, [2])
        self.assertEqual(self.targets(proposals), [2])

    def test_rejected_candidate_stays_available_for_a_fuller_up_osd(self):
        # A skipped candidate must not be used up: the next shard, arriving
        # on a fuller OSD, can still take it.
        shards = [stuck("1.0", up_osd=1), stuck("1.1", up_osd=3)]
        proposals, unplaceable = self.assign({1: 86.0, 2: 88.0, 3: 90.0}, [2], shards)
        self.assertEqual(
            [(p.shard.pgid, p.target_osd) for p in proposals], [("1.1", 2)]
        )
        self.assertEqual([s.pgid for s in unplaceable], ["1.0"])

    # -- reuse ---------------------------------------------------------------

    def test_target_is_reused_up_to_the_limit(self):
        shards = [stuck(f"1.{i}") for i in range(4)]
        proposals, unplaceable = self.assign(
            {1: 95.0, 2: 50.0}, [2], shards, max_uses=3
        )
        self.assertEqual(self.targets(proposals), [2, 2, 2])
        self.assertEqual([s.pgid for s in unplaceable], ["1.3"])

    def test_limit_of_one_gives_every_target_a_single_shard(self):
        shards = [stuck(f"1.{i}") for i in range(3)]
        proposals, unplaceable = self.assign(
            {1: 95.0, 2: 50.0, 3: 51.0}, [2, 3], shards, max_uses=1
        )
        self.assertEqual(self.targets(proposals), [2, 3])
        self.assertEqual(len(unplaceable), 1)

    def test_the_limit_is_per_osd_not_per_run(self):
        shards = [stuck(f"1.{i}") for i in range(4)]
        proposals, _ = self.assign(
            {1: 95.0, 2: 50.0, 3: 51.0}, [2, 3], shards, max_uses=2
        )
        self.assertEqual(sorted(self.targets(proposals)), [2, 2, 3, 3])

    def test_a_pg_still_cannot_use_one_host_twice(self):
        # Reuse is across PGs; within one PG the host exclusion still holds,
        # and osd.5 shares host h2 with osd.2.
        up_set = [1, 3]
        shards = [stuck("1.0", 1, up_set, shard=0), stuck("1.0", 3, up_set, shard=1)]
        proposals, unplaceable = self.assign(
            {1: 95.0, 2: 50.0, 3: 95.0, 5: 51.0}, [2, 5], shards
        )
        self.assertEqual(self.targets(proposals), [2])
        self.assertEqual(len(unplaceable), 1)

    # -- projection ----------------------------------------------------------

    def test_a_target_stops_being_used_when_the_next_shard_would_fill_it(self):
        # 40% -> 65% -> 90%, and a third 25% would make 115%.
        shards = [stuck(f"1.{i}", size_pct=25) for i in range(3)]
        proposals, unplaceable = self.assign(
            {1: 99.0, 2: 40.0}, [2], shards, max_target_util=95.0
        )
        self.assertEqual(self.targets(proposals), [2, 2])
        self.assertEqual([p.target_projected for p in proposals], [65.0, 90.0])
        self.assertEqual([s.pgid for s in unplaceable], ["1.2"])

    def test_the_proposal_reports_current_and_projected_utilization(self):
        (proposal,), _ = self.assign(
            {1: 99.0, 2: 40.0}, [2], [stuck("1.0", size_pct=25)]
        )
        self.assertEqual(proposal.target_utilization, 40.0)
        self.assertEqual(proposal.target_projected, 65.0)

    def test_projecting_exactly_to_the_cap_is_allowed_and_beyond_is_refused(self):
        # 50% + 25% is exactly 75%: "not exceed", so allowed at 75, refused
        # for a cap just below.
        shard = [stuck("1.0", size_pct=25)]
        allowed, _ = self.assign({1: 99.0, 2: 50.0}, [2], shard, max_target_util=75.0)
        self.assertEqual(self.targets(allowed), [2])
        refused, unplaceable = self.assign(
            {1: 99.0, 2: 50.0}, [2], shard, max_target_util=74.5
        )
        self.assertEqual((refused, len(unplaceable)), ([], 1))

    def test_a_target_already_above_the_cap_is_refused_even_for_an_empty_shard(self):
        # Projection can only go up, so this is what the old current-utilization
        # pre-filter used to catch.
        refused, unplaceable = self.assign(
            {1: 99.0, 2: 80.0}, [2], max_target_util=75.0
        )
        self.assertEqual((refused, len(unplaceable)), ([], 1))

    def test_a_shard_too_big_for_every_candidate_is_unplaceable_even_below_the_cap(
        self,
    ):
        # 89% is a fine target for a zero-size shard, not for a 5% one.
        proposals, unplaceable = self.assign(
            {1: 99.0, 2: 89.0}, [2], [stuck("1.0", size_pct=5)]
        )
        self.assertEqual((proposals, len(unplaceable)), ([], 1))

    def test_shards_of_different_sizes_are_each_projected_by_their_own_size(self):
        shards = [stuck("1.0", size_pct=30), stuck("1.1", size_pct=10)]
        proposals, _ = self.assign({1: 99.0, 2: 40.0}, [2], shards)
        self.assertEqual([p.target_projected for p in proposals], [70.0, 80.0])

    def test_the_least_projected_candidate_wins_and_load_spreads(self):
        # 2 and 3 alternate as each takes 10% and overtakes the other.
        shards = [stuck(f"1.{i}", size_pct=10) for i in range(4)]
        proposals, _ = self.assign({1: 99.0, 2: 50.0, 3: 55.0}, [2, 3], shards)
        self.assertEqual(self.targets(proposals), [2, 3, 2, 3])

    def test_a_bigger_emptier_osd_after_the_move_beats_a_smaller_one(self):
        # Equal current utilization, but the shard is a smaller fraction of
        # the OSD with more capacity, so that one ends up emptier.
        df = osd_df_of({1: 99.0, 2: 50.0, 3: 50.0})
        df[3]["kb"] *= 2
        df[3]["kb_used"] *= 2
        shards = [stuck("1.0", size_pct=20)]
        proposals, _ = ut.assign_targets(
            shards,
            {"hdd": [2, 3]},
            self.HOSTS,
            df,
            {},
            projection=placement.ProjectedUsage(df, shards),
            max_uses=5,
            max_target_util=91.0,
        )
        self.assertEqual(self.targets(proposals), [3])

    def test_ties_go_to_the_lower_osd_id(self):
        proposals, _ = self.assign({1: 99.0, 2: 50.0, 3: 50.0}, [3, 2])
        self.assertEqual(self.targets(proposals), [2])

    # -- order ---------------------------------------------------------------

    def test_the_fullest_acting_osd_is_served_first(self):
        # Room for one shard only, and the earlier shard's acting OSD is the
        # emptier one: the later shard takes it.
        shards = [
            stuck("1.0", acting=6, size_pct=10),
            stuck("1.1", acting=7, size_pct=10),
        ]
        proposals, unplaceable = self.assign(
            {1: 99.0, 2: 50.0, 6: 80.0, 7: 90.0}, [2], shards, max_uses=1
        )
        self.assertEqual([p.shard.pgid for p in proposals], ["1.1"])
        self.assertEqual([s.pgid for s in unplaceable], ["1.0"])

    def test_priority_rotates_as_a_placed_shard_relieves_its_acting_osd(self):
        # osd.6 (90%) holds two shards, osd.7 (85%) one, each 10%. Placing one
        # of osd.6's drops it to 80%, below osd.7, so the order is 6, 7, 6 and
        # not 6, 6, 7: each takes the next 10% on osd.2, so projections are
        # 20, 30, 40 in placement order.
        shards = [
            stuck("1.0", acting=6, size_pct=10),
            stuck("1.1", acting=6, size_pct=10),
            stuck("1.2", acting=7, size_pct=10),
        ]
        proposals, _ = self.assign({1: 99.0, 2: 10.0, 6: 90.0, 7: 85.0}, [2], shards)
        self.assertEqual(
            {p.shard.pgid: p.target_projected for p in proposals},
            {"1.0": 20.0, "1.2": 30.0, "1.1": 40.0},
        )

    def test_a_shard_with_no_known_acting_osd_goes_last(self):
        shards = [stuck("1.0", size_pct=10), stuck("1.1", acting=6, size_pct=10)]
        proposals, unplaceable = self.assign(
            {1: 99.0, 2: 50.0, 6: 60.0}, [2], shards, max_uses=1
        )
        self.assertEqual([p.shard.pgid for p in proposals], ["1.1"])
        self.assertEqual([s.pgid for s in unplaceable], ["1.0"])

    def test_ties_keep_the_order_given(self):
        # Same acting OSD utilization, and so equally pressing: first come.
        shards = [
            stuck("1.0", acting=6, size_pct=10),
            stuck("1.1", acting=7, size_pct=10),
        ]
        proposals, _ = self.assign(
            {1: 99.0, 2: 50.0, 6: 90.0, 7: 90.0}, [2], shards, max_uses=1
        )
        self.assertEqual([p.shard.pgid for p in proposals], ["1.0"])

    def test_results_come_back_in_the_order_given(self):
        shards = [
            stuck("1.0", acting=6, size_pct=10),
            stuck("1.1", acting=7, size_pct=10),
            stuck("1.2", acting=8, size_pct=10),
        ]
        proposals, _ = self.assign(
            {1: 99.0, 2: 50.0, 3: 51.0, 6: 80.0, 7: 90.0, 8: 85.0}, [2, 3], shards
        )
        self.assertEqual([p.shard.pgid for p in proposals], ["1.0", "1.1", "1.2"])

    def test_the_queue_per_acting_osd_ranks_like_picking_the_best_shard_overall(self):
        # assign_targets ranks queues of shards per acting OSD in a heap. That
        # must be the same as, each turn, taking the best of all shards: check
        # it against that naive definition on random input full of ties. All
        # shards fit on osd.2, which takes each one's size in turn, so the
        # projection it reports for a shard is its position in placement order.
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
                    key=lambda i: (
                        -used.get(shards[i].acting_osd, -math.inf),
                        i,
                    ),
                )
                pending.remove(i)
                expected.append(i)
                if shards[i].acting_osd is not None:
                    used[shards[i].acting_osd] -= shards[i].size_bytes / PCT

            proposals, unplaceable = self.assign(
                {1: 99.0, 2: 0.0, **acting_utils}, [2], shards, max_uses=n
            )
            self.assertEqual(unplaceable, [], trial)
            actual = sorted(range(n), key=lambda i: proposals[i].target_projected)
            self.assertEqual(actual, expected, trial)

    def test_placing_a_shard_does_not_free_room_on_its_acting_osd_as_a_target(self):
        # osd.2 is both the acting OSD of the first shard and a target for the
        # second. Its space frees only once the backfill finishes, so it is
        # not credited to the target-side projection: 50% + 10% = 60%.
        shards = [
            stuck("1.0", acting=2, size_pct=10),
            stuck("1.1", up_osd=3, up_set=[3], size_pct=10),
        ]
        proposals, _ = self.assign(
            {1: 99.0, 2: 50.0, 3: 99.0, 4: 40.0}, [2, 4], shards, max_uses=1
        )
        by_pg = {p.shard.pgid: p for p in proposals}
        self.assertEqual(by_pg["1.0"].target_osd, 4)
        self.assertEqual(by_pg["1.1"].target_osd, 2)
        self.assertEqual(by_pg["1.1"].target_projected, 60.0)

    def test_a_shard_left_alone_reduces_the_room_on_its_osd(self):
        left_alone = [stuck("9.9", up_osd=2, size_pct=20)]
        shard = [stuck("1.0", size_pct=10)]
        # 50% + 20% (already on its way) + 10% = 80%
        with_it, _ = self.assign(
            {1: 99.0, 2: 50.0},
            [2],
            shard,
            arriving=shard + left_alone,
            max_target_util=85.0,
        )
        self.assertEqual(with_it[0].target_projected, 80.0)
        # ... which leaves no room under a 75% ratio, where 60% alone would.
        refused, _ = self.assign(
            {1: 99.0, 2: 50.0},
            [2],
            shard,
            arriving=shard + left_alone,
            max_target_util=75.0,
        )
        self.assertEqual(refused, [])
        alone, _ = self.assign({1: 99.0, 2: 50.0}, [2], shard, max_target_util=75.0)
        self.assertEqual(self.targets(alone), [2])

    def test_an_osds_own_unplaceable_stuck_shard_still_counts_against_it(self):
        # osd.2 is a candidate for osd.1's shard but is itself the arriving
        # OSD of a stuck shard that nothing emptier can take (osd.3 is
        # fuller), so that shard still lands there: 50% + 20% + 10% = 80%, not
        # 60%. (osd.3 would be 78% + 10% = 88%, so osd.2 still wins.)
        own = stuck("9.9", up_osd=2, size_pct=20)
        shard = stuck("1.0", up_osd=1, size_pct=10)
        proposals, unplaceable = self.assign(
            {1: 99.0, 2: 50.0, 3: 78.0}, [2, 3], [own, shard]
        )
        self.assertEqual(
            [(p.shard.pgid, p.target_osd, p.target_projected) for p in proposals],
            [("1.0", 2, 80.0)],
        )
        self.assertEqual([s.pgid for s in unplaceable], ["9.9"])

    def test_an_osds_own_stuck_shard_stops_counting_once_it_is_diverted(self):
        # Same, but osd.4 is emptier than osd.2 so its own shard is diverted
        # there first: osd.2 is back to 50% and takes the next shard at 60%
        # (osd.4 would be 40% + 20% + 10% = 70%).
        own = stuck("9.9", up_osd=2, size_pct=20)
        shard = stuck("1.0", up_osd=1, size_pct=10)
        proposals, _ = self.assign(
            {1: 99.0, 2: 50.0, 3: 90.0, 4: 40.0}, [2, 4], [own, shard]
        )
        self.assertEqual(
            [(p.shard.pgid, p.target_osd, p.target_projected) for p in proposals],
            [("9.9", 4, 60.0), ("1.0", 2, 60.0)],
        )

    def test_the_order_of_shards_matters_to_what_looks_full(self):
        # The reverse order of the test above: osd.2's own shard has not been
        # diverted yet when osd.1's shard is placed, so osd.2 still carries it
        # and osd.4 (60% + 10%) wins instead. Conservative, and documented.
        own = stuck("9.9", up_osd=2, size_pct=20)
        shard = stuck("1.0", up_osd=1, size_pct=10)
        proposals, _ = self.assign(
            {1: 99.0, 2: 50.0, 3: 90.0, 4: 40.0}, [2, 4], [shard, own]
        )
        self.assertEqual(
            [(p.shard.pgid, p.target_osd, p.target_projected) for p in proposals],
            [("1.0", 4, 50.0), ("9.9", 4, 70.0)],
        )


def run_ceph2(*extra):
    """Run the script on the cluster-sized fixture; return the CompletedProcess."""
    return subprocess.run(
        [
            *cli(os.path.join(TEST_DATA, CEPH2_FIXTURE)),
            *extra,
        ],
        capture_output=True,
        text=True,
        check=True,
    )


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
        cls.proposals = cls.result.proposals

    def util(self, osd_id):
        return self.result.osd_df[osd_id]["utilization"]

    def test_proposal_and_unplaceable_counts_match_the_readme(self):
        self.assertEqual(len(self.proposals), CEPH2_PROPOSED)
        self.assertEqual(len(self.result.unplaceable), CEPH2_UNPLACEABLE)

    def test_shard_counts_match_the_readme(self):
        self.assertEqual(self.result.arriving_count, CEPH2_ARRIVING)
        self.assertEqual(self.result.stuck_count, CEPH2_STUCK)
        self.assertEqual(self.result.left_alone_count, CEPH2_ARRIVING - CEPH2_STUCK)

    def test_every_usable_osd_is_a_candidate(self):
        self.assertEqual(
            sum(map(len, self.result.candidates.values())), CEPH2_CANDIDATES
        )

    def test_no_target_is_projected_above_the_default_cap(self):
        # The headline invariant: every one of these remaps can actually
        # complete. Before --max-target-util defaulted, 342 could not.
        over = [p for p in self.proposals if p.target_projected > CEPH2_MAX_TARGET_UTIL]
        self.assertEqual(over, [])

    def test_default_cap_keeps_a_margin_below_backfillfull(self):
        # Capping at backfillfull_ratio itself lets targets be projected
        # into the last point below it. Pin that the margin is what keeps
        # them out, not merely that the cap is below the ratio.
        proposals = fixture_plan(
            CEPH2_FIXTURE, "--max-target-util", f"{CEPH2_BACKFILLFULL:g}"
        ).proposals
        self.assertEqual(len(proposals), CEPH2_NO_MARGIN_PROPOSED)
        in_margin = [p for p in proposals if CEPH2_MAX_TARGET_UTIL < p.target_projected]
        self.assertTrue(in_margin)

    def test_no_shard_is_diverted_off_a_healthy_osd(self):
        # The two new hosts sit around 70% and are absorbing shards, not
        # blocking them; diverting off them wasted targets.
        under = [
            p for p in self.proposals if self.util(p.shard.up_osd) < CEPH2_NEARFULL
        ]
        self.assertEqual(under, [])

    def test_the_new_empty_hosts_receive_shards_instead_of_losing_them(self):
        targets = {p.target_host for p in self.proposals}
        self.assertTrue({"host50", "host51"} <= targets)

    def test_every_target_is_strictly_emptier_than_the_osd_it_replaces(self):
        not_emptier = [
            p
            for p in self.proposals
            if self.util(p.target_osd) >= self.util(p.shard.up_osd)
        ]
        self.assertEqual(not_emptier, [])

    def test_no_target_is_used_more_than_the_default_limit(self):
        uses = Counter(p.target_osd for p in self.proposals)
        self.assertEqual(max(uses.values()), CEPH2_MAX_USES)

    def test_targets_are_reused(self):
        # 52 shards land on 27 of the 900 candidates: some are reused.
        targets = {p.target_osd for p in self.proposals}
        self.assertLess(len(targets), len(self.proposals))
        self.assertLessEqual(len(targets), CEPH2_CANDIDATES)

    def test_projection_stays_within_the_cap_and_grows_with_each_use(self):
        # Proposals are in PG order, not the order shards were placed in, so
        # an OSD's successive uses are compared as a set: each adds a shard,
        # so its projections are all different and all above its current
        # utilization.
        projections = {}
        for p in self.proposals:
            self.assertGreater(p.target_projected, p.target_utilization)
            self.assertLessEqual(p.target_projected, CEPH2_MAX_TARGET_UTIL)
            projections.setdefault(p.target_osd, []).append(p.target_projected)
        for osd, values in projections.items():
            with self.subTest(osd=osd):
                self.assertEqual(len(values), len(set(values)))

    def test_proposals_are_in_pg_order_whatever_order_shards_were_placed_in(self):
        keys = [(ut.pgid_sort_key(p.shard.pgid), p.shard.shard) for p in self.proposals]
        self.assertEqual(keys, sorted(keys))

    def test_only_shards_on_the_fullest_acting_osds_get_the_scarce_room(self):
        # 578 of the 976 stuck shards have an acting OSD at or above
        # backfillfull_ratio and there is room for only 52, so every placed
        # shard should come from one. (In PG order far fewer did.)
        below = [
            p
            for p in self.proposals
            if self.util(p.shard.acting_osd) < CEPH2_BACKFILLFULL
        ]
        self.assertEqual(below, [])

    def test_single_use_gives_every_osd_at_most_one_shard(self):
        result = fixture_plan(CEPH2_FIXTURE, "--max-target-uses", "1")
        self.assertEqual(len(result.proposals), CEPH2_SINGLE_USE_PROPOSED)
        self.assertEqual(len(result.unplaceable), CEPH2_SINGLE_USE_UNPLACEABLE)
        targets = {p.target_osd for p in result.proposals}
        self.assertEqual(len(targets), len(result.proposals))

    def test_a_higher_limit_places_at_least_as_many(self):
        # The projection, not the count, becomes the constraint: 10 barely
        # beats 5 because the OSDs run out of room first.
        counts = [
            len(fixture_plan(CEPH2_FIXTURE, "--max-target-uses", n).proposals)
            for n in ("1", "2", "5", "10")
        ]
        self.assertEqual(counts, sorted(counts))
        self.assertLess(counts[0], counts[-1])

    def test_no_target_is_on_a_host_already_in_its_pgs_up_set(self):
        osd_host = self.result.osd_host
        for p in self.proposals:
            up_hosts = {osd_host[o] for o in p.shard.up_set if o in osd_host}
            self.assertNotIn(p.target_host, up_hosts)

    def test_loosest_thresholds_still_never_target_past_backfillfull(self):
        # Opting out of the thresholds used to re-open targets that re-wedge on
        # arrival (93 of them), with a warning. The loosest cap allowed is
        # backfillfull_ratio itself, so nothing is doomed any more.
        proposals = fixture_plan(
            CEPH2_FIXTURE,
            "--min-up-util",
            "0",
            "--max-target-util",
            f"{CEPH2_BACKFILLFULL:g}",
        ).proposals
        self.assertEqual(len(proposals), CEPH2_UNCAPPED_PROPOSED)
        for p in proposals:
            self.assertLess(p.target_utilization, CEPH2_BACKFILLFULL)
            self.assertLessEqual(p.target_projected, CEPH2_BACKFILLFULL)

    def test_a_cap_equal_to_backfillfull_is_accepted(self):
        fixture_plan(CEPH2_FIXTURE, "--max-target-util", f"{CEPH2_BACKFILLFULL:g}")


class Ceph2FixtureOutputTest(unittest.TestCase):
    """The cluster-sized capture run as a subprocess: CLI checks and output."""

    @classmethod
    def setUpClass(cls):
        cls.proc = run_ceph2()
        cls.proposals = fixture_plan(CEPH2_FIXTURE).proposals

    def test_the_counts_are_reported_on_stderr(self):
        self.assertIn(
            f"proposed {CEPH2_PROPOSED} remap(s), {CEPH2_UNPLACEABLE} unplaceable",
            self.proc.stderr,
        )
        self.assertIn(
            f"{CEPH2_ARRIVING} arriving shard(s), of which {CEPH2_STUCK} on an OSD",
            self.proc.stderr,
        )
        skipped = CEPH2_ARRIVING - CEPH2_STUCK
        self.assertIn(f"({skipped} left alone as not the blocker)", self.proc.stderr)

    def test_the_table_has_a_row_per_proposal(self):
        group_and_label_lines = 2
        self.assertEqual(
            len(self.proc.stdout.splitlines()),
            group_and_label_lines + CEPH2_PROPOSED,
        )

    def test_non_positive_limit_is_refused(self):
        for value in ("0", "-1", "many"):
            with self.subTest(value=value):
                proc = subprocess.run(
                    [
                        *cli(os.path.join(TEST_DATA, CEPH2_FIXTURE)),
                        "--max-target-uses",
                        value,
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(proc.returncode, 2)
                self.assertIn("--max-target-uses", proc.stderr)

    def test_the_limit_and_ratio_are_reported_on_stderr(self):
        self.assertIn(f"--max-target-uses {CEPH2_MAX_USES}", self.proc.stderr)
        self.assertIn(
            f"--max-target-util {CEPH2_MAX_TARGET_UTIL:g}% (backfillfull_ratio 91%)",
            self.proc.stderr,
        )

    def test_a_cap_above_backfillfull_is_an_error(self):
        # 100 used to mean "no cap"; it must now fail rather than be honored.
        for value in ("91.1", "100"):
            with self.subTest(value=value):
                proc = subprocess.run(
                    [
                        *cli(os.path.join(TEST_DATA, CEPH2_FIXTURE)),
                        "--max-target-util",
                        value,
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(proc.stdout, "")
                self.assertIn("ERROR: max target utilization", proc.stderr)
                self.assertIn("backfillfull_ratio (91%)", proc.stderr)

    def test_a_non_positive_cap_is_an_error(self):
        for value in ("0", "-5"):
            with self.subTest(value=value):
                proc = subprocess.run(
                    [
                        *cli(os.path.join(TEST_DATA, CEPH2_FIXTURE)),
                        f"--max-target-util={value}",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("ERROR: max target utilization", proc.stderr)

    def test_pgremapper_mappings_mode_prints_the_planned_proposals(self):
        entries = json.loads(run_ceph2("--pgremapper-mappings").stdout)
        expected = [
            {
                "pgid": p.shard.pgid,
                "mapping": {"from": p.shard.up_osd, "to": p.target_osd},
            }
            for p in self.proposals
        ]
        self.assertEqual(entries, expected)

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

    def test_pgremapper_mappings_mode_reports_the_unplaceable_count_on_stderr(self):
        proc = run_ceph2("--pgremapper-mappings")
        self.assertIn(f"{CEPH2_UNPLACEABLE} shard(s) could not be placed", proc.stderr)
        self.assertNotRegex(proc.stderr, r"\d+\.\w+:[\d-]+, ")
        # Stdout stays parseable: only the JSON array.
        self.assertNotIn("could not be placed", proc.stdout)


class UnknownPoolTest(unittest.TestCase):
    """A stuck PG whose pool is missing from 'ceph osd pool ls detail'."""

    def test_unknown_pool_is_refused_rather_than_analyzed_wrongly(self):
        # Silently skipping it would bypass the failure-domain check and
        # diff the pool's EC shards as interchangeable replicas — both
        # failures produce plausible-looking rows.
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(TEST_DATA, "divert-toofull-osd457-down")
            dst = os.path.join(tmp, "fixture")
            shutil.copytree(src, dst)
            path = os.path.join(dst, "pool_ls_detail.json")
            with open(path) as f:
                pools = json.load(f)
            with open(path, "w") as f:
                json.dump([p for p in pools if p["pool_id"] != 19], f)
            proc = subprocess.run(
                [*cli(dst)],
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(proc.returncode, 1)
        self.assertIn("pool id(s) 19", proc.stderr)
        self.assertIn("does not list", proc.stderr)


if __name__ == "__main__":
    unittest.main()
