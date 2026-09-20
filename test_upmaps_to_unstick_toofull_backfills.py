"""Unit tests for upmaps-to-unstick-toofull-backfills.py.

The table's column layout is the main thing under test here: the columns
are grouped under the PG set they come from (ACTING/UP/TARGET) and
ordered along the shard's path, and a row that silently drifts out of that
order is exactly the kind of bug that reads as plausible output.
"""

import contextlib
import importlib.util
import io
import os
import re
import subprocess
import sys
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


if __name__ == "__main__":
    unittest.main()
