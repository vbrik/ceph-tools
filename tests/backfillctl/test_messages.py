"""Unit tests for messages.py, the text more than one backfillctl command prints.

Each builder is pinned to its exact words, since keeping the commands from
drifting apart is the module's point. Also checked: that messages.py stays a
leaf module (shared.py imports it, so importing back would be a cycle).
"""

import ast
import contextlib
import io
import unittest

from _support import REPO_ROOT, flat, messages, shared


def stderr_of(fn, *args, **kwargs) -> str:
    """Call fn and return what it printed on stderr, whitespace collapsed."""
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        fn(*args, **kwargs)
    return flat(err.getvalue())


class LeafModuleTest(unittest.TestCase):
    def test_imports_nothing_from_backfillctl_at_runtime(self):
        tree = ast.parse((REPO_ROOT / "backfillctl" / "messages.py").read_text())
        own = {p.stem for p in (REPO_ROOT / "backfillctl").glob("*.py")}
        # Top level only: imports under 'if TYPE_CHECKING:' never run.
        imported = set()
        for node in tree.body:
            if isinstance(node, ast.Import):
                imported |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & own, set())


class FormatTest(unittest.TestCase):
    def test_format_bytes(self):
        for num, text in (
            (None, "?"),
            (0, "0 B"),
            (1023, "1023 B"),
            (1024, "1.0 KiB"),
            (1536 * 1024**2, "1.5 GiB"),
            (5 * 1024**5, "5120.0 TiB"),  # no unit above TiB
        ):
            with self.subTest(num=num):
                self.assertEqual(messages.format_bytes(num), text)

    def test_osd_list_sorts_by_id(self):
        self.assertEqual(messages.osd_list([682, 74, 9]), "osd.9, osd.74, osd.682")
        self.assertEqual(messages.osd_list([]), "")


class NoteCellTest(unittest.TestCase):
    def test_blocking_reason(self):
        self.assertEqual(
            messages.blocking_reason(31, 91.04),
            "osd.31 projected at 91.0%, at or over backfillfull_ratio",
        )

    def test_companion_note(self):
        self.assertEqual(messages.companion_note(3), "companion of shard 3")
        self.assertEqual(
            messages.companion_note("-", of_blocker=True),
            "companion of blocker shard -",
        )


class ProgressNoteTest(unittest.TestCase):
    def test_printed_only_for_an_estimate(self):
        for progress, printed in (
            ([], False),
            ([(50.0, True), (100.0, True)], False),
            ([(None, False)], False),  # no figure, so no '~' to explain
            ([(50.0, True), (10.0, False)], True),
        ):
            with self.subTest(progress=progress):
                text = stderr_of(messages.print_progress_note, iter(progress))
                self.assertEqual(text.startswith("NOTE: ~ marks PROGRESS"), printed)
                self.assertEqual(text == "", not printed)


class PgidFilterNoteTest(unittest.TestCase):
    def capture(self, f):
        return stderr_of(
            messages.print_pgid_filter, "--pgs", f, "have movement", "not moving"
        )

    def test_all_matched(self):
        self.assertEqual(
            self.capture(shared.PgidFilter(1, 1, [])),
            "NOTE: --pgs: 1 of 1 given PG id(s) have movement.",
        )

    def test_unmatched_ids_are_named_with_what_else_they_may_be(self):
        self.assertEqual(
            self.capture(shared.PgidFilter(3, 1, ["19.yyy", "19.zzz"])),
            "NOTE: --pgs: 1 of 3 given PG id(s) have movement; 2 matched nothing "
            "(not moving, or a typo): 19.yyy, 19.zzz.",
        )


class WarnChainsTest(unittest.TestCase):
    def test_warning_gives_whole_entries_in_apply_order(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            messages.warn_chains({"19.9": [(7, 8), (20, 30), (682, 20)]})
        text = err.getvalue()
        # Unwrapped, for copy-pasting.
        self.assertIn("\n  ceph osd pg-upmap-items 19.9 7 8 20 30 682 20\n", text)
        self.assertIn("the pins of 1 PG(s) chain", flat(text))
        self.assertIn("pgremapper cannot apply", flat(text))
        self.assertIn("removes part of such a chain as stale", flat(text))


def totals(**kwargs) -> shared.PinTotals:
    """PinTotals for 2 requested shards of 3 GiB in all, overridden by kwargs."""
    return shared.PinTotals(
        requested=2, pgs=1, others=0, blockers=0, size_bytes=3 * 1024**3, unknown_size=0
    )._replace(**kwargs)


class PinSummaryTest(unittest.TestCase):
    def summary(self, t, skipped=(), **kwargs):
        return stderr_of(
            messages.print_pin_summary, "2 moving shard(s)", t, list(skipped), **kwargs
        )

    def test_plain(self):
        self.assertEqual(
            self.summary(totals()),
            "2 moving shard(s) can be pinned back, ~3.0 GiB of data; "
            "0 cannot be pinned.",
        )

    def test_unknown_size_share_and_unjudged(self):
        self.assertEqual(
            self.summary(
                totals(unknown_size=1),
                share=", 0.5% of its capacity",
                unjudged=True,
            ),
            "2 moving shard(s) can be pinned back, ~3.0 GiB of data (+1 of unknown "
            "size), 0.5% of its capacity; 0 cannot be judged or pinned.",
        )

    def test_companions_and_blockers(self):
        self.assertTrue(
            self.summary(totals(others=3)).endswith(
                " 3 more shard(s), moving to other OSDs, are pinned too (companions)."
            )
        )
        self.assertTrue(
            self.summary(totals(others=3, blockers=1)).endswith(
                "are pinned too (companions; blockers: 1)."
            )
        )

    def test_skipped_are_counted_then_listed(self):
        skipped = [shared.Skipped("7.2", "-", "why"), shared.Skipped("19.1", 3, "x")]
        self.assertEqual(
            self.summary(totals(), skipped),
            "2 moving shard(s) can be pinned back, ~3.0 GiB of data; "
            "2 cannot be pinned. cannot pin 7.2 shard -: why "
            "cannot pin 19.1 shard 3: x",
        )


class PinFooterTest(unittest.TestCase):
    def test_only_the_cost_of_cancelling_by_default(self):
        self.assertEqual(
            stderr_of(messages.print_pin_footer, {}), flat(messages.CANCEL_NOTE)
        )

    def test_chains_then_extra_notes_then_the_cost(self):
        text = stderr_of(
            messages.print_pin_footer, {"19.9": [(7, 8)]}, ["NOTE: extra."]
        )
        self.assertLess(text.index("WARNING: the pins"), text.index("NOTE: extra."))
        self.assertTrue(text.endswith("NOTE: extra. " + flat(messages.CANCEL_NOTE)))


class PlacementTextTest(unittest.TestCase):
    def test_targets_clause(self):
        self.assertEqual(
            messages.targets_clause(5, 90.0, 91.5),
            "up to --max-target-uses 5 shard(s) each, projected at or below "
            "--max-target-util 90% (backfillfull_ratio 91.5%)",
        )

    def test_unplaceable_items_then_the_caveat(self):
        text = stderr_of(
            messages.print_unplaceable,
            iter([("19.1", 3, "(headed for osd.31)"), ("7.2", "-", "off osd.4")]),
        )
        self.assertEqual(
            text,
            "cannot place 19.1 shard 3 (headed for osd.31): no legal target "
            "cannot place 7.2 shard - off osd.4: no legal target "
            + flat(messages.UNPLACEABLE_NOTE),
        )

    def test_nothing_when_everything_is_placed(self):
        self.assertEqual(stderr_of(messages.print_unplaceable, iter([])), "")


if __name__ == "__main__":
    unittest.main()
