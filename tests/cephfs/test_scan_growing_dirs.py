"""Unit tests for cephfs/scan-growing-dirs.py."""

import argparse
import contextlib
import errno
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from _scan_support import (
    ACTIVE,
    IDLE,
    NOW,
    SCRIPT,
    FakeDir,
    FakeFs,
    MiB,
    bfs,
    growing,
    mk,
    mkrun,
    oserror,
    rates,
    sg,
)

T = 1 * MiB  # the default threshold, in bytes/s


def flat(text: str) -> str:
    """text with runs of whitespace collapsed, for wrap-proof checks."""
    return " ".join(text.split())


class ParseRateTest(unittest.TestCase):
    def test_units_and_durations(self):
        cases = {
            "1MiB/s": MiB,
            "1mib/S": MiB,
            "1 MiB/s": MiB,
            "500MB/min": 500e6 / 60,
            "1.5GiB/h": 1.5 * 2**30 / 3600,
            "10kB/s": 10_000,
            "2KiB/s": 2048,
            "86400B/d": 1.0,
            "1TiB/d": 2**40 / 86400,
            "1PB/s": 1e15,
            "1PiB/s": 2**50,
        }
        for text, expected in cases.items():
            with self.subTest(text):
                self.assertAlmostEqual(sg.parse_rate(text), expected)

    def test_rejects_bad_forms(self):
        bad = (
            "",
            "1",
            "1MiB",
            "MiB/s",
            "1M/s",
            "1G/s",
            "1MiB/sec",
            "1MB/x",
            "-1MiB/s",
            "1MiB/s/s",
            "1e3B/s",
            "0MiB/s",
            "0.0B/s",
        )
        for text in bad:
            with self.subTest(text), self.assertRaises(ValueError):
                sg.parse_rate(text)

    def test_rejects_infinite_rate(self):
        with self.assertRaises(ValueError) as cm:
            sg.parse_rate("9" * 400 + "B/s")
        self.assertIn("too large", str(cm.exception))

    def test_message_names_input_and_forms(self):
        with self.assertRaises(ValueError) as cm:
            sg.parse_rate("1M/s")
        self.assertIn("'1M/s'", str(cm.exception))
        self.assertIn("KiB..PiB", str(cm.exception))

    def test_zero_has_its_own_message(self):
        with self.assertRaises(ValueError) as cm:
            sg.parse_rate("0MiB/s")
        self.assertIn("must be above 0", str(cm.exception))


class FormatRateTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(sg.format_rate(0), "0.0 B/s")
        self.assertEqual(sg.format_rate(512), "512.0 B/s")
        self.assertEqual(sg.format_rate(1.5 * MiB), "1.5 MiB/s")
        self.assertEqual(sg.format_rate(52.1 * MiB), "52.1 MiB/s")
        self.assertEqual(sg.format_rate(3 * 2**50), "3.0 PiB/s")
        self.assertEqual(sg.format_rate(5000 * 2**50), "5000.0 PiB/s")

    def test_rounds_into_next_unit(self):
        # 1023.96 B/s shows as 1024.0 at one decimal, so it must roll over.
        self.assertEqual(sg.format_rate(1023.96), "1.0 KiB/s")

    def test_negative(self):
        self.assertEqual(sg.format_rate(-1.5 * MiB), "-1.5 MiB/s")


class AnalyzeTest(unittest.TestCase):
    def test_worked_example(self):
        r = mk("/r", rate=55.5, idle={"/r/c": IDLE})
        mk("/r/a", r, rate=5, reason=sg.Reason.DEPTH)
        b = mk("/r/b", r, rate=50)
        for i in range(100):
            mk(f"/r/b/{i:03}", b, rate=0.5, reason=sg.Reason.DEPTH)
        rows = sg.analyze(bfs(r), T)
        self.assertEqual(rates(rows), {("/r/a", "tree"): 5.0, ("/r/b", "spread"): 50.0})

    def test_shrinking_sibling_does_not_hide_growing_child(self):
        r = mk("/r", rate=0.2)
        mk("/r/x", r, rate=5, reason=sg.Reason.DEPTH)
        mk("/r/y", r, rate=-4.8, reason=sg.Reason.DEPTH)
        self.assertEqual(rates(sg.analyze(bfs(r), T)), {("/r/x", "tree"): 5.0})

    def test_own_files_next_to_shrinking_child(self):
        # Signed arithmetic: clamping /r/y to 0 would hide /r's own +2.
        r = mk("/r", rate=0)
        mk("/r/y", r, rate=-2, reason=sg.Reason.DEPTH)
        self.assertEqual(rates(sg.analyze(bfs(r), T)), {("/r", "files"): 2.0})

    def test_own_files_and_spread_both_reported(self):
        r = mk("/r", rate=8)
        for i in range(10):
            mk(f"/r/{i}", r, rate=0.5, reason=sg.Reason.DEPTH)
        rows = sg.analyze(bfs(r), T)
        self.assertEqual(rates(rows), {("/r", "files"): 3.0, ("/r", "spread"): 5.0})

    def test_spread_reported_once_at_lowest_dir(self):
        p = mk("/p", rate=1.5)
        r = mk("/p/r", p, rate=1.5)
        for name in "abc":
            mk(f"/p/r/{name}", r, rate=0.5)
        self.assertEqual(rates(sg.analyze(bfs(p), T)), {("/p/r", "spread"): 1.5})

    def test_threshold_is_inclusive(self):
        at = mk("/at", rate=1.0)
        below = mk("/below", rate=0.999)
        rows = sg.analyze(bfs(at, below), T)
        self.assertEqual(rates(rows), {("/at", "files"): 1.0})

    def test_listed_leaf_without_subdirs_is_files(self):
        rows = sg.analyze([mk("/r", rate=2)], T)
        self.assertEqual(rates(rows), {("/r", "files"): 2.0})
        self.assertFalse(rows[0].approx)
        self.assertEqual(rows[0].note, "")

    def test_tree_reasons_and_notes(self):
        r = mk("/r", rate=8)
        mk("/r/d", r, rate=2, reason=sg.Reason.DEPTH)
        mk("/r/w", r, rate=2, reason=sg.Reason.WIDE, entries=12345)
        mk("/r/u", r, rate=2, reason=sg.Reason.UNLISTABLE, list_error="Denied")
        mk("/r/i", r, rate=2, reason=sg.Reason.IDLE)
        notes = {row.path: row.note for row in sg.analyze(bfs(r), T)}
        self.assertEqual(
            notes,
            {
                "/r/d": "depth limit",
                "/r/w": "12345 entries",
                "/r/u": "unlistable",
                "/r/i": "idle at start",
            },
        )

    def test_unreadable_subdir_marks_files_row(self):
        r = mk("/r", rate=3, unreadable={"/r/u": "Permission denied"})
        (row,) = sg.analyze([r], T)
        self.assertTrue(row.approx)
        self.assertEqual(row.note, "1 unreadable subdir")
        self.assertEqual(row.unreadable, ("/r/u",))

    def test_woken_subdirs_mark_files_row(self):
        (row,) = sg.analyze([mk("/r", rate=3, woke_up=["/r/w1", "/r/w2"])], T)
        self.assertTrue(row.approx)
        self.assertEqual(row.note, "2 subdirs became active")
        self.assertEqual(row.woke_up, ("/r/w1", "/r/w2"))

    def test_unsampled_child_not_counted_twice(self):
        r = mk("/r", rate=7)
        u = mk("/r/u", r, rate=None, sample_error="Permission denied")
        mk("/r/u/v", u, rate=5, reason=sg.Reason.DEPTH)
        rows = sg.analyze(bfs(r), T)
        self.assertEqual(rates(rows), {("/r", "files"): 2.0, ("/r/u/v", "tree"): 5.0})
        files = next(row for row in rows if row.kind is sg.Kind.FILES)
        self.assertEqual((files.approx, files.unreadable), (True, ("/r/u",)))

    def test_spread_collects_unmeasured_from_uncovered_part(self):
        r = mk("/r", rate=1.5)
        mk("/r/a", r, rate=0.5, unreadable={"/r/a/u": "Permission denied"})
        mk("/r/b", r, rate=0.5, woke_up=["/r/b/w"])
        mk("/r/c", r, rate=0.5)
        (row,) = sg.analyze(bfs(r), T)
        self.assertEqual((row.kind, row.approx), (sg.Kind.SPREAD, True))
        self.assertEqual(row.note, "1 unreadable subdir; 1 subdir became active")
        self.assertEqual((row.unreadable, row.woke_up), (("/r/a/u",), ("/r/b/w",)))

    def test_vanished_child_counts_as_shrinking(self):
        r = mk("/r", rate=None)
        v = mk("/r/v", r, rate=None, reason=sg.Reason.DEPTH, vanished=True)
        v.s1, v.s2 = sg.Sample(6 * 10**9, 0.0), sg.Sample(0, 60.0)
        r.s1 = sg.Sample(10**12, 0.0)
        r.s2 = sg.Sample(10**12 - 6 * 10**9 + 2 * MiB * 60, 60.0)
        self.assertEqual(rates(sg.analyze(bfs(r), T)), {("/r", "files"): 2.0})

    def test_nothing_grows(self):
        r = mk("/r", rate=-3)
        mk("/r/a", r, rate=-1, reason=sg.Reason.DEPTH)
        self.assertEqual(sg.analyze(bfs(r), T), [])
        self.assertEqual(sg.analyze([], T), [])

    def test_unsampled_root_has_no_rows(self):
        self.assertEqual(sg.analyze([mk("/r", rate=None, sample_error="x")], T), [])

    def test_multiple_roots(self):
        rows = sg.analyze(bfs(mk("/r1", rate=2), mk("/r2", rate=3)), T)
        self.assertEqual(
            {(row.path, row.root, row.depth) for row in rows},
            {("/r1", "/r1", 0), ("/r2", "/r2", 0)},
        )


def row(path, kind=sg.Kind.FILES, rate=1.0, depth=0, approx=False, note=""):
    return sg.Row(path, "/", depth, kind, rate * MiB, approx, note)


class SortRowsTest(unittest.TestCase):
    ROWS = (
        row("/b", sg.Kind.SPREAD, 5, depth=1),
        row("/b", sg.Kind.FILES, 2, depth=1),
        row("/a", sg.Kind.TREE, 5, depth=1),
        row("/z", sg.Kind.FILES, 1, depth=0),
    )

    def test_depth_then_path_then_kind(self):
        order = [(r.path, r.kind) for r in sg.sort_rows(self.ROWS, "depth")]
        self.assertEqual(
            order, [("/z", "files"), ("/a", "tree"), ("/b", "files"), ("/b", "spread")]
        )

    def test_rate_then_depth_then_path(self):
        order = [(r.path, r.kind) for r in sg.sort_rows(self.ROWS, "rate")]
        self.assertEqual(
            order, [("/a", "tree"), ("/b", "spread"), ("/b", "files"), ("/z", "files")]
        )


class RenderTest(unittest.TestCase):
    def test_table(self):
        rows = [
            row("/a/x", rate=52.1),
            row("/a/wide", sg.Kind.TREE, 3.0, note="12345 entries"),
            row("/b", rate=2.2, approx=True, note="2 unreadable subdirs"),
        ]
        self.assertEqual(
            sg.render_table(rows),
            "      RATE  KIND    PATH     NOTE\n"
            "52.1 MiB/s  files   /a/x\n"
            " 3.0 MiB/s  tree    /a/wide  12345 entries\n"
            " 2.2 MiB/s~ files   /b       2 unreadable subdirs",
        )

    def test_footnotes_cover_only_kinds_shown(self):
        text = sg.footnotes([row("/a"), row("/b", sg.Kind.TREE)])
        self.assertEqual(
            text.splitlines(),
            [
                f"files   {sg.KIND_TEXT[sg.Kind.FILES]}",
                f"tree    {sg.KIND_TEXT[sg.Kind.TREE]}",
            ],
        )

    def test_footnotes_explain_approx_mark(self):
        text = sg.footnotes([row("/a", approx=True)])
        self.assertIn(f"~       {sg.APPROX_TEXT}", text)

    def test_summary(self):
        r = mk("/r", rate=1)
        a = mk("/r/a", r, rate=1)
        a.s2.t = 61.5
        mk("/r/b", r, rate=None, sample_error="x")
        self.assertEqual(
            sg.summary(mkrun(bfs(r), probed=7)),
            "Tracked 3 dirs under 1 root (probed 7 subdirs); "
            "sample intervals 60.0-61.5 s.",
        )

    def test_summary_without_samples(self):
        r = mk("/r", rate=None, sample_error="x")
        self.assertEqual(
            sg.summary(mkrun([r], probed=1)),
            "Tracked 1 dir under 1 root (probed 1 subdir).",
        )

    def test_no_notes_when_everything_was_read(self):
        self.assertEqual(sg.notes(mkrun([mk("/r", rate=1)])), [])

    def test_permission_warning_names_each_problem(self):
        r = mk("/r", rate=1, unreadable={"/r/u": "Permission denied"})
        mk("/r/x", r, rate=1, reason=sg.Reason.UNLISTABLE, list_error="Denied")
        mk("/r/y", r, rate=None, sample_error="Denied")
        (warning,) = sg.notes(mkrun(bfs(r)))
        self.assertTrue(
            warning.startswith(
                "WARNING: couldn't read 3 dirs: /r/u (unreadable: Permission "
                "denied), /r/x (unlistable: Denied), /r/y (not sampled: Denied)."
            )
        )

    def test_permission_warning_states_only_effects_that_apply(self):
        r = mk("/r", rate=None, sample_error="Denied")
        mk("/r/a", r, rate=1, reason=sg.Reason.DEPTH)
        (warning,) = sg.notes(mkrun(bfs(r)))
        self.assertIn(
            "A root that couldn't be sampled gets no rows of its own.", flat(warning)
        )
        self.assertNotIn("parent's rate", warning)
        self.assertNotIn("unlistable dir", warning)

    def test_permission_warning_counts_each_dir_once(self):
        r = mk("/r", rate=1)
        mk("/r/x", r, rate=None, reason=sg.Reason.UNLISTABLE, list_error="Denied",
           sample_error="Denied")  # fmt: skip
        (warning,) = sg.notes(mkrun(bfs(r)))
        self.assertIn(
            "couldn't read 1 dir: /r/x (unlistable: Denied; not sampled: Denied).",
            flat(warning),
        )

    def test_permission_warning_lists_up_to_limit(self):
        unreadable = {f"/r/u{i:02}": "Denied" for i in range(12)}
        (warning,) = sg.notes(mkrun([mk("/r", rate=1, unreadable=unreadable)]))
        self.assertIn(
            "/r/u09 (unreadable: Denied), +2 more (--json lists all).", warning
        )
        self.assertNotIn("/r/u10", warning)

    def test_not_cephfs_and_vanished_notes(self):
        r = mk("/r", rate=1)
        mk("/r/v", r, rate=-1, vanished=True)
        found = sg.notes(mkrun(bfs(r), not_cephfs=["/r/m"]))
        self.assertEqual(
            found,
            [
                "NOTE: skipped 1 subdir not on CephFS (mount points?): /r/m.",
                (
                    "NOTE: 1 tracked dir vanished during the run; each counts as "
                    "0 bytes from then on."
                ),
            ],
        )

    def test_paragraph_does_not_break_paths(self):
        path = "/" + "x" * 100
        self.assertIn(path, sg.paragraph(f"NOTE: see {path} now"))

    def test_json(self):
        r = mk("/r", rate=3, unreadable={"/r/u": "Permission denied"})
        run = mkrun([r], probed=4, not_cephfs=["/r/m"])
        doc = json.loads(sg.render_json(run, sg.analyze([r], T), T))
        self.assertEqual(doc["version"], sg.JSON_VERSION)
        self.assertEqual(doc["threshold"], T)
        self.assertEqual(
            doc["rows"],
            [
                {
                    "path": "/r",
                    "root": "/r",
                    "depth": 0,
                    "kind": "files",
                    "rate": 3.0 * MiB,
                    "approx": True,
                    "note": "1 unreadable subdir",
                    "unreadable": ["/r/u"],
                    "woke_up": [],
                }
            ],
        )
        self.assertEqual(
            doc["unreadable"],
            [{"path": "/r/u", "problem": "unreadable: Permission denied"}],
        )
        self.assertEqual(doc["not_cephfs"], ["/r/m"])
        self.assertEqual(
            doc["summary"],
            {
                "roots": 1,
                "tracked": 1,
                "probed": 4,
                "interval_min": 60.0,
                "interval_max": 60.0,
            },
        )


class RealFsTest(unittest.TestCase):
    def test_subdirs_skips_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as d:
            for name in ("b", "a"):
                os.mkdir(os.path.join(d, name))
            open(os.path.join(d, "file"), "w").close()
            os.symlink(os.path.join(d, "a"), os.path.join(d, "link"))
            a, b = os.path.join(d, "a"), os.path.join(d, "b")
            self.assertEqual(
                sg.Fs().subdirs(d), {a: os.stat(a).st_ino, b: os.stat(b).st_ino}
            )

    def test_getxattr_does_not_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as d:
            real, link = os.path.join(d, "real"), os.path.join(d, "link")
            os.mkdir(real)
            try:
                os.setxattr(real, "user.scan_test", b"1")
            except OSError:
                self.skipTest("no user xattrs on the temp filesystem")
            os.symlink(real, link)
            self.assertEqual(sg.Fs().getxattr(real, "user.scan_test"), b"1")
            with self.assertRaises(OSError):
                sg.Fs().getxattr(link, "user.scan_test")

    def test_subdirs_of_missing_dir_raises(self):
        with self.assertRaises(OSError):
            sg.Fs().subdirs("/nonexistent/scan-growing-dirs-test")


def preflight(paths, fs):
    with ThreadPoolExecutor(4) as pool:
        return sg.preflight(paths, fs, pool)


class PreflightTest(unittest.TestCase):
    def fs(self, **dirs):
        return FakeFs(
            {"/r": FakeDir(), "/r/a": FakeDir(), "/s": FakeDir(), **dirs}, files={"/f"}
        )

    def problems(self, paths, fs=None):
        with self.assertRaises(sg.FatalError) as cm:
            preflight(paths, fs or self.fs())
        return list(cm.exception.args)

    def test_roots_become_depth_zero_nodes(self):
        roots = preflight(["/r", "/s/"], self.fs())
        self.assertEqual(
            [(n.path, n.root, n.depth) for n in roots],
            [("/r", "/r", 0), ("/s", "/s", 0)],
        )

    def test_symlinked_root_is_resolved(self):
        fs = self.fs()
        fs.links["/link"] = "/r"
        (root,) = preflight(["/link"], fs)
        self.assertEqual((root.path, root.root), ("/r", "/r"))

    def test_missing_and_not_a_directory(self):
        self.assertEqual(
            self.problems(["/nope", "/f"]),
            ["/nope: no such directory", "/f: not a directory"],
        )

    def test_not_cephfs(self):
        fs = self.fs(**{"/x": FakeDir(errors={sg.RBYTES: errno.ENODATA})})
        (problem,) = self.problems(["/x"], fs)
        self.assertIn("is it on a CephFS mount?", problem)

    def test_permission_denied(self):
        fs = self.fs(**{"/x": FakeDir(errors={sg.ENTRIES: errno.EACCES})})
        (problem,) = self.problems(["/x"], fs)
        self.assertIn("permission denied", problem)

    def test_duplicate_and_nested_roots(self):
        fs = self.fs()
        fs.links["/s"] = "/r"
        self.assertEqual(
            self.problems(["/r", "/s"], fs), ["/s is the same directory as /r"]
        )
        self.assertEqual(self.problems(["/r", "/r/a"]), ["/r/a is inside /r"])
        self.assertEqual(self.problems(["/r/a", "/r"]), ["/r/a is inside /r"])

    def test_sibling_with_common_prefix_is_not_nested(self):
        roots = preflight(["/r", "/r2"], self.fs(**{"/r2": FakeDir()}))
        self.assertEqual(len(roots), 2)

    def test_all_problems_reported_together(self):
        self.assertEqual(len(self.problems(["/nope", "/f", "/r", "/r/a"])), 3)


def build(fs, roots=("/r",), threads=4, **limits):
    run = mkrun(preflight(list(roots), fs), **limits)
    with ThreadPoolExecutor(threads) as pool:
        sg.build(run, fs, pool, sg.Progress(io.StringIO()))
    return run


class BuildTest(unittest.TestCase):
    def test_tracks_active_subdirs_in_bfs_order(self):
        fs = FakeFs(
            {
                "/r": FakeDir(),
                "/r/b": FakeDir(),
                "/r/a": FakeDir(),
                "/r/a/x": FakeDir(),
                "/r/c": FakeDir(rctime=IDLE),
                "/r/c/y": FakeDir(),
            }
        )
        run = build(fs)
        self.assertEqual([n.path for n in run.nodes], ["/r", "/r/a", "/r/b", "/r/a/x"])
        r = run.nodes[0]
        self.assertEqual([c.path for c in r.children], ["/r/a", "/r/b"])
        self.assertEqual(r.idle, {"/r/c": IDLE})
        self.assertNotIn("/r/c", fs.listed())
        self.assertEqual(run.probed, 4)  # a, b, c, then x
        self.assertTrue(all(n.reason is None for n in run.nodes))

    def test_cutoff_includes_grace_and_is_inclusive(self):
        cutoff = NOW - 60 - sg.IDLE_GRACE
        fs = FakeFs(
            {
                "/r": FakeDir(),
                "/r/edge": FakeDir(rctime=cutoff),
                "/r/grace": FakeDir(rctime=NOW - 61),
                "/r/old": FakeDir(rctime=cutoff - 0.001),
                "/r/future": FakeDir(rctime=NOW + 5),
            },
            latency=0,
        )
        r = build(fs).nodes[0]
        self.assertEqual(
            [c.path for c in r.children], ["/r/edge", "/r/future", "/r/grace"]
        )
        self.assertEqual(list(r.idle), ["/r/old"])

    def test_idle_root_is_not_listed(self):
        fs = FakeFs({"/r": FakeDir(rctime=IDLE), "/r/a": FakeDir()})
        (r,) = build(fs).nodes
        self.assertEqual(r.reason, sg.Reason.IDLE)
        self.assertEqual(fs.listed(), [])

    def test_depth_limit(self):
        fs = FakeFs({"/r": FakeDir(), "/r/a": FakeDir(), "/r/a/b": FakeDir()})
        run = build(fs, depth=1)
        self.assertEqual(
            [(n.path, n.reason) for n in run.nodes],
            [("/r", None), ("/r/a", sg.Reason.DEPTH)],
        )
        self.assertEqual(fs.listed(), ["/r"])

    def test_depth_zero_tracks_only_roots(self):
        fs = FakeFs({"/r": FakeDir(), "/r/a": FakeDir()})
        (r,) = build(fs, depth=0).nodes
        self.assertEqual(r.reason, sg.Reason.DEPTH)
        self.assertEqual(fs.listed(), [])

    def test_wide_dir_is_never_listed(self):
        fs = FakeFs(
            {"/r": FakeDir(), "/r/w": FakeDir(entries=5), "/r/ok": FakeDir(entries=4)}
        )
        run = build(fs, max_entries=4)
        w = next(n for n in run.nodes if n.path == "/r/w")
        self.assertEqual((w.reason, w.entries), (sg.Reason.WIDE, 5))
        self.assertNotIn("/r/w", fs.listed())
        self.assertIn("/r/ok", fs.listed())

    def tree(self, n):
        return FakeFs({"/r": FakeDir(), **{f"/r/{i}": FakeDir() for i in range(n)}})

    def test_max_dirs_at_limit_passes(self):
        self.assertEqual(len(build(self.tree(3), max_dirs=4).nodes), 4)

    def test_max_dirs_exceeded_stops(self):
        with self.assertRaises(sg.FatalError) as cm:
            build(self.tree(3), max_dirs=3)
        message = flat(cm.exception.args[0])
        self.assertIn("more than 3 dirs changed in the last 90 s", message)
        self.assertIn("stopped at depth 1", message)

    def test_probes_stream_through_a_bounded_window(self):
        n = sg.PROBE_BATCH * 5 + 3
        with mock.patch.object(sg, "IN_FLIGHT_BATCHES", 2):
            r = build(self.tree(n), threads=2).nodes[0]
        self.assertEqual(
            [c.path for c in r.children], sorted(f"/r/{i}" for i in range(n))
        )

    def test_max_dirs_exceeded_cancels_pending_probes(self):
        class SlowFs(FakeFs):  # takes real time, as CephFS does, so cancelling can win
            def getxattr(self, path, name):
                time.sleep(0.001)
                return super().getxattr(path, name)

        fs = SlowFs(self.tree(400).dirs)
        with self.assertRaises(sg.FatalError):
            build(fs, threads=1, max_dirs=3)
        self.assertLess(len(fs.reads(sg.RCTIME)), 100)  # not all 400

    def test_more_roots_than_max_dirs(self):
        fs = FakeFs({"/r": FakeDir(), "/s": FakeDir()})
        with self.assertRaises(sg.FatalError) as cm:
            build(fs, roots=("/r", "/s"), max_dirs=1)
        self.assertIn("2 roots given, but --max-dirs is 1", cm.exception.args[0])

    def test_unreadable_not_cephfs_and_vanished_subdirs(self):
        fs = FakeFs(
            {
                "/r": FakeDir(),
                "/r/denied": FakeDir(errors={sg.RCTIME: errno.EACCES}),
                "/r/mnt": FakeDir(errors={sg.RCTIME: errno.ENODATA}),
                "/r/gone": FakeDir(errors={sg.RCTIME: errno.ENOENT}),
            }
        )
        run = build(fs)
        r = run.nodes[0]
        self.assertEqual(r.unreadable, {"/r/denied": os.strerror(errno.EACCES)})
        self.assertEqual(run.not_cephfs, ["/r/mnt"])
        self.assertEqual((r.children, r.idle), ([], {}))

    def test_unlistable_dir(self):
        fs = FakeFs(
            {
                "/r": FakeDir(),
                "/r/a": FakeDir(errors={"list": errno.EACCES}),
                "/r/b": FakeDir(errors={sg.ENTRIES: errno.EACCES}),
            }
        )
        run = build(fs)
        for node in run.nodes[1:]:
            self.assertEqual(node.reason, sg.Reason.UNLISTABLE)
            self.assertEqual(node.list_error, os.strerror(errno.EACCES))

    def test_many_subdirs_span_batches_in_order(self):
        n = sg.PROBE_BATCH * 2 + 3
        fs = self.tree(n)
        r = build(fs, threads=8).nodes[0]
        self.assertEqual(
            [c.path for c in r.children], sorted(f"/r/{i}" for i in range(n))
        )


class ProgressTest(unittest.TestCase):
    class Tty(io.StringIO):
        def isatty(self):
            return True

    def test_tty_rewrites_one_line_and_clears_it(self):
        out = self.Tty()
        progress = sg.Progress(out)
        progress.phase("Depth 1: exploring 3 dirs")
        progress.phase("Sampling")
        progress.done()
        self.assertEqual(
            out.getvalue(),
            "\rDepth 1: exploring 3 dirs"
            + "\rSampling"
            + " " * 17
            + "\r"
            + " " * 8
            + "\r",
        )

    def test_tty_updates_are_throttled(self):
        out = self.Tty()
        progress = sg.Progress(out)
        progress.update("probed 1 subdir")
        progress.update("probed 2 subdirs")
        self.assertEqual(out.getvalue(), "\rprobed 1 subdir")

    def test_pipe_gets_one_line_per_phase_and_no_updates(self):
        out = io.StringIO()
        progress = sg.Progress(out)
        progress.phase("Sampling 3 dirs")
        progress.update("probed 5 subdirs")
        progress.done()
        self.assertEqual(out.getvalue(), "Sampling 3 dirs\n")


def sampled(fs, roots=("/r",), **options):
    """The Run that sample_run() makes of fs, with SAMPLING_DEFAULTS except
    the options given."""
    args = argparse.Namespace(roots=list(roots), **(sg.SAMPLING_DEFAULTS | options))
    with contextlib.redirect_stderr(io.StringIO()):
        return sg.sample_run(args, fs)


def add_new_subdir(fs):
    fs.dirs["/r/new"] = FakeDir()


class SampleTest(unittest.TestCase):
    def test_intervals_at_least_delay_and_rates_exact(self):
        fs = FakeFs(
            {
                "/r": FakeDir(rbytes=growing(3 * MiB)),
                **{f"/r/{i}": FakeDir(rbytes=growing(MiB)) for i in range(20)},
            }
        )
        run = sampled(fs, delay=10.0)
        self.assertTrue(all(span >= 10.0 for span in sg.intervals(run.nodes)))
        self.assertAlmostEqual(run.nodes[0].rate, 3 * MiB, delta=1)
        self.assertAlmostEqual(run.nodes[1].rate, MiB, delta=1)

    def test_passes_bracket_the_sleep_in_the_same_order(self):
        fs = FakeFs({"/r": FakeDir(), "/r/b": FakeDir(), "/r/a": FakeDir()})
        sampled(fs, threads=1)
        sleep = fs.calls.index(("sleep", 60.0))
        before = [c[1] for c in fs.calls[:sleep] if c[2:] == (sg.RBYTES,)]
        after = [c[1] for c in fs.calls[sleep:] if c[2:] == (sg.RBYTES,)]
        self.assertEqual(before, ["/r", "/r", "/r/a", "/r/b"])  # preflight first
        self.assertEqual(after, ["/r", "/r/a", "/r/b"])

    def test_dir_gone_in_second_pass_counts_as_zero(self):
        fs = FakeFs(
            {"/r": FakeDir(rbytes=100), "/r/a": FakeDir(rbytes=6000)},
            after_sleep=lambda fs: fs.dirs.pop("/r/a"),
        )
        a = sampled(fs).nodes[1]
        self.assertTrue(a.vanished)
        self.assertEqual((a.s1.rbytes, a.s2.rbytes), (6000, 0))
        self.assertIsNone(a.sample_error)

    def test_error_in_first_pass_leaves_dir_unsampled(self):
        fs = FakeFs(
            {"/r": FakeDir(), "/r/a": FakeDir(errors={sg.RBYTES: errno.EACCES})}
        )
        run = sampled(fs)
        a = run.nodes[1]
        self.assertEqual((a.s1, a.s2), (None, None))
        self.assertEqual(a.sample_error, os.strerror(errno.EACCES))
        self.assertEqual(fs.reads(sg.RBYTES).count("/r/a"), 1)  # not retried

    def test_error_in_second_pass_keeps_first_sample(self):
        def deny(fs):
            fs.dirs["/r/a"].errors[sg.RBYTES] = errno.EACCES

        fs = FakeFs({"/r": FakeDir(), "/r/a": FakeDir()}, after_sleep=deny)
        a = sampled(fs).nodes[1]
        self.assertIsNotNone(a.s1)
        self.assertIsNone(a.s2)
        self.assertIsNone(a.rate)


class RecheckSubdirsTest(unittest.TestCase):
    def fs(self):
        return FakeFs(
            {
                "/r": FakeDir(rbytes=growing(2 * MiB)),
                "/r/i1": FakeDir(rctime=IDLE),
                "/r/i2": FakeDir(rctime=IDLE),
                "/r/i3": FakeDir(rctime=IDLE),
            }
        )

    def run_with(self, fs, change):
        """Sample fs with change(fs) during the sleep. Return the root, and
        the rctime probes and listings made after the sleep: only following
        renames and the re-check make those."""
        fs.after_sleep = change
        run = sampled(fs)
        after = fs.calls[fs.calls.index(("sleep", 60.0)) + 1 :]
        probes = sum(
            1 for call in after if call[0] == "getxattr" and call[2] == sg.RCTIME
        )
        return run.nodes[0], probes, [call[1] for call in after if call[0] == "list"]

    def test_records_idle_subdirs_whose_rctime_moved(self):
        def change(fs):
            fs.dirs["/r/i1"].rctime = ACTIVE
            del fs.dirs["/r/i2"]

        r, probes, _ = self.run_with(self.fs(), change)
        self.assertEqual(r.woke_up, ["/r/i1"])
        self.assertEqual(probes, 3)
        self.assertEqual(r.unreadable, {})

    def test_unreadable_on_recheck(self):
        def change(fs):
            fs.dirs["/r/i3"].errors[sg.RCTIME] = errno.EACCES

        r, _, _ = self.run_with(self.fs(), change)
        self.assertEqual(r.unreadable, {"/r/i3": os.strerror(errno.EACCES)})

    def test_records_subdirs_created_during_sampling(self):
        def change(fs):
            fs.dirs["/r/new"] = FakeDir()

        r, _, listed = self.run_with(self.fs(), change)
        self.assertEqual(r.woke_up, ["/r/new"])
        self.assertEqual(listed, ["/r"])

    def test_known_subdirs_are_not_new(self):
        fs = FakeFs(
            {
                "/r": FakeDir(rbytes=growing(2 * MiB)),
                "/r/active": FakeDir(),
                "/r/idle": FakeDir(rctime=IDLE),
                "/r/denied": FakeDir(errors={sg.RCTIME: errno.EACCES}),
                "/r/mnt": FakeDir(errors={sg.RCTIME: errno.ENODATA}),
            }
        )
        r, _, listed = self.run_with(fs, lambda fs: None)
        self.assertEqual(listed, ["/r"])
        self.assertEqual(r.woke_up, [])

    def test_relist_failure_is_ignored(self):
        def change(fs):
            fs.dirs["/r"].errors["list"] = errno.EACCES

        r, _, listed = self.run_with(self.fs(), change)
        self.assertEqual((listed, r.woke_up), (["/r"], []))

    def test_rename_within_parent_is_followed(self):
        fs = FakeFs(
            {
                "/r": FakeDir(rbytes=10**9),
                "/r/job.tmp": FakeDir(rbytes=growing(MiB)),
                "/r/job.tmp/sub": FakeDir(rbytes=growing(MiB)),
            }
        )
        r, _, _ = self.run_with(fs, lambda fs: fs.rename("/r/job.tmp", "/r/job"))
        (job,) = r.children
        (sub,) = job.children
        self.assertEqual((job.path, sub.path), ("/r/job", "/r/job/sub"))
        self.assertEqual(job.renamed_from, "/r/job.tmp")
        self.assertFalse(job.vanished or sub.vanished)
        # Threads share the fake clock, so a sample's time can be off by one
        # call's latency: allow 0.1%.
        self.assertAlmostEqual(job.rate, MiB, delta=MiB / 1000)
        self.assertEqual(r.woke_up, [])

    def renamed_job(self):
        return FakeFs(
            {
                "/r": FakeDir(rbytes=10**9),
                "/r/job.tmp": FakeDir(rbytes=growing(MiB)),
                "/r/job.tmp/sub": FakeDir(rbytes=growing(MiB)),
            }
        )

    def test_new_subdir_inside_renamed_subtree_is_found(self):
        def change(fs):
            fs.rename("/r/job.tmp", "/r/job")
            fs.dirs["/r/job/sub/new"] = FakeDir()

        r, _, _ = self.run_with(self.renamed_job(), change)
        (sub,) = r.children[0].children
        self.assertEqual(sub.woke_up, ["/r/job/sub/new"])

    def test_failed_reread_after_rename_is_unsampled(self):
        def change(fs):
            fs.rename("/r/job.tmp", "/r/job")
            fs.dirs["/r/job"].errors[sg.RBYTES] = errno.EACCES

        r, _, _ = self.run_with(self.renamed_job(), change)
        (job,) = r.children
        self.assertEqual((job.path, job.vanished), ("/r/job", False))
        self.assertEqual(job.sample_error, os.strerror(errno.EACCES))

    def test_new_dir_with_another_inode_is_not_a_rename(self):
        def change(fs):
            del fs.dirs["/r/i1"]
            fs.dirs["/r/new"] = FakeDir()

        fs = self.fs()
        fs.dirs["/r/i1"].rctime = ACTIVE  # tracked
        r, _, _ = self.run_with(fs, change)
        (i1,) = r.children
        self.assertTrue(i1.vanished)
        self.assertIsNone(i1.renamed_from)
        self.assertEqual(r.woke_up, ["/r/new"])

    def test_skipped_when_own_rate_is_zero(self):
        fs = self.fs()
        fs.dirs["/r"].rbytes = 5

        def change(fs):
            fs.dirs["/r/new"] = FakeDir()

        r, probes, listed = self.run_with(fs, change)
        self.assertEqual((r.woke_up, probes, listed), ([], 0, []))


class StateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "state.json")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, text):
        with open(self.path, "w") as f:
            f.write(text)

    def fatal(self):
        with self.assertRaises(sg.FatalError) as cm:
            sg.load_state(self.path)
        return cm.exception.args[0]

    def test_round_trip(self):
        r = mk("/r", rate=8, unreadable={"/r/u": "Permission denied"}, woke_up=["/r/w"])
        mk("/r/a", r, rate=2, reason=sg.Reason.WIDE, entries=99, ino=7,
           renamed_from="/r/a.tmp")  # fmt: skip
        u = mk("/r/b", r, rate=None, sample_error="Permission denied")
        mk("/r/b/c", u, rate=3, reason=sg.Reason.DEPTH, vanished=True)
        run = mkrun(bfs(r), probed=5, not_cephfs=["/r/m"])
        sg.save_state(self.path, run)
        loaded = sg.load_state(self.path)
        self.assertEqual(sg.analyze(loaded.nodes, T), sg.analyze(run.nodes, T))
        self.assertEqual(sg.notes(loaded), sg.notes(run))
        self.assertEqual(sg.summary(loaded), sg.summary(run))
        a = loaded.nodes[1]
        self.assertEqual((a.ino, a.renamed_from), (7, "/r/a.tmp"))
        self.assertEqual(
            [n.parent and n.parent.path for n in loaded.nodes],
            [None, "/r", "/r", "/r/b"],
        )
        for name in ("roots", "delay", "depth", "max_dirs", "max_entries", "created"):
            self.assertEqual(getattr(loaded, name), getattr(run, name))

    def test_missing_file(self):
        self.assertIn("can't read", self.fatal())

    def test_not_json(self):
        self.write("not json")
        self.assertIn("is not a state file", self.fatal())

    def test_other_json(self):
        self.write('{"format": "something-else"}')
        self.assertIn("is not a scan-growing-dirs state file", self.fatal())

    def test_other_version(self):
        self.write(json.dumps({"format": sg.STATE_FORMAT, "version": 99}))
        self.assertIn("has state version 99", self.fatal())

    def test_damaged(self):
        self.write(json.dumps({"format": sg.STATE_FORMAT, "version": 1}))
        self.assertIn("damaged state file", self.fatal())

    def test_damaged_values(self):
        r = mk("/r", rate=8)
        mk("/r/a", r, rate=2, reason=sg.Reason.DEPTH)
        sg.save_state(self.path, mkrun(bfs(r)))
        with open(self.path) as f:
            good = json.load(f)

        s1_time = json.dumps(good["nodes"][1]["s1"][1])
        cases = {  # name: (JSON literal, where it goes)
            "negative parent": ("-1", ("nodes", 1, "parent")),
            "forward parent": ("5", ("nodes", 1, "parent")),
            "string depth": ('"1"', ("nodes", 1, "depth")),
            "int path": ("5", ("nodes", 1, "path")),
            "zero interval": (s1_time, ("nodes", 1, "s2", 1)),
            "NaN time": ("NaN", ("nodes", 1, "s2", 1)),
            "overflowing time": ("1e400", ("nodes", 1, "s2", 1)),
            "huge integer time": ("9" * 400, ("nodes", 1, "s2", 1)),
            "overflowing rbytes": ("1e400", ("nodes", 1, "s1", 0)),
            "non-string not_cephfs": ("[1]", ("not_cephfs",)),
            "string roots": ('"/r"', ("roots",)),
            "non-string woke_up": ("[1]", ("nodes", 1, "woke_up")),
            "non-string unreadable": ('{"/r/u": 1}', ("nodes", 1, "unreadable")),
            "bool depth": ("true", ("nodes", 1, "depth")),
            "string vanished": ('"no"', ("nodes", 1, "vanished")),
            "string entries": ('"many"', ("nodes", 1, "entries")),
            "int list_error": ("7", ("nodes", 1, "list_error")),
            "string run delay": ('"nan"', ("delay",)),
            "float run probed": ("5.9", ("probed",)),
        }
        for name, (literal, keys) in cases.items():
            with self.subTest(name):
                state = json.loads(json.dumps(good))
                target = state
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = "@@"
                self.write(json.dumps(state).replace('"@@"', literal))
                self.assertIn(self.path, self.fatal())

    def test_failed_save_keeps_old_file_and_leaves_no_temp(self):
        self.write("old")
        for error in (
            OSError(errno.ENOSPC, "No space left on device"),
            KeyboardInterrupt(),
        ):
            with self.subTest(type(error).__name__):
                with (
                    mock.patch.object(sg.json, "dump", side_effect=error),
                    self.assertRaises((sg.FatalError, KeyboardInterrupt)),
                ):
                    sg.save_state(self.path, mkrun([mk("/r")]))
                with open(self.path) as f:
                    self.assertEqual(f.read(), "old")
                self.assertEqual(os.listdir(self.tmp.name), ["state.json"])

    def test_save_writes_through_a_symlink(self):
        link = os.path.join(self.tmp.name, "link.json")
        os.symlink(self.path, link)
        sg.save_state(link, mkrun([mk("/r")]))
        self.assertTrue(os.path.islink(link))
        self.assertEqual(sg.load_state(self.path).roots, ["/r"])

    def test_unwritable(self):
        with self.assertRaises(sg.FatalError) as cm:
            sg.save_state("/nonexistent/dir/state.json", mkrun([mk("/r")]))
        self.assertIn("can't write", cm.exception.args[0])


class ParseArgsTest(unittest.TestCase):
    def usage_error(self, *argv):
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as cm:
            sg.parse_args(list(argv))
        self.assertEqual(cm.exception.code, 2)
        return flat(err.getvalue())

    def test_defaults(self):
        args = sg.parse_args(["/r"])
        self.assertEqual(
            (args.delay, args.depth, args.max_dirs, args.max_entries, args.threads),
            (60.0, 5, 1000, 10_000, 32),
        )
        self.assertEqual((args.threshold, args.sort, args.json), (MiB, "depth", False))

    def test_load_conflicts_name_the_options(self):
        message = self.usage_error("--load", "f", "/r", "--delay", "5", "--save", "g")
        self.assertIn("--load can't be combined with ROOT, --delay, --save", message)
        for option in ("--depth", "--max-dirs", "--max-entries", "--threads"):
            with self.subTest(option):
                message = self.usage_error("--load", "f", option, "3")
                self.assertIn(f"with {option}", message)

    def test_load_takes_report_options(self):
        args = sg.parse_args(["--load", "f", "--threshold", "1kB/s", "--sort", "rate"])
        self.assertEqual((args.threshold, args.sort), (1000, "rate"))

    def test_needs_root_or_load(self):
        self.assertIn("give at least one ROOT", self.usage_error())

    def test_bad_values(self):
        cases = {
            ("--delay", "0"): "--delay must be a number above 0",
            ("--delay", "nan"): "--delay must be a number above 0",
            ("--delay", "inf"): "--delay must be a number above 0",
            ("--delay", "1e20"): "--delay must be a number above 0 and at most 86400",
            ("--delay", "86401"): "--delay must be a number above 0 and at most 86400",
            ("--depth", "-1"): "--depth must be 0 or more",
            ("--max-dirs", "0"): "--max-dirs must be at least 1",
            ("--max-entries", "0"): "--max-entries must be at least 1",
            ("--threads", "0"): "--threads must be at least 1",
            ("--threshold", "1M/s"): "bad rate '1M/s'",
        }
        for argv, expected in cases.items():
            with self.subTest(argv):
                self.assertIn(expected, self.usage_error("/r", *argv))

    def test_save_path_checked_before_sampling(self):
        with tempfile.TemporaryDirectory() as d:
            locked = os.path.join(d, "locked")
            os.mkdir(locked, 0o500)
            cases = {
                "": "--save '': needs a file name",
                d: "is a directory",
                "/nonexistent/dir/s.json": "no such directory: /nonexistent/dir",
            }
            if os.geteuid() != 0:  # root can write anywhere
                cases[os.path.join(locked, "s.json")] = f"can't write in {locked}"
            for path, expected in cases.items():
                with self.subTest(path):
                    self.assertIn(expected, self.usage_error("/r", "--save", path))

    def test_help_matches_code(self):
        epilog = sg.build_parser().epilog
        self.assertIn(f"within {sg.IDLE_GRACE:g} s", flat(epilog))
        for text in (*sg.KIND_TEXT.values(), sg.APPROX_TEXT):
            self.assertIn(text, epilog)

    def test_help_says_how_to_act_on_each_row(self):
        epilog = flat(sg.build_parser().epilog)
        node = sg.Node("/d", "/d", 0, entries=12345)
        for reason in sg.Reason:
            node.reason = reason
            note = sg.reason_note(node).replace("12345", "N")
            self.assertIn(f'"{note}"', epilog, reason)
        for remedy in ("lower --threshold", "--max-entries", "read permission"):
            self.assertIn(remedy, epilog)

    def test_help_warns_that_lagging_clocks_hide_growth(self):
        self.assertIn("files rate, without ~", flat(sg.build_parser().epilog))


class MainTest(unittest.TestCase):
    def fs(self):
        return FakeFs(
            {
                "/r": FakeDir(rbytes=growing(5 * MiB)),
                "/r/busy": FakeDir(rbytes=growing(5 * MiB)),
                "/r/quiet": FakeDir(rbytes=7),
                "/r/old": FakeDir(rctime=IDLE),
            }
        )

    def main(self, *argv, fs=None):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = sg.main(list(argv), fs=fs)
        return code, out.getvalue(), err.getvalue()

    def test_table(self):
        code, out, err = self.main("/r", "--delay", "10", fs=self.fs())
        self.assertEqual(code, 0)
        self.assertEqual(
            out, "     RATE  KIND    PATH     NOTE\n5.0 MiB/s  files   /r/busy\n"
        )
        self.assertIn(f"files   {sg.KIND_TEXT[sg.Kind.FILES]}", err)
        self.assertIn("Tracked 3 dirs under 1 root (probed 3 subdirs)", err)

    def test_sample_run_sleeps_between_passes(self):
        args = sg.parse_args(["/r", "--delay", "10"])
        with contextlib.redirect_stderr(io.StringIO()):
            run = sg.sample_run(args, self.fs())
        self.assertGreaterEqual(min(sg.intervals(run.nodes)), 10.0)

    def test_subdir_created_during_sleep_marks_parent(self):
        fs = FakeFs(
            {"/r": FakeDir(rbytes=growing(5 * MiB))}, after_sleep=add_new_subdir
        )
        code, out, _ = self.main("/r", fs=fs)
        self.assertEqual(code, 0)
        self.assertIn("5.0 MiB/s~ files   /r    1 subdir became active", out)

    def test_rename_during_sleep_is_not_growth(self):
        fs = FakeFs(
            {"/r": FakeDir(rbytes=6 * 10**9), "/r/job.tmp": FakeDir(rbytes=6 * 10**9)},
            after_sleep=lambda fs: fs.rename("/r/job.tmp", "/r/job"),
        )
        code, out, err = self.main("/r", fs=fs)
        self.assertEqual((code, out), (0, ""))
        self.assertIn("Nothing grew", err)
        self.assertIn("renamed during sampling", err)

    def test_new_subdir_behind_shrinking_files_marks_spread(self):
        kids = {f"/r/k{i}": FakeDir(rbytes=growing(0.5 * MiB)) for i in range(4)}
        fs = FakeFs(
            {"/r": FakeDir(rbytes=growing(1.5 * MiB)), **kids},
            after_sleep=add_new_subdir,
        )
        code, out, _ = self.main("/r", fs=fs)
        self.assertEqual(code, 0)
        self.assertIn("1.5 MiB/s~ spread  /r", out)

    def test_ctrl_c_during_build(self):
        class Interrupted(FakeFs):
            def getxattr(self, path, name):
                if path != "/r":
                    raise KeyboardInterrupt
                return super().getxattr(path, name)

        code, _, _ = self.main("/r", fs=Interrupted(self.fs().dirs))
        self.assertEqual(code, sg.EXIT_INTERRUPTED)

    def test_json(self):
        code, out, _ = self.main("/r", "--json", fs=self.fs())
        self.assertEqual(code, 0)
        doc = json.loads(out)
        self.assertEqual(
            [(r["path"], r["kind"]) for r in doc["rows"]], [("/r/busy", "files")]
        )

    def test_nothing_grew(self):
        code, out, err = self.main("/r", "--threshold", "1TiB/s", fs=self.fs())
        self.assertEqual((code, out), (0, ""))
        self.assertIn(
            "Nothing grew at 1.0 TiB/s or more; a lower --threshold shows slower "
            "growth.",
            flat(err),
        )

    def test_nothing_grew_among_readable_dirs(self):
        fs = FakeFs(
            {"/r": FakeDir(), "/r/denied": FakeDir(errors={sg.RCTIME: errno.EACCES})}
        )
        code, out, err = self.main("/r", fs=fs)
        self.assertEqual((code, out), (0, ""))
        self.assertIn("Nothing that could be read grew at 1.0 MiB/s or more;", err)

    def test_nothing_sampled_is_an_error(self):
        class FailsAfterPreflight(FakeFs):
            def getxattr(self, path, name):
                if name == sg.RBYTES and self.reads(sg.RBYTES):
                    raise oserror(errno.EACCES, path)
                return super().getxattr(path, name)

        with tempfile.TemporaryDirectory() as d:
            state = os.path.join(d, "state.json")
            code, out, err = self.main(
                "/r", "--save", state, fs=FailsAfterPreflight(self.fs().dirs)
            )
            self.assertTrue(os.path.exists(state))  # saved anyway, for a bug report
        self.assertEqual((code, out), (1, ""))
        self.assertNotIn("Nothing", err)
        self.assertIn("ERROR: no dir could be sampled", err)
        self.assertIn("/r (not sampled: Permission denied)", flat(err))

        code, out, _ = self.main("/r", "--json", fs=FailsAfterPreflight(self.fs().dirs))
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["summary"]["interval_min"], None)

    def test_save_then_load_reports_the_same(self):
        with tempfile.TemporaryDirectory() as d:
            state = os.path.join(d, "state.json")
            _, live, _ = self.main("/r", "--save", state, fs=self.fs())
            code, loaded, _ = self.main("--load", state)
            self.assertEqual((code, loaded), (0, live))
            _, lower, _ = self.main("--load", state, "--threshold", "1B/s")
        self.assertIn("/r/busy", lower)
        self.assertNotIn("/r/quiet", lower)  # constant rbytes: rate 0

    def test_bad_save_path_fails_before_any_io(self):
        fs = self.fs()
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            sg.main(["/r", "--save", "/nonexistent/dir/s.json"], fs=fs)
        self.assertEqual(fs.calls, [])

    def test_empty_load_path_is_an_error(self):
        code, _, err = self.main("--load", "")
        self.assertEqual(code, 1)
        self.assertIn("ERROR: can't read", err)

    def test_dir_deleted_mid_run_is_vanished_not_unreadable(self):
        cases = {
            "before listing": {sg.ENTRIES: errno.ENOENT, "list": errno.ENOENT,
                               sg.RBYTES: errno.ENOENT},
            "before sample 1": {sg.RBYTES: errno.ENOENT},
        }  # fmt: skip
        for name, errors in cases.items():
            with self.subTest(name):
                fs = FakeFs(
                    {
                        "/r": FakeDir(rbytes=growing(5 * MiB)),
                        "/r/tmp": FakeDir(errors=errors),
                    }
                )
                code, out, err = self.main("/r", fs=fs)
                self.assertEqual(code, 0)
                self.assertIn("5.0 MiB/s  files   /r", out)  # no ~
                self.assertNotIn("WARNING", err)
                self.assertIn("NOTE: 1 tracked dir vanished during the run", flat(err))

    def test_closed_pipe_still_saves_state(self):
        class ClosedPipe(io.StringIO):
            def write(self, text):
                raise BrokenPipeError(errno.EPIPE, "Broken pipe")

        class ClosedOnFlush(io.StringIO):  # a small report, still in the buffer
            def flush(self):
                raise BrokenPipeError(errno.EPIPE, "Broken pipe")

        for stdout in (ClosedPipe, ClosedOnFlush):
            with self.subTest(stdout.__name__), tempfile.TemporaryDirectory() as d:
                state = os.path.join(d, "state.json")
                with (
                    contextlib.redirect_stdout(stdout()),
                    contextlib.redirect_stderr(io.StringIO()),
                ):
                    code = sg.main(["/r", "--save", state], fs=self.fs())
                self.assertEqual(code, sg.EXIT_PIPE_CLOSED)
                self.assertEqual(sg.load_state(state).roots, ["/r"])

    def test_bad_root(self):
        code, out, err = self.main("/nope", fs=self.fs())
        self.assertEqual((code, out), (1, ""))
        self.assertIn("ERROR: /nope: no such directory", err)

    def test_too_many_dirs(self):
        code, _, err = self.main("/r", "--max-dirs", "2", fs=self.fs())
        self.assertEqual(code, 1)
        self.assertIn("ERROR: more than 2 dirs", err)

    def test_ctrl_c(self):
        def interrupt(fs):
            raise KeyboardInterrupt

        fs = FakeFs(self.fs().dirs, after_sleep=interrupt)
        self.assertEqual(self.main("/r", fs=fs)[0], 130)

    def test_non_utf8_names_printed_as_bytes(self):
        name = os.fsdecode(b"/r/caf\xe9")
        fs = FakeFs({"/r": FakeDir(), name: FakeDir(rbytes=growing(5 * MiB))})
        raw = io.BytesIO()
        out = io.TextIOWrapper(raw, encoding="utf-8", errors="strict")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(sg.main(["/r"], fs=fs), 0)
            out.flush()
        self.assertIn(b"/r/caf\xe9", raw.getvalue())


class RunCliTest(unittest.TestCase):
    def test_ctrl_c_exits_without_joining_workers(self):
        with (
            mock.patch.object(sg, "main", return_value=sg.EXIT_INTERRUPTED),
            mock.patch.object(sg.os, "_exit", side_effect=SystemExit) as hard_exit,
            self.assertRaises(SystemExit),
        ):
            sg.run_cli()
        hard_exit.assert_called_once_with(130)

    def test_other_statuses_exit_normally(self):
        with (
            mock.patch.object(sg, "main", return_value=1),
            mock.patch.object(sg.os, "_exit") as hard_exit,
            self.assertRaises(SystemExit) as cm,
        ):
            sg.run_cli()
        self.assertEqual(cm.exception.code, 1)
        hard_exit.assert_not_called()


class ScriptTest(unittest.TestCase):
    def run_script(self, *argv):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *argv],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def test_help(self):
        result = self.run_script("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Rows (only rates at or above --threshold", result.stdout)

    def test_usage_error(self):
        self.assertEqual(self.run_script().returncode, 2)


if __name__ == "__main__":
    unittest.main()
