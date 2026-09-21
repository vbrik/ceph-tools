"""Unit tests for find-growing-dirs.py."""

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(REPO_ROOT, "cephfs", "find-growing-dirs.py")

spec = importlib.util.spec_from_file_location("find_growing_dirs", SCRIPT)
cg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cg)


class HumanTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(cg.human(0), "0.00 B")
        self.assertEqual(cg.human(1536), "1.50 KiB")
        self.assertEqual(cg.human(-1536), "-1.50 KiB")

    def test_rounds_up_to_next_unit(self):
        # 1023.995 rounds to "1024.00" at 2dp, which must roll over
        # to the next unit rather than displaying "1024.00 B".
        self.assertEqual(cg.human(1023.995), "1.00 KiB")
        self.assertEqual(cg.human(1048575.6), "1.00 MiB")


class SubdirsTest(unittest.TestCase):
    def test_skips_entry_that_fails_stat_but_keeps_others(self):
        with tempfile.TemporaryDirectory() as d:
            good = os.path.join(d, "good")
            bad = os.path.join(d, "bad")
            os.mkdir(good)
            os.mkdir(bad)

            real_scandir = os.scandir

            class FlakyEntry:
                def __init__(self, entry):
                    self._entry = entry
                    self.path = entry.path

                def is_dir(self, follow_symlinks=False):
                    if self._entry.path == bad:
                        raise OSError("stat race")
                    return self._entry.is_dir(follow_symlinks=follow_symlinks)

            class FlakyScandirIter:
                def __init__(self, path):
                    self._it = real_scandir(path)

                def __iter__(self):
                    return (FlakyEntry(e) for e in self._it)

                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    self._it.close()
                    return False

            with mock.patch.object(os, "scandir", side_effect=FlakyScandirIter):
                result = cg.subdirs(d)

            self.assertEqual(result, [good])

    def test_returns_empty_on_unreadable_dir(self):
        result = cg.subdirs("/nonexistent/path/for/testing")
        self.assertEqual(result, [])


class MeasureLevelTest(unittest.TestCase):
    def _run(self, before, after, top=10, subdir_list=None):
        calls = {"n": 0}

        def fake_sample(paths, pool):
            calls["n"] += 1
            d = before if calls["n"] == 1 else after
            return {p: d[p] for p in paths if p in d}

        with (
            mock.patch.object(
                cg, "subdirs", return_value=subdir_list or list(before)[1:]
            ),
            mock.patch.object(cg, "sample", side_effect=fake_sample),
            mock.patch.object(cg.time, "sleep", return_value=None),
        ):
            return cg.measure_level("/r", 1.0, top, None)

    def test_picks_largest_grower(self):
        before = {"/r": 1000, "/r/a": 100, "/r/b": 100}
        after = {"/r": 1300, "/r/a": 250, "/r/b": 150}
        self.assertEqual(self._run(before, after), "/r/a")

    def test_no_growers_returns_none(self):
        before = {"/r": 1000, "/r/a": 100}
        after = {"/r": 1000, "/r/a": 100}
        self.assertIsNone(self._run(before, after))

    def test_root_missing_from_second_sample_treated_as_unchanged(self):
        # Root vanishing from the "after" sample must not be reported as a
        # drop to zero bytes; it should read as no measurable change.
        before = {"/r": 1000, "/r/a": 100}
        after = {"/r/a": 100}
        result = self._run(before, after)
        self.assertIsNone(result)

    def test_top_truncation_does_not_lose_the_extra_bytes(self):
        before = {"/r": 1000, "/r/a": 100, "/r/b": 100, "/r/c": 100}
        after = {"/r": 1300, "/r/a": 250, "/r/b": 150, "/r/c": 100}
        # With top=1, "/r/b"'s growth must still be counted; it should not
        # silently vanish from accounting entirely.
        result = self._run(before, after, top=1)
        self.assertEqual(result, "/r/a")


class CliValidationTest(unittest.TestCase):
    def _run_cli(self, *args):
        return subprocess.run(
            [sys.executable, SCRIPT, "/tmp", *args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_negative_interval_rejected(self):
        result = self._run_cli("--interval", "-1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--interval must be non-negative", result.stderr)

    def test_negative_top_rejected(self):
        result = self._run_cli("--top", "-1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--top must be at least 1", result.stderr)

    def test_negative_workers_rejected(self):
        result = self._run_cli("--workers", "0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--workers must be at least 1", result.stderr)


if __name__ == "__main__":
    unittest.main()
