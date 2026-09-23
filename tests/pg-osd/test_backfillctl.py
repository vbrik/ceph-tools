"""Tests for backfillctl's dispatcher (__main__.py), run as a subprocess.

What's covered here is what the per-subcommand tests can't see, because they
build their own parsers (see _support.parse_args): the real top-level parser,
and that --load-state is global, accepted before the subcommand name only.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from _support import REPO_ROOT

FIXTURE = (
    REPO_ROOT / "tests" / "pg-osd" / "test-data" / "ceph1-backfills-stuck-at-100-pct"
)


def run_backfillctl(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / "backfillctl"), *argv],
        capture_output=True,
        text=True,
        check=False,
    )


class GlobalLoadStateTest(unittest.TestCase):
    def test_accepted_before_the_subcommand(self):
        result = run_backfillctl("--load-state", str(FIXTURE), "show-backfill")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PGID", result.stdout)

    def test_rejected_after_the_subcommand(self):
        result = run_backfillctl("show-backfill", "--load-state", str(FIXTURE))
        self.assertEqual(result.returncode, 2)
        self.assertIn("unrecognized arguments: --load-state", result.stderr)

    def test_listed_in_top_level_help_not_subcommand_help(self):
        self.assertIn("--load-state", run_backfillctl("--help").stdout)
        self.assertNotIn(
            "--load-state DIR", run_backfillctl("show-backfill", "--help").stdout
        )

    def test_missing_directory_is_reported(self):
        result = run_backfillctl("--load-state", "/nonexistent-dir", "show-backfill")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--load-state directory not found", result.stderr)

    def test_save_state_rejects_it_without_touching_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "capture"
            result = run_backfillctl(
                "--load-state", str(FIXTURE), "save-state", str(target)
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("--load-state does not apply", result.stderr)
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()


class TopLevelHelpTest(unittest.TestCase):
    COMMANDS = (
        "show-pg-osds",
        "show-backfill",
        "divert-toofull",
        "cancel-backfill",
        "cancel-uphill",
        "drain",
        "save-state",
    )

    def test_every_subcommand_is_described(self):
        """Each subcommand is listed in --help followed by a description."""
        help_lines = run_backfillctl("--help").stdout.splitlines()
        for name in self.COMMANDS:
            with self.subTest(command=name):
                line = next(
                    (ln.strip() for ln in help_lines if ln.strip().startswith(name)), ""
                )
                self.assertTrue(line, f"{name} not listed in --help")
                self.assertTrue(line.removeprefix(name).strip(), f"{name}: no help")
