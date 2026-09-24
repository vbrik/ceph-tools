"""Tests for backfillctl's dispatcher (__main__.py), run as a subprocess.

What's covered here is what the per-subcommand tests can't see, because they
build their own parsers (see _support.parse_args): the real top-level parser,
and that --load-state works before or after the subcommand name.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from _support import REPO_ROOT

FIXTURE = (
    REPO_ROOT
    / "tests"
    / "backfillctl"
    / "test-data"
    / "ceph1-backfills-stuck-at-100-pct"
)


def run_backfillctl(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / "backfillctl"), *argv],
        capture_output=True,
        text=True,
        check=False,
        # Plain help text even when the caller's shell sets FORCE_COLOR.
        env={k: v for k, v in os.environ.items() if k != "FORCE_COLOR"}
        | {"PYTHON_COLORS": "0"},
    )


class LoadStateTest(unittest.TestCase):
    def test_accepted_before_the_subcommand(self):
        result = run_backfillctl("--load-state", str(FIXTURE), "show-backfill")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PGID", result.stdout)

    def test_accepted_after_the_subcommand_with_the_same_output(self):
        before = run_backfillctl("--load-state", str(FIXTURE), "show-backfill")
        after = run_backfillctl("show-backfill", "--load-state", str(FIXTURE))
        self.assertEqual(after.returncode, 0, after.stderr)
        self.assertEqual((after.stdout, after.stderr), (before.stdout, before.stderr))

    def test_every_state_reading_subcommand_accepts_it_after_its_name(self):
        for argv in (
            ["show-pg-osds", "1.0"],
            ["show-backfill"],
            ["divert-toofull"],
            ["cancel-backfill"],
            ["cancel-uphill"],
            ["drain", "--osds", "0"],
            ["balance"],
        ):
            with self.subTest(command=argv[0]):
                result = run_backfillctl(*argv, "--load-state", "/nonexistent-dir")
                # Parsed, and read: the directory check is the run's own.
                self.assertIn("--load-state directory not found", result.stderr)

    def test_the_same_directory_twice_is_fine_different_ones_are_not(self):
        same = run_backfillctl(
            "--load-state", str(FIXTURE), "show-backfill", "--load-state", str(FIXTURE)
        )
        self.assertEqual(same.returncode, 0, same.stderr)
        different = run_backfillctl(
            "--load-state", "/x", "show-backfill", "--load-state", str(FIXTURE)
        )
        self.assertEqual(different.returncode, 2)
        self.assertIn("--load-state given twice", different.stderr)
        self.assertEqual(different.stdout, "")

    def test_listed_in_top_level_and_subcommand_help(self):
        self.assertIn("--load-state DIR", run_backfillctl("--help").stdout)
        for command in ("show-backfill", "cancel-backfill"):
            with self.subTest(command=command):
                help_text = run_backfillctl(command, "--help").stdout
                self.assertIn("[--load-state DIR]", help_text)
                self.assertIn("--load-state DIR  ", help_text)  # the option list

    def test_save_state_rejects_it_without_touching_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "capture"
            result = run_backfillctl(
                "--load-state", str(FIXTURE), "save-state", str(target)
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("--load-state does not apply", result.stderr)
            self.assertFalse(target.exists())

    def test_save_state_has_no_option_of_its_own(self):
        # save-state writes a capture; its own parser has no --load-state.
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "capture"
            result = run_backfillctl(
                "save-state", str(target), "--load-state", str(FIXTURE)
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("unrecognized arguments: --load-state", result.stderr)
            self.assertFalse(target.exists())

    def test_stdout_is_the_table_and_notes_follow_it_on_stderr(self):
        alone = run_backfillctl("--load-state", str(FIXTURE), "show-backfill")
        self.assertTrue(alone.stdout.startswith("PGID"))
        self.assertNotIn("movement(s)", alone.stdout)
        self.assertIn("movement(s)", alone.stderr)
        # 2>&1: stdout is flushed before each note, so the order holds.
        both = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "backfillctl"),
                "--load-state",
                str(FIXTURE),
                "show-backfill",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        ).stdout
        self.assertTrue(both.startswith("PGID"))
        self.assertLess(both.rindex("act+"), both.index("shard movement(s)"))

    def test_missing_directory_is_reported(self):
        result = run_backfillctl("--load-state", "/nonexistent-dir", "show-backfill")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--load-state directory not found", result.stderr)


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

    def test_every_subcommand_help_renders(self):
        """-h works (no stray '%' in help text) and keeps paragraphs apart."""
        for name in self.COMMANDS:
            with self.subTest(command=name):
                result = run_backfillctl(name, "-h")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("\n\n", result.stdout.split("options:")[0].strip())

    def test_shared_options_have_one_help_text(self):
        def option_help(command, option):
            """The help text of option in command's option list."""
            lines = run_backfillctl(command, "--help").stdout.splitlines()
            lines = lines[lines.index("options:") :]
            i = next(i for i, ln in enumerate(lines) if ln.startswith(f"  {option} "))
            block = [lines[i]]
            for line in lines[i + 1 :]:
                if not line.startswith(" " * 20):  # the next option, or the end
                    break
                block.append(line)
            return " ".join(" ".join(block).split()[2:])

        for option in ("--toofull-util", "--max-target-util", "--max-target-uses"):
            with self.subTest(option=option):
                self.assertEqual(
                    option_help("divert-toofull", option), option_help("drain", option)
                )

    def test_cancel_backfill_usage_shows_pin_blockers_needs_osds(self):
        usage = run_backfillctl("cancel-backfill", "-h").stdout
        self.assertIn("[--osds OSD [OSD ...] [--pin-blockers]]", usage)
