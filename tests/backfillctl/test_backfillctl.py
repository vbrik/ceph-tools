"""Tests for backfillctl's dispatcher (__main__.py) and entry points.

What's covered here is what the per-subcommand tests can't see, because they
build their own parsers (see _support.parse_args): the real top-level parser,
that --load-state works before or after the subcommand name, and that every
documented way of starting backfillctl works. Runs go through a subprocess,
as a user's would; help text only needs the parser, so it is rendered
in-process.
"""

import argparse
import contextlib
import io
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Iterator, Sequence
from pathlib import Path
from unittest import mock

import shtab
from _support import BACKFILLCTL, REPO_ROOT, TEST_DATA, child_env, run_command

from backfillctl import divert_toofull as dt
from backfillctl.__main__ import _COMMAND_MODULES, build_parser

FIXTURE = TEST_DATA / "ceph1-backfills-stuck-at-100-pct"


def command_names() -> list[str]:
    """Every subcommand the dispatcher registers, in its order."""
    subparsers = argparse.ArgumentParser().add_subparsers()
    for module in _COMMAND_MODULES:
        module.build_parser(subparsers)
    return list(subparsers.choices)


# What a subcommand needs on its command line besides --load-state.
REQUIRED_ARGS = {"show-pg-osds": ["1.0"], "drain": ["--osds", "0"]}


def help_text(*argv: str) -> str:
    """What 'backfillctl *argv --help' prints, 80 columns wide and uncolored."""
    out = io.StringIO()
    with (
        mock.patch.dict(os.environ, {"COLUMNS": "80", "PYTHON_COLORS": "0"}),
        contextlib.redirect_stdout(out),
    ):
        try:
            build_parser().parse_args([*argv, "--help"])
        except SystemExit as exc:
            if exc.code != 0:
                raise
    return out.getvalue()


def run_backfillctl(
    *argv: str,
    launch: Sequence[str] = BACKFILLCTL,
    cwd: Path | None = None,
    working_tree: bool = True,
) -> subprocess.CompletedProcess:
    """Run 'backfillctl *argv', started by the command line launch, in cwd.

    working_tree=False leaves PYTHONPATH alone, so the package is imported
    from wherever it is installed rather than from this repo's src/.
    """
    env = {k: v for k, v in os.environ.items() if k != "FORCE_COLOR"}
    if working_tree:
        env = child_env(env)
    return subprocess.run(
        [*launch, *argv],
        capture_output=True,
        text=True,
        check=False,
        cwd=cwd,
        # Plain text, wrapped at 80 columns, whatever the caller's shell sets.
        env=env | {"PYTHON_COLORS": "0", "COLUMNS": "80"},
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
        for name in command_names():
            if name == "save-state":  # writes a capture; see the tests below
                continue
            with self.subTest(command=name):
                result = run_backfillctl(
                    name, *REQUIRED_ARGS.get(name, []), "--load-state", "/nonexistent"
                )
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
        self.assertIn("--load-state DIR", help_text())
        for command in ("show-backfill", "cancel-backfill"):
            with self.subTest(command=command):
                text = help_text(command)
                self.assertIn("[--load-state DIR]", text)
                self.assertIn("--load-state DIR  ", text)  # the option list

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
        self.assertTrue(alone.stdout.splitlines()[1].startswith("PGID"))
        self.assertNotIn("movement(s)", alone.stdout)
        self.assertIn("movement(s)", alone.stderr)
        # 2>&1: stdout is flushed before each note, so the order holds.
        both = subprocess.run(
            [*BACKFILLCTL, "--load-state", str(FIXTURE), "show-backfill"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=child_env(),
            check=False,
        ).stdout
        # A note on the input first: this capture has no backfill positions.
        self.assertLess(both.index("has no backfill_positions"), both.index("PGID"))
        self.assertLess(both.rindex("act+"), both.index("copy movement(s)"))

    def test_missing_directory_is_reported(self):
        result = run_backfillctl("--load-state", "/nonexistent-dir", "show-backfill")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--load-state directory not found", result.stderr)


# Where 'pip install' puts the console script: next to this interpreter.
CONSOLE_SCRIPT = Path(sys.executable).parent / "backfillctl"


class EntryPointTest(unittest.TestCase):
    """Each way the README starts backfillctl prints the same."""

    ARGV = ("--load-state", str(FIXTURE), "show-backfill")

    def assert_like_reference(self, result: subprocess.CompletedProcess) -> None:
        reference = run_backfillctl(*self.ARGV)
        self.assertEqual(reference.returncode, 0, reference.stderr)
        self.assertEqual(
            (result.returncode, result.stdout, result.stderr),
            (0, reference.stdout, reference.stderr),
        )

    def test_python_m_backfillctl_outside_the_repo(self):
        # The package is found on the path, not relative to the current dir.
        with tempfile.TemporaryDirectory() as tmp:
            self.assert_like_reference(run_backfillctl(*self.ARGV, cwd=Path(tmp)))

    @unittest.skipUnless(
        CONSOLE_SCRIPT.exists(), "not installed: pip install -e . to test this"
    )
    def test_installed_console_script(self):
        # The package as installed, not src/: checks the [project.scripts]
        # entry, and flags a stale non-editable install of an older tree.
        with tempfile.TemporaryDirectory() as tmp:
            self.assert_like_reference(
                run_backfillctl(
                    *self.ARGV,
                    launch=[str(CONSOLE_SCRIPT)],
                    cwd=Path(tmp),
                    working_tree=False,
                )
            )


class TopLevelHelpTest(unittest.TestCase):
    COMMANDS = command_names()

    def test_every_subcommand_is_described(self):
        """Each subcommand is listed in --help followed by a description."""
        help_lines = help_text().splitlines()
        for name in self.COMMANDS:
            with self.subTest(command=name):
                line = next(
                    (ln.strip() for ln in help_lines if ln.strip().startswith(name)), ""
                )
                self.assertTrue(line, f"{name} not listed in --help")
                self.assertTrue(line.removeprefix(name).strip(), f"{name}: no help")

    def test_every_subcommand_help_renders(self):
        """--help works (no stray '%' in help text) and keeps paragraphs apart."""
        for name in self.COMMANDS:
            with self.subTest(command=name):
                self.assertIn("\n\n", help_text(name).split("options:")[0].strip())

    def test_shared_options_have_one_help_text(self):
        """An option several subcommands take reads the same in each."""

        def option_help(lines, option):
            """The help text of option in an option list, or None if absent."""
            i = next(
                (i for i, ln in enumerate(lines) if ln.startswith(f"  {option} ")), None
            )
            if i is None:
                return None
            block = [lines[i]]
            for line in lines[i + 1 :]:
                if not line.startswith(" " * 20):  # the next option, or the end
                    break
                block.append(line)
            return " ".join(" ".join(block).split()[2:])

        option_lists = {}
        for name in self.COMMANDS:
            lines = help_text(name).splitlines()
            option_lists[name] = lines[lines.index("options:") :]
        for option in ("--max-target-util", "--until-util"):
            with self.subTest(option=option):
                helps = {
                    name: text
                    for name, lines in option_lists.items()
                    if (text := option_help(lines, option)) is not None
                }
                self.assertGreater(len(helps), 1, helps)
                self.assertEqual(len(set(helps.values())), 1, helps)

    def test_cancel_backfill_usage_shows_pin_blockers_needs_osds(self):
        usage = help_text("cancel-backfill")
        self.assertIn("[--osds OSD [OSD ...] [--pin-blockers]]", usage)


# The README's one-time completion setup: bash-completion sources this file the
# first time <Tab> completes 'backfillctl', in each new shell.
COMPLETION_STUB = (
    'command -v backfillctl >/dev/null && eval "$(backfillctl --print-completion bash)"'
)


def all_actions(parser: argparse.ArgumentParser) -> Iterator[argparse.Action]:
    """Every argument of parser and, recursively, of its subcommands."""
    for action in parser._actions:
        yield action
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                yield from all_actions(subparser)


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class CompletionTest(unittest.TestCase):
    """--print-completion, and the README's setup that loads it in bash."""

    def setUp(self):
        # A 'backfillctl' on PATH that runs this working tree, as installed.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        launcher = self.bin / "backfillctl"
        launcher.write_text(f'#!/bin/sh\nexec {shlex.join(BACKFILLCTL)} "$@"\n')
        launcher.chmod(0o755)
        self.stub = self.tmp / "stub"
        self.stub.write_text(COMPLETION_STUB + "\n")

    def bash(self, script: str, path: str | None = None) -> subprocess.CompletedProcess:
        """Run script in a fresh bash, in self.tmp, after sourcing the stub.

        path replaces PATH; by default self.bin goes in front of it.
        """
        env = child_env()
        env["PATH"] = path or os.pathsep.join([str(self.bin), env["PATH"]])
        return subprocess.run(
            ["bash", "--norc", "--noprofile", "-c", f"source ./stub\n{script}"],
            capture_output=True,
            text=True,
            check=False,
            cwd=self.tmp,
            env=env,
        )

    def complete(self, *words: str) -> list[str]:
        """What <Tab> offers, sorted, after 'backfillctl *words' (the last
        word is the one being completed)."""
        result = self.bash(
            f"COMP_WORDS=(backfillctl {shlex.join(words)})\n"
            "COMP_CWORD=$(( ${#COMP_WORDS[@]} - 1 ))\n"
            'COMP_LINE="${COMP_WORDS[*]}"; COMP_POINT=${#COMP_LINE}\n'
            '_shtab_backfillctl && printf "%s\\n" "${COMPREPLY[@]}"\n'
        )
        self.assertEqual(result.stderr, "")
        return sorted(result.stdout.split())

    def test_readme_gives_the_setup_this_tests(self):
        self.assertIn(COMPLETION_STUB, (REPO_ROOT / "README.md").read_text())

    def test_bash_script_is_valid(self):
        script = run_backfillctl("--print-completion", "bash")
        self.assertEqual(script.returncode, 0, script.stderr)
        check = subprocess.run(
            ["bash", "-n"],
            input=script.stdout,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(check.returncode, 0, check.stderr)

    def test_completes_subcommands(self):
        self.assertEqual(self.complete(""), sorted(command_names()))
        self.assertEqual(self.complete("show-"), ["show-backfill", "show-pg-osds"])

    def test_dir_arguments_offer_only_directories(self):
        (self.tmp / "capture").mkdir()
        (self.tmp / "cat.txt").write_text("")
        for words in (
            ("--load-state", "ca"),
            ("measure-rate", "--save-state", "ca"),
            ("save-state", "ca"),
        ):
            with self.subTest(words=words):
                self.assertEqual(self.complete(*words), ["capture"])

    def test_every_dir_argument_is_marked_for_completion(self):
        dirs = [a for a in all_actions(build_parser()) if a.metavar == "DIR"]
        self.assertGreaterEqual(len(dirs), 4)  # --load-state twice, save dirs
        for action in dirs:
            with self.subTest(action=action.dest):
                self.assertEqual(getattr(action, "complete", None), shtab.DIRECTORY)

    def test_setup_is_silent_without_backfillctl_on_path(self):
        # E.g. <Tab> before the venv is activated: no completion, no error.
        result = self.bash("complete -p backfillctl 2>/dev/null", path="/usr/bin:/bin")
        self.assertEqual((result.stdout, result.stderr), ("", ""))
        self.assertNotEqual(result.returncode, 0)

    def test_setup_registers_the_completion(self):
        result = self.bash("complete -p backfillctl")
        self.assertIn("-F _shtab_backfillctl backfillctl", result.stdout)


class RunCommandTest(unittest.TestCase):
    """_support.run_command, which most end-to-end tests use instead of a
    subprocess, prints exactly what backfillctl does."""

    def test_matches_a_subprocess_run(self):
        fixture = TEST_DATA / "divert-toofull-osd457-down"
        for argv in (
            [],  # stderr paragraphs, and a table
            ["--max-target-util", "-5"],  # an argparse error, naming the program
        ):
            with self.subTest(argv=argv):
                real = run_backfillctl(
                    "--load-state", str(fixture), "divert-toofull", *argv
                )
                # Twice: nothing a run leaves behind may change the next one.
                for _ in range(2):
                    fake = run_command(dt, *argv, load_state=fixture)
                    self.assertEqual(
                        (fake.returncode, fake.stdout, fake.stderr),
                        (real.returncode, real.stdout, real.stderr),
                    )


if __name__ == "__main__":
    unittest.main()
