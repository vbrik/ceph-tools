"""Unit tests for cephfs/top.py."""

import contextlib
import importlib.util
import io
import os
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(REPO_ROOT, "cephfs", "top.py")

spec = importlib.util.spec_from_file_location("cephfs_top", SCRIPT)
top = importlib.util.module_from_spec(spec)
spec.loader.exec_module(top)

CAPS_GROUP = ("recall_caps", "release_caps")
COMPLETED_GROUP = ("num_completed_requests", "num_completed_flushes")
NOTED_COLUMNS = [name for names in top.COLUMN_NOTES for name in names]


def notes_output(hidden):
    """Return (stdout, stderr) of print_column_notes with `hidden` columns removed."""
    columns = [c for c in top.build_columns() if c.name not in hidden]
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        top.print_column_notes(columns)
    return out.getvalue(), err.getvalue()


def defines(text, name):
    return any(line.startswith(f"  {name}: ") for line in text.splitlines())


class ColumnNotesTest(unittest.TestCase):
    def test_groups_cover_real_columns(self):
        self.assertLessEqual(set(NOTED_COLUMNS), set(top.COLUMN_NAMES))

    def test_each_noted_column_defined_on_its_own(self):
        for names, text in top.COLUMN_NOTES.items():
            for name in names:
                self.assertTrue(defines(text, name), f"{name} has no definition line")

    def test_fits_80_columns(self):
        for text in top.COLUMN_NOTES.values():
            for line in text.splitlines():
                self.assertLessEqual(len(line), 80, line)

    def test_all_groups_printed_to_stderr_only(self):
        out, err = notes_output(hidden=set())
        self.assertEqual(out, "")
        self.assertIn("Column notes:", err)
        for name in NOTED_COLUMNS:
            self.assertTrue(defines(err, name), name)

    def test_group_printed_while_one_of_its_columns_shown(self):
        _, err = notes_output(hidden={"recall_caps"})
        self.assertTrue(defines(err, "release_caps"))

    def test_group_omitted_when_its_columns_hidden(self):
        _, err = notes_output(hidden=set(CAPS_GROUP))
        self.assertFalse(any(defines(err, name) for name in CAPS_GROUP))
        self.assertTrue(all(defines(err, name) for name in COMPLETED_GROUP))

    def test_nothing_printed_when_all_noted_columns_hidden(self):
        self.assertEqual(notes_output(hidden=set(NOTED_COLUMNS)), ("", ""))


if __name__ == "__main__":
    unittest.main()
