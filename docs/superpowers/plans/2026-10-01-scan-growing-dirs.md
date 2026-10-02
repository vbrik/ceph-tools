# scan-growing-dirs.py Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `cephfs/scan-growing-dirs.py`, which finds every CephFS
directory growing faster than a threshold under one or more roots, in one
sampling interval, from `ceph.dir.rbytes` and `ceph.dir.rctime` alone.

**Architecture:** One standalone script, in eight sections: rates, the tree
model, analysis, rendering, filesystem access and preflight, sampling, saved
state, and the command line. I/O goes through an `Fs` object, so tests
substitute an in-memory `FakeFs` with a fake clock. `analyze()` is pure, so a
live run, `--load` and the tests all share the same decision code.

**Tech Stack:** Python 3.11+ standard library only (`argparse`,
`concurrent.futures`, `dataclasses`, `enum.StrEnum`), `unittest`, ruff 0.16.

**Spec:** `docs/superpowers/specs/2026-10-01-scan-growing-dirs-design.md`

## Global Constraints

- **Python and dependencies:** Python 3.11 or later; standard library only.
- **The script:** one executable file `cephfs/scan-growing-dirs.py`, mode
  755, starting with `#!/usr/bin/env python3` and
  `# SPDX-License-Identifier: MIT`.
- **Defaults, verbatim from the spec:** `--delay 60`, `--depth 5`,
  `--threshold 1MiB/s`, `--max-dirs 1000`, `--max-entries 10000`,
  `--threads 32` and `--sort depth`; `IDLE_GRACE = 30` s.
- **Output streams:** stdout carries only results (the table, or JSON).
  Everything else goes to stderr, as one wrapped paragraph per message with
  an `ERROR:`, `WARNING:` or `NOTE:` prefix.
- **Exit codes:** 0 for success (with or without rows); 1 for `ERROR:`; 2
  for usage errors; 130 for Ctrl-C.
- **Lint:** `ruff format` and `ruff check` must both pass. Ruff 0.16 runs
  its broad default rule set; the repo has no ruff config.
- **Tests:** unittest only, and no cluster needed. Run from the repo root
  (`/home/vbrik/proj/ceph-tools`):
  `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`.
- **Git:** no commits (CLAUDE.md); the user commits. Task 9 drafts the
  message.
- **Off limits:** don't modify `external/`, and don't add or delete files
  in `gitignore/`.
- **README:** keep `README.md` in sync (CLAUDE.md); Task 9 does this.

## Review Focus

These inputs are the likeliest to bite a user. Each one is pinned by a test
in the task that owns its code:

1. **Directory names that aren't UTF-8**, common on old research
   filesystems. They print as their original bytes rather than crashing the
   table with `UnicodeEncodeError`.
   `MainTest.test_non_utf8_names_printed_as_bytes`, Task 8.
2. **Roots given as `/mnt/x/`, through a symlink, or as siblings with a
   common prefix (`/r` and `/r2`).** Paths are normalized, duplicates are
   caught, and siblings aren't mistaken for nested roots.
   `PreflightTest.test_roots_become_depth_zero_nodes`,
   `test_duplicate_and_nested_roots` and
   `test_sibling_with_common_prefix_is_not_nested`, Task 4.
3. **An rctime in the future**, from a writer whose clock runs ahead (seen
   on this cluster). It counts as active, not idle.
   `BuildTest.test_cutoff_includes_grace_and_is_inclusive` (`/r/future`),
   Task 5.
4. **`--delay nan` or `--delay inf`.** Both are usage errors, rather than a
   NaN comparison that passes or a sleep that never ends.
   `ParseArgsTest.test_bad_values`, Task 8.
5. **Every sample failing**, e.g. permissions changed mid-run. The summary
   omits intervals, JSON gets nulls, and nothing crashes.
   `RenderTest.test_summary_without_samples`, Task 3.

## File Structure

- **Create `cephfs/scan-growing-dirs.py`:** the tool. Each task appends one
  section, in final order: Rates, Tree, Analysis, Rendering, Filesystem,
  Sampling, State, Command line. Each task also replaces the
  import/constants header at the top; the header only grows.
- **Create `tests/cephfs/_scan_support.py`:** loads the script as module
  `sg`, and holds `FakeDir`/`FakeFs` and the tree helpers `mk`, `bfs`,
  `mkrun` and `rates`. It follows the same pattern as
  `tests/backfillctl/_support.py`.
- **Create `tests/cephfs/test_scan_growing_dirs.py`:** the unit tests. Each
  task appends test classes above the `__main__` block and replaces the
  import header.
- **Modify `README.md`:** the "CephFS trees" list and the Python version
  under Requirements.
- **Already written:** the spec, which tracks this plan.

All code below was assembled from these exact task slices and run before the
plan was written. Every intermediate state passes `ruff format --check` and
`ruff check`, and each task's new tests fail before its code and pass after.
Copy the code verbatim.

---

### Task 1: Rates, and the test scaffolding

**Files:**
- Create: `cephfs/scan-growing-dirs.py`
- Create: `tests/cephfs/_scan_support.py`
- Create: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `parse_rate(text: str) -> float`: bytes/s. Raises `ValueError` whose
    message names the input and the accepted forms.
  - `rate_arg(text: str) -> float`: the same, but raises
    `argparse.ArgumentTypeError`.
  - `format_rate(rate: float) -> str`: e.g. `'52.1 MiB/s'`.
  - `plural(count: int, noun: str) -> str`: e.g. `'2 dirs'`.
  - Test support, as `from _scan_support import ...`: the loaded script
    `sg`; `MiB`; the fake wall-clock times `NOW`, `ACTIVE` and `IDLE`;
    - `mk(path, parent=None, rate=0.0, **fields) -> sg.Node`: samples
      rate MiB/s over 60 s; `rate=None` leaves the node unsampled;
    - `bfs(*roots) -> list[sg.Node]`;
    - `mkrun(nodes, probed=0, not_cephfs=()) -> sg.Run`;
    - `rates(rows) -> dict[(path, kind), MiB/s]`.
  - `mk`, `mkrun` and `rates` use `sg.Node`, `sg.Sample` and `sg.Run`,
    which Task 2 adds. Python resolves those names only when the helpers
    are called.

- [ ] **Step 1: Create the test support module**

`tests/cephfs/_scan_support.py`:

```python
"""Test support for scan-growing-dirs.py: the module itself, and helpers
that build sampled trees directly."""

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "cephfs" / "scan-growing-dirs.py"

_spec = importlib.util.spec_from_file_location("scan_growing_dirs", SCRIPT)
sg = importlib.util.module_from_spec(_spec)
# dataclasses resolves the script's string annotations through sys.modules.
sys.modules[_spec.name] = sg
_spec.loader.exec_module(sg)

MiB = 2**20
NOW = 2_000_000_000.0  # FakeFs wall-clock time at fake clock 0
ACTIVE = NOW - 1  # an rctime that counts as active
IDLE = NOW - 10_000  # an rctime that counts as idle


def mk(path, parent=None, rate=0.0, **fields):
    """A Node growing at rate MiB/s over a 60 s interval (rate None:
    unsampled), appended to parent's children."""
    node = sg.Node(
        path,
        root=parent.root if parent else path,
        depth=parent.depth + 1 if parent else 0,
        parent=parent,
        **fields,
    )
    if rate is not None:
        node.s1 = sg.Sample(10**12, 0.0)
        node.s2 = sg.Sample(10**12 + round(rate * MiB * 60), 60.0)
    if parent is not None:
        parent.children.append(node)
    return node


def bfs(*roots):
    """The trees under roots, in BFS order."""
    nodes, queue = [], list(roots)
    while queue:
        node = queue.pop(0)
        nodes.append(node)
        queue += node.children
    return nodes


def mkrun(nodes, probed=0, not_cephfs=()):
    """A Run of nodes (BFS order) with default limits."""
    return sg.Run(
        roots=[node.path for node in nodes if node.parent is None],
        nodes=nodes,
        delay=60.0,
        depth=5,
        max_dirs=1000,
        max_entries=10_000,
        created="2033-05-18T03:33:20+00:00",
        probed=probed,
        not_cephfs=list(not_cephfs),
    )


def rates(rows) -> dict[tuple[str, str], float]:
    """{(path, kind): rate in MiB/s} of rows, rounded to 3 decimals."""
    return {(row.path, str(row.kind)): round(row.rate / MiB, 3) for row in rows}
```

- [ ] **Step 2: Write the failing tests**

`tests/cephfs/test_scan_growing_dirs.py`:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import unittest

from _scan_support import MiB, sg


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


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=1)`, with errors like `FileNotFoundError: [Errno 2] No such file or directory: '…/cephfs/scan-growing-dirs.py'`.

- [ ] **Step 4: Create the script with the Rates section**

Create `cephfs/scan-growing-dirs.py` starting with this header:

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import re
```

then one blank line, then:

```python
# --- Rates ---------------------------------------------------------------

_SIZE_UNITS = {
    "b": 1,
    **{f"{prefix}b": 1000 ** (i + 1) for i, prefix in enumerate("kmgtp")},
    **{f"{prefix}ib": 1024 ** (i + 1) for i, prefix in enumerate("kmgtp")},
}
_DURATIONS = {"s": 1, "min": 60, "h": 3600, "d": 86400}
_RATE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*([a-z]+)/([a-z]+)", re.IGNORECASE)


def parse_rate(text: str) -> float:
    """Return a rate such as '1MiB/s' or '500MB/min' in bytes/s.

    Units are case-insensitive: B, kB..PB (powers of 1000) and KiB..PiB
    (powers of 1024). Durations are s, min, h and d. Raises ValueError for
    anything else, and for a rate of 0.
    """
    if match := _RATE_RE.fullmatch(text.strip()):
        number, unit, duration = match.groups()
        size = _SIZE_UNITS.get(unit.lower())
        seconds = _DURATIONS.get(duration.lower())
        if size and seconds:
            if (rate := float(number) * size / seconds) > 0:
                return rate
            raise ValueError(f"rate {text!r} must be above 0")
    raise ValueError(
        f"bad rate {text!r}: expected <number><unit>/<duration>, e.g. 1MiB/s or "
        "500MB/min, with units B, kB..PB or KiB..PiB and durations s, min, h, d"
    )


def rate_arg(text: str) -> float:
    """parse_rate() as an argparse type, so its message reaches the user."""
    try:
        return parse_rate(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def format_rate(rate: float) -> str:
    """Return bytes/s as e.g. '52.1 MiB/s': binary units, one decimal."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if round(abs(rate), 1) < 1024:
            return f"{rate:.1f} {unit}/s"
        rate /= 1024
    return f"{rate:.1f} PiB/s"


def plural(count: int, noun: str) -> str:
    """'1 dir', '2 dirs'."""
    return f"{count} {noun}{'' if count == 1 else 's'}"
```

Make it executable: `chmod 755 cephfs/scan-growing-dirs.py`

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 7 tests` … `OK`.

- [ ] **Step 6: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 7: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 2: Tree model and analysis

This task implements the measurement model and the row rules from the spec
(sections "Measurement model" and "Rows"):
- R(n) is `Node.rate`.
- M(n) is `measured()`.
- own(n) is `own_rate()`.
- `analyze()` builds the `files`, `tree` and `spread` rows bottom-up, with
  `~` for unmeasured subdirs.

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (the header; append the Tree and
  Analysis sections)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: `plural()` (Task 1).
- Produces:
  - `Reason`: a `StrEnum` of `DEPTH`, `WIDE`, `UNLISTABLE` and `IDLE`.
  - `Kind`: a `StrEnum` of `FILES`, `SPREAD` and `TREE`.
  - `KIND_TEXT: dict[Kind, str]` and `APPROX_TEXT: str`, shared by the
    footnotes and `--help`.
  - `Sample(rbytes: int, t: float)`.
  - `Node(path, root, depth, parent=None, children=[], reason=None,
    entries=None, list_error=None, idle={}, unreadable={},
    woke_up=[], s1=None, s2=None, sample_error=None, vanished=False)`,
    with the property `Node.rate -> float | None`.
  - `measured(node) -> float` and `own_rate(node) -> float | None`.
  - `Run(roots, nodes, delay, depth, max_dirs, max_entries, created,
    probed=0, not_cephfs=[])`.
  - `Row(path, root, depth, kind, rate, approx, note, unreadable=(),
    woke_up=())`, frozen.
  - `analyze(nodes: list[Node], threshold: float) -> list[Row]`.
  - `reason_note(node) -> str` and `unmeasured_note(unreadable, woke_up)
    -> str`.

- [ ] **Step 1: Write the failing tests**

In `tests/cephfs/test_scan_growing_dirs.py`, replace everything above `class ParseRateTest` with:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import unittest

from _scan_support import IDLE, MiB, bfs, mk, rates, sg

T = 1 * MiB  # the default threshold, in bytes/s
```

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=16)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'analyze'`.

- [ ] **Step 3: Implement**

In `cephfs/scan-growing-dirs.py`, replace everything above `# --- Rates` with this header (it only adds imports and constants):

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import enum
import re
from dataclasses import dataclass, field
```

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
# --- Tree ----------------------------------------------------------------


class Reason(enum.StrEnum):
    """Why a tracked dir is a leaf whose subdirs weren't explored."""

    DEPTH = "depth"  # at --depth
    WIDE = "wide"  # more than --max-entries entries
    UNLISTABLE = "unlistable"  # its entries or subdirs couldn't be read
    IDLE = "idle"  # a root that was idle at the start


class Kind(enum.StrEnum):
    FILES = "files"
    SPREAD = "spread"
    TREE = "tree"


# What each row kind and the ~ mark mean, for --help and the footnotes.
KIND_TEXT = {
    Kind.FILES: "growth of files directly in the dir",
    Kind.SPREAD: "growth in the dir's subtree that no row below it shows",
    Kind.TREE: "growth of a subtree that wasn't explored (NOTE says why)",
}
APPROX_TEXT = "may include growth in subdirs that couldn't be measured"


@dataclass(slots=True)
class Sample:
    rbytes: int
    t: float  # time.monotonic() midpoint of the getxattr call


# eq=False: nodes are dict keys by identity.
@dataclass(eq=False)
class Node:
    """A tracked dir: a root, or a subdir that was active at build time."""

    path: str
    root: str
    depth: int  # levels below root
    parent: Node | None = None
    children: list[Node] = field(default_factory=list)  # tracked subdirs
    reason: Reason | None = None  # None: its subdirs were listed and probed
    entries: int | None = None  # ceph.dir.entries, for Reason.WIDE
    list_error: str | None = None  # for Reason.UNLISTABLE
    idle: dict[str, float] = field(default_factory=dict)  # subdir -> rctime
    unreadable: dict[str, str] = field(default_factory=dict)  # subdir -> error
    woke_up: list[str] = field(default_factory=list)  # idle subdirs that changed
    s1: Sample | None = None
    s2: Sample | None = None
    sample_error: str | None = None  # why s1 or s2 is missing
    vanished: bool = False  # gone by sample 2, which counts it as 0 bytes

    @property
    def rate(self) -> float | None:
        """R(n): the subtree's growth in bytes/s, or None if unsampled."""
        if self.s1 is None or self.s2 is None:
            return None
        return (self.s2.rbytes - self.s1.rbytes) / (self.s2.t - self.s1.t)


def measured(node: Node) -> float:
    """M(n): R(n) if node was sampled, else the sum of M over its children.

    Subtracting M rather than R keeps the growth of rows found below an
    unsampled dir from also counting in its parent's own rate.
    """
    if (rate := node.rate) is not None:
        return rate
    return sum(measured(child) for child in node.children)


def own_rate(node: Node) -> float | None:
    """own(n) in bytes/s: growth of the files directly in a listed, sampled
    dir, plus that of its unmeasured subdirs. None for other dirs."""
    if node.reason is not None or (rate := node.rate) is None:
        return None
    return rate - sum(measured(child) for child in node.children)


@dataclass
class Run:
    """The tracked dirs and how they were found: what --save writes."""

    roots: list[str]
    nodes: list[Node]  # BFS order, roots first
    delay: float
    depth: int
    max_dirs: int
    max_entries: int
    created: str  # ISO 8601 time sampling started
    probed: int = 0  # rctime probes while building the tree
    not_cephfs: list[str] = field(default_factory=list)  # skipped subdirs


# --- Analysis ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Row:
    path: str
    root: str
    depth: int
    kind: Kind
    rate: float  # bytes/s
    approx: bool  # may include unmeasured subdirs: the ~ mark
    note: str
    unreadable: tuple[str, ...] = ()  # unmeasured subdirs behind approx
    woke_up: tuple[str, ...] = ()


@dataclass
class _Uncovered:
    """A subtree's state while analyze() works bottom-up."""

    covered: float = 0.0  # sum of the rates of the subtree's rows
    unreadable: list[str] = field(default_factory=list)  # in the uncovered part
    woke_up: list[str] = field(default_factory=list)


def analyze(nodes: list[Node], threshold: float) -> list[Row]:
    """Return the rows whose rate is at least threshold (bytes/s).

    nodes must be in BFS order. A listed dir gets a files row for its own
    rate; an unexplored leaf a tree row for its subtree's; and a non-leaf a
    spread row for growth that no row below it shows. Rows come out
    bottom-up; order them with sort_rows().
    """
    state: dict[Node, _Uncovered] = {}
    rows: list[Row] = []

    def add_row(node: Node, kind: Kind, rate: float, unreadable=(), woke_up=()):
        if kind is Kind.TREE:
            note = reason_note(node)
        else:
            note = unmeasured_note(unreadable, woke_up)
        rows.append(
            Row(
                node.path,
                node.root,
                node.depth,
                kind,
                rate,
                bool(unreadable or woke_up),
                note,
                tuple(unreadable),
                tuple(woke_up),
            )
        )

    for node in reversed(nodes):
        acc = state[node] = _Uncovered()
        for child in node.children:
            sub = state.pop(child)
            acc.covered += sub.covered
            acc.unreadable += sub.unreadable
            acc.woke_up += sub.woke_up
        if (rate := node.rate) is None:
            continue  # unsampled: its parent counts it as unmeasured
        if node.reason is not None:
            if rate >= threshold:
                add_row(node, Kind.TREE, rate)
                acc.covered += rate
            continue
        own = own_rate(node)
        unreadable = [
            *node.unreadable,
            *(child.path for child in node.children if child.rate is None),
        ]
        if own >= threshold:
            add_row(node, Kind.FILES, own, unreadable, node.woke_up)
            acc.covered += own
        else:
            acc.unreadable += unreadable
            acc.woke_up += node.woke_up
        if node.children and rate - acc.covered >= threshold:
            add_row(node, Kind.SPREAD, rate - acc.covered, acc.unreadable, acc.woke_up)
            state[node] = _Uncovered(covered=rate)
    return rows


def reason_note(node: Node) -> str:
    """NOTE for a tree row: why its subtree wasn't explored."""
    match node.reason:
        case Reason.DEPTH:
            return "depth limit"
        case Reason.WIDE:
            return f"{node.entries} entries"
        case Reason.UNLISTABLE:
            return "unlistable"
        case Reason.IDLE:
            return "idle at start"
    raise ValueError(f"{node.path} has no leaf reason")


def unmeasured_note(unreadable, woke_up) -> str:
    """NOTE for a ~ row: the unmeasured subdirs its rate may include."""
    parts = []
    if unreadable:
        parts.append(plural(len(unreadable), "unreadable subdir"))
    if woke_up:
        parts.append(f"{plural(len(woke_up), 'subdir')} became active")
    return "; ".join(parts)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 23 tests` … `OK`.

- [ ] **Step 5: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 6: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 3: Sorting and rendering

This task covers sorting, the table, the footnotes, the summary line, the
permission warning and other notes, and the `--json` document. It
implements the spec's "Output" section, except for `--save`/`--load`.

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (the header; append the sorting
  code and the Rendering section)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: `Row`, `Kind`, `KIND_TEXT`, `APPROX_TEXT`, `Node`, `Run` and
  `analyze()` (Task 2); `format_rate()` and `plural()` (Task 1).
- Produces:
  - `sort_rows(rows, key: str) -> list[Row]`, where key is `'depth'` or
    `'rate'`.
  - Text output, each returning `str`: `render_table(rows)`,
    `footnotes(rows)`, `summary(run)` and `paragraph(text)`.
    `_legend(kinds, approx: bool, indent="")` is used again by `--help` in
    Task 8.
  - `intervals(nodes) -> list[float]`.
  - `read_problems(nodes) -> list[tuple[str, str]]`.
  - `some_paths(items) -> str` and `notes(run) -> list[str]`.
  - `render_json(run, rows, threshold) -> str`.
  - The constants `MAX_LISTED_PATHS = 10` and `JSON_VERSION = 1`.

- [ ] **Step 1: Write the failing tests**

In `tests/cephfs/test_scan_growing_dirs.py`, replace everything above `class ParseRateTest` with:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import json
import unittest

from _scan_support import IDLE, MiB, bfs, mk, mkrun, rates, sg

T = 1 * MiB  # the default threshold, in bytes/s
```

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
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
                    "NOTE: 1 tracked dir vanished during sampling; each counts as "
                    "shrinking to 0 bytes."
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=13)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'footnotes'`.

- [ ] **Step 3: Implement**

In `cephfs/scan-growing-dirs.py`, replace everything above `# --- Rates` with this header (it only adds imports and constants):

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import enum
import json
import re
import textwrap
from dataclasses import asdict, dataclass, field

# Warnings and notes name at most this many paths; --json lists them all.
MAX_LISTED_PATHS = 10

JSON_VERSION = 1
```

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
_KIND_ORDER = {Kind.FILES: 0, Kind.SPREAD: 1, Kind.TREE: 2}


def _depth_key(row: Row) -> tuple:
    return row.depth, row.path, _KIND_ORDER[row.kind]


def sort_rows(rows: list[Row], key: str) -> list[Row]:
    """Rows by 'depth' (shallow first, then path) or 'rate' (fastest first,
    then depth and path). Kind breaks the remaining ties."""
    if key == "rate":
        return sorted(rows, key=lambda row: (-row.rate, *_depth_key(row)))
    return sorted(rows, key=_depth_key)


# --- Rendering -----------------------------------------------------------

_KIND_WIDTH = max(map(len, Kind))


def render_table(rows: list[Row]) -> str:
    """The rows as an aligned table under a header line."""
    rates = [format_rate(row.rate) for row in rows]
    rate_w = max(map(len, ["RATE", *rates]))
    path_w = max(map(len, ["PATH", *(row.path for row in rows)]))
    lines = [f"{'RATE':>{rate_w}}  {'KIND':<{_KIND_WIDTH}}  {'PATH':<{path_w}}  NOTE"]
    for row, rate in zip(rows, rates, strict=True):
        mark = "~" if row.approx else " "
        lines.append(
            f"{rate:>{rate_w}}{mark} {row.kind:<{_KIND_WIDTH}}  "
            f"{row.path:<{path_w}}  {row.note}"
        )
    return "\n".join(line.rstrip() for line in lines)


def _legend(kinds, approx: bool, indent: str = "") -> str:
    lines = [
        f"{indent}{kind:<{_KIND_WIDTH}}  {text}"
        for kind, text in KIND_TEXT.items()
        if kind in kinds
    ]
    if approx:
        lines.append(f"{indent}{'~':<{_KIND_WIDTH}}  {APPROX_TEXT}")
    return "\n".join(lines)


def footnotes(rows: list[Row]) -> str:
    """One line for each row kind, and for ~, that appears in rows."""
    return _legend({row.kind for row in rows}, any(row.approx for row in rows))


def intervals(nodes: list[Node]) -> list[float]:
    """The sample interval of each sampled node, in seconds."""
    return [
        node.s2.t - node.s1.t
        for node in nodes
        if node.s1 is not None and node.s2 is not None
    ]


def summary(run: Run) -> str:
    """One line: how many dirs were tracked and probed, and the intervals."""
    text = (
        f"Tracked {plural(len(run.nodes), 'dir')} under "
        f"{plural(len(run.roots), 'root')} "
        f"(probed {plural(run.probed, 'subdir')})"
    )
    if spans := intervals(run.nodes):
        text += f"; sample intervals {min(spans):.1f}-{max(spans):.1f} s"
    return text + "."


def read_problems(nodes: list[Node]) -> list[tuple[str, str]]:
    """(path, problem) for each dir that couldn't be read, in BFS order."""
    found = []
    for node in nodes:
        if node.list_error is not None:
            found.append((node.path, f"unlistable: {node.list_error}"))
        if node.sample_error is not None:
            found.append((node.path, f"not sampled: {node.sample_error}"))
        found += [(path, f"unreadable: {err}") for path, err in node.unreadable.items()]
    return found


def some_paths(items: list[str]) -> str:
    """Up to MAX_LISTED_PATHS of items, comma-separated, then '+N more'."""
    text = ", ".join(items[:MAX_LISTED_PATHS])
    if (more := len(items) - MAX_LISTED_PATHS) > 0:
        text += f", +{more} more (--json lists all)"
    return text


def notes(run: Run) -> list[str]:
    """Warnings and notes about what couldn't be measured, one per paragraph."""
    found = []
    if problems := read_problems(run.nodes):
        found.append(
            f"WARNING: couldn't read {plural(len(problems), 'dir')}: "
            + some_paths([f"{path} ({problem})" for path, problem in problems])
            + ". Growth in an unreadable or unsampled dir is counted in its "
            "parent's rate, marked ~; an unlistable dir is measured as a whole "
            "subtree."
        )
    if run.not_cephfs:
        found.append(
            f"NOTE: skipped {plural(len(run.not_cephfs), 'subdir')} not on "
            f"CephFS (mount points?): {some_paths(run.not_cephfs)}."
        )
    if vanished := sum(node.vanished for node in run.nodes):
        found.append(
            f"NOTE: {plural(vanished, 'tracked dir')} vanished during sampling; "
            "each counts as shrinking to 0 bytes."
        )
    return found


def paragraph(text: str) -> str:
    """text wrapped for a terminal, never breaking paths."""
    return textwrap.fill(text, width=79, break_long_words=False, break_on_hyphens=False)


def render_json(run: Run, rows: list[Row], threshold: float) -> str:
    """The rows and the run's problems and summary as a JSON document."""
    spans = intervals(run.nodes)
    return json.dumps(
        {
            "version": JSON_VERSION,
            "threshold": threshold,
            "rows": [asdict(row) for row in rows],
            "unreadable": [
                {"path": path, "problem": problem}
                for path, problem in read_problems(run.nodes)
            ],
            "not_cephfs": run.not_cephfs,
            "summary": {
                "roots": len(run.roots),
                "tracked": len(run.nodes),
                "probed": run.probed,
                "interval_min": min(spans, default=None),
                "interval_max": max(spans, default=None),
            },
        },
        indent=2,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 36 tests` … `OK`.

- [ ] **Step 5: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 6: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 4: Filesystem access and preflight

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (the header; append the Filesystem
  section)
- Modify: `tests/cephfs/_scan_support.py` (replace it with the version
  below, which adds `FakeDir`, `FakeFs`, `oserror` and `growing`)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: `Node` (Task 2).
- Produces:
  - `FatalError(*messages)`: `main()` prints each message after `ERROR:`.
  - `RBYTES`, `RCTIME`, `ENTRIES` and `NOT_CEPHFS`.
  - `Fs`, with `getxattr(path, name) -> bytes`; `subdirs(path) ->
    list[str]`, which is sorted, doesn't follow symlinks, and raises
    `OSError` if the dir can't be listed; `exists`, `isdir` and
    `realpath`; and the clocks `clock()` (monotonic), `now()` (wall time)
    and `sleep(s)`.
  - `_strerror(exc) -> str`.
  - `preflight(paths: list[str], fs) -> list[Node]`: depth-0 nodes. It
    raises `FatalError` with one argument per problem.
  - Test support:
    - `FakeDir(rctime=ACTIVE, rbytes=0 | f(t), entries=None, errors={})`.
    - `FakeFs(dirs, files=(), latency=0.001)`, which also has `.links`,
      `.calls`, `.listed()` and `.reads(name)`.
    - `oserror(code, path)`.
    - `growing(rate, start=10**9)`.

- [ ] **Step 1: Extend the test support module**

Replace `tests/cephfs/_scan_support.py` with:

```python
"""Test support for scan-growing-dirs.py: the module itself, a fake CephFS
with a fake clock, and helpers that build sampled trees directly."""

import errno
import importlib.util
import os
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "cephfs" / "scan-growing-dirs.py"

_spec = importlib.util.spec_from_file_location("scan_growing_dirs", SCRIPT)
sg = importlib.util.module_from_spec(_spec)
# dataclasses resolves the script's string annotations through sys.modules.
sys.modules[_spec.name] = sg
_spec.loader.exec_module(sg)

MiB = 2**20
NOW = 2_000_000_000.0  # FakeFs wall-clock time at fake clock 0
ACTIVE = NOW - 1  # an rctime that counts as active
IDLE = NOW - 10_000  # an rctime that counts as idle


def oserror(code: int, path: str = "") -> OSError:
    return OSError(code, os.strerror(code), path)


def growing(rate: float, start: int = 10**9) -> Callable[[float], int]:
    """rbytes that grow by rate bytes per second of fake clock time."""
    return lambda t: start + int(rate * t)


@dataclass
class FakeDir:
    """A directory in FakeFs. rbytes may be a function of the fake clock."""

    rctime: float = ACTIVE
    rbytes: int | Callable[[float], int] = 0
    entries: int | None = None  # None: the number of subdirs
    errors: dict[str, int] = field(default_factory=dict)  # xattr or "list" -> errno


class FakeFs:
    """An in-memory CephFS. Each getxattr and listing takes `latency` seconds
    of fake clock time; sleep() advances the clock without waiting."""

    def __init__(self, dirs: dict[str, FakeDir], files=(), latency=0.001):
        self.dirs = dirs
        self.files = set(files)
        self.links: dict[str, str] = {}  # path -> realpath
        self.latency = latency
        self.t = 0.0
        self.calls: list[tuple] = []
        self.lock = threading.Lock()

    def _call(self, *call) -> float:
        with self.lock:
            self.calls.append(call)
            self.t += self.latency
            return self.t

    def _dir(self, path: str, op: str) -> FakeDir:
        if (d := self.dirs.get(path)) is None:
            raise oserror(errno.ENOENT, path)
        if op in d.errors:
            raise oserror(d.errors[op], path)
        return d

    def children(self, path: str) -> list[str]:
        return sorted(p for p in self.dirs if p != path and os.path.dirname(p) == path)

    def getxattr(self, path, name):
        t = self._call("getxattr", path, name)
        d = self._dir(path, name)
        if name == sg.RBYTES:
            value = d.rbytes(t) if callable(d.rbytes) else d.rbytes
        elif name == sg.RCTIME:
            value = d.rctime
        elif name == sg.ENTRIES:
            value = len(self.children(path)) if d.entries is None else d.entries
        else:
            raise oserror(errno.ENODATA, path)
        return str(value).encode()

    def subdirs(self, path):
        self._call("list", path)
        self._dir(path, "list")
        return self.children(path)

    def exists(self, path):
        return path in self.dirs or path in self.files

    def isdir(self, path):
        return path in self.dirs

    def realpath(self, path):
        return self.links.get(path, path)

    def clock(self):
        with self.lock:
            return self.t

    def now(self):
        return NOW + self.clock()

    def sleep(self, seconds):
        with self.lock:
            self.calls.append(("sleep", seconds))
            self.t += seconds

    def listed(self) -> list[str]:
        """Paths listed so far, in call order."""
        return [call[1] for call in self.calls if call[0] == "list"]

    def reads(self, name: str) -> list[str]:
        """Paths whose xattr `name` was read so far, in call order."""
        return [c[1] for c in self.calls if c[0] == "getxattr" and c[2] == name]


def mk(path, parent=None, rate=0.0, **fields):
    """A Node growing at rate MiB/s over a 60 s interval (rate None:
    unsampled), appended to parent's children."""
    node = sg.Node(
        path,
        root=parent.root if parent else path,
        depth=parent.depth + 1 if parent else 0,
        parent=parent,
        **fields,
    )
    if rate is not None:
        node.s1 = sg.Sample(10**12, 0.0)
        node.s2 = sg.Sample(10**12 + round(rate * MiB * 60), 60.0)
    if parent is not None:
        parent.children.append(node)
    return node


def bfs(*roots):
    """The trees under roots, in BFS order."""
    nodes, queue = [], list(roots)
    while queue:
        node = queue.pop(0)
        nodes.append(node)
        queue += node.children
    return nodes


def mkrun(nodes, probed=0, not_cephfs=()):
    """A Run of nodes (BFS order) with default limits."""
    return sg.Run(
        roots=[node.path for node in nodes if node.parent is None],
        nodes=nodes,
        delay=60.0,
        depth=5,
        max_dirs=1000,
        max_entries=10_000,
        created="2033-05-18T03:33:20+00:00",
        probed=probed,
        not_cephfs=list(not_cephfs),
    )


def rates(rows) -> dict[tuple[str, str], float]:
    """{(path, kind): rate in MiB/s} of rows, rounded to 3 decimals."""
    return {(row.path, str(row.kind)): round(row.rate / MiB, 3) for row in rows}
```

- [ ] **Step 2: Write the failing tests**

In `tests/cephfs/test_scan_growing_dirs.py`, replace everything above `class ParseRateTest` with:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import errno
import json
import os
import tempfile
import unittest

from _scan_support import IDLE, FakeDir, FakeFs, MiB, bfs, mk, mkrun, rates, sg

T = 1 * MiB  # the default threshold, in bytes/s
```

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
class RealFsTest(unittest.TestCase):
    def test_subdirs_skips_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as d:
            for name in ("b", "a"):
                os.mkdir(os.path.join(d, name))
            open(os.path.join(d, "file"), "w").close()
            os.symlink(os.path.join(d, "a"), os.path.join(d, "link"))
            self.assertEqual(
                sg.Fs().subdirs(d), [os.path.join(d, "a"), os.path.join(d, "b")]
            )

    def test_subdirs_of_missing_dir_raises(self):
        with self.assertRaises(OSError):
            sg.Fs().subdirs("/nonexistent/scan-growing-dirs-test")


class PreflightTest(unittest.TestCase):
    def fs(self, **dirs):
        return FakeFs(
            {"/r": FakeDir(), "/r/a": FakeDir(), "/s": FakeDir(), **dirs}, files={"/f"}
        )

    def problems(self, paths, fs=None):
        with self.assertRaises(sg.FatalError) as cm:
            sg.preflight(paths, fs or self.fs())
        return list(cm.exception.args)

    def test_roots_become_depth_zero_nodes(self):
        roots = sg.preflight(["/r", "/s/"], self.fs())
        self.assertEqual(
            [(n.path, n.root, n.depth) for n in roots],
            [("/r", "/r", 0), ("/s", "/s", 0)],
        )

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
        roots = sg.preflight(["/r", "/r2"], self.fs(**{"/r2": FakeDir()}))
        self.assertEqual(len(roots), 2)

    def test_all_problems_reported_together(self):
        self.assertEqual(len(self.problems(["/nope", "/f", "/r", "/r/a"])), 3)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=9)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'FatalError'`.

- [ ] **Step 4: Implement**

In `cephfs/scan-growing-dirs.py`, replace everything above `# --- Rates` with this header (it only adds imports and constants):

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import enum
import errno
import json
import os
import re
import textwrap
import time
from dataclasses import asdict, dataclass, field

RBYTES = "ceph.dir.rbytes"
RCTIME = "ceph.dir.rctime"
ENTRIES = "ceph.dir.entries"

# Warnings and notes name at most this many paths; --json lists them all.
MAX_LISTED_PATHS = 10

JSON_VERSION = 1

# getxattr errnos meaning the directory isn't on CephFS.
NOT_CEPHFS = frozenset({errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP})


class FatalError(Exception):
    """Stops the run. main() prints each argument after 'ERROR: ' and exits 1."""
```

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
# --- Filesystem ------------------------------------------------------------


class Fs:
    """The filesystem and the clocks. Tests substitute a fake."""

    def getxattr(self, path: str, name: str) -> bytes:
        return os.getxattr(path, name)

    def subdirs(self, path: str) -> list[str]:
        """Sorted paths of path's subdirs, not following symlinks.

        Raises OSError if path can't be listed. Skips entries that vanish
        while it is listed.
        """
        found = []
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        found.append(entry.path)
                except OSError:
                    pass
        return sorted(found)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def isdir(self, path: str) -> bool:
        return os.path.isdir(path)

    def realpath(self, path: str) -> str:
        return os.path.realpath(path)

    def clock(self) -> float:
        return time.monotonic()

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def _strerror(exc: OSError) -> str:
    return exc.strerror or str(exc)


def _xattr_problem(exc: OSError) -> str:
    """Why a root's ceph.dir.* attributes couldn't be read."""
    if exc.errno in NOT_CEPHFS:
        return "no ceph.dir.* attributes; is it on a CephFS mount?"
    if exc.errno in (errno.EACCES, errno.EPERM):
        return (
            "permission denied reading ceph.dir.* attributes (they need read "
            "permission on the directory)"
        )
    return f"can't read ceph.dir.* attributes: {_strerror(exc)}"


def preflight(paths: list[str], fs: Fs) -> list[Node]:
    """Return a depth-0 Node for each root, its path made absolute.

    Raises FatalError, with one argument per problem, if any root is
    missing, isn't a directory, has unreadable or no ceph.dir.* attributes,
    or is the same as or inside another root.
    """
    problems = []
    roots = []
    for given in paths:
        path = os.path.abspath(given)
        if not fs.isdir(path):
            what = "not a directory" if fs.exists(path) else "no such directory"
            problems.append(f"{given}: {what}")
            continue
        try:
            for name in (RBYTES, RCTIME, ENTRIES):
                fs.getxattr(path, name)
        except OSError as exc:
            problems.append(f"{given}: {_xattr_problem(exc)}")
            continue
        roots.append(Node(path, root=path, depth=0))
    real = [(fs.realpath(node.path), node.path) for node in roots]
    for i, (real_a, a) in enumerate(real):
        for real_b, b in real[:i]:
            common = os.path.commonpath([real_a, real_b])
            if real_a == real_b:
                problems.append(f"{a} is the same directory as {b}")
            elif common == real_b:
                problems.append(f"{a} is inside {b}")
            elif common == real_a:
                problems.append(f"{b} is inside {a}")
    if problems:
        raise FatalError(*problems)
    return roots
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 45 tests` … `OK`.

- [ ] **Step 6: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 7: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 5: Building the tree of tracked dirs

This task implements step 2 of the spec's pipeline:
- the idle cutoff, `start − delay − IDLE_GRACE`, which is inclusive;
- idle roots, wide dirs and unlistable dirs;
- rctime probes, batched across the thread pool;
- `--max-dirs`, failing as soon as it is exceeded, with pending probes
  cancelled;
- the progress line on stderr.

Results are always assembled in BFS order, whatever order the futures
complete in.

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (the header; append the first part
  of the Sampling section)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: `Fs`, `FatalError`, `RCTIME`, `ENTRIES` and `NOT_CEPHFS`
  (Task 4); `Node`, `Run` and `Reason` (Task 2); `plural()` (Task 1).
- Produces:
  - `Progress(stream)`, with `.phase(text)`, `.update(text, force=False)`
    and `.done()`.
  - `_batched(node, paths)` and `_probe(fs, paths) -> list[float |
    OSError]`, both reused in Task 6.
  - `build(run: Run, fs, pool: Executor, progress) -> None`, which grows
    `run.nodes` in BFS order and raises `FatalError`.
  - The constants `IDLE_GRACE = 30.0`, `PROBE_BATCH = 16` and
    `PROGRESS_INTERVAL = 0.2`.
  - Test helpers `make_run(fs, roots=("/r",), **limits)` and `build(fs,
    roots=("/r",), threads=4, **limits)`, plus `flat(text)` near the top
    of the test file.

- [ ] **Step 1: Write the failing tests**

In `tests/cephfs/test_scan_growing_dirs.py`, replace everything above `class ParseRateTest` with:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import errno
import io
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from _scan_support import IDLE, NOW, FakeDir, FakeFs, MiB, bfs, mk, mkrun, rates, sg

T = 1 * MiB  # the default threshold, in bytes/s


def flat(text: str) -> str:
    """text with runs of whitespace collapsed, for wrap-proof checks."""
    return " ".join(text.split())
```

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
def make_run(fs, roots=("/r",), **limits):
    """A Run of the preflighted roots, with default limits overridden."""
    nodes = sg.preflight(list(roots), fs)
    kw = {"delay": 60.0, "depth": 5, "max_dirs": 1000, "max_entries": 10_000} | limits
    return sg.Run(roots=[n.path for n in nodes], nodes=nodes, created="", **kw)


def build(fs, roots=("/r",), threads=4, **limits):
    run = make_run(fs, roots, **limits)
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

    def test_more_roots_than_max_dirs(self):
        fs = FakeFs({"/r": FakeDir(), "/s": FakeDir()})
        with self.assertRaises(sg.FatalError):
            build(fs, roots=("/r", "/s"), max_dirs=1)

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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=15)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'IDLE_GRACE'`.

- [ ] **Step 3: Implement**

In `cephfs/scan-growing-dirs.py`, replace everything above `# --- Rates` with this header (it only adds imports and constants):

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import enum
import errno
import json
import math
import os
import re
import textwrap
import time
from concurrent.futures import Executor, as_completed
from dataclasses import asdict, dataclass, field
from functools import partial

RBYTES = "ceph.dir.rbytes"
RCTIME = "ceph.dir.rctime"
ENTRIES = "ceph.dir.entries"

# Seconds added to --delay when deciding a subdir is idle: absorbs writers'
# clock skew and late size and rstat updates.
IDLE_GRACE = 30.0

# Subdirs per rctime probe task: enough tasks to keep the threads busy on one
# wide dir, few enough futures for millions of probes.
PROBE_BATCH = 16

# Warnings and notes name at most this many paths; --json lists them all.
MAX_LISTED_PATHS = 10

# Minimum seconds between rewrites of the progress line.
PROGRESS_INTERVAL = 0.2

JSON_VERSION = 1

# getxattr errnos meaning the directory isn't on CephFS.
NOT_CEPHFS = frozenset({errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP})


class FatalError(Exception):
    """Stops the run. main() prints each argument after 'ERROR: ' and exits 1."""
```

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
# --- Sampling --------------------------------------------------------------


class Progress:
    """Status on stderr: one line rewritten in place on a TTY, otherwise one
    line per phase."""

    def __init__(self, stream) -> None:
        self.stream = stream
        self.tty = stream.isatty()
        self.width = 0  # length of the status line on screen
        self.last = -math.inf  # time.monotonic() of the last rewrite

    def phase(self, text: str) -> None:
        if self.tty:
            self.update(text, force=True)
        else:
            print(text, file=self.stream, flush=True)

    def update(self, text: str, force: bool = False) -> None:
        """Rewrite the TTY status line, at most every PROGRESS_INTERVAL s."""
        now = time.monotonic()
        if self.tty and (force or now - self.last >= PROGRESS_INTERVAL):
            self.stream.write("\r" + text.ljust(self.width))
            self.stream.flush()
            self.width, self.last = len(text), now

    def done(self) -> None:
        """Clear the TTY status line."""
        if self.tty and self.width:
            self.stream.write("\r" + " " * self.width + "\r")
            self.stream.flush()
            self.width = 0


def _batched(node: Node, paths: list[str]) -> list[tuple[Node, list[str]]]:
    return [
        (node, paths[i : i + PROBE_BATCH]) for i in range(0, len(paths), PROBE_BATCH)
    ]


def _probe(fs: Fs, paths: list[str]) -> list[float | OSError]:
    """The rctime of each path, or the error reading it."""
    results: list[float | OSError] = []
    for path in paths:
        try:
            results.append(float(fs.getxattr(path, RCTIME)))
        except OSError as exc:
            results.append(exc)
    return results


def _list_subdirs(node: Node, fs: Fs, cutoff: float, max_entries: int) -> list[str]:
    """Return node's subdirs to probe, or [] after making node an unexplored
    leaf: an idle root, a wide dir, or an unlistable one."""
    try:
        if node.depth == 0 and float(fs.getxattr(node.path, RCTIME)) < cutoff:
            node.reason = Reason.IDLE
            return []
        if (entries := int(fs.getxattr(node.path, ENTRIES))) > max_entries:
            node.reason, node.entries = Reason.WIDE, entries
            return []
        return fs.subdirs(node.path)
    except OSError as exc:
        node.reason, node.list_error = Reason.UNLISTABLE, _strerror(exc)
        return []


def _too_many(run: Run, depth: int) -> FatalError:
    return FatalError(
        f"more than {run.max_dirs} dirs changed in the last "
        f"{run.delay + IDLE_GRACE:g} s (stopped at depth {depth}); lower "
        "--depth, pick narrower roots, or raise --max-dirs"
    )


def _expand(
    level: list[Node],
    cutoff: float,
    run: Run,
    fs: Fs,
    pool: Executor,
    progress: Progress,
) -> list[Node]:
    """List and probe the subdirs of each dir in level, and return the
    tracked ones: the next level, in BFS order."""
    depth = level[0].depth + 1
    list_one = partial(_list_subdirs, fs=fs, cutoff=cutoff, max_entries=run.max_entries)
    batches = [
        batch
        for node, subdirs in zip(level, pool.map(list_one, level), strict=True)
        for batch in _batched(node, subdirs)
    ]
    futures = {
        pool.submit(_probe, fs, paths): i for i, (_, paths) in enumerate(batches)
    }
    results: list[list[float | OSError]] = [[] for _ in batches]
    active = 0
    try:
        for future in as_completed(futures):
            probed = results[futures[future]] = future.result()
            run.probed += len(probed)
            active += sum(
                not isinstance(result, OSError) and result >= cutoff
                for result in probed
            )
            if len(run.nodes) + active > run.max_dirs:
                raise _too_many(run, depth)
            progress.update(
                f"Depth {depth}: probed {plural(run.probed, 'subdir')}, "
                f"tracking {plural(len(run.nodes) + active, 'dir')}"
            )
    except BaseException:
        for future in futures:
            future.cancel()
        raise

    next_level = []
    for (node, paths), probed in zip(batches, results, strict=True):
        for path, result in zip(paths, probed, strict=True):
            if isinstance(result, OSError):
                if result.errno in NOT_CEPHFS:
                    run.not_cephfs.append(path)
                elif result.errno != errno.ENOENT:
                    node.unreadable[path] = _strerror(result)
            elif result >= cutoff:
                child = Node(path, node.root, depth, parent=node)
                node.children.append(child)
                next_level.append(child)
            else:
                node.idle[path] = result
    run.nodes += next_level
    return next_level


def build(run: Run, fs: Fs, pool: Executor, progress: Progress) -> None:
    """Grow run.nodes, the roots, into the tree of tracked dirs, in BFS order.

    A subdir is tracked if its rctime is at most delay + IDLE_GRACE seconds
    before the start. Raises FatalError as soon as more than run.max_dirs
    dirs are tracked.
    """
    cutoff = fs.now() - run.delay - IDLE_GRACE
    if len(run.nodes) > run.max_dirs:
        raise _too_many(run, 0)
    level = list(run.nodes)
    for depth in range(run.depth):
        progress.phase(f"Depth {depth + 1}: exploring {plural(len(level), 'dir')}")
        if not (level := _expand(level, cutoff, run, fs, pool, progress)):
            break
    for node in level:
        node.reason = Reason.DEPTH
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 60 tests` … `OK`.

- [ ] **Step 5: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 6: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 6: The two samples and the wake-up re-check

This task implements pipeline steps 3–6:
- Each sample is timestamped at the monotonic midpoint of its getxattr call.
- Both passes read in the same order. The caller sleeps between them, so
  every interval is at least `--delay`.
- A dir that has vanished by the second pass counts as 0 bytes.
- An error leaves a dir unsampled, and it isn't retried.
- Idle subdirs of dirs with `own > 0` are re-probed.

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (append the rest of the Sampling
  section; the header is unchanged)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: `_batched()` and `_probe()` (Task 5); `own_rate()` and
  `Sample` (Task 2); `RBYTES`, `NOT_CEPHFS` and `_strerror()` (Task 4).
- Produces:
  - `_read_rbytes(fs, path) -> (int | OSError, float)`.
  - `sample_all(nodes, fs, pool, *, second: bool) -> None`.
  - `recheck_idle(nodes, fs, pool) -> None`.
  - The test helper `sampled(fs, roots=("/r",), threads=4, delay=60.0,
    **limits)`.

- [ ] **Step 1: Write the failing tests**

In `tests/cephfs/test_scan_growing_dirs.py`, replace everything above `class ParseRateTest` with:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import errno
import io
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from _scan_support import (
    ACTIVE,
    IDLE,
    NOW,
    FakeDir,
    FakeFs,
    MiB,
    bfs,
    growing,
    mk,
    mkrun,
    rates,
    sg,
)

T = 1 * MiB  # the default threshold, in bytes/s


def flat(text: str) -> str:
    """text with runs of whitespace collapsed, for wrap-proof checks."""
    return " ".join(text.split())
```

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
def sampled(fs, roots=("/r",), threads=4, delay=60.0, **limits):
    """Build, sample twice and re-check, like sample_run()."""
    run = make_run(fs, roots, delay=delay, **limits)
    with ThreadPoolExecutor(threads) as pool:
        sg.build(run, fs, pool, sg.Progress(io.StringIO()))
        sg.sample_all(run.nodes, fs, pool, second=False)
        fs.sleep(delay)
        sg.sample_all(run.nodes, fs, pool, second=True)
        sg.recheck_idle(run.nodes, fs, pool)
    return run


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
        fs = FakeFs({"/r": FakeDir(rbytes=100), "/r/a": FakeDir(rbytes=6000)})
        run = make_run(fs)
        with ThreadPoolExecutor(2) as pool:
            sg.build(run, fs, pool, sg.Progress(io.StringIO()))
            sg.sample_all(run.nodes, fs, pool, second=False)
            del fs.dirs["/r/a"]
            sg.sample_all(run.nodes, fs, pool, second=True)
        a = run.nodes[1]
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
        fs = FakeFs({"/r": FakeDir(), "/r/a": FakeDir()})
        run = make_run(fs)
        with ThreadPoolExecutor(2) as pool:
            sg.build(run, fs, pool, sg.Progress(io.StringIO()))
            sg.sample_all(run.nodes, fs, pool, second=False)
            fs.dirs["/r/a"].errors[sg.RBYTES] = errno.EACCES
            sg.sample_all(run.nodes, fs, pool, second=True)
        a = run.nodes[1]
        self.assertIsNotNone(a.s1)
        self.assertIsNone(a.s2)
        self.assertIsNone(a.rate)


class RecheckIdleTest(unittest.TestCase):
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
        run = make_run(fs)
        with ThreadPoolExecutor(2) as pool:
            sg.build(run, fs, pool, sg.Progress(io.StringIO()))
            sg.sample_all(run.nodes, fs, pool, second=False)
            change(fs)
            sg.sample_all(run.nodes, fs, pool, second=True)
            probes_before = len(fs.reads(sg.RCTIME))
            sg.recheck_idle(run.nodes, fs, pool)
        return run.nodes[0], len(fs.reads(sg.RCTIME)) - probes_before

    def test_records_subdirs_whose_rctime_moved(self):
        def change(fs):
            fs.dirs["/r/i1"].rctime = ACTIVE
            del fs.dirs["/r/i2"]

        r, probes = self.run_with(self.fs(), change)
        self.assertEqual(r.woke_up, ["/r/i1"])
        self.assertEqual(probes, 3)
        self.assertEqual(r.unreadable, {})

    def test_unreadable_on_recheck(self):
        def change(fs):
            fs.dirs["/r/i3"].errors[sg.RCTIME] = errno.EACCES

        r, _ = self.run_with(self.fs(), change)
        self.assertEqual(r.unreadable, {"/r/i3": os.strerror(errno.EACCES)})

    def test_skipped_when_own_rate_not_positive(self):
        fs = self.fs()
        fs.dirs["/r"].rbytes = 5
        r, probes = self.run_with(fs, lambda fs: None)
        self.assertEqual((r.woke_up, probes), ([], 0))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=8)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'sample_all'`.

- [ ] **Step 3: Implement**

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
def _read_rbytes(fs: Fs, path: str) -> tuple[int | OSError, float]:
    """(path's rbytes, or the error reading it; monotonic midpoint of the call)."""
    start = fs.clock()
    try:
        result: int | OSError = int(fs.getxattr(path, RBYTES))
    except OSError as exc:
        result = exc
    return result, (start + fs.clock()) / 2


def sample_all(nodes: list[Node], fs: Fs, pool: Executor, *, second: bool) -> None:
    """Read the rbytes of each node into s1, or into s2 if second, in order.

    In the second pass a vanished dir counts as 0 bytes. Other errors leave
    a node unsampled, with sample_error set; later passes skip it.
    """
    todo = [node for node in nodes if node.sample_error is None]
    read = pool.map(partial(_read_rbytes, fs), [node.path for node in todo])
    for node, (result, t) in zip(todo, read, strict=True):
        if isinstance(result, OSError):
            if not (second and result.errno == errno.ENOENT):
                node.sample_error = _strerror(result)
                continue
            node.vanished, result = True, 0
        if second:
            node.s2 = Sample(result, t)
        else:
            node.s1 = Sample(result, t)


def recheck_idle(nodes: list[Node], fs: Fs, pool: Executor) -> None:
    """Re-probe the idle subdirs of each dir whose own rate is above 0, and
    record in woke_up those whose rctime moved since the build."""
    batches = [
        batch
        for node in nodes
        if node.idle and (own := own_rate(node)) is not None and own > 0
        for batch in _batched(node, list(node.idle))
    ]
    probe = partial(_probe, fs)
    for (node, paths), results in zip(
        batches, pool.map(probe, [paths for _, paths in batches]), strict=True
    ):
        for path, result in zip(paths, results, strict=True):
            if not isinstance(result, OSError):
                if result != node.idle[path]:
                    node.woke_up.append(path)
            elif result.errno != errno.ENOENT and result.errno not in NOT_CEPHFS:
                node.unreadable[path] = _strerror(result)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 68 tests` … `OK`.

- [ ] **Step 5: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 6: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 7: Saving and loading state

This task implements `--save` and `--load`. The state is versioned JSON,
with nodes in BFS order and each parent given by its index. Loading
rebuilds `children`, and every way loading can fail becomes a `FatalError`.

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (the header; append the State
  section)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: `Run`, `Node`, `Sample` and `Reason` (Task 2); `FatalError`
  and `_strerror()` (Task 4).
- Produces:
  - `save_state(path: str, run: Run) -> None`.
  - `load_state(path: str) -> Run`.
  - The constants `STATE_FORMAT = "scan-growing-dirs-state"` and
    `STATE_VERSION = 1`.

- [ ] **Step 1: Write the failing tests**

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
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
        mk("/r/a", r, rate=2, reason=sg.Reason.WIDE, entries=99)
        u = mk("/r/b", r, rate=None, sample_error="Permission denied")
        mk("/r/b/c", u, rate=3, reason=sg.Reason.DEPTH, vanished=True)
        run = mkrun(bfs(r), probed=5, not_cephfs=["/r/m"])
        sg.save_state(self.path, run)
        loaded = sg.load_state(self.path)
        self.assertEqual(sg.analyze(loaded.nodes, T), sg.analyze(run.nodes, T))
        self.assertEqual(sg.notes(loaded), sg.notes(run))
        self.assertEqual(sg.summary(loaded), sg.summary(run))
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

    def test_unwritable(self):
        with self.assertRaises(sg.FatalError) as cm:
            sg.save_state("/nonexistent/dir/state.json", mkrun([mk("/r")]))
        self.assertIn("can't write", cm.exception.args[0])
```

(The test-file header is unchanged.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (errors=7)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'STATE_FORMAT'`.

- [ ] **Step 3: Implement**

In `cephfs/scan-growing-dirs.py`, replace everything above `# --- Rates` with this header (it only adds imports and constants):

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import enum
import errno
import json
import math
import os
import re
import textwrap
import time
from concurrent.futures import Executor, as_completed
from dataclasses import asdict, dataclass, field
from functools import partial

RBYTES = "ceph.dir.rbytes"
RCTIME = "ceph.dir.rctime"
ENTRIES = "ceph.dir.entries"

# Seconds added to --delay when deciding a subdir is idle: absorbs writers'
# clock skew and late size and rstat updates.
IDLE_GRACE = 30.0

# Subdirs per rctime probe task: enough tasks to keep the threads busy on one
# wide dir, few enough futures for millions of probes.
PROBE_BATCH = 16

# Warnings and notes name at most this many paths; --json lists them all.
MAX_LISTED_PATHS = 10

# Minimum seconds between rewrites of the progress line.
PROGRESS_INTERVAL = 0.2

STATE_FORMAT = "scan-growing-dirs-state"
STATE_VERSION = 1
JSON_VERSION = 1

# getxattr errnos meaning the directory isn't on CephFS.
NOT_CEPHFS = frozenset({errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP})


class FatalError(Exception):
    """Stops the run. main() prints each argument after 'ERROR: ' and exits 1."""
```

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
# --- State -------------------------------------------------------------------


def _sample_state(sample: Sample | None) -> list | None:
    return None if sample is None else [sample.rbytes, sample.t]


def _sample(state: list | None) -> Sample | None:
    return None if state is None else Sample(int(state[0]), float(state[1]))


def save_state(path: str, run: Run) -> None:
    """Write run to path as JSON, for --load. Raises FatalError on failure."""
    index = {node: i for i, node in enumerate(run.nodes)}
    state = {
        "format": STATE_FORMAT,
        "version": STATE_VERSION,
        "created": run.created,
        "roots": run.roots,
        "delay": run.delay,
        "depth": run.depth,
        "max_dirs": run.max_dirs,
        "max_entries": run.max_entries,
        "probed": run.probed,
        "not_cephfs": run.not_cephfs,
        "nodes": [
            {
                "path": node.path,
                "root": node.root,
                "depth": node.depth,
                "parent": None if node.parent is None else index[node.parent],
                "reason": node.reason,
                "entries": node.entries,
                "list_error": node.list_error,
                "unreadable": node.unreadable,
                "woke_up": node.woke_up,
                "s1": _sample_state(node.s1),
                "s2": _sample_state(node.s2),
                "sample_error": node.sample_error,
                "vanished": node.vanished,
            }
            for node in run.nodes
        ],
    }
    try:
        with open(path, "w") as f:
            json.dump(state, f, indent=1)
            f.write("\n")
    except OSError as exc:
        raise FatalError(f"can't write {path}: {_strerror(exc)}") from None


def load_state(path: str) -> Run:
    """Read a run written by save_state().

    Raises FatalError if path can't be read, or isn't a state file of
    STATE_VERSION.
    """
    try:
        with open(path) as f:
            state = json.load(f)
    except OSError as exc:
        raise FatalError(f"can't read {path}: {_strerror(exc)}") from None
    except ValueError as exc:
        raise FatalError(f"{path} is not a state file: {exc}") from None
    if not isinstance(state, dict) or state.get("format") != STATE_FORMAT:
        raise FatalError(f"{path} is not a scan-growing-dirs state file")
    if (version := state.get("version")) != STATE_VERSION:
        raise FatalError(
            f"{path} has state version {version}; this tool reads version "
            f"{STATE_VERSION}"
        )
    try:
        nodes: list[Node] = []
        for item in state["nodes"]:
            parent = None if item["parent"] is None else nodes[item["parent"]]
            reason = item["reason"]
            node = Node(
                item["path"],
                item["root"],
                item["depth"],
                parent=parent,
                reason=None if reason is None else Reason(reason),
                entries=item["entries"],
                list_error=item["list_error"],
                unreadable=dict(item["unreadable"]),
                woke_up=list(item["woke_up"]),
                s1=_sample(item["s1"]),
                s2=_sample(item["s2"]),
                sample_error=item["sample_error"],
                vanished=bool(item["vanished"]),
            )
            if parent is not None:
                parent.children.append(node)
            nodes.append(node)
        return Run(
            roots=list(state["roots"]),
            nodes=nodes,
            delay=float(state["delay"]),
            depth=int(state["depth"]),
            max_dirs=int(state["max_dirs"]),
            max_entries=int(state["max_entries"]),
            created=str(state["created"]),
            probed=int(state["probed"]),
            not_cephfs=list(state["not_cephfs"]),
        )
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise FatalError(f"{path} is a damaged state file ({exc!r})") from None
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 75 tests` … `OK`.

- [ ] **Step 5: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 6: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 8: Command line

This task covers:
- `--help`: the module docstring, plus an epilog generated from
  `KIND_TEXT`, `APPROX_TEXT` and `IDLE_GRACE`, so help and code can't
  drift apart;
- argument validation, including the `--load` conflicts;
- `sample_run()`, which runs build, sample, sleep, sample and re-check on
  one thread pool;
- `report()`;
- `main()`, which maps outcomes to exit codes and reconfigures stdout with
  `surrogateescape` for directory names that aren't UTF-8;
- the `__main__` entry point.

**Files:**
- Modify: `cephfs/scan-growing-dirs.py` (the header; append the Command
  line section)
- Modify: `tests/cephfs/test_scan_growing_dirs.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `build_parser() -> ArgumentParser`.
  - `parse_args(argv) -> Namespace`, which exits 2 on bad usage.
  - `sample_run(args, fs) -> Run`.
  - `report(run, threshold, sort_key, as_json) -> None`.
  - `main(argv=None, fs=None) -> int`.
  - The constants `DEFAULT_THRESHOLD` and `SAMPLING_DEFAULTS`.

- [ ] **Step 1: Write the failing tests**

In `tests/cephfs/test_scan_growing_dirs.py`, replace everything above `class ParseRateTest` with:

```python
"""Unit tests for cephfs/scan-growing-dirs.py."""

import contextlib
import errno
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

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
    rates,
    sg,
)

T = 1 * MiB  # the default threshold, in bytes/s


def flat(text: str) -> str:
    """text with runs of whitespace collapsed, for wrap-proof checks."""
    return " ".join(text.split())
```

Insert this just above the `if __name__ == "__main__":` block at the end of `tests/cephfs/test_scan_growing_dirs.py` (two blank lines around it):

```python
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
        message = self.usage_error("--load", "f", "--max-entries", "3")
        self.assertIn("with --max-entries", message)

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
            ("--depth", "-1"): "--depth must be 0 or more",
            ("--max-dirs", "0"): "--max-dirs must be at least 1",
            ("--max-entries", "0"): "--max-entries must be at least 1",
            ("--threads", "0"): "--threads must be at least 1",
            ("--threshold", "1M/s"): "bad rate '1M/s'",
        }
        for argv, expected in cases.items():
            with self.subTest(argv):
                self.assertIn(expected, self.usage_error("/r", *argv))

    def test_help_matches_code(self):
        epilog = sg.build_parser().epilog
        self.assertIn(f"within {sg.IDLE_GRACE:g} s", flat(epilog))
        for text in (*sg.KIND_TEXT.values(), sg.APPROX_TEXT):
            self.assertIn(text, epilog)


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
        self.assertIn("Nothing grew at 1.0 TiB/s or more.", err)

    def test_save_then_load_reports_the_same(self):
        with tempfile.TemporaryDirectory() as d:
            state = os.path.join(d, "state.json")
            _, live, _ = self.main("/r", "--save", state, fs=self.fs())
            code, loaded, _ = self.main("--load", state)
            self.assertEqual((code, loaded), (0, live))
            _, lower, _ = self.main("--load", state, "--threshold", "1B/s")
        self.assertIn("/r/busy", lower)
        self.assertNotIn("/r/quiet", lower)  # constant rbytes: rate 0

    def test_bad_root(self):
        code, out, err = self.main("/nope", fs=self.fs())
        self.assertEqual((code, out), (1, ""))
        self.assertIn("ERROR: /nope: no such directory", err)

    def test_too_many_dirs(self):
        code, _, err = self.main("/r", "--max-dirs", "2", fs=self.fs())
        self.assertEqual(code, 1)
        self.assertIn("ERROR: more than 2 dirs", err)

    def test_ctrl_c(self):
        class Interrupted(FakeFs):
            def sleep(self, seconds):
                raise KeyboardInterrupt

        fs = Interrupted(self.fs().dirs)
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `FAILED (failures=2, errors=22)`, with errors like `AttributeError: module 'scan_growing_dirs' has no attribute 'main'`.

- [ ] **Step 3: Implement**

In `cephfs/scan-growing-dirs.py`, replace everything above `# --- Rates` with this header (it only adds imports and constants):

```python
#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Find the directories growing anywhere under CephFS roots, in one sampling
interval, from recursive statistics (rstats) alone.

Takes two samples of each active directory's ceph.dir.rbytes, --delay
seconds apart. Subtrees whose ceph.dir.rctime is older than that, plus a
grace period, are assumed idle and skipped. No files are walked, so it is
fast on huge trees. It needs a CephFS mount and read permission on the
directories, not cluster credentials.

Use it to find what is filling a filesystem. To follow just the fastest
grower down, one level per interval, use find-growing-dirs.py.
"""

from __future__ import annotations

import argparse
import datetime
import enum
import errno
import json
import math
import os
import re
import sys
import textwrap
import time
from concurrent.futures import Executor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from functools import partial

RBYTES = "ceph.dir.rbytes"
RCTIME = "ceph.dir.rctime"
ENTRIES = "ceph.dir.entries"

# Seconds added to --delay when deciding a subdir is idle: absorbs writers'
# clock skew and late size and rstat updates.
IDLE_GRACE = 30.0

# Subdirs per rctime probe task: enough tasks to keep the threads busy on one
# wide dir, few enough futures for millions of probes.
PROBE_BATCH = 16

# Warnings and notes name at most this many paths; --json lists them all.
MAX_LISTED_PATHS = 10

# Minimum seconds between rewrites of the progress line.
PROGRESS_INTERVAL = 0.2

STATE_FORMAT = "scan-growing-dirs-state"
STATE_VERSION = 1
JSON_VERSION = 1

# getxattr errnos meaning the directory isn't on CephFS.
NOT_CEPHFS = frozenset({errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP})

DEFAULT_THRESHOLD = "1MiB/s"
# Options that only matter when sampling. --load rejects them, so the parser
# leaves them None and parse_args() fills these in afterwards.
SAMPLING_DEFAULTS = {
    "delay": 60.0,
    "depth": 5,
    "max_dirs": 1000,
    "max_entries": 10_000,
    "threads": 32,
}


class FatalError(Exception):
    """Stops the run. main() prints each argument after 'ERROR: ' and exits 1."""
```

Append this to the end of `cephfs/scan-growing-dirs.py`, with two blank lines before it:

```python
# --- Command line --------------------------------------------------------------


def _option(dest: str) -> str:
    return "--" + dest.replace("_", "-")


def _epilog() -> str:
    legend = _legend(set(Kind), approx=True, indent="  ")
    return f"""\
Rows (only rates at or above --threshold are shown):
{legend}
To look inside a tree row, rerun with its dir as ROOT or with a larger --depth.

Limitations:
  - files and spread rates are a dir's rate minus its subdirs' rates. rstats
    propagate lazily (seconds; longer across MDS ranks) and clients report
    file sizes late, so these rates are noisy for bursty writers; tree rates
    are not.
  - Moving data into a dir shows as growth there.
  - Skipping idle subtrees assumes writers' clocks are within {IDLE_GRACE:g} s
    of this host's.
  - A subdir that starts changing during the sleep isn't measured on its own;
    its growth shows on its parent's row, marked ~.

Example:
  scan-growing-dirs.py /mnt/cephfs --threshold 10MiB/s --sort rate
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    defaults = SAMPLING_DEFAULTS
    parser.add_argument(
        "roots", nargs="*", metavar="ROOT", help="directory on a CephFS mount"
    )
    parser.add_argument(
        "--delay",
        type=float,
        metavar="SECONDS",
        help="minimum interval between a dir's two samples "
        f"(default: {defaults['delay']:g})",
    )
    parser.add_argument(
        "--depth",
        type=int,
        metavar="N",
        help=f"levels to descend below each ROOT (default: {defaults['depth']})",
    )
    parser.add_argument(
        "--threshold",
        type=rate_arg,
        default=DEFAULT_THRESHOLD,
        metavar="RATE",
        help="hide rows growing slower, e.g. 500MB/min or 10GiB/h "
        f"(default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--max-dirs",
        type=int,
        metavar="N",
        help=f"error out if more dirs are active (default: {defaults['max_dirs']})",
    )
    parser.add_argument(
        "--max-entries",
        type=int,
        metavar="N",
        help="don't descend into dirs with more entries "
        f"(default: {defaults['max_entries']})",
    )
    parser.add_argument(
        "--threads",
        type=int,
        metavar="N",
        help=f"parallel getxattr and readdir calls (default: {defaults['threads']})",
    )
    parser.add_argument(
        "--sort",
        choices=("depth", "rate"),
        default="depth",
        help="order rows by depth and path, or fastest first (default: depth)",
    )
    parser.add_argument("--json", action="store_true", help="print rows as JSON")
    parser.add_argument(
        "--save", metavar="FILE", help="also write the sampled state to FILE"
    )
    parser.add_argument(
        "--load",
        metavar="FILE",
        help="report from a state written by --save, without sampling",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and check the command line, and fill in sampling defaults.

    Exits with status 2 on bad usage.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.load is not None:
        given = ["ROOT"] if args.roots else []
        given += [_option(d) for d in SAMPLING_DEFAULTS if getattr(args, d) is not None]
        if args.save is not None:
            given.append("--save")
        if given:
            parser.error(f"--load can't be combined with {', '.join(given)}")
        return args
    if not args.roots:
        parser.error("give at least one ROOT, or --load FILE")
    for dest, default in SAMPLING_DEFAULTS.items():
        if getattr(args, dest) is None:
            setattr(args, dest, default)
    if not (math.isfinite(args.delay) and args.delay > 0):
        parser.error("--delay must be a number above 0")
    if args.depth < 0:
        parser.error("--depth must be 0 or more")
    for dest in ("max_dirs", "max_entries", "threads"):
        if getattr(args, dest) < 1:
            parser.error(f"{_option(dest)} must be at least 1")
    return args


def sample_run(args: argparse.Namespace, fs: Fs) -> Run:
    """Build the tree under args.roots and sample it twice."""
    roots = preflight(args.roots, fs)
    run = Run(
        roots=[node.path for node in roots],
        nodes=roots,
        delay=args.delay,
        depth=args.depth,
        max_dirs=args.max_dirs,
        max_entries=args.max_entries,
        created=datetime.datetime.fromtimestamp(fs.now(), datetime.UTC).isoformat(
            timespec="seconds"
        ),
    )
    progress = Progress(sys.stderr)
    pool = ThreadPoolExecutor(max_workers=args.threads)
    try:
        build(run, fs, pool, progress)
        progress.phase(f"Sampling {plural(len(run.nodes), 'dir')}")
        sample_all(run.nodes, fs, pool, second=False)
        progress.phase(f"Sampling again in {run.delay:g} s")
        fs.sleep(run.delay)
        sample_all(run.nodes, fs, pool, second=True)
        progress.phase("Re-checking idle subdirs")
        recheck_idle(run.nodes, fs, pool)
    finally:
        progress.done()
        pool.shutdown(wait=False, cancel_futures=True)
    return run


def report(run: Run, threshold: float, sort_key: str, as_json: bool) -> None:
    """Print the rows on stdout; footnotes, notes and a summary on stderr."""
    rows = sort_rows(analyze(run.nodes, threshold), sort_key)
    if as_json:
        print(render_json(run, rows, threshold))
    elif rows:
        print(render_table(rows))
        print(footnotes(rows), file=sys.stderr)
    else:
        print(f"Nothing grew at {format_rate(threshold)} or more.", file=sys.stderr)
    for note in notes(run):
        print(paragraph(note), file=sys.stderr)
    print(summary(run), file=sys.stderr)


def main(argv: list[str] | None = None, fs: Fs | None = None) -> int:
    """Run the command line and return the exit status."""
    # Print directory names that aren't UTF-8 as their original bytes.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="surrogateescape")
    args = parse_args(argv)
    try:
        run = load_state(args.load) if args.load else sample_run(args, fs or Fs())
        report(run, args.threshold, args.sort, args.json)
        if args.save:
            save_state(args.save, run)
    except FatalError as exc:
        for message in exc.args:
            print(paragraph(f"ERROR: {message}"), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest discover -s tests/cephfs -p test_scan_growing_dirs.py`
Expected: `Ran 92 tests` … `OK`.

- [ ] **Step 5: Verify the finished file**

Run: `wc -l cephfs/scan-growing-dirs.py`
Expected: `1088 cephfs/scan-growing-dirs.py`.

Run: `./cephfs/scan-growing-dirs.py --help`
Expected: the docstring, the options with their defaults, a "Rows" legend
with `files`, `spread`, `tree` and `~`, "Limitations", and an example.

- [ ] **Step 6: Format and lint**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!` (the code above
is already formatted; a change means it wasn't copied verbatim).

- [ ] **Step 7: Checkpoint (no commit)**

CLAUDE.md: don't create git commits; the user commits. Leave the changes in
the working tree and move on.

---

### Task 9: README, full verification, live smoke run

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: the finished tool.
- Produces: README entries, verification evidence, and a draft commit
  message.

- [ ] **Step 1: Update the README**

In `README.md`, under "### CephFS trees (on a mounted filesystem)",
replace:

```markdown
- **`cephfs/du`**: Size of files and directories, without walking the tree.
- **`cephfs/find-growing-dirs.py`**: Find the fastest-growing subtree without
  walking the tree.
```

with:

```markdown
- **`cephfs/du`**: Size of files and directories, without walking the tree.
- **`cephfs/scan-growing-dirs.py`**: Find every directory growing faster
  than a threshold, anywhere under one or more roots, in one sampling
  interval. It reports growing files, unexplored subtrees and growth spread
  over many subdirs, each with a rate; `--json`, and `--save`/`--load` to
  re-report without re-sampling. Needs Python 3.11 or later.
- **`cephfs/find-growing-dirs.py`**: Follow the fastest-growing subtree down,
  one level per sampling interval.
```

Leave "## Requirements" as it is; the Python floor is stated in the tool's
own entry. The intro sentence ("… finding large, wide or fast-growing
directories on CephFS") is still accurate; leave it too.

- [ ] **Step 2: Run every test suite**

Run: `python3 -m unittest discover -s tests/cephfs`
Expected: `OK`. This includes the existing `find-growing-dirs.py` and
`top.py` tests.

Run: `python3 -m unittest discover -s tests/backfillctl`
Expected: `OK`. That suite is unchanged; this is a sanity check.

- [ ] **Step 3: Lint everything new**

Run: `ruff format cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py && ruff check cephfs/scan-growing-dirs.py tests/cephfs/_scan_support.py tests/cephfs/test_scan_growing_dirs.py`
Expected: `3 files left unchanged` and `All checks passed!`

- [ ] **Step 4: Live smoke run** (read-only; `/mnt/ceph2` is mounted `ro`)

Make a scratch directory with `mktemp -d`, and use its path for `$d` below.
Shell variables don't persist between separate tool calls, so either run
the commands in one shell or paste the literal path. Then run:
`./cephfs/scan-growing-dirs.py /mnt/ceph2 --delay 20 --depth 4 --threshold 100KiB/s --save "$d/smoke.json"; echo exit=$?`

Expected:
- One progress line per phase on stderr.
- A table, or "Nothing grew…", depending on what is active.
- A summary with `sample intervals 20.0-20.x s`.
- `exit=0`.

During planning, a scratch build tracked 10 dirs and reported three `tree`
rows under `/mnt/ceph2/sim`.

Run the lag check from the spec's Testing section:

```bash
SMOKE="$d/smoke.json" python3 - <<"EOF"
import os
import sys
sys.path.insert(0, "tests/cephfs")
from _scan_support import sg, MiB
run = sg.load_state(os.environ["SMOKE"])
for n in run.nodes:
    if (own := sg.own_rate(n)) is not None:
        kids = sum(sg.measured(c) for c in n.children)
        print(f"{n.rate / MiB:8.2f} children {kids / MiB:8.2f} own {own / MiB:7.3f}  {n.path}")
EOF
```

Expected: each `own` is tiny compared with the dir's rate. During planning
it was at most 0.015 MiB/s at about 10 MiB/s total. Record what you see in
the final report.

Then check replay, JSON and the error paths:
- `./cephfs/scan-growing-dirs.py --load "$d/smoke.json" --threshold 1B/s`:
  more rows, and no sampling delay.
- `./cephfs/scan-growing-dirs.py --load "$d/smoke.json" --json | python3 -m json.tool | head -30`:
  valid JSON.
- `./cephfs/scan-growing-dirs.py /mnt/ceph2/exp/ingest; echo exit=$?`:
  `ERROR: /mnt/ceph2/exp/ingest: permission denied reading ceph.dir.*
  attributes …` and `exit=1`. That dir is `dr-xr-x--x` for this user.
- `./cephfs/scan-growing-dirs.py /mnt/ceph2 /mnt/ceph2/exp; echo exit=$?`:
  `ERROR: /mnt/ceph2/exp is inside /mnt/ceph2` and `exit=1`.

- [ ] **Step 5: Re-read the docs against the code**

Compare `--help`, the README entry and the spec's CLI section. Defaults,
option names, row kinds and limitations must agree. Fix any text that
doesn't match the code.

- [ ] **Step 6: Draft the commit message (don't commit)**

CLAUDE.md asks for design choices and the intended effect in commit
messages. Give the user this draft. They decide whether
`docs/superpowers/` goes in the same commit.

```text
cephfs: add scan-growing-dirs.py to find all growing dirs in one interval

A new tool next to find-growing-dirs.py. It tracks the active dirs under
one or more roots (pruning by ceph.dir.rctime), samples each tracked dir's
ceph.dir.rbytes twice, and reports rows of three kinds: files (a dir's own
files), tree (an unexplored subtree) and spread (diffuse growth).

Design choices:
- Timestamps are per-call monotonic midpoints, not rctime. rctime is a
  max ctime, not an "as of" time; it is skewed by writers' clocks and
  moves on non-growth changes.
- Sampling is a separate pass after the build, in the same order both
  times. That aligns the windows for own = R(dir) - sum M(children), and
  every interval is >= --delay.
- Rates stay signed, and only rows >= --threshold are shown. Each dir is
  judged on its own: after sampling, threshold pruning saves no I/O and
  would hide growing children of net-flat parents.
- spread rows report growth that no lower row shows, once, at the lowest
  dir where it adds up to the threshold.
- ~ marks rates that may include unreadable, unsampled or woken subdirs.
  Idle subdirs of dirs with own > 0 are re-probed after sampling.
- The idle cutoff is --delay + 30 s (IDLE_GRACE), for writer clock skew and
  late size and rstat updates.
- --max-dirs fails fast. There is no probe limit (user's choice), only
  progress on stderr.
- ceph.dir.entries decides "too wide" without a readdir.
- --json, and --save/--load to re-report with another threshold or sort.

Intended effect: one run shows every branch growing faster than the
threshold, with the dirs to look at, instead of one drill-down per
interval.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
```

---

## Addendum (during execution): subdirs created during sampling

A side review found that a subdir created during the sleep was invisible.
Its growth showed as an unmarked `files` row on its parent. At the user's
request, this was fixed after Task 9's smoke run:
- `recheck_idle(nodes, fs, pool)` became `recheck_subdirs(run, fs, pool)`.
  It also re-lists each listed dir with `own > 0`, and adds subdirs it
  didn't know about to `woke_up`.
- `RecheckIdleTest` became `RecheckSubdirsTest`, with new tests for
  created, known and no-longer-listable subdirs.
- `MainTest.test_subdir_created_during_sleep_marks_parent` covers it end
  to end. It failed before the fix (no `~`) and passes after it.
- `--help`'s limitations line and the spec were updated to match.

The code in Tasks 6 and 8 above shows the state before this fix; the files
in the repo have the fix. The ledger records it as a ruling.

## Addendum: fixes from the final review

- An empty `--save` or `--load` path is now an error. Before, an empty
  `--save` silently skipped the save, and an empty `--load` crashed.
  Tests: `MainTest.test_empty_save_path_is_an_error` and
  `test_empty_load_path_is_an_error`.
- `load_state()` checks values through the new `_node_from_state()`:
  parent index range, field types, positive intervals, and no NaN.
  Test: `StateTest.test_damaged_values`.
- `--help` gained a "What to do with a row" section, with a correct action
  for each tree NOTE. It also gained the spec's lagging-clock limitation.
  Tests: `ParseArgsTest.test_help_says_how_to_act_on_each_row` and
  `test_help_warns_that_lagging_clocks_hide_growth`.
- The README entry now uses the spec's search terms ("fast-growing
  directories", CephFS, `rbytes`). The `Run.created` comment was corrected,
  and `Row` gained a docstring.

## Addendum: deferred minors fixed at the user's request

- `save_state()` is atomic: it writes a temp file next to the target,
  fsyncs it, and `os.replace`s it. Symlinks are written through. A failure
  or Ctrl-C leaves the old file intact and no temp file behind.
  Tests: `StateTest.test_failed_save_keeps_old_file_and_leaves_no_temp`
  and `test_save_writes_through_a_symlink`.
- If no dir could be sampled, the run ends with `ERROR: no dir could be
  sampled…` and exits 1, after saving. If some dirs couldn't be read, the
  "Nothing grew" line says "Nothing that could be read grew". The
  permission warning states only the effects that apply, including the
  unsampled-root case (`_problem_effects()`).
  Tests: `MainTest.test_nothing_sampled_is_an_error`,
  `test_nothing_grew_among_readable_dirs`, and
  `RenderTest.test_permission_warning_states_only_effects_that_apply`.

## Addendum: fixes from the user's `/code-review xhigh`

The user approved these fixes, with tests that failed first:
- **Vanished dirs:** ENOENT and ENOTDIR mean *vanished* at every stage. A
  dir gone before sample 1 contributes 0, and isn't a read problem.
- **Warning:** it lists and counts each dir once.
- **`--load` checks:** finite numbers (`parse_float`), overflow, and the
  element types of `roots`, `not_cephfs`, `woke_up` and `unreadable`.
- **Up-front checks:** `--save` is checked before sampling (exit 2),
  `--threshold` must be finite, and `--delay` is at most `MAX_DELAY`
  (86400 s).
- **Symlinks:** `Fs.getxattr` doesn't follow them. Roots are resolved in
  preflight, and problems still name them as typed.
- **Closed pipe:** `report()` flushes stdout. On `BrokenPipeError`,
  `main()` still saves and returns 141.
- **Ctrl-C:** `run_cli()` exits with `os._exit(130)` after flushing.
- **Renames:** they are followed by inode. `Fs.subdirs()` returns path →
  inode, and nodes record `ino` and `renamed_from`, which are saved too.
  `_follow_renames()` moves a renamed subtree and re-reads its second
  sample. A note lists the renames.
- **Re-check gate:** it is now `own != 0`, so a new subdir behind
  shrinking own files marks the `spread` row.
- **Smaller fixes:**
  - the README wording;
  - the docstrings of `_legend`, `_epilog`, `build_parser` and
    `_too_many`;
  - "Nothing grew…" now says what to do next;
  - helper type hints;
  - a too-many-roots message.
- **Tests that pin existing behaviour:** `--max-dirs` cancellation, with
  a fake that takes real time; Ctrl-C during the build; and all `--load`
  conflicts.

The issues left as they are: bind mounts of the same CephFS inside a root
(#9), and the per-level listing barrier.

