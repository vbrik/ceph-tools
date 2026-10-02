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
import contextlib
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
from collections.abc import Collection, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import NoReturn, TextIO

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

# Errnos meaning a directory is gone: deleted, or replaced by a file.
VANISHED = frozenset({errno.ENOENT, errno.ENOTDIR})

# Upper bound for --delay, in seconds: longer is a typo, and extreme values
# overflow time.sleep().
MAX_DELAY = 86400.0

# Exit statuses besides 0, 1 (ERROR:) and 2 (usage), as a shell reports them.
EXIT_INTERRUPTED = 130  # Ctrl-C: 128 + SIGINT
EXIT_PIPE_CLOSED = 141  # stdout's reader went away, e.g. | head: 128 + SIGPIPE

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
            rate = float(number) * size / seconds
            if not math.isfinite(rate):
                raise ValueError(f"rate {text!r} is too large")
            if rate > 0:
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
    woke_up: list[str] = field(default_factory=list)  # active only after the build
    s1: Sample | None = None
    s2: Sample | None = None
    sample_error: str | None = None  # why s1 or s2 is missing
    vanished: bool = False  # gone during the run (see sample_all)
    ino: int | None = None  # inode, from the parent's listing; None for roots
    renamed_from: str | None = None  # path at build time, if renamed since

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
    created: str  # ISO 8601 time the run started
    probed: int = 0  # rctime probes while building the tree
    not_cephfs: list[str] = field(default_factory=list)  # skipped subdirs


# --- Analysis ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Row:
    """A reported row: one kind of growth (files, spread or tree) at a dir."""

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

    def add_row(
        node: Node,
        kind: Kind,
        rate: float,
        unreadable: Sequence[str] = (),
        woke_up: Sequence[str] = (),
    ) -> None:
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
            *(c.path for c in node.children if c.rate is None and not c.vanished),
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


def unmeasured_note(unreadable: Sequence[str], woke_up: Sequence[str]) -> str:
    """NOTE for a ~ row: the unmeasured subdirs its rate may include."""
    parts = []
    if unreadable:
        parts.append(plural(len(unreadable), "unreadable subdir"))
    if woke_up:
        parts.append(f"{plural(len(woke_up), 'subdir')} became active")
    return "; ".join(parts)


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


def _legend(kinds: Collection[Kind], approx: bool, indent: str = "") -> str:
    """The row-kind legend: a line for each kind in kinds, then one for ~ if
    approx."""
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
    """(path, problems) for each dir that couldn't be fully read, in BFS
    order; a dir with several problems appears once."""
    found: dict[str, list[str]] = {}
    for node in nodes:
        if node.list_error is not None:
            found.setdefault(node.path, []).append(f"unlistable: {node.list_error}")
        if node.sample_error is not None:
            found.setdefault(node.path, []).append(f"not sampled: {node.sample_error}")
        for path, err in node.unreadable.items():
            found.setdefault(path, []).append(f"unreadable: {err}")
    return [(path, "; ".join(problems)) for path, problems in found.items()]


def some_paths(items: list[str]) -> str:
    """Up to MAX_LISTED_PATHS of items, comma-separated, then '+N more'."""
    text = ", ".join(items[:MAX_LISTED_PATHS])
    if (more := len(items) - MAX_LISTED_PATHS) > 0:
        text += f", +{more} more (--json lists all)"
    return text


def _problem_effects(nodes: list[Node]) -> list[str]:
    """What the read problems in nodes do to the rows, one clause each."""
    effects = []
    unsampled = [node for node in nodes if node.sample_error is not None]
    if any(node.unreadable for node in nodes) or any(
        node.parent is not None for node in unsampled
    ):
        effects.append(
            "growth in an unreadable or unsampled subdir is counted in its "
            "parent's rate, marked ~"
        )
    if any(node.parent is None for node in unsampled):
        effects.append("a root that couldn't be sampled gets no rows of its own")
    if any(node.list_error is not None for node in nodes):
        effects.append("an unlistable dir is measured as a whole subtree")
    return effects


def notes(run: Run) -> list[str]:
    """Warnings and notes about what couldn't be measured, one per paragraph."""
    found = []
    if problems := read_problems(run.nodes):
        effects = "; ".join(_problem_effects(run.nodes))
        found.append(
            f"WARNING: couldn't read {plural(len(problems), 'dir')}: "
            + some_paths([f"{path} ({problem})" for path, problem in problems])
            + f". {effects[:1].upper()}{effects[1:]}."
        )
    if run.not_cephfs:
        found.append(
            f"NOTE: skipped {plural(len(run.not_cephfs), 'subdir')} not on "
            f"CephFS (mount points?): {some_paths(run.not_cephfs)}."
        )
    if renamed := [node for node in run.nodes if node.renamed_from is not None]:
        moves = [f"{node.renamed_from} -> {node.path}" for node in renamed]
        found.append(
            f"NOTE: {plural(len(renamed), 'tracked dir')} renamed during sampling "
            f"and measured under the new name: {some_paths(moves)}."
        )
    if vanished := sum(node.vanished for node in run.nodes):
        found.append(
            f"NOTE: {plural(vanished, 'tracked dir')} vanished during the run; "
            "each counts as 0 bytes from then on."
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


# --- Filesystem ------------------------------------------------------------


class Fs:
    """The filesystem and the clocks. Tests substitute a fake."""

    def getxattr(self, path: str, name: str) -> bytes:
        """path's xattr name. Doesn't follow a symlink at path, so a subdir
        swapped for a link mid-run isn't read through it."""
        return os.getxattr(path, name, follow_symlinks=False)

    def subdirs(self, path: str) -> dict[str, int]:
        """path's subdirs, not following symlinks, sorted by path, mapped to
        their inodes (from the listing itself, without a stat).

        Raises OSError if path can't be listed. Skips entries that vanish
        while it is listed.
        """
        found = {}
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        found[entry.path] = entry.inode()
                except OSError:
                    pass
        return dict(sorted(found.items()))

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
    """Return a depth-0 Node for each root, its path resolved: made absolute,
    with symlinks followed.

    Raises FatalError, with one argument per problem, if any root is
    missing, isn't a directory, has unreadable or no ceph.dir.* attributes,
    or is the same as or inside another root. Problems name roots as given.
    """
    problems = []
    found: list[tuple[str, str]] = []  # (resolved path, as given)
    for given in paths:
        path = fs.realpath(os.path.abspath(given))
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
        found.append((path, given))
    for i, (a, given_a) in enumerate(found):
        for b, given_b in found[:i]:
            common = os.path.commonpath([a, b])
            if a == b:
                problems.append(f"{given_a} is the same directory as {given_b}")
            elif common == b:
                problems.append(f"{given_a} is inside {given_b}")
            elif common == a:
                problems.append(f"{given_b} is inside {given_a}")
    if problems:
        raise FatalError(*problems)
    return [Node(path, root=path, depth=0) for path, _ in found]


# --- Sampling --------------------------------------------------------------


class Progress:
    """Status on stderr: one line rewritten in place on a TTY, otherwise one
    line per phase."""

    def __init__(self, stream: TextIO) -> None:
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


def _list_subdirs(
    node: Node, fs: Fs, cutoff: float, max_entries: int
) -> dict[str, int]:
    """Return node's subdirs to probe (path -> inode), or {} after making node
    an unexplored leaf (an idle root, a wide dir, or an unlistable one) or
    marking it vanished."""
    try:
        if node.depth == 0 and float(fs.getxattr(node.path, RCTIME)) < cutoff:
            node.reason = Reason.IDLE
            return {}
        if (entries := int(fs.getxattr(node.path, ENTRIES))) > max_entries:
            node.reason, node.entries = Reason.WIDE, entries
            return {}
        return fs.subdirs(node.path)
    except OSError as exc:
        if exc.errno in VANISHED:
            node.vanished = True
        else:
            node.reason, node.list_error = Reason.UNLISTABLE, _strerror(exc)
        return {}


def _too_many(run: Run, depth: int) -> FatalError:
    """The error for tracking more than --max-dirs dirs, found at depth."""
    if depth == 0:
        return FatalError(
            f"{plural(len(run.nodes), 'root')} given, but --max-dirs is "
            f"{run.max_dirs}; give fewer roots or raise --max-dirs"
        )
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
    listings = list(pool.map(list_one, level))
    inodes = {path: ino for listing in listings for path, ino in listing.items()}
    batches = [
        batch
        for node, listing in zip(level, listings, strict=True)
        for batch in _batched(node, list(listing))
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
                elif result.errno not in VANISHED:
                    node.unreadable[path] = _strerror(result)
            elif result >= cutoff:
                child = Node(path, node.root, depth, parent=node, ino=inodes[path])
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

    A dir that vanished is marked so. Gone by the second pass, it counts as
    0 bytes then; gone before the first, it has no samples, and since its
    bytes left before measuring began, it contributes nothing. Other errors
    leave a node unsampled, with sample_error set. Later passes skip both.
    """
    todo = [node for node in nodes if node.sample_error is None and not node.vanished]
    read = pool.map(partial(_read_rbytes, fs), [node.path for node in todo])
    for node, (result, t) in zip(todo, read, strict=True):
        if isinstance(result, OSError):
            if result.errno not in VANISHED:
                node.sample_error = _strerror(result)
                continue
            node.vanished = True
            if not second:
                continue
            result = 0
        if second:
            node.s2 = Sample(result, t)
        else:
            node.s1 = Sample(result, t)


def _relist(fs: Fs, node: Node) -> dict[str, int]:
    """node's subdirs now (path -> inode), or {} if it can no longer be
    listed."""
    try:
        return fs.subdirs(node.path)
    except OSError:
        return {}


def _subtree(node: Node) -> list[Node]:
    """node and its tracked descendants."""
    nodes = [node]
    for n in nodes:
        nodes += n.children
    return nodes


def _move(node: Node, new: str, run: Run) -> None:
    """Re-root node's subtree at new, its path after a rename."""
    old = node.path

    def moved(path: str) -> str:
        inside = path == old or path.startswith(old + "/")
        return new + path[len(old) :] if inside else path

    node.renamed_from = old
    for n in _subtree(node):
        n.path = moved(n.path)
        n.idle = {moved(p): rctime for p, rctime in n.idle.items()}
        n.unreadable = {moved(p): error for p, error in n.unreadable.items()}
        n.woke_up = [moved(p) for p in n.woke_up]
    run.not_cephfs = [moved(p) for p in run.not_cephfs]


def _follow_renames(
    listings: dict[Node, dict[str, int]], run: Run, fs: Fs, pool: Executor
) -> list[Node]:
    """Find tracked subdirs that vanished but reappear in their parent's new
    listing under another name, with the same inode. Move each one, and
    re-read the second sample of its tracked subtree at the new paths, so
    the rename isn't counted as the dir shrinking to 0 and a new dir
    appearing. Return the renamed nodes."""
    renamed = []
    for node, listing in listings.items():
        gone = {
            child.ino: child
            for child in node.children
            if child.vanished and child.s1 is not None and child.ino is not None
        }
        for path, ino in listing.items():
            if (child := gone.pop(ino, None)) is not None:
                _move(child, path, run)
                renamed.append(child)
    resample = [n for child in renamed for n in _subtree(child) if n.vanished]
    read = pool.map(partial(_read_rbytes, fs), [n.path for n in resample])
    for node, (result, t) in zip(resample, read, strict=True):
        if not isinstance(result, OSError) and node.s1 is not None:
            node.vanished, node.s2 = False, Sample(result, t)
    return renamed


def recheck_subdirs(run: Run, fs: Fs, pool: Executor) -> None:
    """After sampling, look again at the subdirs of each listed dir whose own
    rate isn't 0, since that own rate absorbs every subdir not measured on
    its own:
    - a tracked subdir renamed within its parent is followed by its inode
      (see _follow_renames);
    - a subdir created since the build, or an idle one whose rctime moved,
      became active during sampling: it goes in woke_up.
    """
    suspects = [
        node for node in run.nodes if (own := own_rate(node)) is not None and own != 0
    ]
    relist = partial(_relist, fs)
    listings = dict(zip(suspects, pool.map(relist, suspects), strict=True))
    for node in _follow_renames(listings, run, fs, pool):
        if node in listings:  # listed again at its new path
            listings[node] = _relist(fs, node)
    skipped = set(run.not_cephfs)
    for node, listing in listings.items():
        known = skipped | node.idle.keys() | node.unreadable.keys()
        known |= {child.path for child in node.children}
        node.woke_up += [path for path in listing if path not in known]

    batches = [
        batch
        for node in suspects
        if node.idle
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
            elif result.errno not in VANISHED | NOT_CEPHFS:
                node.unreadable[path] = _strerror(result)


# --- State -------------------------------------------------------------------


def _sample_state(sample: Sample | None) -> list | None:
    return None if sample is None else [sample.rbytes, sample.t]


def _sample(state: list | None) -> Sample | None:
    """A saved sample, or None. Raises TypeError, ValueError or OverflowError
    if it's damaged."""
    if state is None:
        return None
    rbytes, t = state
    if not (isinstance(rbytes, int) and isinstance(t, int | float)):
        raise TypeError(f"bad sample {state!r}")
    if not math.isfinite(t := float(t)):
        raise ValueError(f"sample time {t} is not finite")
    return Sample(rbytes, t)


def _strings(value: object) -> list[str]:
    """value, if it's a list of strings. Raises TypeError otherwise."""
    if not (isinstance(value, list) and all(isinstance(v, str) for v in value)):
        raise TypeError(f"expected a list of strings, got {value!r}")
    return value


def _str_dict(value: object) -> dict[str, str]:
    """value, if it maps strings to strings. Raises TypeError otherwise."""
    if not (
        isinstance(value, dict) and all(isinstance(v, str) for v in value.values())
    ):
        raise TypeError(f"expected a map of strings, got {value!r}")
    return value


def save_state(path: str, run: Run) -> None:
    """Write run to path as JSON, for --load. Raises FatalError on failure.

    The state goes to a temporary file next to path's target (a symlink is
    written through), then replaces it, so a failed or interrupted save
    leaves any earlier file intact.
    """
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
                "ino": node.ino,
                "renamed_from": node.renamed_from,
            }
            for node in run.nodes
        ],
    }
    target = os.path.realpath(path)
    temp = f"{target}.{os.getpid()}.tmp"
    try:
        with open(temp, "x") as f:
            json.dump(state, f, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, target)
    except BaseException as exc:  # also Ctrl-C: never leave the temp behind
        with contextlib.suppress(OSError):
            os.unlink(temp)
        if isinstance(exc, OSError):
            raise FatalError(f"can't write {path}: {_strerror(exc)}") from None
        raise


def _reject_constant(name: str) -> float:
    raise ValueError(f"{name} is not a valid number")


def _finite_float(text: str) -> float:
    if not math.isfinite(value := float(text)):
        raise ValueError(f"{text} is out of range")
    return value


def _node_from_state(item: dict, nodes: list[Node]) -> Node:
    """Rebuild a saved node, linked to its parent among nodes, the ones
    loaded before it. Raises KeyError, TypeError, ValueError or
    OverflowError if item is damaged."""
    if (parent := item["parent"]) is not None and not 0 <= parent < len(nodes):
        raise ValueError(f"node {len(nodes)} has parent index {parent}")
    path, root, depth = item["path"], item["root"], item["depth"]
    if not (isinstance(path, str) and isinstance(root, str) and isinstance(depth, int)):
        raise TypeError(f"node {len(nodes)} has a bad path, root or depth")
    s1, s2 = _sample(item["s1"]), _sample(item["s2"])
    if s1 is not None and s2 is not None and not s2.t > s1.t:
        raise ValueError(f"{path} has a sample interval of {s2.t - s1.t} s")
    reason, ino, renamed_from = item["reason"], item["ino"], item["renamed_from"]
    if not (ino is None or isinstance(ino, int)):
        raise TypeError(f"{path} has inode {ino!r}")
    if not (renamed_from is None or isinstance(renamed_from, str)):
        raise TypeError(f"{path} was renamed from {renamed_from!r}")
    node = Node(
        path,
        root,
        depth,
        parent=None if parent is None else nodes[parent],
        reason=None if reason is None else Reason(reason),
        entries=item["entries"],
        list_error=item["list_error"],
        unreadable=_str_dict(item["unreadable"]),
        woke_up=_strings(item["woke_up"]),
        s1=s1,
        s2=s2,
        sample_error=item["sample_error"],
        vanished=bool(item["vanished"]),
        ino=ino,
        renamed_from=renamed_from,
    )
    if node.parent is not None:
        node.parent.children.append(node)
    return node


def load_state(path: str) -> Run:
    """Read a run written by save_state().

    Raises FatalError if path can't be read, or isn't a state file of
    STATE_VERSION.
    """
    try:
        with open(path) as f:
            state = json.load(
                f, parse_float=_finite_float, parse_constant=_reject_constant
            )
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
            nodes.append(_node_from_state(item, nodes))
        return Run(
            roots=_strings(state["roots"]),
            nodes=nodes,
            delay=float(state["delay"]),
            depth=int(state["depth"]),
            max_dirs=int(state["max_dirs"]),
            max_entries=int(state["max_entries"]),
            created=str(state["created"]),
            probed=int(state["probed"]),
            not_cephfs=_strings(state["not_cephfs"]),
        )
    except (KeyError, IndexError, TypeError, ValueError, OverflowError) as exc:
        raise FatalError(f"{path} is a damaged state file ({exc!r})") from None


# --- Command line --------------------------------------------------------------


def _option(dest: str) -> str:
    return "--" + dest.replace("_", "-")


def _epilog() -> str:
    """--help text after the options: the row legend, what to do with each
    row, the limitations, and an example."""
    legend = _legend(set(Kind), approx=True, indent="  ")
    return f"""\
Rows (only rates at or above --threshold are shown):
{legend}

What to do with a row:
  files   look for the newest or largest files directly in the dir
  spread  lower --threshold to see its parts (with --load, no new sampling)
  tree    by NOTE: "depth limit": rerun with the dir as ROOT or a larger
          --depth; "N entries": raise --max-entries; "unlistable": the dir
          needs read permission; "idle at start": rerun

Limitations:
  - files and spread rates are a dir's rate minus its subdirs' rates. rstats
    propagate lazily (seconds; longer across MDS ranks) and clients report
    file sizes late, so these rates are noisy for bursty writers; tree rates
    are not.
  - Moving data into a dir shows as growth there. A tracked dir renamed
    within its parent is followed, by its inode.
  - Skipping idle subtrees assumes writers' clocks are within {IDLE_GRACE:g} s of
    this host's. A subdir written by a client further behind can look idle;
    its growth then shows in its parent's files rate, without ~.
  - A subdir that is created or starts changing during the sleep isn't
    measured on its own; its growth shows on its parent's row, marked ~.

Example:
  scan-growing-dirs.py /mnt/cephfs --threshold 10MiB/s --sort rate
"""


def build_parser() -> argparse.ArgumentParser:
    """The argument parser. Sampling options default to None, so parse_args()
    can tell which ones were given (see SAMPLING_DEFAULTS)."""
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


def _save_problem(path: str) -> str | None:
    """Why the state can't be saved to path, or None. Checked before
    sampling, so a long run isn't wasted on a typo."""
    if not path:
        return "needs a file name"
    target = os.path.realpath(path)
    if os.path.isdir(target):
        return "is a directory"
    parent = os.path.dirname(target)
    if not os.path.isdir(parent):
        return f"no such directory: {parent}"
    if not os.access(parent, os.W_OK):
        return f"can't write in {parent}"
    return None


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
    if not (math.isfinite(args.delay) and 0 < args.delay <= MAX_DELAY):
        parser.error(f"--delay must be a number above 0 and at most {MAX_DELAY:g}")
    if args.depth < 0:
        parser.error("--depth must be 0 or more")
    for dest in ("max_dirs", "max_entries", "threads"):
        if getattr(args, dest) < 1:
            parser.error(f"{_option(dest)} must be at least 1")
    if args.save is not None and (problem := _save_problem(args.save)):
        parser.error(f"--save {args.save!r}: {problem}")
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
        progress.phase("Re-checking subdirs")
        recheck_subdirs(run, fs, pool)
    finally:
        progress.done()
        pool.shutdown(wait=False, cancel_futures=True)
    return run


def report(run: Run, threshold: float, sort_key: str, as_json: bool) -> bool:
    """Print the rows on stdout; footnotes, notes and a summary on stderr.

    Return False if no dir could be sampled, so nothing was measured.
    """
    rows = sort_rows(analyze(run.nodes, threshold), sort_key)
    sampled = bool(intervals(run.nodes))
    if as_json:
        print(render_json(run, rows, threshold))
    elif rows:
        print(render_table(rows))
    # A closed pipe surfaces here, where main() handles it, not at exit.
    sys.stdout.flush()
    if not as_json:
        if rows:
            print(footnotes(rows), file=sys.stderr)
        elif sampled:
            scope = "that could be read " if read_problems(run.nodes) else ""
            print(
                f"Nothing {scope}grew at {format_rate(threshold)} or more; a "
                "lower --threshold shows slower growth.",
                file=sys.stderr,
            )
    for note in notes(run):
        print(paragraph(note), file=sys.stderr)
    print(summary(run), file=sys.stderr)
    return sampled


def main(argv: list[str] | None = None, fs: Fs | None = None) -> int:
    """Run the command line and return the exit status."""
    # Print directory names that aren't UTF-8 as their original bytes.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="surrogateescape")
    args = parse_args(argv)
    try:
        if args.load is not None:
            run = load_state(args.load)
        else:
            run = sample_run(args, fs or Fs())
        pipe_closed = False
        try:
            sampled = report(run, args.threshold, args.sort, args.json)
        except BrokenPipeError:  # e.g. | head: still save, then exit quietly
            _discard_stdout()
            sampled, pipe_closed = True, True
        if args.save is not None:
            save_state(args.save, run)
        if pipe_closed:
            return EXIT_PIPE_CLOSED
        if not sampled:
            raise FatalError("no dir could be sampled, so nothing was measured")
    except FatalError as exc:
        for message in exc.args:
            print(paragraph(f"ERROR: {message}"), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    return 0


def _discard_stdout() -> None:
    """Point stdout's file descriptor at /dev/null, so flushing it at exit
    doesn't fail again on the closed pipe."""
    with contextlib.suppress(OSError, ValueError):
        fd = sys.stdout.fileno()
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, fd)
        os.close(devnull)


def run_cli() -> NoReturn:
    """Exit with main()'s status. After Ctrl-C, exit at once: a normal exit
    joins the worker threads, which can hang in getxattr on a stuck MDS."""
    status = main()
    if status == EXIT_INTERRUPTED:
        with contextlib.suppress(OSError, ValueError):
            sys.stdout.flush()
            sys.stderr.flush()
        os._exit(status)
    sys.exit(status)


if __name__ == "__main__":
    run_cli()
