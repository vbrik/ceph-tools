"""Test support for scan-growing-dirs.py: the module itself, a fake CephFS
with a fake clock, and helpers that build sampled trees directly."""

import errno
import importlib.util
import itertools
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
    ino: int = field(default_factory=itertools.count(1000).__next__)


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
        return {child: self.dirs[child].ino for child in self.children(path)}

    def rename(self, old: str, new: str) -> None:
        """Move the dir at old, and everything under it, to new; inodes stay."""
        for path in [p for p in self.dirs if p == old or p.startswith(old + "/")]:
            self.dirs[new + path[len(old) :]] = self.dirs.pop(path)

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
