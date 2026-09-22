"""Test support: import backfillctl's modules, and fake cluster state.

REPO_ROOT is put on sys.path so `from backfillctl import ...` resolves.
backfillctl/'s own directory is put on sys.path here too (the same thing
backfillctl/__init__.py does when the package is imported, and what running
it directly relies on), so a bare `import shared` below -- and the command
modules' own `import shared` / `from shared import ...` -- resolve to the
exact same module object. That identity matters: tests that
mock.patch.object(shared, ...) need to be patching the module the code under
test actually calls, not a separate `backfillctl.shared` copy. This is set up
with an explicit sys.path.insert rather than an `import backfillctl` side
effect, since an isort/ruff cleanup could otherwise reorder that import after
`import shared` and silently break it.
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKFILLCTL_DIR = REPO_ROOT / "backfillctl"

for path in (REPO_ROOT, BACKFILLCTL_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import shared

__all__ = ["REPO_ROOT", "FakeStore", "parse_args", "shared"]


def parse_args(module, argv: list[str]) -> argparse.Namespace:
    """Parse argv through module.build_parser, as backfillctl's dispatcher would.

    module is one of backfillctl's command modules (e.g.
    backfillctl.osds_of_pg); argv excludes the subcommand name, which is
    inferred from the single subparser the module registers.
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    module.build_parser(subparsers)
    (name,) = subparsers.choices
    return parser.parse_args([name, *argv])


class FakeStore:
    """Stand-in for shared.SnapshotStore serving canned snapshots by key.

    Enough for the fetch_* helpers, which only call json() and read commands.
    Pass either {key: parsed JSON} or, to also exercise error messages that name
    the ceph command, the store's commands.
    """

    def __init__(self, snapshots: dict[str, object], commands=None):
        self.snapshots = snapshots
        self.commands = commands or {
            k: ["ceph", k, "--format", "json"] for k in snapshots
        }

    def json(self, key: str) -> object:
        return self.snapshots[key]
