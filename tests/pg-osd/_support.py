"""Test support: import backfillctl's modules, and fake cluster state.

REPO_ROOT is put on sys.path so `from backfillctl import ...` resolves.
backfillctl/'s own directory is put on sys.path here too (the same thing
backfillctl/__init__.py does when the package is imported, and what running
it directly relies on), so a bare `import shared` below -- and the command
modules' own `import shared` / `from shared import ...` -- resolve to the
exact same module object (and likewise `placement`). That identity matters: tests that
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

import placement
import shared

__all__ = [
    "REPO_ROOT",
    "FakeStore",
    "parse_args",
    "placement",
    "plan_from_state",
    "shared",
]


def parse_args(
    module, argv: list[str], *, load_state: str | None = None
) -> argparse.Namespace:
    """Parse argv through module.build_parser, as backfillctl's dispatcher would.

    module is one of backfillctl's command modules (e.g.
    backfillctl.show_pg_osds); argv excludes the subcommand name, which is
    inferred from the single subparser the module registers. load_state, if
    given, is passed as the global --load-state, before the subcommand name.
    """
    parser = argparse.ArgumentParser()
    shared.add_load_state_arg(parser)
    subparsers = parser.add_subparsers(dest="command")
    module.build_parser(subparsers)
    (name,) = subparsers.choices
    global_argv = ["--load-state", load_state] if load_state is not None else []
    return parser.parse_args([*global_argv, name, *argv])


def plan_from_state(module, state_dir: str | Path, *argv: str):
    """Return module.plan()'s result for a --load-state capture, run in-process.

    argv is parsed as parse_args does. This is how tests check what a command
    decides without going through how it prints it (see each module's
    render()).
    """
    args = parse_args(module, list(argv), load_state=str(state_dir))
    return module.plan(
        args, shared.SnapshotStore.from_args(args, module.SNAPSHOT_COMMANDS)
    )


class FakeStore:
    """Stand-in for shared.SnapshotStore serving canned snapshots by key.

    Enough for the fetch_* helpers, which only call json(), read commands and
    (for the ones with a live/--load-state fork, e.g. fetch_remapped_pg_stats)
    check load_dir. Pass either {key: parsed JSON} or, to also exercise error
    messages that name the ceph command, the store's commands. load_dir
    defaults to non-None: FakeStore always stands in for already-fetched
    data, the --load-state side of that fork, never a live ceph call.
    """

    def __init__(self, snapshots: dict[str, object], commands=None, load_dir="fake"):
        self.snapshots = snapshots
        self.commands = commands or {
            k: ["ceph", k, "--format", "json"] for k in snapshots
        }
        self.load_dir = load_dir

    def json(self, key: str) -> object:
        return self.snapshots[key]
