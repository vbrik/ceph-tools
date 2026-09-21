"""Test support: load the hyphen-named scripts in pg-osd/, and fake cluster state.

The scripts import their sibling `shared`, which only resolves when pg-osd/ is
on sys.path (as it is when a script is run directly), so it is added here.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

SCRIPT_DIR = Path(__file__).resolve().parents[2] / "pg-osd"

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import shared

__all__ = ["SCRIPT_DIR", "FakeStore", "load_script", "script_path", "shared"]


def script_path(filename: str) -> str:
    """Return the path of a script in pg-osd/, e.g. for running it as a subprocess."""
    return str(SCRIPT_DIR / filename)


def load_script(filename: str) -> ModuleType:
    """Import a script in pg-osd/ (e.g. 'osds-of-pg.py') without running its main()."""
    name = Path(filename).stem.replace("-", "_")
    spec = importlib.util.spec_from_file_location(name, script_path(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
