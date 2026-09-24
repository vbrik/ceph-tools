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

# A live (non --load-state) run queries each shown PG's backfill position
# with 'ceph pg query', over librados or the ceph CLI. No test may reach a real
# cluster, so that is stubbed out for every test to "no positions" (progress
# from Ceph's counters). Tests of the query itself use the saved original.
real_query_backfill_positions = shared.query_backfill_positions
shared.query_backfill_positions = lambda pgids: {}

__all__ = [
    "EC_POOL",
    "KB",
    "PCT",
    "REPO_ROOT",
    "REP_POOL",
    "FakeStore",
    "SyntheticCluster",
    "parse_args",
    "placement",
    "plan_from_state",
    "real_query_backfill_positions",
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


KB = 1_000_000  # every synthetic OSD's capacity, in KiB
PCT = KB * shared.KIB // 100  # bytes in 1% of an OSD

EC_POOL, REP_POOL = 1, 2
HOST_RULE = {
    "rule_id": 0,
    "steps": [{"op": "take"}, {"op": "chooseleaf_indep", "type": "host"}],
}


class SyntheticCluster:
    """Builder for a small synthetic cluster's snapshots.

    Six hosts h0..h5 with two hdd OSDs each, numbered host*10 + j (0, 1, 10,
    11, ... 51), every OSD of KB KiB so that utilizations and shard sizes are
    exact percentages. Pool 1 is EC k=2 m=1 (size 3), pool 2 replicated size
    3, both with a host failure domain. backfillfull_ratio is 90%, so
    --max-target-util defaults to 89%.
    """

    def __init__(self, default_util: float = 50.0):
        self.util = {h * 10 + j: default_util for h in range(6) for j in range(2)}
        self.pgs: list[dict] = []
        self.upmaps: list[dict] = []
        self.classes: dict[int, str] = {}  # device class, if not hdd
        self.rule = HOST_RULE

    def pg(self, pgid, up, acting=None, *, shard_pct=1.0, state="active+clean"):
        """Add a PG whose shards are each shard_pct of an OSD."""
        is_ec = pgid.startswith(f"{EC_POOL}.")
        num_bytes = int(shard_pct * PCT) * (2 if is_ec else 1)
        acting = up if acting is None else acting
        if up != acting and "remapped" not in state:
            state += "+remapped"
        self.pgs.append(
            {
                "pgid": pgid,
                "state": state,
                "up": up,
                "acting": acting,
                "stat_sum": {"num_bytes": num_bytes},
            }
        )
        return self

    def snapshots(self) -> dict:
        hosts = [
            {
                "id": -1 - h,
                "type": "host",
                "name": f"h{h}",
                "children": [h * 10, h * 10 + 1],
            }
            for h in range(6)
        ]
        osds = [
            {
                "id": o,
                "type": "osd",
                "device_class": self.classes.get(o, "hdd"),
                "utilization": u,
                "kb": KB,
                "kb_used": int(u * KB / 100),
                "status": "up",
                "reweight": 1.0,
                "crush_weight": 1.0,
            }
            for o, u in self.util.items()
        ]
        return {
            "osd_tree": {"nodes": hosts + osds},
            "osd_df": {"nodes": osds},
            "osd_dump": {
                "nearfull_ratio": 0.85,
                "backfillfull_ratio": 0.90,
                "erasure_code_profiles": {"p": {"k": "2", "m": "1"}},
                "pg_upmap_items": self.upmaps,
            },
            "pool_ls_detail": [
                {
                    "pool_id": EC_POOL,
                    "pool_name": "ec",
                    "type": 3,
                    "crush_rule": 0,
                    "erasure_code_profile": "p",
                },
                {"pool_id": REP_POOL, "pool_name": "rep", "type": 1, "crush_rule": 0},
            ],
            "crush_rule_dump": [self.rule],
            "pg_dump_pgs": self.pgs,
        }

    def plan_with(self, module, *argv):
        """Run module.plan() on this cluster; leading bare OSD ids go to --osds."""
        argv = [str(a) for a in argv]
        if argv and not argv[0].startswith("--"):
            argv.insert(0, "--osds")
        return module.plan(parse_args(module, argv), FakeStore(self.snapshots()))
