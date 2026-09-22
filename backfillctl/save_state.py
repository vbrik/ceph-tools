# SPDX-License-Identifier: MIT
"""
Capture the live cluster state the other subcommands need, into DIR, so they
can be re-run offline later with --load-state instead of hitting the cluster.

One snapshot serves all of them: DIR ends up holding one '<key>.json' file
per 'ceph ... --format json' command in SNAPSHOT_COMMANDS, including a full
'ceph pg dump pgs' (every PG, not just the remapped or backfill_toofull
ones some subcommands ask for live -- see "Cost" below). Each subcommand's
--load-state then reads the files it needs out of DIR, filtering pg_dump_pgs
itself for the PGs it cares about.

DIR is created if missing and must be empty, so a capture is never partially
overwritten by an unrelated one. The global --load-state is rejected here:
this command exists to capture a live cluster, not to copy a capture.

Anonymization
--------------
The capture is anonymized before being written (cluster fsid, OSD addresses
and uuids, hostnames, pool and CRUSH rule names replaced by deterministic
fake values -- see shared.anonymize_snapshots) and cut down to the fields any
subcommand actually reads (see anonymize_snapshots below), so it is safe to
share or commit. PG ids, OSD ids, utilizations, weights, device classes and
the overall topology are left untouched, since the analysis (and a replay via
--load-state) depends on them.

Cost
----
'ceph pg dump pgs' lists every PG in the cluster, which is exactly what the
per-subcommand listings (pg ls remapped, pg ls backfill_toofull, pg <pgid>
query) exist to avoid paying for on a live run. That cost is paid once here,
at capture time, not on every analysis run: a live 'backfillctl
show-backfill'/'cancel-backfill'/'divert-toofull' still
issues its own narrower command, and only pays the full 'pg dump pgs' cost
when replaying a --load-state snapshot this command produced.
"""

import argparse
import sys

from shared import (
    PROGRESS_COUNTERS,
    SnapshotStore,
    extract_pg_stats,
    resolve_save_dir,
)
from shared import anonymize_snapshots as anonymize_common

# One command per file written to DIR. pg_dump_pgs is the one subcommands'
# own SNAPSHOT_COMMANDS also lean on for --load-state (see their
# fetch_remapped_pg_stats-style helpers), even where their live run uses a
# narrower, mons-filtered command instead.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}

# The parts of each pg_stat entry that some subcommand reads: identity and
# movement (show-backfill, show-pg-osds), the flags cancel-backfill and
# divert-toofull filter on (part of 'state'), and the progress/size
# counters (shared.pg_progress_pct, shared.shard_size_bytes).
KEPT_PG_STAT_KEYS = ("pgid", "state", "up", "acting", "acting_primary", "up_primary")
KEPT_STAT_SUM_KEYS = (*PROGRESS_COUNTERS, "num_bytes")

# The parts of 'ceph osd dump' some subcommand reads: erasure code profiles
# (shard sizing), the full/backfillfull/nearfull ratios (divert-toofull,
# cancel-backfill), and pg_upmap_items (show-pg-osds, divert-toofull).
KEPT_OSD_DUMP_KEYS = (
    "erasure_code_profiles",
    "full_ratio",
    "backfillfull_ratio",
    "nearfull_ratio",
    "pg_upmap_items",
)


def anonymize_snapshots(snapshots: dict[str, object]) -> None:
    """Anonymize a complete capture in place, cutting each command's output
    down to the fields any subcommand reads.

    shared.anonymize_snapshots does the general scrub (hostnames, pool and
    CRUSH rule names, fsid, OSD addresses and uuids); this then trims
    'osd_dump' and every pg_stat in 'pg_dump_pgs' to KEPT_OSD_DUMP_KEYS /
    KEPT_PG_STAT_KEYS, dropping everything else (peering timestamps, scrub
    schedules, per-object counters no subcommand uses, ...).
    """
    anonymize_common(snapshots)
    dump = snapshots["osd_dump"]
    snapshots["osd_dump"] = {k: dump[k] for k in KEPT_OSD_DUMP_KEYS if k in dump}
    pg_stats = extract_pg_stats(snapshots["pg_dump_pgs"], "ceph pg dump pgs")
    snapshots["pg_dump_pgs"] = {
        "pg_stats": [
            {
                **{k: pg[k] for k in KEPT_PG_STAT_KEYS if k in pg},
                "stat_sum": {
                    k: v
                    for k, v in pg.get("stat_sum", {}).items()
                    if k in KEPT_STAT_SUM_KEYS
                },
            }
            for pg in pg_stats
        ]
    }


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "save-state",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "dir",
        metavar="DIR",
        help="Directory to write the capture into (created if missing; must "
        "be empty or not yet exist).",
    )
    return parser


def run(args: argparse.Namespace) -> None:
    if args.load_state:
        sys.exit(
            "ERROR: save-state captures the live cluster; --load-state does not apply to it."
        )
    save_dir = resolve_save_dir(args.dir)
    store = SnapshotStore(
        SNAPSHOT_COMMANDS, save_dir=save_dir, anonymize=anonymize_snapshots
    )
    store.save()
    print(f"Saved {len(SNAPSHOT_COMMANDS)} snapshot(s) to {save_dir}")
