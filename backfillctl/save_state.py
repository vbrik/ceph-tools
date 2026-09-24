# SPDX-License-Identifier: MIT
"""
Capture the cluster state the other subcommands read into DIR, for replay
with '--load-state DIR', before or after the subcommand.

One capture serves every subcommand: it includes a full 'ceph pg dump pgs'
and the backfill positions of remapped PGs. It is anonymized (fsid,
addresses, hostnames, pool and rule names) and trimmed to the fields the
subcommands use, so it is safe to share.
"""

import argparse
import json
import sys

from shared import (
    BACKFILL_POSITIONS_FILE,
    PROGRESS_COUNTERS,
    HelpFormatter,
    SnapshotStore,
    extract_pg_stats,
    query_backfill_positions,
    resolve_save_dir,
)
from shared import anonymize_snapshots as anonymize_common

# One file per command. Subcommands filter pg_dump_pgs in place of their
# narrower live listings.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}

# The pg_stat fields some subcommand reads.
KEPT_PG_STAT_KEYS = ("pgid", "state", "up", "acting", "acting_primary", "up_primary")
KEPT_STAT_SUM_KEYS = (*PROGRESS_COUNTERS, "num_bytes")

# The 'ceph osd dump' fields some subcommand reads.
KEPT_OSD_DUMP_KEYS = (
    "erasure_code_profiles",
    "full_ratio",
    "backfillfull_ratio",
    "nearfull_ratio",
    "pg_upmap_items",
)


def anonymize_snapshots(snapshots: dict[str, object]) -> None:
    """Anonymize a capture in place (shared.anonymize_snapshots), then trim
    'osd_dump' and 'pg_dump_pgs' to the fields subcommands read.
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
        help="Capture cluster state for --load-state.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "dir",
        metavar="DIR",
        help="Output directory; created if missing, must be empty.",
    )
    return parser


def run(args: argparse.Namespace) -> None:
    if args.load_state:
        sys.exit("ERROR: --load-state does not apply to save-state.")
    save_dir = resolve_save_dir(args.dir)
    store = SnapshotStore(
        SNAPSHOT_COMMANDS, save_dir=save_dir, anonymize=anonymize_snapshots
    )
    store.save()
    # The cached dump, not the anonymized copy.
    pg_stats = extract_pg_stats(store.json("pg_dump_pgs"), "ceph pg dump pgs")
    positions = query_backfill_positions(
        pg["pgid"] for pg in pg_stats if pg["up"] != pg["acting"]
    )
    (save_dir / BACKFILL_POSITIONS_FILE).write_text(
        json.dumps(positions, separators=(",", ":"))
    )
    print(
        f"Saved {len(SNAPSHOT_COMMANDS)} snapshot(s) and the backfill positions "
        f"of {len(positions)} remapped PG(s) to {save_dir}"
    )
