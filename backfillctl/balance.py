# SPDX-License-Identifier: MIT
"""
Propose upmaps that move shards off the fullest OSDs of a device class onto
the emptiest, until each is below a level.

The level is --until-util, or by default the class's mean projected
utilization plus --max-deviation points. Sources are the class's up and in
OSDs at or above it (or those given by --osds). Each turn relieves the
fullest source by moving its largest shard that has a legal target; a source
is done once it is below the level, or when none of its shards can move.

Utilization here is projected (PROJ in the table): what an OSD will hold once
every backfill in motion and every proposal completes, crediting data leaving
it. The mean is capacity-weighted: where every OSD would be if the class's
data were spread evenly.

The target is the OSD that ends up least utilized among those that:

- are up and in, of the class, and not sources;
- are on a host the PG does not use (the source's own host is allowed), and
  not in its CRUSH mapping or acting set;
- stay at or below --max-target-util, counting the shards already arriving
  there and those proposed in this run, but not data leaving (Ceph checks
  before any is freed);
- end up below both the level and the source.

So no move raises the class's highest projected utilization or takes another
OSD to the level (a blocker pinned back keeps its data where it is, even above
the level). A blocker of another device class, in a PG whose CRUSH rule spans
classes, need only end up below the OSD it relieves. Shards still backfilling
onto a source are redirected.
Left alone, and counted on stderr: PGs that are not active, or are degraded,
undersized, recovering or peering, and PGs whose existing upmap pairs chain.
As in drain, a shard of a moved PG that would hold it in backfill_toofull is
diverted or pinned back (see drain --help).

stderr names the sources projected to stay at or above the level. For more,
apply the output, let the backfills finish and run again:

    backfillctl balance --pgremapper-mappings > m.json
    pgremapper import-mappings m.json

Turn off the upmap balancer ('ceph balancer off') for as long as the upmaps
should hold, or it may undo them. Assumes the CRUSH failure domain is host.
"""

import argparse
import sys
from typing import NamedTuple

from messages import (
    blockers_clause,
    left_alone_clause,
    level_text,
    osd_list,
    stderr_para,
    targets_clause,
)
from placement import add_max_target_util_arg, build_candidate_osds
from shared import (
    HelpFormatter,
    SnapshotStore,
    add_load_state_arg,
    add_pgremapper_mappings_arg,
    check_osds_exist,
    parse_osd,
    percentage_points,
)
from shed import (
    SNAPSHOT_COMMANDS,
    Cluster,
    ShedResult,
    add_until_util_arg,
    print_moves,
    print_notes,
    print_outcome,
    shed,
)

DEFAULT_CLASS = "hdd"

# How far above the class mean the default level is, in percentage points.
# The mean itself is out of reach: shards are too coarse for every source to
# get below it while every target stays below it.
DEFAULT_MAX_DEVIATION = 0


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "balance",
        help="Move data off the fullest OSDs of a device class.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--class",
        dest="osd_class",
        default=DEFAULT_CLASS,
        metavar="CLASS",
        help="Device class to balance (default: %(default)s).",
    )
    parser.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        metavar="OSD",
        help="Move data off only these OSDs.",
    )
    level = parser.add_mutually_exclusive_group()
    add_until_util_arg(level)
    level.add_argument(
        "--max-deviation",
        type=percentage_points,
        default=DEFAULT_MAX_DEVIATION,
        metavar="POINTS",
        help="Without --until-util, the level is the class mean plus this "
        "(default: %(default)g).",
    )
    add_max_target_util_arg(parser)
    add_pgremapper_mappings_arg(parser)
    add_load_state_arg(parser, after_command=True)
    return parser


class BalanceResult(NamedTuple):
    """What plan() decided, for render() to print."""

    shed: ShedResult
    osd_class: str
    class_size: int  # the class's up and in OSDs
    mean: float  # the class's mean final projection, before the moves
    max_before: tuple[float, int]  # the class's highest final projection, OSD
    max_after: tuple[float, int]


def plan(args: argparse.Namespace, store: SnapshotStore) -> BalanceResult:
    """Fetch the cluster state and work out what to move where.

    Exits on an unknown device class, a bad --osds, an invalid
    --max-target-util, or a pool it cannot analyze safely.
    """
    cluster = Cluster(store, args.max_target_util)
    candidates = build_candidate_osds(cluster.osd_df)
    class_osds = candidates.get(args.osd_class)
    if not class_osds:
        sys.exit(
            f"ERROR: --class: no up and in OSDs of class {args.osd_class!r}; "
            f"classes present: {', '.join(sorted(candidates)) or 'none'}."
        )
    if args.osds:
        check_osds_exist("--osds", args.osds, cluster.osd_df)
        bad = sorted(set(args.osds) - set(class_osds))
        if bad:
            sys.exit(
                "ERROR: --osds: not up and in OSDs of this device class, with "
                "a utilization: " + osd_list(bad)
            )
    # Only now the PG dump: the projections need it.
    final = cluster.final
    mean = final.mean_utilization(class_osds)
    level = mean + args.max_deviation if args.until_util is None else args.until_util
    if args.osds:
        sources = set(args.osds)
    else:
        sources = {o for o in class_osds if final.utilization(o) >= level}

    def class_max() -> tuple[float, int]:
        util, neg_osd = max((final.utilization(o), -o) for o in class_osds)
        return util, -neg_osd

    max_before = class_max()
    result = shed(cluster, sources, level, level_class=args.osd_class)
    return BalanceResult(
        shed=result,
        osd_class=args.osd_class,
        class_size=len(class_osds),
        mean=mean,
        max_before=max_before,
        max_after=class_max(),
    )


def describe_level(result: BalanceResult, args: argparse.Namespace) -> str:
    """Return the level and where it comes from, e.g. '52.3% (mean 50.3% + ...)'."""
    level = level_text(result.shed.level)
    if args.until_util is not None:
        return f"{level} (--until-util)"
    return (
        f"{level} (the class mean {level_text(result.mean)} + --max-deviation "
        f"{args.max_deviation:g})"
    )


def render(result: BalanceResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    r, cls = result.shed, result.osd_class
    chosen = "given by --osds" if args.osds else "at or above it"
    stderr_para(
        f"Balancing {cls} to below {describe_level(result, args)}. Sources: "
        f"{len(r.sources)} of {result.class_size} up and in {cls} OSD(s), "
        f"{chosen}; {r.mapped_count} shard(s) on them can move "
        f"({r.leaving_count} more already moving off)."
        + left_alone_clause(r.unsettled_pgs, r.chained_pgs)
    )
    if not r.sources:
        stderr_para(f"No {cls} OSD is at or above the level: nothing to move.")
        print_moves(r, args)
        return
    stderr_para(
        f"Targets: the other {cls} OSDs, "
        + targets_clause(r.max_target_util, r.ratios.backfillfull)
        + ", ending up below the level and their source. "
        + blockers_clause(r.ratios.nearfull)
    )
    print_moves(r, args)
    print_outcome(r, "the sources")
    (before, before_osd), (after, after_osd) = result.max_before, result.max_after
    stderr_para(
        f"Highest projected {cls} utilization: {before:.1f}% (osd.{before_osd}) "
        f"-> {after:.1f}% (osd.{after_osd})."
    )
    print_notes(r, "source(s)")


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
