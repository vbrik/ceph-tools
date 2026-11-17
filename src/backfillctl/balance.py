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
it. A backfill under way adds only what it has yet to copy, by its position
in 'ceph pg query'. The mean is capacity-weighted: where every OSD would be
if the class's data were spread evenly.

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

--target-hosts restricts the targets to the OSDs of the given hosts, for
diverted blockers too. Meant for newly added hosts, when CRUSH wants to move
data all over the cluster but relieving the fullest OSDs is what is urgent:

    ceph balancer off
    (add the hosts)
    backfillctl cancel-backfill                      pin every backfill
    backfillctl balance --target-hosts NEW1 NEW2     in rounds, as below
    ceph balancer on                                 let it finish gradually

Apply each command's output with pgremapper, as below.

Run cancel-backfill first: backfills in motion count in the projections, so
CRUSH's own moves would leave little for balance to do. A shard pinned away
from a target host's OSD by an upmap pair goes back to that OSD rather than
to the emptiest, if it is a legal target and ends more than the shard's size
below the level (closer, it could hold back later moves off the source): the
data then moves only once, and pgremapper drops the pair. So that most pins
go, each source sheds such shards before its larger ones.

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

from backfillctl.messages import (
    blockers_clause,
    left_alone_clause,
    level_text,
    osd_list,
    stderr_para,
    targets_clause,
)
from backfillctl.placement import add_max_target_util_arg
from backfillctl.shared import (
    HelpFormatter,
    SnapshotStore,
    add_load_state_arg,
    add_pgremapper_mappings_arg,
    check_osds_exist,
    host_osds,
    parse_osd,
    percentage_points,
    short_hosts,
)
from backfillctl.shed import (
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
# 0 asks for utilization as even as the shards allow: they are too coarse for
# every source to get below the mean while every target stays below it, so
# a run reports sources left at or above it.
DEFAULT_MAX_DEVIATION = 0


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "balance",
        help="Propose upmaps that move data off a device class's fullest OSDs.",
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
        help="Set the level to the class mean plus this many percentage points "
        "(default: %(default)g).",
    )
    parser.add_argument(
        "--target-hosts",
        nargs="+",
        metavar="HOST",
        help="Move data onto OSDs of only these hosts, e.g. newly added ones "
        "(see below).",
    )
    add_max_target_util_arg(parser)
    add_pgremapper_mappings_arg(parser)
    add_load_state_arg(parser, after_command=True)
    return parser


def check_target_hosts(
    hosts: list[str], osd_host: dict[int, str], class_osds: list[int], osd_class: str
) -> frozenset[str]:
    """Return the --target-hosts by short name; exit naming those without class OSDs.

    Each must be in 'ceph osd tree' and hold an up and in OSD of osd_class.
    """
    host_osds(hosts, osd_host, "--target-hosts")
    names = short_hosts(hosts)
    usable = {osd_host.get(o) for o in class_osds}
    bad = [h for h in names if h not in usable]
    if bad:
        sys.exit(
            f"ERROR: --target-hosts: no up and in {osd_class} OSDs with a size and "
            f"utilization in 'ceph osd df': {', '.join(bad)}"
        )
    return frozenset(names)


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

    Exits on an unknown device class, a bad --osds or --target-hosts, an
    invalid --max-target-util, or a pool it cannot analyze safely.
    """
    cluster = Cluster(store, args.max_target_util)
    candidates = cluster.candidates
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
                f"ERROR: --osds: not among the up and in {args.osd_class} OSDs "
                "with a size and utilization in 'ceph osd df': " + osd_list(bad)
            )
    target_hosts = None
    if args.target_hosts:
        target_hosts = check_target_hosts(
            args.target_hosts, cluster.osd_host, class_osds, args.osd_class
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
    result = shed(
        cluster, sources, level, level_class=args.osd_class, target_hosts=target_hosts
    )
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
        f"{chosen}; {r.mapped_count} shard(s) mapped to them "
        f"({r.leaving_count} more already moving off)."
        + left_alone_clause(r.unsettled_pgs, r.chained_pgs)
    )
    if not r.sources:
        stderr_para(f"No {cls} OSD is at or above the level: nothing to move.")
        print_moves(r.moves, r.osd_host, r.osd_df, args)
        return
    targets = (
        f"the {cls} OSDs on {', '.join(r.target_hosts)}"
        if r.target_hosts
        else f"the other {cls} OSDs"
    )
    stderr_para(
        f"Targets: {targets}, "
        + targets_clause(r.max_target_util, r.ratios.backfillfull)
        + ", ending up below the level and their source. "
        + blockers_clause(r.ratios.nearfull)
    )
    print_moves(r.moves, r.osd_host, r.osd_df, args)
    print_outcome(r, "the sources")
    if n := r.unpinned_count:
        stderr_para(
            f"{n} move{'s send' if n > 1 else ' sends'} a shard back to the OSD "
            "CRUSH maps it to, dropping its upmap pair."
        )
    (before, before_osd), (after, after_osd) = result.max_before, result.max_after
    stderr_para(
        f"Highest projected {cls} utilization: {before:.1f}% (osd.{before_osd}) "
        f"-> {after:.1f}% (osd.{after_osd})."
    )
    print_notes(r, "source(s)")


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
