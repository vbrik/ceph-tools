# SPDX-License-Identifier: MIT
"""
Propose upmaps that move every PG shard off the given OSDs, or off every OSD
of the given hosts.

Marking an OSD out lets a host-level CRUSH rule pile its shards onto the same
host's other OSDs. This spreads them over the least-utilized OSDs
cluster-wide instead, relieving the fullest drained OSD first, its largest
shard first. A target is up and in, of the shard's device class, not
drained, on a host the PG does not use (the shard's own host is allowed), and
projected at or below --max-target-util. Shards still backfilling onto a
drained OSD are redirected too.

With --until-util, an OSD sheds shards only while its projected utilization
is at or above that level, and a target must end up below the OSD it relieves,
though not necessarily below the level: relieving an overfull host may take
fuller targets. This projection (PROJ) counts every backfill in motion and
every proposal as done, crediting data leaving an OSD. The last move may take
an OSD well below the level.

Left alone, and counted on stderr: PGs that are not active, or are degraded,
undersized, recovering or peering (re-run once they settle), and PGs whose
existing upmap pairs chain (A->B, B->C).

backfill_toofull holds back the whole PG, so a moved shard also waits on any
other shard of its PG heading for an OSD projected at or over
backfillfull_ratio or, if the PG is backfill_toofull now, at or above
nearfull_ratio. Such a blocker is diverted if there is room, otherwise pinned
back to its acting OSD (with companions, as in cancel-backfill). If neither
is possible, the NOTE column says the PG will stay stuck, and why.

Keep the drained OSDs up and in until they are empty (with --until-util,
for as long as the upmaps should hold): marking one out voids these upmaps,
since Ceph honors only a 'from' that CRUSH chose. After a full drain,
external/upmap-remapped.py can pin CRUSH's new mapping to where the data is.

Apply the output with pgremapper. In the JSON, each entry has its NOTE as
'note', plus 'shard' and 'role': requested (an evacuee) or blocker (a
blocker, or a pinned blocker's companion). Keep a PG's blocker entries with
its evacuees:

    backfillctl drain --hosts host07 --pgremapper-mappings > m.json
    pgremapper import-mappings m.json

balance works the same way on a device class's fullest OSDs. Turn off the
upmap balancer ('ceph balancer off') for as long as the upmaps should hold,
or it may undo them. Assumes the CRUSH failure domain is host.
"""

import argparse

from messages import (
    RELIEVE_CLAUSE,
    blockers_clause,
    left_alone_clause,
    level_text,
    osd_list,
    stderr_para,
    targets_clause,
)
from placement import add_max_target_util_arg
from shared import (
    HelpFormatter,
    SnapshotStore,
    add_load_state_arg,
    add_pgremapper_mappings_arg,
    check_osds_exist,
    host_osds,
    parse_osd,
    short_hosts,
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
    unsized_sources,
)


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "drain",
        help="Propose upmaps that move all PG shards off given OSDs or hosts.",
        description=__doc__,
        formatter_class=HelpFormatter,
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        metavar="OSD",
        help="OSDs to drain.",
    )
    which.add_argument(
        "--hosts",
        nargs="+",
        metavar="HOST",
        help="Drain every OSD of these hosts.",
    )
    add_until_util_arg(parser)
    add_max_target_util_arg(parser)
    add_pgremapper_mappings_arg(parser)
    add_load_state_arg(parser, after_command=True)
    return parser


def plan(args: argparse.Namespace, store: SnapshotStore) -> ShedResult:
    """Fetch the cluster state and work out where each shard goes.

    Exits on an unknown OSD or host, an invalid --max-target-util, or a pool
    it cannot analyze safely.
    """
    cluster = Cluster(store, args.max_target_util)
    if args.hosts:
        drained = host_osds(args.hosts, cluster.osd_host)
    else:
        check_osds_exist("--osds", args.osds, cluster.osd_df)
        drained = set(args.osds)
    return shed(cluster, drained, args.until_util)


def render(r: ShedResult, args: argparse.Namespace) -> None:
    """Print r: proposals on stdout in the format args asks for, notes on stderr."""
    osds = osd_list(r.sources)
    if args.hosts:
        osds = f"host(s) {', '.join(short_hosts(args.hosts))} ({osds})"
    leaving = r.leaving_count
    left_alone = left_alone_clause(r.unsettled_pgs, r.chained_pgs)
    if not r.mapped_count:
        what = (
            f"every PG with a shard mapped to {osds} is left alone"
            if left_alone
            else f"no shard is mapped to {osds}"
        )
        stderr_para(
            f"Nothing to drain: {what}"
            + (f"; {leaving} already moving off." if leaving else ".")
            + left_alone
        )
        print_moves(r.moves, r.osd_host, r.osd_df, args)
        return
    to_level = (
        "" if r.level is None else f" to below --until-util {level_text(r.level)}"
    )
    stderr_para(
        f"Draining {osds}{to_level}: {r.mapped_count} shard(s) mapped to them "
        f"({leaving} more already moving off). Targets: "
        + targets_clause(r.max_target_util, r.ratios.backfillfull)
        + ("" if r.level is None else RELIEVE_CLAUSE)
        + ". "
        + blockers_clause(r.ratios.nearfull)
        + left_alone
    )
    print_moves(r.moves, r.osd_host, r.osd_df, args)
    print_outcome(r, "the drained OSDs")
    if r.level is not None and (unsized := unsized_sources(r)):
        stderr_para(
            "No size in 'ceph osd df', so drained in full despite --until-util: "
            f"{osd_list(unsized)}."
        )
    print_notes(r, "drained OSD(s)")


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
