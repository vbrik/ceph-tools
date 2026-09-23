# SPDX-License-Identifier: MIT
"""
Propose upmaps that drain OSDs of every PG shard mapped to them.

What it does
------------
The OSDs to drain are given either directly (--osds) or as the hosts whose
OSDs are all to be drained (--hosts, matched against the short host names in
'ceph osd tree'; a fully qualified name is shortened the same way).

Every PG shard whose 'up' slot is one of those OSDs is an evacuee, whether
its data is already there or is still backfilling onto it: either way, the
upmap pair '<drained OSD> -> <target>' sends it elsewhere. A shard the OSD
still holds but that is already moving away (in 'acting', not in 'up') needs
nothing and is only counted on stderr.

It only prints proposals; it changes nothing. Unlike marking the OSD out,
which leaves it to CRUSH to re-place its shards (for a host-level rule, onto
the same host's other OSDs -- see divert_toofull's "The situation this
solves"), this spreads them over the least-utilized OSDs cluster-wide.

How targets are chosen
----------------------
As in divert-toofull (see placement.pick_target and divert_toofull's module
docstring): an OSD of the evacuee's device class that is up and in, not on a
host another shard of the PG is mapped to, not in the PG's up set or raw CRUSH
mapping, not the target of --max-target-uses shards already, and at or below
--max-target-util (default backfillfull_ratio - 1) once the shard is projected
onto it. The one with the lowest such projection wins, OSD id breaking ties.

The differences:

  - None of the drained OSDs is ever a target.
  - There is no "strictly emptier than the OSD it leaves" rule: the point is
    to empty that OSD, whatever its utilization.
  - The evacuee's own host is not excluded: moving a shard to another OSD of
    the same host keeps the PG's failure domains as they were. (With
    --hosts, all of that host's OSDs are drained, so this never applies.)
  - The projection counts the shards arriving at every remapped PG in the
    cluster, not just the backfill_toofull ones.
  - The largest shards are placed first (PG id, then shard, breaking ties):
    ordering by the ACTING OSD's fullness, as divert-toofull does, says
    nothing here, where that is the drained OSD for most evacuees; and big
    shards first packs the room that is left better.

An evacuee with no legal target is left unplaceable and counted on stderr
rather than sent somewhere over the cap. The pools of the affected PGs must
fail over at 'host' (the unit targets are kept apart by); the subcommand
exits with an error otherwise.

Blockers
--------
backfill_toofull is a property of the PG: while any of its backfill targets
refuses a reservation, the whole PG waits, evacuee included. So after every
evacuee has been placed, each affected PG's other arriving shards
("siblings") are checked. A sibling is a blocker if the OSD it is heading for
is projected above --max-target-util. That test ignores the PG's state: even
a PG already backfilling re-peers when its upmap changes and has to reserve
again. A PG that is backfill_toofull right now is known to be refused by
*some* target, and Ceph can refuse one below the cap (it judges the target's
projected usage, which this run can only estimate), so for such a PG a
sibling is also a blocker if its OSD is at or above --min-up-util -- the same
test, and default (nearfull_ratio), divert-toofull uses to guess which shard
of a backfill_toofull PG is the refused one (see its "How PGs are
identified"). For each blocker, in order:

  1. Divert it: pick a target for it the same way as for an evacuee (NOTE
     "diverted: unblocks osd.N"). Diverting uses the same room as evacuees,
     which is why evacuees are placed first.
  2. Otherwise pin it back to its acting OSD, i.e. cancel its backfill, with
     whatever companion pins that needs (shared.close_pins; NOTE "pinned:
     unblocks osd.N" and "companion of shard N"). Refused, like a pin
     cancel-backfill cannot make, if the shard has no acting OSD, if a
     replicated PG's pairing is ambiguous, or if the pins would clash with
     the PG's new up set; and also if a pin would send data back to a
     drained OSD, would undo a move proposed in this run, or would chain
     with another pair (which pgremapper cannot apply, see
     shared.warn_chained_pgs).
  3. Otherwise keep the evacuee's proposal regardless -- it still moves the
     shard off the OSD once the blocker clears -- and say so in its NOTE:
     "PG stays toofull: shard N -> osd.X (now U%, projected P%; ...)".

A PG that is backfill_toofull right now but has no sibling passing either
test gets a NOTE saying its blocker is unidentified (unless one of its
evacuees was itself arriving on a drained OSD, whose redirect may be the
fix), since the evacuee may well wait regardless.

Keep the drained OSD in
-----------------------
Ceph only honors an upmap pair whose 'from' is an OSD CRUSH itself chose (see
divert_toofull's "Why the raw CRUSH mapping matters"). These pairs therefore
only hold while the drained OSD stays in CRUSH's mapping: marking it out (or
removing it) before the data has moved voids them, and CRUSH re-places the
shards onto the same host's other OSDs -- the pile-up this subcommand avoids.
Leave it up and in until it is empty; once it is, marking it out and running
external/upmap-remapped.py pins the resulting remaps to where the data
already is.

Testing against saved cluster state
------------------------------------
Live, the subcommand reads 'ceph pg ls-by-osd' for each drained OSD and 'ceph
pg ls remapped' for the projection. 'backfillctl --load-state DIR drain
...' instead filters the 'pg_dump_pgs' of a 'backfillctl save-state' capture
for both.

Applying the output
-------------------
As for divert-toofull: --pgremapper-mappings prints a JSON array for
'pgremapper import-mappings' (from is UP OSD, to is TARGET OSD), which
applies all pairs of a PG together and keeps its existing upmap pairs. Keep a
blocker's entry with the evacuee it unblocks, and a companion's with the pin
it belongs to. The upmap balancer may undo manual upmaps; consider 'ceph
balancer off' while the drain runs.

    backfillctl drain --hosts host07 --pgremapper-mappings > mappings.json
    pgremapper-v1.0.0-linux-amd64 import-mappings mappings.json
"""

import argparse
import json
import sys
from collections import Counter
from typing import NamedTuple

from placement import (
    ArrivingShard,
    FullRatios,
    ProjectedUsage,
    add_target_args,
    build_candidate_osds,
    check_host_failure_domain,
    ec_pool_ids_from,
    fetch_full_ratios,
    find_arriving_shards,
    osd_class,
    pick_target,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
)
from shared import (
    NOT_APPLICABLE,
    SnapshotStore,
    close_pins,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_remapped_pg_stats,
    fetch_upmap_items,
    is_real_osd,
    osd_cells,
    parse_osd,
    pgid_pool_id,
    pgid_sort_key,
    pin_replica,
    print_table,
    real_osd_set,
    slot,
    stderr_para,
)

# Static snapshot keys. A live run adds one 'pg ls-by-osd' per drained OSD
# once it knows them (see ls_by_osd_commands); --load-state filters pg_dump_pgs instead, both for
# those and for pg_ls_remapped (see fetch_drained_pg_stats and
# shared.fetch_remapped_pg_stats).
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_ls_remapped": ["ceph", "pg", "ls", "remapped", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


def ls_by_osd_key(osd: int) -> str:
    """Snapshot key for 'ceph pg ls-by-osd' of one OSD (live runs only)."""
    return f"pg_ls_by_osd_{osd}"


def ls_by_osd_commands(osds: set[int]) -> dict[str, list[str]]:
    """Return a 'pg ls-by-osd' snapshot command per drained OSD.

    Added to the store's commands once the drained OSDs are known, which with
    --hosts is only after 'ceph osd tree' has been read.
    """
    return {
        ls_by_osd_key(osd): [
            "ceph",
            "pg",
            "ls-by-osd",
            f"osd.{osd}",
            "--format",
            "json",
        ]
        for osd in osds
    }


def fetch_drained_pg_stats(store: SnapshotStore, osds: set[int]) -> list[dict]:
    """Return the PGs with any of osds in 'up' or 'acting', in PG id order.

    Live, the union of 'ceph pg ls-by-osd' for each OSD; with --load-state,
    pg_dump_pgs filtered client-side the same way.
    """
    if store.load_dir is None:
        by_pgid = {
            pg["pgid"]: pg
            for osd in sorted(osds)
            for pg in fetch_pg_stats(store, ls_by_osd_key(osd))
        }
        pgs = list(by_pgid.values())
    else:
        pgs = [
            pg
            for pg in fetch_pg_stats(store, "pg_dump_pgs")
            if osds & (real_osd_set(pg["up"]) | real_osd_set(pg["acting"]))
        ]
    return sorted(pgs, key=lambda pg: pgid_sort_key(pg["pgid"]))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "drain",
        description="Propose upmaps that move every PG shard mapped to the "
        "given OSDs, or to every OSD of the given hosts, to the least-utilized OSDs of the same device class "
        "that the PG can legally use, never projecting a target above "
        "--max-target-util nor using one for more than --max-target-uses "
        "shards. Largest shards are placed first. Where another shard of the "
        "same PG is headed for an OSD over --max-target-util (and would hold "
        "the PG in backfill_toofull), that shard is diverted too, or else "
        "pinned back, or else the evacuee's row says the PG will stay stuck. "
        "Only the proposals are printed; nothing is changed. Assumes the "
        "affected pools' CRUSH failure domain is 'host'.",
        epilog="See the docstring at the top of drain.py for the details.",
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument(
        "--osds",
        nargs="+",
        type=parse_osd,
        metavar="OSD",
        help="OSD(s) to drain, e.g. --osds 12 osd.13.",
    )
    which.add_argument(
        "--hosts",
        nargs="+",
        metavar="HOST",
        help="Drain every OSD of these host(s), as named in 'ceph osd tree' "
        "(short or fully qualified), e.g. --hosts host07 host08.",
    )
    parser.add_argument(
        "--min-up-util",
        type=float,
        metavar="PERCENT",
        help="For a PG that is backfill_toofull now, also treat another "
        "shard as blocking it if the OSD that shard is arriving on is at or "
        "above PERCENT today (as divert-toofull does). Default: the "
        "cluster's nearfull_ratio.",
    )
    add_target_args(
        parser,
        max_target_util_help="Never let a target's projected utilization "
        "(its 'ceph osd df' usage plus the shards arriving on it and those "
        "already proposed onto it) exceed PERCENT; a sibling shard arriving "
        "on an OSD projected above PERCENT counts as blocking its PG. "
        "Defaults to the cluster's backfillfull_ratio - 1; a value above "
        "backfillfull_ratio is an error.",
    )
    return parser


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


class Evacuee(NamedTuple):
    """A shard mapped to a drained OSD, to be moved off it."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # the drained OSD: the 'from' of the upmap pair
    acting_osd: int | None  # where its data is now, None if unknown
    size_bytes: int


def find_evacuees(
    pg: dict, is_ec: bool, drained: set[int], size_bytes: int
) -> tuple[list[Evacuee], int]:
    """Return (the PG's shards mapped to a drained OSD, count already leaving one).

    A replica resident on the drained OSD names it as its acting OSD; one
    arriving there names the departing replica only when the pairing is
    unambiguous (see placement.find_arriving_shards).
    """
    pgid, up, acting = pg["pgid"], pg["up"], pg["acting"]
    if is_ec:
        evacuees = [
            Evacuee(pgid, i, osd, slot(acting, i), size_bytes)
            for i, osd in enumerate(up)
            if osd in drained
        ]
        leaving = sum(
            1 for i, osd in enumerate(acting) if osd in drained and slot(up, i) != osd
        )
        return evacuees, leaving

    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    departing = sorted(acting_set - up_set)
    pairing_is_clear = len(departing) == 1 and len(up_set - acting_set) == 1
    evacuees = []
    for osd in sorted(up_set & drained):
        if osd in acting_set:
            acting_osd = osd
        else:
            acting_osd = departing[0] if pairing_is_clear else None
        evacuees.append(Evacuee(pgid, "-", osd, acting_osd, size_bytes))
    return evacuees, len((acting_set - up_set) & drained)


class Move(NamedTuple):
    """One proposed upmap pair, and why it is proposed."""

    pgid: str
    shard: "int | str"
    acting_osd: int | None  # where the shard's data is now
    up_osd: int  # the 'from' of the pair
    target_osd: int  # the 'to' of the pair
    projected: float | None  # target's projected utilization; None for pins
    note: str


class DrainResult(NamedTuple):
    """Everything a run decides, independent of how it is printed.

    plan() computes it and render() prints it, so tests can assert on these
    fields. osd_df and osd_host are carried along only because the table
    shows them.
    """

    osds: list[int]
    hosts: list[str]  # with --hosts, the (short) host names; else empty
    moves: list[Move]  # in PG order; within a PG, in the order proposed
    unplaceable: list[Evacuee]
    evacuee_count: int
    leaving_count: int  # shards already moving off a drained OSD
    diverted_count: int  # blockers diverted
    pinned_count: int  # blockers pinned back (companions not counted)
    stuck_pgs: list[str]  # PGs with an evacuee that will stay toofull
    unexplained_pgs: list[str]  # toofull now, blocker not identified
    max_target_util: float
    min_up_util: float
    ratios: FullRatios
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


class PgState:
    """One affected PG, as this run's proposals change its up set."""

    def __init__(self, pg: dict, is_ec: bool, size_bytes: int, raw: set[int]):
        self.pg = pg
        self.is_ec = is_ec
        self.size_bytes = size_bytes
        self.new_up = list(pg["up"])  # 'up' once the proposals are applied
        self.forbidden_osds = raw | real_osd_set(pg["up"])
        self.changed: set[int | str] = set()  # EC slots / replica OSDs moved
        self.moves: list[Move] = []

    def forbidden_hosts(self, moving_osd: int, osd_host: dict[int, str]) -> set:
        """Hosts a shard leaving moving_osd must not go to: the PG's other ones."""
        return {
            osd_host.get(o) for o in self.new_up if is_real_osd(o) and o != moving_osd
        }

    def retarget(self, from_osd: int, to_osd: int) -> None:
        """Record that the shard on from_osd (in new_up) now goes to to_osd."""
        self.new_up[self.new_up.index(from_osd)] = to_osd
        self.forbidden_osds.add(to_osd)


class Planner:
    """The shared state of one run's placements: room, uses and the PGs."""

    def __init__(
        self,
        osd_df: dict[int, dict],
        osd_host: dict[int, str],
        candidates: dict[str, list[int]],
        projection: ProjectedUsage,
        *,
        max_uses: int,
        max_target_util: float,
        min_up_util: float,
        drained: set[int],
    ):
        self.osd_df = osd_df
        self.osd_host = osd_host
        self.candidates = candidates  # see placement.build_candidate_osds
        self.projection = projection
        self.max_uses = max_uses
        self.max_target_util = max_target_util
        self.min_up_util = min_up_util
        self.drained = drained
        self.uses: Counter[int] = Counter()

    def place(self, state: PgState, from_osd: int) -> tuple[float, int] | None:
        """Pick a target for the PG's shard currently headed for from_osd."""
        return pick_target(
            self.candidates.get(osd_class(self.osd_df, from_osd), []),
            state.size_bytes,
            forbidden_hosts=state.forbidden_hosts(from_osd, self.osd_host),
            forbidden_osds=state.forbidden_osds,
            osd_host=self.osd_host,
            osd_df=self.osd_df,
            projection=self.projection,
            uses=self.uses,
            max_uses=self.max_uses,
            max_target_util=self.max_target_util,
        )

    def commit(self, state: PgState, shard, target: int) -> None:
        """Record that shard (with up_osd, size_bytes) goes to target."""
        self.projection.redirect(shard, target)
        self.uses[target] += 1
        state.retarget(shard.up_osd, target)

    def is_blocker(self, sibling: ArrivingShard, toofull_now: bool) -> str | None:
        """Return why the sibling blocks its PG, e.g. 'now 86.0%, projected
        87.1%', or None if it does not.

        It blocks if its OSD is projected over the cap or, for a PG that is
        backfill_toofull now (toofull_now), is at or above min_up_util today.
        """
        if not self.projection.knows(sibling.up_osd):
            return None
        projected = self.projection.utilization_after(sibling.up_osd, 0)
        now = self.osd_df[sibling.up_osd].get("utilization")
        near_full = toofull_now and now is not None and now >= self.min_up_util
        if projected <= self.max_target_util and not near_full:
            return None
        return f"now {now:.1f}%, projected {projected:.1f}%"

    def try_pin(
        self, state: PgState, blocker: ArrivingShard
    ) -> tuple[list[tuple["int | str", int, int]], str | None]:
        """Return the (shard, from, to) pins that cancel blocker, or ([], why not).

        The first pin is the blocker's own; the rest are companions.
        """
        pg = state.pg
        acting = pg["acting"]
        if state.is_ec:
            if blocker.acting_osd is None:
                return [], "no acting OSD"
            pins, why = close_pins(
                state.new_up, acting, {blocker.shard: blocker.acting_osd}, self.osd_host
            )
            if why is not None:
                return [], why
            moves = [(s, state.new_up[s], a) for s, a in pins.items()]
        else:
            up_set, acting_set = real_osd_set(pg["up"]), real_osd_set(acting)
            departing = acting_set - up_set
            arriving = (up_set - acting_set) - self.drained
            if len(departing) != 1 or len(arriving) != 1:
                return [], "replica pairing is ambiguous"
            (to_osd,) = departing
            why = pin_replica(state.new_up, blocker.up_osd, to_osd, self.osd_host)
            if why is not None:
                return [], why
            moves = [("-", blocker.up_osd, to_osd)]

        for s, from_osd, to_osd in moves:
            if to_osd in self.drained:
                return [], f"it would pin data back onto drained osd.{to_osd}"
            if (s if state.is_ec else from_osd) in state.changed:
                return [], "it would undo a move proposed in this run"
        froms = {m.up_osd for m in state.moves} | {f for _, f, _ in moves}
        tos = {m.target_osd for m in state.moves} | {t for _, _, t in moves}
        if froms & tos:
            return [], "the PG's pairs would chain, which pgremapper cannot apply"
        return moves, None


def shard_key(evacuee: Evacuee) -> int:
    """Order a PG's evacuees: EC by shard index, replicated by drained OSD id."""
    return evacuee.shard if isinstance(evacuee.shard, int) else evacuee.up_osd


def place_evacuees(
    planner: Planner, states: dict[str, PgState], evacuees: list[Evacuee]
) -> list[Evacuee]:
    """Place evacuees, largest first; return the unplaceable ones in PG order."""
    unplaceable = []
    order = sorted(
        evacuees, key=lambda e: (-e.size_bytes, pgid_sort_key(e.pgid), shard_key(e))
    )
    for evacuee in order:
        state = states[evacuee.pgid]
        picked = planner.place(state, evacuee.up_osd)
        if picked is None:
            unplaceable.append(evacuee)
            continue
        projected, target = picked
        planner.commit(state, evacuee, target)
        state.changed.add(evacuee.shard if state.is_ec else evacuee.up_osd)
        state.moves.append(
            Move(
                evacuee.pgid,
                evacuee.shard,
                evacuee.acting_osd,
                evacuee.up_osd,
                target,
                projected,
                "",
            )
        )
    unplaceable.sort(key=lambda e: (pgid_sort_key(e.pgid), shard_key(e)))
    return unplaceable


# resolve_blockers' verdicts on a PG.
STUCK = "stuck"
UNEXPLAINED = "unexplained"


def resolve_blockers(planner: Planner, state: PgState) -> tuple[int, int, str | None]:
    """Divert or pin the PG's blockers; return (diverted, pinned, verdict).

    Only called for a PG with at least one placed evacuee. verdict is None,
    or, with a NOTE on each of the PG's evacuees saying why:

      - STUCK: a blocker could be neither diverted nor pinned;
      - UNEXPLAINED: the PG is backfill_toofull now, yet no sibling is a
        blocker (see Planner.is_blocker) and no evacuee was arriving on a drained OSD (whose
        redirect might have been the fix), so the blocker is unidentified.
        Ceph refuses on usage it projects itself, so this PG may well stay
        stuck.
    """
    toofull_now = "backfill_toofull" in state.pg["state"].split("+")
    evacuated = list(state.moves)
    unblocks = "unblocks " + ", ".join(f"osd.{m.up_osd}" for m in evacuated)
    siblings = [
        s
        for s in find_arriving_shards(state.pg, state.is_ec, state.size_bytes)
        if s.up_osd not in planner.drained
    ]
    diverted = pinned = 0
    stuck_reasons = []
    for sibling in siblings:
        # Already pinned as an earlier blocker's companion: no longer arriving.
        if (sibling.shard if state.is_ec else sibling.up_osd) in state.changed:
            continue
        blocking = planner.is_blocker(sibling, toofull_now)
        if blocking is None:
            continue
        picked = planner.place(state, sibling.up_osd)
        if picked is not None:
            projected, target = picked
            planner.commit(state, sibling, target)
            state.changed.add(sibling.shard if state.is_ec else sibling.up_osd)
            state.moves.append(
                Move(
                    sibling.pgid,
                    sibling.shard,
                    sibling.acting_osd,
                    sibling.up_osd,
                    target,
                    projected,
                    f"diverted: {unblocks}",
                )
            )
            diverted += 1
            continue
        pins, why = planner.try_pin(state, sibling)
        if not pins:
            stuck_reasons.append(
                f"shard {sibling.shard} -> osd.{sibling.up_osd} "
                f"({blocking}; cannot pin: {why})"
            )
            continue
        for k, (s, from_osd, to_osd) in enumerate(pins):
            planner.projection.cancel(
                ArrivingShard(
                    state.pg["pgid"], s, from_osd, to_osd, [], state.size_bytes
                )
            )
            state.new_up[state.new_up.index(from_osd)] = to_osd
            state.changed.add(s if state.is_ec else from_osd)
            note = (
                f"pinned: {unblocks}"
                if k == 0
                else f"companion of shard {sibling.shard}"
            )
            state.moves.append(
                Move(state.pg["pgid"], s, to_osd, from_osd, to_osd, None, note)
            )
        pinned += 1

    if stuck_reasons:
        verdict, note = STUCK, "PG stays toofull: " + "; ".join(stuck_reasons)
    elif (
        not diverted
        and not pinned
        and toofull_now
        and all(m.acting_osd == m.up_osd for m in evacuated)
    ):
        verdict = UNEXPLAINED
        note = (
            "PG is backfill_toofull now, but no other shard is arriving on "
            "an OSD at or above --min-up-util or projected over "
            "--max-target-util: blocker unidentified"
        )
    else:
        return diverted, pinned, None
    state.moves = [m._replace(note=note) if m in evacuated else m for m in state.moves]
    return diverted, pinned, verdict


def host_osds(hosts: list[str], osd_host: dict[int, str]) -> set[int]:
    """Return every OSD of the given hosts (short or fully qualified names).

    Exits with an error naming any host that has no OSDs in 'ceph osd tree'
    (usually a typo), rather than draining the rest and silently not that one.
    """
    wanted = {h.split(".")[0] for h in hosts}
    osds = {o for o, h in osd_host.items() if h in wanted}
    missing = sorted(wanted - {osd_host[o] for o in osds})
    if missing:
        sys.exit(
            f"ERROR: no OSDs under host(s) in 'ceph osd tree': {', '.join(missing)}"
        )
    return osds


def plan(args: argparse.Namespace, store: SnapshotStore) -> DrainResult:
    """Fetch the cluster state from store and work out where each shard goes.

    Exits with an error if a given OSD or host is unknown, --max-target-util is
    out of range, or an affected pool cannot be analyzed safely.
    """
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    if args.hosts:
        drained = host_osds(args.hosts, osd_host)
    else:
        drained = set(args.osds)
        unknown = sorted(drained - osd_df.keys())
        if unknown:
            sys.exit(
                "ERROR: not in 'ceph osd df': " + ", ".join(f"osd.{o}" for o in unknown)
            )
    store.commands.update(ls_by_osd_commands(drained))
    upmap_items = fetch_upmap_items(store)
    pools_by_id = fetch_pools(store)
    ec_pool_ids = ec_pool_ids_from(list(pools_by_id.values()))
    crush_rules = fetch_crush_rules(store)
    ec_profiles = fetch_ec_profiles(store)
    ratios = fetch_full_ratios(store)
    max_target_util = resolve_max_target_util(args.max_target_util, ratios)
    min_up_util = ratios.nearfull if args.min_up_util is None else args.min_up_util

    drained_pgs = fetch_drained_pg_stats(store, drained)
    remapped_pgs = fetch_remapped_pg_stats(store)
    affected_pool_ids = {pgid_pool_id(pg["pgid"]) for pg in drained_pgs}
    unknown_pools = sorted(affected_pool_ids - pools_by_id.keys())
    if unknown_pools:
        sys.exit(
            "ERROR: PGs on the drained OSD(s) belong to pool id(s) "
            f"{', '.join(map(str, unknown_pools))}, which 'ceph osd pool ls "
            "detail' does not list, so their shards cannot be analyzed."
        )
    check_host_failure_domain(
        [pools_by_id[i] for i in sorted(affected_pool_ids)],
        crush_rules,
        "with a PG on a drained OSD",
    )

    def pg_info(pg: dict) -> tuple[bool, int]:
        pool_id = pgid_pool_id(pg["pgid"])
        is_ec = pool_id in ec_pool_ids
        pool = pools_by_id.get(pool_id)
        size = shard_size_bytes(pg, pool, ec_profiles) if pool else 0
        return is_ec, size

    # Every shard in motion counts towards its OSD, drained PGs' included.
    arriving = []
    for pg in {p["pgid"]: p for p in [*remapped_pgs, *drained_pgs]}.values():
        arriving.extend(find_arriving_shards(pg, *pg_info(pg)))
    projection = ProjectedUsage(osd_df, arriving)

    states: dict[str, PgState] = {}
    evacuees: list[Evacuee] = []
    leaving = 0
    for pg in drained_pgs:
        is_ec, size = pg_info(pg)
        found, gone = find_evacuees(pg, is_ec, drained, size)
        leaving += gone
        if found:
            raw = raw_crush_osds(pg["up"], upmap_items.get(pg["pgid"], []))
            states[pg["pgid"]] = PgState(pg, is_ec, size, raw)
            evacuees.extend(found)

    planner = Planner(
        osd_df,
        osd_host,
        build_candidate_osds(osd_df, exclude=drained),
        projection,
        max_uses=args.max_target_uses,
        max_target_util=max_target_util,
        min_up_util=min_up_util,
        drained=drained,
    )
    unplaceable = place_evacuees(planner, states, evacuees)

    diverted = pinned = 0
    stuck_pgs, unexplained_pgs = [], []
    moves = []
    for pgid, state in states.items():  # PG order, as drained_pgs is
        if not state.moves:
            continue
        d, p, verdict = resolve_blockers(planner, state)
        diverted += d
        pinned += p
        if verdict == STUCK:
            stuck_pgs.append(pgid)
        elif verdict == UNEXPLAINED:
            unexplained_pgs.append(pgid)
        moves.extend(state.moves)

    return DrainResult(
        osds=sorted(drained),
        hosts=sorted({h.split(".")[0] for h in args.hosts or []}),
        moves=moves,
        unplaceable=unplaceable,
        evacuee_count=len(evacuees),
        leaving_count=leaving,
        diverted_count=diverted,
        pinned_count=pinned,
        stuck_pgs=stuck_pgs,
        unexplained_pgs=unexplained_pgs,
        max_target_util=max_target_util,
        min_up_util=min_up_util,
        ratios=ratios,
        osd_df=osd_df,
        osd_host=osd_host,
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# Along the shard's path, as in divert-toofull: where its data is now
# (ACTING), where it is mapped today (UP, the upmap's 'from') and where it is
# proposed to go (TARGET, the 'to'). PROJ is the target's projected
# utilization once this and all earlier rows have completed; '-' for a pin,
# which sends the shard back where its data already is.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "HOST"),
    ("TARGET", "OSD"),
    ("TARGET", "UTIL"),
    ("TARGET", "PROJ"),
    ("TARGET", "HOST"),
    ("", "NOTE"),
]


def format_row(
    move: Move, osd_host: dict[int, str], osd_df: dict[int, dict]
) -> list[str]:
    return [
        move.pgid,
        str(move.shard),
        *osd_cells(osd_df, osd_host, move.acting_osd),
        *osd_cells(osd_df, osd_host, move.up_osd),
        *osd_cells(osd_df, osd_host, move.target_osd)[:2],
        NOT_APPLICABLE if move.projected is None else f"{move.projected:.1f}%",
        osd_host.get(move.target_osd, "?"),
        move.note,
    ]


def print_pgremapper_mappings(moves: list[Move]) -> None:
    """Print the moves as JSON for 'pgremapper import-mappings'.

    One {pgid, mapping: {from, to}} entry per line in a JSON array ("[]" when
    there are none, so the output is always valid JSON).
    """
    if not moves:
        print("[]")
        return
    print("[")
    for i, m in enumerate(moves):
        entry = {"pgid": m.pgid, "mapping": {"from": m.up_osd, "to": m.target_osd}}
        print(f"  {json.dumps(entry)}{',' if i < len(moves) - 1 else ''}")
    print("]")


def render(result: DrainResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    osds = ", ".join(f"osd.{o}" for o in result.osds)
    if result.hosts:
        osds = f"host(s) {', '.join(result.hosts)} ({osds})"
    stderr_para(
        f"Draining {osds}: {result.evacuee_count} shard(s) mapped to them "
        f"({result.leaving_count} more already moving off). Targets: up to "
        f"--max-target-uses {args.max_target_uses} shard(s) each, projected "
        f"at or below --max-target-util {result.max_target_util:g}% "
        f"(backfillfull_ratio {result.ratios.backfillfull:g}%). A shard of "
        "the same PG heading for an OSD projected over that cap -- or, for a "
        "PG backfill_toofull now, arriving on one at or above --min-up-util "
        f"{result.min_up_util:g}% -- counts as blocking it."
    )
    if args.pgremapper_mappings:
        print_pgremapper_mappings(result.moves)
    elif result.moves:
        print_table(
            COLUMNS,
            [format_row(m, result.osd_host, result.osd_df) for m in result.moves],
        )

    placed = result.evacuee_count - len(result.unplaceable)
    stderr_para(
        f"Proposed {placed} evacuation(s), {len(result.unplaceable)} unplaceable; "
        f"{result.diverted_count} blocking shard(s) diverted, "
        f"{result.pinned_count} pinned back; {len(result.stuck_pgs)} PG(s) "
        "will stay backfill_toofull regardless and "
        f"{len(result.unexplained_pgs)} are backfill_toofull for a reason "
        "not identified (see NOTE)."
    )
    if result.unplaceable:
        stderr_para(
            "NOTE: the unplaceable shards found no OSD with room under "
            "--max-target-util and --max-target-uses. This is a limitation "
            "of the greedy heuristic, not proof that none exists; apply "
            "these, let them drain, then re-run."
        )


def run(args: argparse.Namespace) -> None:
    # A copy: plan() adds the drained OSDs' commands to it.
    store = SnapshotStore.from_args(args, dict(SNAPSHOT_COMMANDS))
    render(plan(args, store), args)
