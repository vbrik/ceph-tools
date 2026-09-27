# SPDX-License-Identifier: MIT
"""Move shards off source OSDs until each is below a level: the engine behind
drain and balance.

The two commands differ only in the sources and the level they pass (drain:
the OSDs asked for, emptied unless given --until-util; balance: a device
class's OSDs at or above a level, which that class's targets must also stay
below). Everything else is decided here, the same way for both.

Utilization is projected (FinalUsage): what an OSD will hold once every
backfill in motion and every proposal completes, crediting data leaving it.
Each turn takes the source projected fullest and moves its largest shard
that has a legal target (one still arriving on it is redirected). A source is
done once it is below the level, or when none of its shards can go anywhere;
the others carry on. Without a level, a source is done when it has no shards
left.

The run's own moves take data off the sources and put it on other OSDs, never
the reverse, so none of them makes an OSD read and write backfill data at
once. Backfills already in motion only count in the projections: an OSD with
data arriving or leaving may still be a source or a target.

A legal target is up and in, of the shard's device class, not a source, not
in the PG's up or acting set or raw CRUSH mapping, not on a host the PG's
other shards use (the shard's own host is fine), and at or below
--max-target-util counting arrivals only (ProjectedUsage: Ceph checks when it
reserves the backfill, before the source frees anything). With a level, it
must also end up below the OSD it relieves, so that no move raises the
maximum or ping-pongs, and, in the level's device class if it has one
(balance), below the level, so that no move pushes another OSD to it. Emptying
an OSD may need fuller targets, so without a level neither applies. The
target that ends up least utilized wins.

Left alone: PGs that are not settled (UNSETTLED_FLAGS), since remapping them
slows recovery, and PGs whose existing upmap pairs chain, which pgremapper
breaks when it changes the PG.

backfill_toofull holds back a whole PG, so a moved shard also waits on any
sibling heading for an OSD projected at or over backfillfull_ratio or, if the
PG is backfill_toofull now, at or above nearfull_ratio (Ceph may count more
than the projection sees), a source included. Such a blocker is diverted if
there is room, otherwise pinned back to its acting OSD with companions, as in
cancel-backfill (resolve_blockers). If the pin clashes with a move of the same
PG onto the acting OSD's host, that move is placed elsewhere first. A pin may
leave its acting OSD above the level, or the class's maximum higher: the data
stays, as Ceph refuses its departure anyway. A pin onto a source is refused
unless the source stays below the level. A blocker in a PG with no proposed
move holds nothing up, and is only reported: its stall predates the run.
"""

import argparse
import heapq
import math
from collections import Counter
from functools import cached_property
from typing import NamedTuple

from messages import (
    blocking_reason,
    companion_note,
    format_bytes,
    level_text,
    print_level_unplaceable,
    print_shed_outcome,
    print_stalled,
    print_still_above,
    print_unplaceable,
)
from placement import (
    ArrivingShard,
    FinalUsage,
    FullRatios,
    MappedShard,
    PgPlacement,
    ProjectedUsage,
    build_candidate_osds,
    ec_pool_ids_from,
    fetch_full_ratios,
    find_arriving_shards,
    find_mapped_shards,
    osd_class,
    pick_target,
    project_usage,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
)
from shared import (
    NOT_APPLICABLE,
    ROLE_BLOCKER,
    ROLE_REQUESTED,
    UNKNOWN_HOST,
    SnapshotStore,
    chain_link,
    check_host_failure_domain,
    check_known_pools,
    close_pins,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    fold_pairs,
    osd_cells,
    osd_columns,
    pgid_pool_id,
    pgid_sort_key,
    pin_replica,
    print_table,
    print_upmap_entries,
    real_osd_set,
    upmap_entry,
    utilization_pct,
)

SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}

# PG state flags that keep a PG's shards in place: some of its data is
# missing or unsettled, and remapping it would slow or block recovery.
UNSETTLED_FLAGS = frozenset(
    {
        "degraded",
        "undersized",
        "recovering",
        "recovery_wait",
        "recovery_toofull",
        "forced_recovery",
        "peering",
        "down",
        "incomplete",
        "stale",
    }
)


def level_pct(text: str) -> float:
    """argparse type: a level, as utilization_pct but above 0.

    No OSD can get below 0%, and no target could end up there.
    """
    value = utilization_pct(text)
    if value == 0:
        raise argparse.ArgumentTypeError("must be above 0")
    return value


def add_until_util_arg(parser: "argparse._ActionsContainer") -> None:
    """Add --until-util, worded the same for drain and balance."""
    parser.add_argument(
        "--until-util",
        type=level_pct,
        metavar="PERCENT",
        help="Move shards off an OSD only while its projected utilization, "
        "crediting data leaving it, is at or above this.",
    )


def is_settled(pg: dict) -> bool:
    """True if the PG is active and none of its UNSETTLED_FLAGS are set."""
    flags = set(pg["state"].split("+"))
    return "active" in flags and not flags & UNSETTLED_FLAGS


# ---------------------------------------------------------------------------
# Cluster state
# ---------------------------------------------------------------------------


class Cluster:
    """The cluster state a run plans against. shed() updates the projections.

    The PG dump, by far the largest fetch, is read only when first needed,
    so that the commands can check their arguments first.
    """

    def __init__(self, store: SnapshotStore, max_target_util: float | None):
        """Fetch the OSD and pool state; exits on an invalid max_target_util."""
        self.store = store
        self.osd_host = fetch_osd_hosts(store)
        self.osd_df = fetch_osd_df(store)
        self.upmap_items = fetch_upmap_items(store)
        self.pools = fetch_pools(store)
        self.ec_pool_ids = ec_pool_ids_from(list(self.pools.values()))
        self.crush_rules = fetch_crush_rules(store)
        self.ec_profiles = fetch_ec_profiles(store)
        self.ratios: FullRatios = fetch_full_ratios(store)
        self.max_target_util = resolve_max_target_util(max_target_util, self.ratios)

    @cached_property
    def pgs(self) -> list[dict]:
        """Every PG ('ceph pg dump pgs')."""
        return fetch_pg_stats(self.store, "pg_dump_pgs")

    @cached_property
    def _projections(self) -> tuple[ProjectedUsage, FinalUsage]:
        """Every backfill in motion, projected; exits on an unknown pool."""
        in_motion = [pg for pg in self.pgs if pg["up"] != pg["acting"]]
        check_known_pools((pg["pgid"] for pg in in_motion), self.pools, "PGs in motion")
        return project_usage(self.osd_df, ((pg, *self.pg_info(pg)) for pg in in_motion))

    @property
    def reservation(self) -> ProjectedUsage:
        """What each OSD will hold, counting arrivals only (target caps, blockers)."""
        return self._projections[0]

    @property
    def final(self) -> FinalUsage:
        """Where each OSD ends up, crediting data leaving it."""
        return self._projections[1]

    def pg_info(self, pg: dict) -> tuple[bool, int]:
        """Return (is the PG's pool EC, its shard size in bytes).

        The pool must be known (check_known_pools).
        """
        pool_id = pgid_pool_id(pg["pgid"])
        return pool_id in self.ec_pool_ids, shard_size_bytes(
            pg, self.pools[pool_id], self.ec_profiles
        )

    def existing_pairs(self, pg: dict) -> list[tuple[int, int]]:
        """Return the PG's existing upmap pairs, as (from, to)."""
        return [(m["from"], m["to"]) for m in self.upmap_items.get(pg["pgid"], [])]

    def pairs_chain(self, pg: dict) -> bool:
        """True if the PG's existing upmap pairs chain (A->B, B->C).

        pgremapper would drop a link of them on any change (see
        shared.avoid_chains), remapping a shard nobody proposed.
        """
        return chain_link(self.existing_pairs(pg)) is not None


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


class Move(NamedTuple):
    """One proposed upmap pair, and why it is proposed."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    acting_osd: int | None  # where the shard's data is now
    up_osd: int  # the 'from' of the pair
    target_osd: int  # the 'to' of the pair
    size_bytes: int
    up_projected: float | None  # FinalUsage once everything is done
    target_projected: float | None  # likewise; None for pins
    note: str = ""
    role: str = ROLE_REQUESTED  # blockers and their companions: ROLE_BLOCKER


def is_pin(move: Move) -> bool:
    """True if the move keeps the shard's data where it is: a pin."""
    return move.target_osd == move.acting_osd


class PgState(PgPlacement):
    """One PG with a shard to move, and the moves proposed for it."""

    def __init__(self, pg: dict, is_ec: bool, size_bytes: int, raw: set[int]):
        super().__init__(pg, is_ec, size_bytes, raw)
        # Moving onto an acting OSD would be a pin, not a move.
        self.forbidden_osds |= real_osd_set(pg["acting"])
        self.avoid_hosts: set[str] = set()  # see Planner.make_room_to_pin
        self.changed: set[int | str] = set()  # EC slots / replica OSDs moved
        self.moves: list[Move] = []

    def forbidden_hosts(self, moving_osd: int, osd_host: dict[int, str]) -> set:
        """PgPlacement's, and avoid_hosts."""
        return super().forbidden_hosts(moving_osd, osd_host) | self.avoid_hosts

    def key(self, shard: "MappedShard | ArrivingShard | Move") -> "int | str":
        """The shard's key in changed: EC slot, or replica's up OSD."""
        return shard.shard if self.is_ec else shard.up_osd

    def is_toofull(self) -> bool:
        """True if the PG is backfill_toofull now."""
        return "backfill_toofull" in self.pg["state"].split("+")

    def add_move(
        self,
        shard: "MappedShard | ArrivingShard",
        target: int,
        note: str = "",
        role: str = ROLE_REQUESTED,
    ) -> None:
        """Record a move of shard to target; projections are filled in later."""
        self.moves.append(
            Move(
                self.pg["pgid"],
                shard.shard,
                shard.acting_osd,
                shard.up_osd,
                target,
                self.size_bytes,
                None,
                None,
                note,
                role,
            )
        )


class Planner:
    """One run's placement rules: sources, level, projections."""

    def __init__(
        self,
        cluster: Cluster,
        sources: set[int],
        level: float | None,
        level_class: str | None,
    ):
        """level_class: the device class whose targets must stay below level."""
        self.cluster = cluster
        self.sources = sources
        self.level = level
        self.level_class = level_class
        self.candidates = build_candidate_osds(cluster.osd_df, exclude=sources)

    def is_below_level(self, osd_id: int, extra_bytes: int = 0) -> bool:
        """True if osd_id's final projection, with extra_bytes more, is below the level.

        False without a level, or for an OSD without a capacity figure: it
        is drained in full.
        """
        final = self.cluster.final
        return (
            self.level is not None
            and final.knows(osd_id)
            and final.utilization(osd_id, extra_bytes) < self.level
        )

    def place(
        self, state: PgState, shard: "MappedShard | ArrivingShard"
    ) -> tuple[float, int] | None:
        """Return (final projection, OSD) of the best legal target, or None."""
        c = self.cluster
        cls = osd_class(c.osd_df, shard.up_osd)
        below = None
        if self.level is not None:
            if c.final.knows(shard.up_osd):
                # Strictly below: equal would leave the maximum where it is.
                below = c.final.utilization(shard.up_osd, -shard.size_bytes)
            if cls is not None and cls == self.level_class:
                below = self.level if below is None else min(below, self.level)
        return pick_target(
            self.candidates.get(cls, []),
            shard.size_bytes,
            forbidden_hosts=state.forbidden_hosts(shard.up_osd, c.osd_host),
            forbidden_osds=state.forbidden_osds,
            osd_host=c.osd_host,
            osd_df=c.osd_df,
            projection=c.reservation,
            max_target_util=c.max_target_util,
            final=c.final,
            final_below=below,
        )

    def commit(
        self, state: PgState, shard: "MappedShard | ArrivingShard", target: int
    ) -> None:
        """Record in the projections and state that shard goes to target."""
        c = self.cluster
        if shard.acting_osd == shard.up_osd:
            c.reservation.add(target, shard.size_bytes)
        else:
            # Still arriving on its up OSD: that backfill is cancelled.
            c.reservation.redirect(shard, target)
        c.final.move(shard.up_osd, target, shard.size_bytes)
        state.retarget(shard.up_osd, target)
        state.changed.add(state.key(shard))

    def uncommit(self, state: PgState, shard: MappedShard, target: int) -> None:
        """Undo commit(state, shard, target). target stays forbidden."""
        c = self.cluster
        c.reservation.add(target, -shard.size_bytes)
        if shard.acting_osd != shard.up_osd and c.reservation.knows(shard.up_osd):
            c.reservation.add(shard.up_osd, shard.size_bytes)
        c.final.move(target, shard.up_osd, shard.size_bytes)
        state.new_up[state.new_up.index(target)] = shard.up_osd
        state.changed.discard(state.key(shard))

    def is_blocker(
        self, shard: "ArrivingShard | MappedShard", toofull_now: bool
    ) -> str | None:
        """Return why the shard arriving on its up OSD blocks its PG, or None.

        It blocks if that OSD is projected at or over backfillfull_ratio
        (messages.blocking_reason, as in cancel-backfill) or, with toofull_now,
        is at or above nearfull_ratio now: Ceph may count more than the
        projection sees. The reason names the OSD.
        """
        c = self.cluster
        osd_id = shard.up_osd
        if not c.reservation.knows(osd_id):
            return None
        projected = c.reservation.utilization_after(osd_id, 0)
        now = c.osd_df[osd_id].get("utilization")
        if projected >= c.ratios.backfillfull:
            return blocking_reason(osd_id, projected)
        if toofull_now and now is not None and now >= c.ratios.nearfull:
            return (
                f"osd.{osd_id} now {now:.1f}% >= nearfull_ratio "
                f"{c.ratios.nearfull:g}% and PG is backfill_toofull"
            )
        return None

    def chains(self, state: PgState, pairs: list[tuple[int, int]]) -> bool:
        """True if the PG's pairs would chain with pairs added: pgremapper cannot apply them.

        Compared as pgremapper applies them: after the run's pairs fold into
        existing ones (shared.fold_pairs); an existing pair whose 'from' is
        still in 'up' is stale, and ignored.
        """
        existing = [
            p
            for p in self.cluster.existing_pairs(state.pg)
            if p[0] not in state.pg["up"]
        ]
        ours = [(m.up_osd, m.target_osd) for m in state.moves] + pairs
        rest, effective = fold_pairs(existing, ours)
        return chain_link(rest + effective) is not None

    def try_pin(
        self, state: PgState, blocker: "ArrivingShard | MappedShard"
    ) -> tuple[list[tuple["int | str", int, int]], str | None]:
        """Return the (shard, from, to) pins that cancel blocker's backfill, or ([], why not).

        blocker is arriving on its up OSD. Its own pin comes first, then
        companions. Refused, besides the reasons close_pins gives, if a pin
        would land on a source (unless it stays below the level), undo a move
        proposed in this run, or chain (pgremapper cannot apply chains).
        """
        pg = state.pg
        acting = pg["acting"]
        osd_host = self.cluster.osd_host
        if state.is_ec:
            if blocker.acting_osd is None:
                return [], "no acting OSD"
            pins, why = close_pins(
                state.new_up, acting, {blocker.shard: blocker.acting_osd}, osd_host
            )
            if why is not None:
                return [], why
            moves = [(s, state.new_up[s], a) for s, a in pins.items()]
        else:
            up_set, acting_set = real_osd_set(pg["up"]), real_osd_set(acting)
            departing = acting_set - up_set
            # Other sources' replicas are redirected, not paired.
            arriving = (up_set - acting_set) - (self.sources - {blocker.up_osd})
            if len(departing) != 1 or len(arriving) != 1:
                return [], "replica pairing is ambiguous"
            (to_osd,) = departing
            why = pin_replica(state.new_up, blocker.up_osd, to_osd, osd_host)
            if why is not None:
                return [], why
            moves = [("-", blocker.up_osd, to_osd)]

        pinned_back: Counter[int] = Counter()  # bytes kept on each source
        for s, from_osd, to_osd in moves:
            if to_osd in self.sources:
                pinned_back[to_osd] += state.size_bytes
                if not self.is_below_level(to_osd, pinned_back[to_osd]):
                    level = (
                        ""
                        if self.level is None
                        else f", taking it to the {level_text(self.level)} level "
                        "or above"
                    )
                    return [], f"it would pin data back onto source osd.{to_osd}{level}"
            if (s if state.is_ec else from_osd) in state.changed:
                return [], "it would undo a move proposed in this run"
        if self.chains(state, [(f, t) for _, f, t in moves]):
            return [], "the PG's pairs would chain, which pgremapper cannot apply"
        return moves, None

    def make_room_to_pin(self, state: PgState, blocker: ArrivingShard) -> bool:
        """Re-place the move of the PG onto the host of blocker's acting OSD.

        That move keeps blocker from being pinned back there. Return True
        if it now goes elsewhere (the host stays avoided for the PG), False
        if there is no such move or no other target, leaving it as it was.
        """
        host = self.cluster.osd_host.get(blocker.acting_osd)
        clash = next(
            (
                (i, m)
                for i, m in enumerate(state.moves)
                if m.role == ROLE_REQUESTED
                and not is_pin(m)
                and self.cluster.osd_host.get(m.target_osd) == host
            ),
            None,
        )
        if host is None or clash is None:
            return False
        i, move = clash
        shard = MappedShard(
            move.pgid, move.shard, move.up_osd, move.acting_osd, move.size_bytes
        )
        self.uncommit(state, shard, move.target_osd)
        state.avoid_hosts.add(host)
        picked = self.place(state, shard)
        if picked is None:
            state.avoid_hosts.discard(host)
            self.commit(state, shard, move.target_osd)
            return False
        self.commit(state, shard, picked[1])
        state.moves[i] = move._replace(target_osd=picked[1])
        return True

    def pin(
        self, state: PgState, pins: list[tuple["int | str", int, int]], note: str
    ) -> None:
        """Record try_pin's pins of a blocker: its own with note, then its companions."""
        c = self.cluster
        for k, (s, from_osd, to_osd) in enumerate(pins):
            pinned = ArrivingShard(
                state.pg["pgid"], s, from_osd, to_osd, [], state.size_bytes
            )
            c.reservation.cancel(pinned)
            c.final.move(from_osd, to_osd, state.size_bytes)
            state.new_up[state.new_up.index(from_osd)] = to_osd
            state.changed.add(s if state.is_ec else from_osd)
            if k:
                note = companion_note(pins[0][0], of_blocker=True)
            state.add_move(pinned, to_osd, note, ROLE_BLOCKER)


def shard_key(shard: "MappedShard | ArrivingShard") -> int:
    """Order a PG's shards: EC by shard index, replicated by up OSD."""
    return shard.shard if isinstance(shard.shard, int) else shard.up_osd


def shard_order(shard: MappedShard) -> tuple:
    """Order a source's shards: largest first, then by PG and shard."""
    return (-shard.size_bytes, pgid_sort_key(shard.pgid), shard_key(shard))


def shed_sources(
    planner: Planner,
    states: dict[str, PgState],
    by_source: dict[int, list[MappedShard]],
) -> list[tuple[int, MappedShard]]:
    """Move shards off the sources, fullest first; return the (source, shard)s left.

    by_source: each source's shards, consumed. Each turn moves the fullest
    source's largest shard with a legal target; one with none is left
    (targets only fill up, so it would not get one later). So is every
    shard of a source once it is below the level. A source without a
    capacity figure goes first, and is drained in full.
    """
    final = planner.cluster.final

    def priority(source: int) -> tuple[float, int]:
        util = final.utilization(source) if final.knows(source) else math.inf
        return -util, source

    heap = [priority(s) for s in by_source]
    heapq.heapify(heap)
    # Reversed, so pop() takes the largest.
    todo = {
        s: sorted(shards, key=shard_order, reverse=True)
        for s, shards in by_source.items()
    }
    left: list[tuple[int, MappedShard]] = []

    while heap:
        _, source = heapq.heappop(heap)
        shards = todo[source]
        if planner.is_below_level(source):
            left.extend((source, shard) for shard in shards)
            shards.clear()
            continue
        while shards:
            shard = shards.pop()
            state = states[shard.pgid]
            picked = planner.place(state, shard)
            if picked is None:
                left.append((source, shard))
                continue
            planner.commit(state, shard, picked[1])
            state.add_move(shard, picked[1])
            heapq.heappush(heap, priority(source))
            break
    return left


# resolve_blockers' verdicts on a PG.
STUCK = "stuck"
UNEXPLAINED = "unexplained"


def resolve_blockers(planner: Planner, state: PgState) -> tuple[int, int, str | None]:
    """Divert or pin the PG's blockers; return (diverted, pinned, verdict).

    Called after the sources are shed, since diverting uses the same room.
    verdict, also noted on each requested move, is None or:

    - STUCK: a blocker could be neither diverted nor pinned;
    - UNEXPLAINED: the PG is backfill_toofull now, but no sibling is a
      blocker and no moved shard was arriving on a source.
    """
    toofull_now = state.is_toofull()
    requested = list(state.moves)
    # What a blocker would hold up, e.g. 'shard 9 leaving osd.231'.
    held_up = " and ".join(
        f"shard {m.shard} leaving osd.{m.up_osd}"
        if state.is_ec
        else f"the replica leaving osd.{m.up_osd}"
        for m in requested
    )

    def blocker_note(action: str, blocking: str) -> str:
        return f"{action}: {blocking}, which would stall the PG, holding up {held_up}"

    # Those arriving on a source too: shards left there.
    siblings = find_arriving_shards(state.pg, state.is_ec, state.size_bytes)
    diverted = pinned = 0
    stuck_reasons = []
    for sibling in siblings:
        # Moved in this run, or pinned as an earlier blocker's companion.
        if state.key(sibling) in state.changed:
            continue
        blocking = planner.is_blocker(sibling, toofull_now)
        if blocking is None:
            continue
        picked = planner.place(state, sibling)
        if picked is not None:
            planner.commit(state, sibling, picked[1])
            state.add_move(
                sibling, picked[1], blocker_note("diverted", blocking), ROLE_BLOCKER
            )
            diverted += 1
            continue
        pins, why = planner.try_pin(state, sibling)
        if not pins and planner.make_room_to_pin(state, sibling):
            pins, why = planner.try_pin(state, sibling)
        if not pins:
            stuck_reasons.append(
                f"shard {sibling.shard} -> {blocking} (cannot pin: {why})"
            )
            continue
        planner.pin(state, pins, blocker_note("pinned, no room to divert", blocking))
        pinned += 1

    # make_room_to_pin may have re-placed a requested move.
    requested = [m for m in state.moves if m.role == ROLE_REQUESTED]
    if stuck_reasons:
        verdict, note = STUCK, "PG stays toofull: " + "; ".join(stuck_reasons)
    elif (
        not diverted
        and not pinned
        and toofull_now
        and all(m.acting_osd == m.up_osd for m in requested)
    ):
        verdict = UNEXPLAINED
        note = (
            "PG is backfill_toofull now, but no other shard is arriving on "
            "an OSD at or above nearfull_ratio or projected at or over "
            "backfillfull_ratio: blocker unidentified"
        )
    else:
        return diverted, pinned, None
    state.moves = [m._replace(note=note) if m in requested else m for m in state.moves]
    return diverted, pinned, verdict


class ShedResult(NamedTuple):
    """What shed() decided, for drain's and balance's render()."""

    sources: list[int]  # in id order
    level: float | None  # None: empty the sources
    level_class: str | None  # the class whose targets must stay below level
    moves: list[Move]  # in PG order; within a PG, in the order proposed
    unplaceable: list[MappedShard]  # in PG, then shard order
    mapped_count: int  # shards mapped to a source, in PGs not left alone
    kept_count: int  # of those, left on a source below the level
    leaving_count: int  # shards already moving off a source
    unsettled_pgs: int  # PGs with a shard mapped to a source, left alone
    chained_pgs: int  # likewise, as their existing upmap pairs chain
    diverted_count: int  # blockers diverted
    pinned_count: int  # blockers pinned back (companions not counted)
    stuck_pgs: list[str]  # PGs with a requested move that will stay toofull
    unexplained_pgs: list[str]  # toofull now, blocker not identified
    # PGs with no proposed move, and a blocker arriving on a source.
    stalled_pgs: list[str]
    # Each source's final projection; sources without a capacity figure
    # are absent.
    final_util: dict[int, float]
    # With a level, the sources whose final_util is at or above it, in id order.
    still_above: list[tuple[int, float]]
    max_target_util: float
    ratios: FullRatios
    osd_df: dict[int, dict]
    osd_host: dict[int, str]

    @property
    def requested(self) -> list[Move]:
        """The moves off the sources, but for blockers and their companions."""
        return [m for m in self.moves if m.role == ROLE_REQUESTED]

    @property
    def moved_bytes(self) -> int:
        """Data the requested moves take off the sources."""
        return sum(m.size_bytes for m in self.requested)


def shed(
    cluster: Cluster,
    sources: set[int],
    level: float | None,
    level_class: str | None = None,
) -> ShedResult:
    """Work out the moves that take each source below level (None: empty them).

    level_class: the device class whose targets must end up below level too.
    Updates cluster's projections to include the moves. Exits if a PG to
    move is in an unknown pool, or one whose CRUSH failure domain is not host.
    """
    touching = [
        pg
        for pg in cluster.pgs
        if sources & (real_osd_set(pg["up"]) | real_osd_set(pg["acting"]))
    ]
    check_known_pools(
        (pg["pgid"] for pg in touching), cluster.pools, "PGs on the sources"
    )
    leaving = unsettled = chained = 0
    movable: list[tuple[dict, bool, int, list[MappedShard]]] = []
    for pg in touching:
        is_ec, size = cluster.pg_info(pg)
        found, gone = find_mapped_shards(pg, is_ec, sources, size)
        leaving += gone
        if not found:
            continue
        if not is_settled(pg):
            unsettled += 1
        elif cluster.pairs_chain(pg):
            chained += 1
        else:
            movable.append((pg, is_ec, size, found))
    movable.sort(key=lambda m: pgid_sort_key(m[0]["pgid"]))
    check_host_failure_domain(
        (pg["pgid"] for pg, *_ in movable),
        cluster.pools,
        cluster.crush_rules,
        "PGs to move",
    )

    states: dict[str, PgState] = {}
    by_source: dict[int, list[MappedShard]] = {s: [] for s in sources}
    for pg, is_ec, size, found in movable:
        raw = raw_crush_osds(pg["up"], cluster.upmap_items.get(pg["pgid"], []))
        states[pg["pgid"]] = PgState(pg, is_ec, size, raw)
        for shard in found:
            by_source[shard.up_osd].append(shard)

    planner = Planner(cluster, sources, level, level_class)
    left = shed_sources(planner, states, by_source)

    diverted = pinned = 0
    stuck_pgs, unexplained_pgs = [], []
    moves: list[Move] = []
    for pgid, state in states.items():  # PG order, as movable is
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

    # Only now: a blocker diverted or pinned off a source may have taken it
    # below the level, or the shard itself.
    unplaceable, kept, stalled = [], 0, set()
    for source, shard in left:
        state = states[shard.pgid]
        if state.key(shard) in state.changed:
            continue
        if planner.is_below_level(source):
            kept += 1
        else:
            unplaceable.append(shard)
        if (
            not state.moves
            and shard.acting_osd != shard.up_osd
            and planner.is_blocker(shard, state.is_toofull())
        ):
            stalled.add(shard.pgid)
    unplaceable.sort(key=lambda s: (pgid_sort_key(s.pgid), shard_key(s)))

    final = cluster.final

    def projected(osd_id: int) -> float | None:
        return final.utilization(osd_id) if final.knows(osd_id) else None

    # Once everything is placed, so that the rows of one OSD agree. A pin's
    # data stays where it is, so its target has no projection.
    moves = [
        m._replace(
            up_projected=projected(m.up_osd),
            target_projected=None if is_pin(m) else projected(m.target_osd),
        )
        for m in moves
    ]
    final_util = {o: final.utilization(o) for o in sorted(sources) if final.knows(o)}
    still_above = (
        [] if level is None else [(o, u) for o, u in final_util.items() if u >= level]
    )
    return ShedResult(
        sources=sorted(sources),
        level=level,
        level_class=level_class,
        moves=moves,
        unplaceable=unplaceable,
        mapped_count=sum(len(shards) for *_, shards in movable),
        kept_count=kept,
        leaving_count=leaving,
        unsettled_pgs=unsettled,
        chained_pgs=chained,
        diverted_count=diverted,
        pinned_count=pinned,
        stuck_pgs=stuck_pgs,
        unexplained_pgs=unexplained_pgs,
        stalled_pgs=sorted(stalled, key=pgid_sort_key),
        final_util=final_util,
        still_above=still_above,
        max_target_util=cluster.max_target_util,
        ratios=cluster.ratios,
        osd_df=cluster.osd_df,
        osd_host=cluster.osd_host,
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# (group, label), along the shard's path: ACTING (data now), UP (where it is
# mapped, the 'from'), TARGET (the 'to'). PROJ: FinalUsage once everything
# is done; '-' for a pin's TARGET, where the data already is.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("", "SIZE"),
    *osd_columns("ACTING"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "PROJ"),
    ("UP", "HOST"),
    ("TARGET", "OSD"),
    ("TARGET", "UTIL"),
    ("TARGET", "PROJ"),
    ("TARGET", "HOST"),
    ("", "NOTE"),
]


def format_projection(value: float | None) -> str:
    return NOT_APPLICABLE if value is None else f"{value:.1f}%"


def format_row(
    move: Move, osd_host: dict[int, str], osd_df: dict[int, dict]
) -> list[str]:
    up_cells = osd_cells(osd_df, osd_host, move.up_osd)
    target_cells = osd_cells(osd_df, osd_host, move.target_osd)
    return [
        move.pgid,
        str(move.shard),
        format_bytes(move.size_bytes),
        *osd_cells(osd_df, osd_host, move.acting_osd),
        *up_cells[:2],
        format_projection(move.up_projected),
        up_cells[2],
        *target_cells[:2],
        format_projection(move.target_projected),
        osd_host.get(move.target_osd, UNKNOWN_HOST),
        move.note,
    ]


def print_pgremapper_mappings(moves: list[Move]) -> None:
    """Print the moves as JSON for 'pgremapper import-mappings', one per line.

    Each entry carries its table row's SHARD, role and NOTE.
    """
    print_upmap_entries(
        upmap_entry(
            m.pgid, m.up_osd, m.target_osd, shard=m.shard, role=m.role, note=m.note
        )
        for m in moves
    )


def print_moves(result: ShedResult, args: argparse.Namespace) -> None:
    """Print the moves on stdout, in the format args asks for."""
    if args.pgremapper_mappings:
        print_pgremapper_mappings(result.moves)
    elif result.moves:
        print_table(
            COLUMNS,
            [format_row(m, result.osd_host, result.osd_df) for m in result.moves],
        )


def print_outcome(result: ShedResult, off: str) -> None:
    """Sum up the moves off the sources on stderr; off names them."""
    r = result
    print_shed_outcome(
        off,
        len(r.requested),
        r.moved_bytes,
        len(r.unplaceable),
        None if r.level is None else r.kept_count,
        r.diverted_count,
        r.pinned_count,
        r.stuck_pgs,
        r.unexplained_pgs,
    )


def print_notes(result: ShedResult, which: str) -> None:
    """Print on stderr what the run leaves undone, naming the sources which.

    The sources left above the level; the shards no target was found for:
    emptying OSDs (no level), each is listed, while with a level (which an
    unreachable one can leave thousands) they are counted; and the PGs left
    in backfill_toofull as they were.
    """
    if result.level is None:
        print_unplaceable(
            ((s.pgid, s.shard, f"off osd.{s.up_osd}") for s in result.unplaceable),
            "--max-target-util",
        )
    else:
        print_still_above(which, result.level, result.still_above)
        guards = (
            "their source"
            if result.level_class is None
            else "the level and their source"
        )
        print_level_unplaceable(len(result.unplaceable), guards)
    print_stalled(result.stalled_pgs)


def unsized_sources(result: ShedResult) -> list[int]:
    """Return the sources without a capacity figure: drained in full, whatever the level."""
    return [o for o in result.sources if o not in result.final_util]
