# SPDX-License-Identifier: MIT
"""Move shards off source OSDs until each is below a level: the engine behind
drain and balance. divert-toofull places its shards with the same Planner.

The two commands differ only in the sources and the level they pass (drain:
the OSDs asked for, emptied unless given --until-util; balance: a device
class's OSDs at or above a level, which that class's targets must also stay
below). Everything else is decided here, the same way for both.

Utilization is projected (FinalUsage): what an OSD will hold once every
backfill in motion and every proposal completes, crediting data leaving it.
A backfill under way adds only what it has yet to copy (placement.with_copied),
so every PG in motion is queried for its backfill position. Each turn takes
the source projected fullest and moves its largest shard that has a legal
target (one still arriving on it is redirected). A source is done once it is
below the level, or when none of its shards can go anywhere; the others carry
on. Without a level, a source is done when it has no shards left.

The run's own moves take data off the sources and put it on other OSDs, never
the reverse, so none of them makes an OSD read and write backfill data at
once. Backfills already in motion only count in the projections: an OSD with
data arriving or leaving may still be a source or a target.

A legal target is up and in, of the shard's device class, not a source, not
in the PG's up or acting set or raw CRUSH mapping, not on a host the PG's
other shards use (the shard's own host is fine), and at or below
--max-target-util counting arrivals only (ProjectedUsage: Ceph checks when it
reserves the backfill, before the source frees anything), and its pairs must
not chain with the PG's others. With a level, it must also end up below the
OSD it relieves, so that no move raises the maximum or ping-pongs, and, in the
level's device class if it has one (balance), below the level, so that no move
pushes another OSD to it. Emptying an OSD may need fuller targets, so without a
level neither applies. The target that ends up least utilized wins.

Ending up below the OSD it relieves holds for the whole run (and in
divert-toofull, which always applies it), not just when a move is chosen: no
later move may take a target to or above an OSD it relieved, or an OSD to or
below one of its targets. Pins are exempt.

Left alone: PGs that are not settled (UNSETTLED_FLAGS), since remapping them
slows recovery, and PGs whose existing upmap pairs chain, which pgremapper
breaks when it changes the PG.

backfill_toofull holds back a whole PG, so a moved shard also waits on any
sibling heading for an OSD projected at or over backfillfull_ratio or, if the
PG is backfill_toofull now, at or above nearfull_ratio (Ceph may count more
than the projection sees), a source included. Such a blocker is diverted if
there is room, otherwise pinned back to its acting OSD with companions, as in
cancel-backfill (resolve_blockers). If the pin clashes with a move of the same
PG onto the acting OSD's host, that move is placed elsewhere, provided the pin
then succeeds. A pin may leave its acting OSD above the level, or the class's
maximum higher: the data stays, as Ceph refuses its departure anyway. A pin onto a source is refused
unless the source stays below the level. A blocker in a PG with no proposed
move holds nothing up, and is only reported: its stall predates the run.
"""

import argparse
import heapq
import math
from bisect import bisect_left, insort
from collections import Counter, defaultdict
from functools import cached_property
from typing import NamedTuple

from messages import (
    PROJECTION_QUERY_EFFECT,
    blocking_reason,
    companion_note,
    format_bytes,
    level_text,
    print_balancer_note,
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
    blocker_projection,
    build_candidate_osds,
    ec_pool_ids_from,
    fetch_full_ratios,
    find_arriving_shards,
    find_mapped_shards,
    osd_class,
    project_usage,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
    target_projection,
)
from shared import (
    KIB,
    ROLE_BLOCKER,
    ROLE_REQUESTED,
    SnapshotStore,
    chain_link,
    check_host_failure_domain,
    check_known_pools,
    close_pins,
    fetch_backfill_positions,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    fold_pairs,
    format_projection,
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


def add_until_util_arg(parser: argparse._ActionsContainer) -> None:
    """Add --until-util, worded the same for drain and balance."""
    parser.add_argument(
        "--until-util",
        type=level_pct,
        metavar="PERCENT",
        help="Move shards off an OSD only while its projected utilization, "
        "crediting data leaving it, is at or above this.",
    )


def is_toofull(pg: dict) -> bool:
    """True if the PG is backfill_toofull now."""
    return "backfill_toofull" in pg["state"].split("+")


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
    def candidates(self) -> dict[str, list[int]]:
        """Usable target OSDs per device class (placement.build_candidate_osds)."""
        return build_candidate_osds(self.osd_df)

    @cached_property
    def _projections(self) -> tuple[ProjectedUsage, FinalUsage]:
        """Every backfill in motion, projected; exits on an unknown pool.

        Queries the backfill position of every PG in motion.
        """
        in_motion = [pg for pg in self.pgs if pg["up"] != pg["acting"]]
        check_known_pools((pg["pgid"] for pg in in_motion), self.pools, "PGs in motion")
        positions = fetch_backfill_positions(
            self.store, (pg["pgid"] for pg in in_motion), PROJECTION_QUERY_EFFECT
        )
        return project_usage(
            self.osd_df, in_motion, self.pools, self.ec_profiles, positions
        )

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

    def pg_state(self, pg: dict) -> "PgState":
        """Return a PgState of the PG, as yet unchanged; its pool must be known."""
        raw = raw_crush_osds(pg["up"], self.upmap_items.get(pg["pgid"], []))
        return PgState(pg, *self.pg_info(pg), raw)

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
    shard: int | str  # EC shard index, or '-' for replicated pools
    acting_osd: int | None  # where the shard's data is now
    up_osd: int  # the 'from' of the pair
    target_osd: int  # the 'to' of the pair
    size_bytes: int
    up_projected: float | None = None  # FinalUsage once everything is done
    target_projected: float | None = None  # likewise; None for pins
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
        self.changed: set[int | str] = set()  # keys (key()) of the shards moved
        self.moves: list[Move] = []

    def key(self, shard: MappedShard | ArrivingShard) -> int | str:
        """The shard's key in changed: EC slot, or replica's up OSD."""
        return shard.shard if self.is_ec else shard.up_osd

    def add_move(
        self,
        shard: MappedShard | ArrivingShard,
        target: int,
        note: str = "",
        role: str = ROLE_REQUESTED,
    ) -> None:
        """Record a move of shard to target; projections are filled in later."""
        self.moves.append(
            Move(
                pgid=self.pg["pgid"],
                shard=shard.shard,
                acting_osd=shard.acting_osd,
                up_osd=shard.up_osd,
                target_osd=target,
                size_bytes=self.size_bytes,
                note=note,
                role=role,
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
        *,
        relieve: bool,
    ):
        """level_class: the device class whose targets must stay below level.

        relieve: a target must end up below the OSD it relieves, once every
        move is done (highest_target, lowest_relieved).
        """
        self.cluster = cluster
        self.sources = sources
        self.level = level
        self.level_class = level_class
        self.relieve = relieve
        # The committed moves, both ways: the OSDs each target relieved, and
        # the targets each relieved OSD sent data to (counted, as a pair may
        # repeat).
        self.relieved: defaultdict[int, Counter[int]] = defaultdict(Counter)
        self.sent_to: defaultdict[int, Counter[int]] = defaultdict(Counter)
        final, osd_df = cluster.final, cluster.osd_df
        # Each class's candidates as (final projection, OSD), kept in order
        # as moves change them (reorder), and the largest capacity: see place.
        self.order: dict[str, list[tuple[float, int]]] = {}
        self.max_capacity: dict[str, int] = {}
        self.order_key: dict[int, tuple[str, tuple[float, int]]] = {}
        for cls, all_osds in cluster.candidates.items():
            osds = [o for o in all_osds if o not in sources]
            if not osds:
                continue
            self.order[cls] = sorted((final.utilization(o), o) for o in osds)
            self.max_capacity[cls] = max(osd_df[o]["kb"] * KIB for o in osds)
            for key in self.order[cls]:
                self.order_key[key[1]] = cls, key

    def move_final(self, from_osd: int, to_osd: int, size_bytes: int) -> None:
        """FinalUsage.move, keeping the candidates in order: every change goes here."""
        self.cluster.final.move(from_osd, to_osd, size_bytes)
        for osd in (from_osd, to_osd):
            if osd not in self.order_key:
                continue
            cls, key = self.order_key[osd]
            order = self.order[cls]
            i = bisect_left(order, key)
            assert order[i] == key, (osd, key)
            del order[i]
            key = self.cluster.final.utilization(osd), osd
            insort(order, key)
            self.order_key[osd] = cls, key

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
        self,
        state: PgState,
        shard: MappedShard | ArrivingShard,
        avoid_host: str | None = None,
    ) -> tuple[float, int] | None:
        """Return (final projection, OSD) of the best legal target, or None.

        avoid_host is forbidden too. Candidates are tried in final
        projection order, so the search stops at the first that could not
        beat the best found, or reach below the guards.
        """
        c = self.cluster
        cls = osd_class(c.osd_df, shard.up_osd)
        below = None
        if self.relieve and c.final.knows(shard.up_osd):
            # Strictly below: equal would leave the maximum where it is.
            below = c.final.utilization(shard.up_osd, -shard.size_bytes)
            if below <= self.highest_target(shard.up_osd):
                return None
        if self.level is not None and cls is not None and cls == self.level_class:
            below = self.level if below is None else min(below, self.level)
        forbidden_hosts = state.forbidden_hosts(shard.up_osd, c.osd_host)
        if avoid_host is not None:
            forbidden_hosts.add(avoid_host)
        # A candidate's projection with the shard is at least bound: its own
        # plus what the shard adds to the largest OSD (less rounding).
        bump = shard.size_bytes / self.max_capacity.get(cls, 1) * 100 - 1e-9
        best = None
        for util, osd in self.order.get(cls, []):
            bound = util + bump
            # The reservation is never below the final projection.
            if (
                bound > c.max_target_util
                or (below is not None and bound >= below)
                or (best is not None and bound > best[0])
            ):
                break
            if (
                target_projection(
                    osd,
                    shard.size_bytes,
                    forbidden_hosts=forbidden_hosts,
                    forbidden_osds=state.forbidden_osds,
                    osd_host=c.osd_host,
                    projection=c.reservation,
                    max_target_util=c.max_target_util,
                )
                is None
            ):
                continue
            projected = c.final.utilization(osd, shard.size_bytes)
            if below is not None and projected >= below:
                continue
            if best is not None and (projected, osd) >= best:
                continue
            if self.relieve and projected >= self.lowest_relieved(osd):
                continue
            if self.chains(state, [(shard.up_osd, osd)]):
                continue
            best = projected, osd
        return best

    def highest_target(self, osd_id: int) -> float:
        """Return the highest final projection of the targets osd_id sent data to.

        -inf if none (or none has a capacity figure). With relieve, moving
        more off osd_id must leave it above this.
        """
        final = self.cluster.final
        return max(
            (final.utilization(t) for t in self.sent_to[osd_id] if final.knows(t)),
            default=-math.inf,
        )

    def lowest_relieved(self, osd_id: int) -> float:
        """Return the lowest final projection of the OSDs target osd_id relieved.

        inf if none (or none has a capacity figure). With relieve, moving
        more onto osd_id must leave it below this.
        """
        final = self.cluster.final
        return min(
            (final.utilization(u) for u in self.relieved[osd_id] if final.knows(u)),
            default=math.inf,
        )

    def commit(
        self, state: PgState, shard: MappedShard | ArrivingShard, target: int
    ) -> None:
        """Record in the projections and state that shard goes to target."""
        c = self.cluster
        if shard.acting_osd == shard.up_osd:
            c.reservation.add(target, shard.size_bytes)
        else:
            # Still arriving on its up OSD: that backfill is cancelled.
            c.reservation.redirect(shard, target)
        self.move_final(shard.up_osd, target, shard.size_bytes)
        self.relieved[target][shard.up_osd] += 1
        self.sent_to[shard.up_osd][target] += 1
        state.retarget(shard.up_osd, target)
        state.changed.add(state.key(shard))

    def uncommit(self, state: PgState, shard: MappedShard, target: int) -> None:
        """Undo commit(state, shard, target), which place() chose.

        So target was not forbidden before: it no longer is.
        """
        c = self.cluster
        c.reservation.add(target, -shard.size_bytes)
        if shard.acting_osd != shard.up_osd:
            c.reservation.uncancel(shard)
        self.move_final(target, shard.up_osd, shard.size_bytes)
        for counts, key in (
            (self.relieved[target], shard.up_osd),
            (self.sent_to[shard.up_osd], target),
        ):
            counts[key] -= 1
            if not counts[key]:
                del counts[key]
        state.retarget(target, shard.up_osd)
        state.forbidden_osds.discard(target)
        state.changed.discard(state.key(shard))

    def is_blocker(
        self, shard: ArrivingShard | MappedShard, toofull_now: bool
    ) -> str | None:
        """Return why the shard arriving on its up OSD blocks its PG, or None.

        It blocks if that OSD is projected at or over backfillfull_ratio
        (placement.blocker_projection, as in cancel-backfill) or, with
        toofull_now, is at or above nearfull_ratio now: Ceph may count more
        than the projection sees. The reason names the OSD.
        """
        c = self.cluster
        osd_id = shard.up_osd
        if not c.reservation.knows(osd_id):
            return None
        projected = blocker_projection(c.reservation, osd_id, c.ratios.backfillfull)
        if projected is not None:
            return blocking_reason(osd_id, projected)
        now = c.osd_df[osd_id].get("utilization")
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
        self, state: PgState, blocker: ArrivingShard | MappedShard
    ) -> tuple[list[ArrivingShard], str | None]:
        """Return the pins that cancel blocker's backfill, or ([], why not).

        blocker is arriving on its up OSD. Each pin is a shard arriving on
        its up OSD, to send back to its acting OSD: blocker's own first, then
        companions. Refused, besides the reasons close_pins gives, if a pin
        would land on a source (unless it stays below the level), undo a move
        proposed in this run, or chain (pgremapper cannot apply chains).
        """
        pgid, size = state.pg["pgid"], state.size_bytes
        osd_host = self.cluster.osd_host
        if blocker.acting_osd is None and not state.is_ec:
            blocker = blocker._replace(acting_osd=self.replica_source(state, blocker))
        if blocker.acting_osd is None:
            return (
                [],
                "no acting OSD" if state.is_ec else "replica pairing is ambiguous",
            )
        if state.is_ec:
            slots, why = close_pins(
                state.new_up,
                state.pg["acting"],
                {blocker.shard: blocker.acting_osd},
                osd_host,
            )
            pins = [
                ArrivingShard(pgid, s, state.new_up[s], a, [], size)
                for s, a in slots.items()
            ]
        else:
            why = pin_replica(
                state.new_up, blocker.up_osd, blocker.acting_osd, osd_host
            )
            pins = [
                ArrivingShard(pgid, "-", blocker.up_osd, blocker.acting_osd, [], size)
            ]
        if why is not None:
            return [], why

        pinned_back: Counter[int] = Counter()  # bytes kept on each source
        for pin in pins:
            if pin.acting_osd in self.sources:
                pinned_back[pin.acting_osd] += size
                if not self.is_below_level(pin.acting_osd, pinned_back[pin.acting_osd]):
                    level = (
                        ""
                        if self.level is None
                        else f", taking it to the {level_text(self.level)} level "
                        "or above"
                    )
                    return [], (
                        f"it would pin data back onto source osd.{pin.acting_osd}{level}"
                    )
            if state.key(pin) in state.changed:
                return [], "it would undo a move proposed in this run"
        if self.chains(state, [(p.up_osd, p.acting_osd) for p in pins]):
            return [], "the PG's pairs would chain, which pgremapper cannot apply"
        return pins, None

    def replica_source(
        self, state: PgState, blocker: ArrivingShard | MappedShard
    ) -> int | None:
        """Return the acting OSD blocker's replica comes from, or None if unclear.

        As placement's pairing (one departing, one arriving), but other
        sources' arriving replicas are this run's to redirect: not counted.
        """
        up, acting = real_osd_set(state.pg["up"]), real_osd_set(state.pg["acting"])
        departing = acting - up
        arriving = (up - acting) - (self.sources - {blocker.up_osd})
        return next(iter(departing)) if len(departing) == len(arriving) == 1 else None

    def try_pin_elsewhere(
        self, state: PgState, blocker: ArrivingShard | MappedShard
    ) -> tuple[list[ArrivingShard], str | None]:
        """Pin blocker back once the PG's move onto its acting OSD's host goes elsewhere.

        That move keeps blocker from being pinned back there. It is
        re-placed with the host forbidden and the pin tried again; if
        either fails, the move is left as it was. Return try_pin's result,
        or ([], None) if there is no such move or no other target for it.
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
            return [], None
        i, move = clash
        shard = MappedShard(
            move.pgid, move.shard, move.up_osd, move.acting_osd, move.size_bytes
        )
        self.uncommit(state, shard, move.target_osd)
        # Out of the PG's moves while re-placed, so chains() does not see it.
        del state.moves[i]
        picked = self.place(state, shard, avoid_host=host)
        pins, why = [], None
        if picked is not None:
            self.commit(state, shard, picked[1])
            state.moves.insert(i, move._replace(target_osd=picked[1]))
            pins, why = self.try_pin(state, blocker)
            if pins:
                return pins, None
            self.uncommit(state, shard, picked[1])
            del state.moves[i]
        self.commit(state, shard, move.target_osd)
        state.moves.insert(i, move)
        return pins, why

    def pin(self, state: PgState, pins: list[ArrivingShard], note: str) -> None:
        """Record try_pin's pins of a blocker: its own with note, then its companions."""
        c = self.cluster
        for k, pin in enumerate(pins):
            c.reservation.cancel(pin)
            self.move_final(pin.up_osd, pin.acting_osd, pin.size_bytes)
            state.retarget(pin.up_osd, pin.acting_osd)
            state.changed.add(state.key(pin))
            if k:
                note = companion_note(pins[0].shard, of_blocker=True)
            state.add_move(pin, pin.acting_osd, note, ROLE_BLOCKER)


def shard_key(shard: MappedShard | ArrivingShard) -> int:
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
    toofull_now = is_toofull(state.pg)
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
        if not pins:
            pins, why_elsewhere = planner.try_pin_elsewhere(state, sibling)
            why = why_elsewhere or why
        if not pins:
            stuck_reasons.append(
                f"shard {sibling.shard} -> {blocking} (cannot pin: {why})"
            )
            continue
        planner.pin(state, pins, blocker_note("pinned, no room to divert", blocking))
        pinned += 1

    # try_pin_elsewhere may have re-placed a requested move.
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


def project_moves(cluster: Cluster, moves: list[Move]) -> list[Move]:
    """Return moves with their UP and TARGET final projections filled in.

    Call once everything is placed, so that the rows of one OSD agree. A
    pin's data stays where it is, so its target has no projection; nor has
    an OSD without a capacity figure.
    """
    final = cluster.final

    def projected(osd_id: int) -> float | None:
        return final.utilization(osd_id) if final.knows(osd_id) else None

    return [
        m._replace(
            up_projected=projected(m.up_osd),
            target_projected=None if is_pin(m) else projected(m.target_osd),
        )
        for m in moves
    ]


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
    max_target_util: float
    ratios: FullRatios
    osd_df: dict[int, dict]
    osd_host: dict[int, str]

    @property
    def still_above(self) -> list[tuple[int, float]]:
        """With a level, the (source, final_util) at or above it, in id order."""
        if self.level is None:
            return []
        return [(o, u) for o, u in self.final_util.items() if u >= self.level]

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
    for pg, *_, found in movable:
        states[pg["pgid"]] = cluster.pg_state(pg)
        for shard in found:
            by_source[shard.up_osd].append(shard)

    planner = Planner(cluster, sources, level, level_class, relieve=level is not None)
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
            and planner.is_blocker(shard, is_toofull(state.pg))
        ):
            stalled.add(shard.pgid)
    unplaceable.sort(key=lambda s: (pgid_sort_key(s.pgid), shard_key(s)))

    final = cluster.final
    final_util = {o: final.utilization(o) for o in sorted(sources) if final.knows(o)}
    return ShedResult(
        sources=sorted(sources),
        level=level,
        level_class=level_class,
        moves=project_moves(cluster, moves),
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
        target_cells[2],
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


def print_moves(
    moves: list[Move],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    args: argparse.Namespace,
) -> None:
    """Print the moves on stdout, in the format args asks for."""
    if args.pgremapper_mappings:
        print_pgremapper_mappings(moves)
    elif moves:
        print_table(COLUMNS, [format_row(m, osd_host, osd_df) for m in moves])


def print_outcome(result: ShedResult, off: str) -> None:
    """Sum up the moves off the sources on stderr; off names them."""
    r = result
    print_shed_outcome(
        off,
        moved=len(r.requested),
        moved_bytes=r.moved_bytes,
        unplaceable=len(r.unplaceable),
        kept=None if r.level is None else r.kept_count,
        diverted=r.diverted_count,
        pinned=r.pinned_count,
        stuck=r.stuck_pgs,
        unexplained=r.unexplained_pgs,
    )


def print_notes(result: ShedResult, which: str) -> None:
    """Print on stderr what the run leaves undone; which names the sources.

    That is: the sources left above the level; the shards no target was
    found for, listed when emptying OSDs (no level) but only counted with a
    level, as an unreachable one can leave thousands; and the PGs left in
    backfill_toofull as they were. Then, if anything moves, the balancer
    advice.
    """
    if result.level is None:
        print_unplaceable(
            (s.pgid, s.shard, f"off osd.{s.up_osd}") for s in result.unplaceable
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
    if result.moves:
        print_balancer_note()


def unsized_sources(result: ShedResult) -> list[int]:
    """Return the sources without a capacity figure: drained in full, whatever the level."""
    return [o for o in result.sources if o not in result.final_util]
