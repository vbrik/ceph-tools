# SPDX-License-Identifier: MIT
"""Choosing target OSDs for shards; shared by divert-toofull, drain and balance.

A target is the legal OSD of the shard's device class with the lowest
projected utilization (pick_target, ProjectedUsage; balance ranks its own).
Also: finding shards in motion or on given OSDs, tracking a PG's up set as
moves are proposed (PgPlacement), the cluster's full ratios and the host
failure-domain check.
"""

import argparse
import sys
from collections import Counter
from collections.abc import Iterable
from typing import NamedTuple, Protocol

import shared
from shared import (
    KIB,
    POOL_TYPE_ERASURE,
    SnapshotStore,
    is_real_osd,
    real_osd_set,
    slot,
)

# Ceph's defaults (OSDMap::build_simple), for an 'osd dump' without them.
DEFAULT_NEARFULL_RATIO = 0.85
DEFAULT_BACKFILLFULL_RATIO = 0.90

DEFAULT_MAX_TARGET_USES = 5


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def positive_int(text: str) -> int:
    """argparse type: an integer of at least 1."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def add_target_args(parser: argparse.ArgumentParser):
    """Add --max-target-util and --max-target-uses."""
    parser.add_argument(
        "--max-target-util",
        type=shared.utilization_pct,
        metavar="PERCENT",
        help="Cap on a target's projected utilization (default: "
        "backfillfull_ratio - 1; at most backfillfull_ratio).",
    )
    parser.add_argument(
        "--max-target-uses",
        type=positive_int,
        default=DEFAULT_MAX_TARGET_USES,
        metavar="N",
        help="Maximum shards per target OSD (default: %(default)s).",
    )


# ---------------------------------------------------------------------------
# Cluster thresholds and checks
# ---------------------------------------------------------------------------


class FullRatios(NamedTuple):
    """The cluster's nearfull and backfillfull ratios, as percentages."""

    nearfull: float
    backfillfull: float


def fetch_full_ratios(store: SnapshotStore) -> FullRatios:
    """Return the cluster's full ratios from 'ceph osd dump'."""
    data = store.json("osd_dump")
    nearfull = data.get("nearfull_ratio") or DEFAULT_NEARFULL_RATIO
    backfillfull = data.get("backfillfull_ratio") or DEFAULT_BACKFILLFULL_RATIO
    return FullRatios(nearfull * 100, backfillfull * 100)


def resolve_max_target_util(given: float | None, ratios: FullRatios) -> float:
    """Return --max-target-util, defaulting to backfillfull_ratio - 1.

    Exits unless 0 < value <= backfillfull_ratio: Ceph would refuse
    backfills onto a target above that.
    """
    value = ratios.backfillfull - 1 if given is None else given
    if not 0 < value <= ratios.backfillfull:
        sys.exit(
            f"ERROR: max target utilization {value:g}% (--max-target-util) "
            "must be above 0 and at most the cluster's backfillfull_ratio "
            f"({ratios.backfillfull:g}%)."
        )
    return value


def ec_pool_ids_from(pools: list[dict]) -> set[int]:
    """Return the set of pool ids that are erasure-coded (type == 3)."""
    return {p["pool_id"] for p in pools if p.get("type") == POOL_TYPE_ERASURE}


def check_host_failure_domain(
    pools: list[dict], crush_rules: dict[int, dict], which: str
) -> None:
    """Exit with an error unless every pool's failure domain is host.

    pick_target keeps a PG's shards on distinct hosts, which is only right
    for host. which describes the pools in the message, e.g. 'with a stuck PG'.
    """
    bad = []
    for pool in pools:
        rule = crush_rules.get(pool["crush_rule"])
        domain = shared.rule_failure_domain(rule) if rule else None
        if domain != "host":
            bad.append((pool["pool_name"], pool["crush_rule"], domain))
    if bad:
        lines = "\n".join(
            f"  pool '{name}' uses crush rule {rule_id} (failure domain: "
            f"{domain or 'unknown'})"
            for name, rule_id, domain in bad
        )
        sys.exit(
            "ERROR: this subcommand requires CRUSH failure domain 'host' for "
            f"every pool {which}; these differ:\n{lines}"
        )


def shard_size_bytes(pg: dict, pool: dict, ec_profiles: dict[str, dict]) -> int:
    """Estimate a shard's size (shared.shard_size_bytes), exiting if unknown.

    Projections need a size; 0 would silently project nothing.
    """
    size = shared.shard_size_bytes(pg, pool, ec_profiles)
    if size is None:
        profile = pool.get("erasure_code_profile")
        sys.exit(
            f"ERROR: pool {pool['pool_id']} uses erasure code profile "
            f"{profile!r}, which 'ceph osd dump' does not describe (or "
            "describes without 'k'), so the size of its shards is unknown."
        )
    return size


# ---------------------------------------------------------------------------
# Shards in motion
# ---------------------------------------------------------------------------


def raw_crush_osds(up: list, upmap_pairs: list[dict]) -> set[int]:
    """Return the OSDs CRUSH chose, by undoing the PG's upmap pairs.

    These are not valid targets even when absent from up: an upmap pair
    'from X to Y' keeps X in the raw mapping, and Ceph silently drops an
    upmap that maps a PG to one OSD twice.
    """
    raw = list(up)
    for pair in upmap_pairs:
        if pair["to"] in raw:
            raw[raw.index(pair["to"])] = pair["from"]
    return {osd_id for osd_id in raw if is_real_osd(osd_id)}


class ArrivingShard(NamedTuple):
    """A shard newly arriving on an OSD: in the PG's 'up' set, not its 'acting'."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # where it is arriving; the 'from' of a re-targeting pair
    acting_osd: int | None  # where its data is; None if unknown (out OSD)
    up_set: list  # the PG's up set
    size_bytes: int = 0  # see shard_size_bytes; 0 if unknown


def find_arriving_shards(
    pg: dict, is_ec: bool, size_bytes: int = 0
) -> list[ArrivingShard]:
    """Return a PG's shards that are arriving on their up OSD.

    EC slots are diffed by position, replicated sets as sets. size_bytes is
    the size of each shard.
    """
    pgid = pg["pgid"]
    up = pg["up"]
    acting = pg["acting"]
    found = []

    if is_ec:
        for i in range(max(len(up), len(acting))):
            up_osd = slot(up, i)
            acting_osd = slot(acting, i)
            if up_osd is None or up_osd == acting_osd:
                continue
            found.append(ArrivingShard(pgid, i, up_osd, acting_osd, up, size_bytes))
    else:
        up_members = {o for o in up if is_real_osd(o)}
        acting_members = {o for o in acting if is_real_osd(o)}
        departing = sorted(acting_members - up_members)
        arriving = sorted(up_members - acting_members)
        # Name the acting OSD only when the pairing is unambiguous.
        pairing_is_clear = len(departing) == 1 and len(arriving) == 1
        for up_osd in arriving:
            acting_osd = departing[0] if pairing_is_clear else None
            found.append(ArrivingShard(pgid, "-", up_osd, acting_osd, up, size_bytes))

    return found


class MappedShard(NamedTuple):
    """A shard mapped to (in 'up' on) one of the OSDs of interest."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # the OSD of interest: the 'from' of an upmap pair
    acting_osd: int | None  # where its data is now, None if unknown
    size_bytes: int


def find_mapped_shards(
    pg: dict, is_ec: bool, osds: set[int], size_bytes: int
) -> tuple[list[MappedShard], int]:
    """Return (the PG's shards mapped to one of osds, count already leaving one).

    An arriving replica's acting OSD is known only if the pairing is
    unambiguous.
    """
    pgid, up, acting = pg["pgid"], pg["up"], pg["acting"]
    if is_ec:
        mapped = [
            MappedShard(pgid, i, osd, slot(acting, i), size_bytes)
            for i, osd in enumerate(up)
            if osd in osds
        ]
        leaving = sum(
            1 for i, osd in enumerate(acting) if osd in osds and slot(up, i) != osd
        )
        return mapped, leaving

    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    departing = sorted(acting_set - up_set)
    pairing_is_clear = len(departing) == 1 and len(up_set - acting_set) == 1
    mapped = []
    for osd in sorted(up_set & osds):
        if osd in acting_set:
            acting_osd = osd
        else:
            acting_osd = departing[0] if pairing_is_clear else None
        mapped.append(MappedShard(pgid, "-", osd, acting_osd, size_bytes))
    return mapped, len((acting_set - up_set) & osds)


# ---------------------------------------------------------------------------
# Candidates and projection
# ---------------------------------------------------------------------------


def osd_class(osd_df: dict[int, dict], osd_id: int) -> str | None:
    """Return an OSD's device class, or None if it is unknown."""
    return osd_df.get(osd_id, {}).get("device_class")


def build_candidate_osds(
    osd_df: dict[int, dict], exclude: Iterable[int] = ()
) -> dict[str, list[int]]:
    """Return usable target OSDs per device class, least-utilized first.

    Excludes OSDs in exclude and those that are down, out, weightless, or
    lack a device class, capacity or utilization ('ceph osd df' reports an
    out OSD at 0%, which would sort first).
    """
    excluded = set(exclude)
    usable = [
        node
        for node in osd_df.values()
        if node.get("status") == "up"
        and node.get("reweight", 0) > 0
        and node.get("crush_weight", 0) > 0
        and node.get("device_class")
        and node.get("kb")  # needed for projections
        and node.get("utilization") is not None
        and node["id"] not in excluded
    ]
    # Tie-break by id, for reproducible runs.
    usable.sort(key=lambda n: (n["utilization"], n["id"]))
    by_class: dict[str, list[int]] = {}
    for node in usable:
        by_class.setdefault(node["device_class"], []).append(node["id"])
    return by_class


def usage_and_capacity(
    osd_df: dict[int, dict],
) -> tuple[dict[int, int], dict[int, int]]:
    """Return ({osd: bytes used}, {osd: capacity}), for OSDs with a capacity."""
    sized = {i: n for i, n in osd_df.items() if n.get("kb")}
    return (
        {i: n["kb_used"] * KIB for i, n in sized.items()},
        {i: n["kb"] * KIB for i, n in sized.items()},
    )


class Retargetable(Protocol):
    """A shard headed for up_osd, of size_bytes: what ProjectedUsage tracks."""

    @property
    def up_osd(self) -> int: ...

    @property
    def size_bytes(self) -> int: ...


class ProjectedUsage:
    """What each OSD will hold once the backfills in motion complete.

    'ceph osd df' usage plus every shard arriving on the OSD (not yet in
    kb_used). A re-targeted shard counts where it was headed until
    redirect() or cancel() moves it.

    Data leaving an OSD is never credited, and backfills not passed in are
    not counted.
    """

    def __init__(self, osd_df: dict[int, dict], arriving: Iterable[Retargetable]):
        self._used, self._capacity = usage_and_capacity(osd_df)
        for shard in arriving:
            if shard.up_osd in self._used:
                self._used[shard.up_osd] += shard.size_bytes

    def knows(self, osd_id: int) -> bool:
        """True if the OSD has a capacity figure, so it can be projected."""
        return osd_id in self._used

    def utilization_after(self, osd_id: int, extra_bytes: int) -> float:
        """Return the OSD's projected utilization (percent) with extra_bytes more."""
        return (self._used[osd_id] + extra_bytes) / self._capacity[osd_id] * 100

    def redirect(self, shard: Retargetable, target_osd: int) -> None:
        """Record that shard goes to target_osd instead of its up OSD."""
        self.cancel(shard)
        self.add(target_osd, shard.size_bytes)

    def add(self, osd_id: int, size_bytes: int) -> None:
        """Record that size_bytes more will arrive on the OSD."""
        self._used[osd_id] += size_bytes

    def cancel(self, shard: Retargetable) -> None:
        """Record that shard no longer goes to its up OSD (e.g. pinned back)."""
        if shard.up_osd in self._used:
            self._used[shard.up_osd] -= shard.size_bytes


class PgPlacement:
    """One PG's up set as proposals change it, and the OSDs it must avoid."""

    def __init__(self, pg: dict, is_ec: bool, size_bytes: int, raw: set[int]):
        self.pg = pg
        self.is_ec = is_ec
        self.size_bytes = size_bytes
        self.new_up = list(pg["up"])  # 'up' once the proposals are applied
        # raw: see raw_crush_osds.
        self.forbidden_osds = raw | real_osd_set(pg["up"])

    def forbidden_hosts(self, moving_osd: int, osd_host: dict[int, str]) -> set:
        """Hosts a shard leaving moving_osd must not go to: the PG's other ones."""
        return {
            osd_host.get(o) for o in self.new_up if is_real_osd(o) and o != moving_osd
        }

    def retarget(self, from_osd: int, to_osd: int) -> None:
        """Record that the shard on from_osd (in new_up) now goes to to_osd."""
        self.new_up[self.new_up.index(from_osd)] = to_osd
        self.forbidden_osds.add(to_osd)


def pick_target(
    pool: list[int],
    size_bytes: int,
    *,
    forbidden_hosts: set[str | None],
    forbidden_osds: set[int],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    projection: ProjectedUsage,
    uses: Counter[int],
    max_uses: int,
    max_target_util: float,
    below_util: float | None = None,
) -> tuple[float, int] | None:
    """Return (projected utilization, OSD) of the best legal target, or None.

    pool: candidates of the shard's class, least-utilized first
    (build_candidate_osds). Legal: host and OSD not forbidden, used fewer
    than max_uses times, currently below below_util (if given), and
    projected at or below max_target_util with the shard added. Lowest
    projection wins, then lowest id.

    Records nothing; the caller updates projection and uses.
    """
    legal = []
    for candidate in pool:
        util = osd_df[candidate]["utilization"]
        # Sorted, and projections never fall below current: the rest are over.
        if util > max_target_util:
            break
        if osd_host.get(candidate) in forbidden_hosts:
            continue
        if candidate in forbidden_osds:
            continue
        if uses[candidate] >= max_uses:
            continue
        if below_util is not None and util >= below_util:
            continue
        projected = projection.utilization_after(candidate, size_bytes)
        if projected > max_target_util:
            continue
        legal.append((projected, candidate))
    return min(legal) if legal else None
