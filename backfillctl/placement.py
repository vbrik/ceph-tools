# SPDX-License-Identifier: MIT
"""Picking a new OSD for a shard: shared by divert-toofull and drain.

Both subcommands re-target shards with upmaps and choose where to send each
one the same way: the least-utilized OSD of the shard's own device class that
the PG may legally use (see pick_target), judged by *projected* utilization --
what the OSD holds now, plus everything already on its way to it, plus what
this run has sent it so far (see ProjectedUsage). The rationale is spelled
out in divert_toofull's module docstring ("How targets are chosen", "How
utilization is projected", "Why the raw CRUSH mapping matters"); what the
two subcommands differ in is which shards they place, and in what order.

Also here: the cluster-ratio defaults and checks both take their thresholds
from, and the host-failure-domain check both rely on.
"""

import argparse
import sys
from collections import Counter
from collections.abc import Iterable
from typing import NamedTuple, Protocol

import shared
from shared import KIB, POOL_TYPE_ERASURE, SnapshotStore, is_real_osd, slot

# Ceph's own defaults for these ratios (OSDMap::build_simple). Used only if
# 'ceph osd dump' somehow omits them, so that thresholds derived from them
# still get a sane cluster-independent default rather than silently falling
# back to "no threshold at all", which is the unsafe direction.
DEFAULT_NEARFULL_RATIO = 0.85
DEFAULT_BACKFILLFULL_RATIO = 0.90

# How many shards one OSD may be proposed as the target of (--max-target-uses).
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


def add_target_args(parser: argparse.ArgumentParser, *, max_target_util_help: str):
    """Add --pgremapper-mappings, --max-target-util and --max-target-uses."""
    parser.add_argument(
        "--pgremapper-mappings",
        action="store_true",
        help="Print a JSON array for 'pgremapper import-mappings' instead of "
        "the table, one {pgid, mapping} entry per line. This is the reliable "
        "way to apply the proposals: all pairs of a PG go in together.",
    )
    parser.add_argument(
        "--max-target-util",
        type=float,
        metavar="PERCENT",
        help=max_target_util_help,
    )
    parser.add_argument(
        "--max-target-uses",
        type=positive_int,
        default=DEFAULT_MAX_TARGET_USES,
        metavar="N",
        help="Hard limit on how many shards may be sent to one OSD "
        "(default: %(default)s). Independently of this limit, an OSD stops "
        "being used once receiving another shard would bring its projected "
        "utilization above --max-target-util; 1 gives every OSD at most one "
        "shard.",
    )


# ---------------------------------------------------------------------------
# Cluster thresholds and checks
# ---------------------------------------------------------------------------


class FullRatios(NamedTuple):
    """The cluster's fullness thresholds, as percentages.

    What the subcommands' thresholds default to, so their safety margins
    track whatever the cluster itself considers 'getting full' and 'too full
    to backfill onto' rather than hard-coded numbers that would be wrong on a
    tuned cluster.
    """

    nearfull: float
    backfillfull: float


def fetch_full_ratios(store: SnapshotStore) -> FullRatios:
    """Return the cluster's nearfull/backfillfull ratios from 'ceph osd dump'.

    Reported by Ceph as fractions (0.85); returned here as the percentages
    the CLI flags and 'ceph osd df' utilizations are expressed in.
    """
    data = store.json("osd_dump")
    nearfull = data.get("nearfull_ratio") or DEFAULT_NEARFULL_RATIO
    backfillfull = data.get("backfillfull_ratio") or DEFAULT_BACKFILLFULL_RATIO
    return FullRatios(nearfull * 100, backfillfull * 100)


def resolve_max_target_util(given: float | None, ratios: FullRatios) -> float:
    """Return --max-target-util, defaulting to backfillfull_ratio - 1.

    Exits with an error if it is not above 0 and at most backfillfull_ratio:
    Ceph refuses to backfill onto an OSD past that, so a higher cap would let
    targets be proposed that re-wedge.
    """
    value = ratios.backfillfull - 1 if given is None else given
    if not 0 < value <= ratios.backfillfull:
        sys.exit(
            f"ERROR: max target utilization {value:g}% (--max-target-util, "
            "or its backfillfull_ratio - 1 default) must be above 0 and "
            f"at most the cluster's backfillfull_ratio ({ratios.backfillfull:g}%): "
            "Ceph refuses to backfill onto an OSD past that, so a higher cap "
            "would let the script propose targets that re-wedge."
        )
    return value


def ec_pool_ids_from(pools: list[dict]) -> set[int]:
    """Return the set of pool ids that are erasure-coded (type == 3)."""
    return {p["pool_id"] for p in pools if p.get("type") == POOL_TYPE_ERASURE}


def check_host_failure_domain(
    pools: list[dict], crush_rules: dict[int, dict], which: str
) -> None:
    """Exit with an error unless every given pool's CRUSH rule fails over at host.

    Target legality (pick_target) is judged by host: a target must not share
    a host with another shard of the PG. That is only the constraint CRUSH
    enforces if the pool's failure domain is 'host'. which describes the
    pools for the error message, e.g. 'with a stuck PG'.
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
            "ERROR: this subcommand assumes the CRUSH failure domain of every "
            f"pool {which} is 'host' (see its module docstring), but the "
            f"following pool(s) do not:\n{lines}"
        )


def shard_size_bytes(pg: dict, pool: dict, ec_profiles: dict[str, dict]) -> int:
    """Estimate the bytes one shard of a PG occupies on its OSD (see shared).

    Exits with an error where shared.shard_size_bytes reports "unknown": every
    placement's utilization projection needs a size, and a silent 0 would
    project no usage at all.
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
    """Reconstruct the OSDs CRUSH itself chose, by undoing the PG's upmaps.

    pg_upmap_items rewrites the CRUSH result: a pair {'from': X, 'to': Y}
    means CRUSH picked X and Y is used instead. Substituting each active
    pair's 'to' back to its 'from' recovers the raw mapping, whose members
    must not be proposed as targets (see divert_toofull's module docstring).
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
    up_osd: int  # OSD the shard is arriving on: in 'up', not yet in
    # 'acting'. This is the 'from' of an upmap that would
    # re-target it. Despite that 'from', no data flows off it.
    acting_osd: int | None  # OSD still holding the shard, i.e. the
    # backfill's data source, or None when the slot reads as
    # CRUSH_ITEM_NONE (the usual out-OSD case)
    up_set: list  # the PG's full up set, for host exclusions and for
    # reconstructing the raw CRUSH mapping
    size_bytes: int = 0  # what the shard will occupy once backfilled, see
    # shard_size_bytes; 0 means "not known", which projects no usage


def find_arriving_shards(
    pg: dict, is_ec: bool, size_bytes: int = 0
) -> list[ArrivingShard]:
    """Return the shards of one PG that are newly arriving on their up OSD.

    EC shards are diffed by position, so the shard index is known; replicated
    ones by set, since replicas are interchangeable (a reordered set is not
    movement). size_bytes is the size of each of them (all shards of a PG are
    the same size), recorded on the result for the utilization projection.
    """
    pgid = pg["pgid"]
    up = pg["up"]
    acting = pg["acting"]
    found = []

    if is_ec:
        # EC: shard identity is positional, so diff index by index. This is
        # what lets the shard index be reported, and keeps two unrelated
        # shard moves in one PG from being conflated.
        for i in range(max(len(up), len(acting))):
            up_osd = slot(up, i)
            acting_osd = slot(acting, i)
            if up_osd is None or up_osd == acting_osd:
                continue
            found.append(ArrivingShard(pgid, i, up_osd, acting_osd, up, size_bytes))
    else:
        # Replicated: replicas are interchangeable, so position means nothing
        # and only the set difference is real movement.
        up_members = {o for o in up if is_real_osd(o)}
        acting_members = {o for o in acting if is_real_osd(o)}
        departing = sorted(acting_members - up_members)
        arriving = sorted(up_members - acting_members)
        # A replicated PG can have several arriving/departing replicas at once
        # with no way to pair them up; name the acting OSD only when exactly
        # one replica is leaving and one arriving. With one leaving and
        # several arriving, naming it on all of them would claim it holds
        # several replicas (and would relieve it several times over).
        pairing_is_clear = len(departing) == 1 and len(arriving) == 1
        for up_osd in arriving:
            acting_osd = departing[0] if pairing_is_clear else None
            found.append(ArrivingShard(pgid, "-", up_osd, acting_osd, up, size_bytes))

    return found


# ---------------------------------------------------------------------------
# Candidates and projection
# ---------------------------------------------------------------------------


def osd_class(osd_df: dict[int, dict], osd_id: int) -> str | None:
    """Return an OSD's device class, or None if it is unknown."""
    return osd_df.get(osd_id, {}).get("device_class")


def build_candidate_osds(
    osd_df: dict[int, dict], exclude: Iterable[int] = ()
) -> dict[str, list[int]]:
    """Return usable target OSD ids per device class, least-utilized first.

    Excludes OSDs that are down, out (reweight 0) or have no CRUSH weight, and
    those in exclude. An out OSD would otherwise sort to the very front, since
    'ceph osd df' reports one at 0% utilization. An OSD with no utilization
    figure at all is excluded for the same reason -- it cannot be ranked, and
    treating a missing value as 0% would make it the first pick.

    Keyed by device class because a shard may only be sent to an OSD of its
    own class: pools' CRUSH rules are typically class-constrained. Each
    class's OSDs fill up independently.
    """
    excluded = set(exclude)
    usable = [
        node
        for node in osd_df.values()
        if node.get("status") == "up"
        and node.get("reweight", 0) > 0
        and node.get("crush_weight", 0) > 0
        and node.get("device_class")
        and node.get("kb")  # capacity unknown: nothing can be projected onto it
        and node.get("utilization") is not None
        and node["id"] not in excluded
    ]
    # OSD id as secondary key: utilizations tie constantly on a uniformly
    # full cluster, and the operator will re-run this.
    usable.sort(key=lambda n: (n["utilization"], n["id"]))
    by_class: dict[str, list[int]] = {}
    for node in usable:
        by_class.setdefault(node["device_class"], []).append(node["id"])
    return by_class


def usage_and_capacity(
    osd_df: dict[int, dict],
) -> tuple[dict[int, int], dict[int, int]]:
    """Return ({osd: bytes used}, {osd: bytes of capacity}) from 'ceph osd df'.

    OSDs with no capacity figure are left out of both: nothing can be
    projected for them.
    """
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
    """What each OSD will hold once the backfills already in motion complete.

    Starts from the usage 'ceph osd df' reports plus the size of every shard
    still arriving on the OSD: those bytes are not in 'kb_used' yet, and it is
    the emptiest OSDs, the ones most attractive as targets, that have the
    most in flight. A shard about to be re-targeted keeps counting where it is
    headed until it is: redirect() then takes its size off the OSD it was
    headed for and puts it on its target, and cancel() just takes it off.

    Not accounted for: data leaving an OSD (never credited) and backfills the
    caller did not pass in, so the projection can still be optimistic. Ceph
    itself refuses a backfill on the target's projected usage rather than
    today's.
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
        self._used[target_osd] += shard.size_bytes

    def cancel(self, shard: Retargetable) -> None:
        """Record that shard no longer goes to its up OSD (e.g. pinned back)."""
        if shard.up_osd in self._used:
            self._used[shard.up_osd] -= shard.size_bytes


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

    pool is the candidates of the shard's device class, least-utilized first
    (see build_candidate_osds). Legal means: host not in forbidden_hosts, OSD
    not in forbidden_osds, not already the target of max_uses shards, strictly
    less utilized than below_util (currently, as in 'ceph osd df'; None
    imposes no limit), and at or below max_target_util (percent) once
    size_bytes has been added to what the projection says it will hold. Of
    the legal candidates the one with the lowest such projection wins (OSD id
    breaks ties), so an OSD is reused only as the others fill up.

    Records nothing: the caller updates projection and uses once it commits.
    """
    legal = []
    for candidate in pool:
        util = osd_df[candidate]["utilization"]
        # Pool is sorted by utilization, and a projection is never below
        # the current figure: everything from here on is over the cap.
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
