#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Propose upmap re-targets that divert backfill_toofull PGs away from a full host.

The situation this solves
-------------------------
When a single OSD goes out/down, CRUSH does *not* spread its PGs across the
cluster. For a rule of the form "choose a host bucket, then a leaf inside it"
(chooseleaf_firstn/indep type host), the failed leaf is retried *within the
same host bucket* — so every PG that lived on the dead OSD is re-placed onto
one of its 20-or-so same-host siblings. On a cluster that is already uniformly
full, those siblings blow past backfillfull_ratio and the PGs wedge in
backfill_toofull, while the rest of the cluster sits several percent emptier
and idle.

Everything above only holds if the relevant pools' CRUSH rules actually fail
over at the host bucket type, so this is checked at startup (see
check_host_failure_domain) and the script exits with an error if it does not
hold.

This script finds those PGs and, for each one, picks an OSD elsewhere in the
cluster that the shard could legally be moved to instead. It only prints the
proposed re-targets; it changes nothing. The output is meant to be fed to
another script (or eyeballed) to actually apply the remaps.

How PGs are identified
----------------------
A PG is selected when it is in backfill_toofull *and* one of its shards is
newly arriving on the source OSD's host — that is, an OSD on that host is in
the PG's 'up' set but not its 'acting' set.

Note this is host-based, not OSD-based provenance. Once an OSD is out, the
slot it vacated in 'acting' reads as CRUSH_ITEM_NONE, not as its OSD id, so
there is no way to prove from the PG map that a given shard used to live on
the source OSD specifically. Matching on "a shard newly landed on the source
OSD's host" is the observable signal, and it is the one that matters: the host
is what is out of space. This also keeps PGs that are remapped but not
degraded (a real OSD id in 'acting') from being dropped.

'up'/'acting' are diffed differently per pool type, for the same reason as in
pg-movements.py: EC shards are identified by position, so index i is diffed
against index i and the shard index is reported. Replicated replicas are
interchangeable, so position carries no identity (a same-OSD-set reorder from
primary-affinity or pg-upmap-items is not movement) and the sets are diffed
instead, with SHARD shown as '-'.

How targets are chosen
----------------------
Candidates are OSDs of the same device class as the source OSD, that are up,
in (reweight > 0), and have a non-zero CRUSH weight, sorted by utilization
ascending (OSD id breaks ties, so re-runs are reproducible). The source OSD
itself is excluded — note it would otherwise sort *first*, since 'ceph osd df'
reports an out OSD at 0% utilization.

For each PG, the least-utilized candidate is taken whose host is not already
used by the PG's 'up' set. Because the arriving OSD being diverted is itself
on the source host, the source host is automatically excluded, which is the
whole point of the tool. A candidate is also rejected if it already appears in
the PG's *raw* CRUSH mapping (see below).

Each chosen OSD is removed from the candidate pool, so no two PGs are sent to
the same OSD. If a PG has several diverted shards, each target host is added
to that PG's exclusion set before its next shard is placed.

Why the raw CRUSH mapping matters
---------------------------------
A PG that already carries pg_upmap_items has OSDs in its raw CRUSH mapping
that are absent from 'up' — an entry "from 409 to 600" means 409 is what CRUSH
chose and 600 is what is actually used. Host 409 lives on is typically *not*
an 'up' host (that is usually why the upmap exists), so a check against 'up'
alone would happily propose 409 as a target. Applying that would put the same
OSD in the mapping twice; Ceph's upmap validation drops such an entry silently,
so the command appears to succeed and then has no effect. Targets are
therefore checked against 'up' union the reconstructed raw mapping.

Applying the output
-------------------
Two output formats are available. The default is a human-readable table.
--pgremapper instead emits one bare '<pgid> <from osd> <target osd>' line per
remap — the same first three columns as the table, which are also exactly the
positional arguments of 'pgremapper remap' (which calls FROM_OSD the "source
osd"). Only the rows go to stdout in either mode — everything else is on
stderr — so the output stays parseable.

Two caveats for whatever consumes this:

  - 'ceph osd pg-upmap-items' *replaces* a PG's entire upmap entry rather than
    adding to it. PGs here frequently already carry unrelated upmap pairs, so
    a command that states only the new pair silently discards the others and
    triggers fresh remapping. The EXISTING_UPMAPS column reports each PG's
    current pairs so they can be restated. 'pgremapper remap' merges into the
    existing entry rather than replacing it, so it needs no such restatement —
    which is why --pgremapper omits that column.

  - If the upmap balancer is active ('ceph balancer status'), it may undo
    manually placed upmap entries. Consider 'ceph balancer off' while the
    diverted backfills drain.

Review the proposals before applying them. To hand them to pgremapper:

    upmaps-to-unstick-toofull-backfills.py --pgremapper <osd> > remaps.txt
    xargs -a remaps.txt -L1 pgremapper-v1.0.0-linux-amd64 remap

Use 'xargs -a', not '< remaps.txt': with a redirect, xargs points each child's
stdin at /dev/null, so pgremapper's per-remap confirmation prompt reads EOF
instead of an answer. '-a' leaves stdin on the terminal. (Add '--yes' to
pgremapper to skip the prompt and its dry-run entirely.)
"""

import argparse
import json
import subprocess
import sys
from typing import NamedTuple

# Sentinel used by CRUSH/Ceph for "no OSD in this slot" (crush/crush.h).
# 'ceph pg ls'/'ceph pg dump' JSON uses this value, not -1, for empty slots.
CRUSH_ITEM_NONE = 0x7FFFFFFF

POOL_TYPE_ERASURE = 3


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "source_osd",
        type=int,
        help="OSD id whose host is absorbing the remaps (e.g. the OSD that "
        "went out). Its host is what backfills are diverted away from.",
    )
    parser.add_argument(
        "--pgremapper",
        action="store_true",
        help="Print '<pgid> <from osd> <target osd>' lines with no header "
        "instead of the table, so each line can be passed as the arguments "
        "of 'pgremapper remap' (see the epilogue above for the xargs form).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def _ceph_json(cmd: list[str]) -> object:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        sys.exit(f"ERROR: ceph command failed:\n{exc.stderr.strip()}")
    except FileNotFoundError:
        sys.exit("ERROR: 'ceph' binary not found in PATH.")
    return json.loads(proc.stdout)


def fetch_osd_hosts() -> dict[int, str]:
    """Return {osd_id: short_hostname} from 'ceph osd tree'."""
    data = _ceph_json(["ceph", "osd", "tree", "--format", "json"])
    nodes = data.get("nodes", []) + data.get("stray", [])
    by_id = {n["id"]: n for n in nodes}
    result = {}
    for n in nodes:
        if n.get("type") == "host":
            short = n["name"].split(".")[0]
            for child_id in n.get("children", []):
                if by_id.get(child_id, {}).get("type") == "osd":
                    result[child_id] = short
    return result


def fetch_osd_df() -> dict[int, dict]:
    """Return {osd_id: node} from 'ceph osd df'.

    Each node carries device_class, utilization, status, reweight and
    crush_weight, which together are everything needed to decide whether an
    OSD is a usable backfill target — no separate 'ceph osd dump' pass.
    """
    data = _ceph_json(["ceph", "osd", "df", "--format", "json"])
    nodes = data.get("nodes", []) + data.get("stray", [])
    return {n["id"]: n for n in nodes}


def fetch_upmap_items() -> dict[str, list[dict]]:
    """Return {pgid: [{'from': osd, 'to': osd}, ...]} from 'ceph osd dump'."""
    data = _ceph_json(["ceph", "osd", "dump", "--format", "json"])
    return {e["pgid"]: e["mappings"] for e in data.get("pg_upmap_items", [])}


def fetch_pool_details() -> list[dict]:
    """Return the list of pool dicts from 'ceph osd pool ls detail'."""
    return _ceph_json(["ceph", "osd", "pool", "ls", "detail", "--format", "json"])


def ec_pool_ids_from(pools: list[dict]) -> set[int]:
    """Return the set of pool ids that are erasure-coded (type == 3)."""
    return {p["pool_id"] for p in pools if p.get("type") == POOL_TYPE_ERASURE}


def fetch_crush_rules() -> dict[int, dict]:
    """Return {rule_id: rule} from 'ceph osd crush rule dump'."""
    data = _ceph_json(["ceph", "osd", "crush", "rule", "dump", "--format", "json"])
    return {r["rule_id"]: r for r in data}


def fetch_backfill_toofull_pgs() -> list[dict]:
    """Return pg_stat dicts for PGs in backfill_toofull.

    Filtered server-side by 'ceph pg ls', which is dramatically cheaper than
    dumping every PG in the cluster and filtering here.
    """
    raw = _ceph_json(["ceph", "pg", "ls", "backfill_toofull", "--format", "json"])
    return _extract_pg_stats(raw)


def _extract_pg_stats(raw) -> list[dict]:
    """Pull the pg_stat list out of the several shapes ceph releases return."""
    if isinstance(raw, list):
        return raw

    if isinstance(raw, dict):
        if "pg_stats" in raw:
            return raw["pg_stats"]

        pg_map = raw.get("pg_map", {})
        if "pg_stats" in pg_map:
            return pg_map["pg_stats"]

        for val in raw.values():
            if (
                isinstance(val, list)
                and val
                and isinstance(val[0], dict)
                and "pgid" in val[0]
            ):
                return val

        # When no PGs match the filter, 'ceph pg ls' omits 'pg_stats'
        # entirely and returns just {"pg_ready": true}.
        if "pg_ready" in raw:
            return []

    raise SystemExit(
        f"ERROR: unrecognised JSON structure from 'ceph pg ls'.\n"
        f"Top-level type: {type(raw).__name__}"
        + (f", keys: {list(raw.keys())}" if isinstance(raw, dict) else "")
    )


# ---------------------------------------------------------------------------
# Failure domain validation
# ---------------------------------------------------------------------------


def rule_failure_domain(rule: dict) -> "str | None":
    """Return the bucket type CRUSH spreads shards over for redundancy.

    This is the 'type' of the first choose*/chooseleaf* step in the rule
    (after 'take'). For a plain replicated rule that is its one chooseleaf
    step; for the common EC shape ('choose indep 0 type host' followed by
    'chooseleaf indep 1 type osd') it is the outer choose step, which is the
    one that determines the failure domain — the inner osd pick is just
    which leaf within that bucket, not what CRUSH spreads shards over.
    """
    for step in rule.get("steps", []):
        if step.get("op", "").startswith("choose"):
            return step.get("type")
    return None


def check_host_failure_domain(pools: list[dict], crush_rules: dict[int, dict]) -> None:
    """Exit with an error unless every given pool's CRUSH rule fails over at host.

    The diversion strategy this script implements (see module docstring)
    only makes sense if a failed leaf is retried within the same host
    bucket — that is what makes "a shard newly landed on the source OSD's
    host" a meaningful signal in find_diverted_shards(). If a pool's rule
    fails over at some other bucket type, that signal means nothing for it.
    """
    bad = []
    for pool in pools:
        rule = crush_rules.get(pool["crush_rule"])
        domain = rule_failure_domain(rule) if rule else None
        if domain != "host":
            bad.append((pool["pool_name"], pool["crush_rule"], domain))
    if bad:
        lines = "\n".join(
            f"  pool '{name}' uses crush rule {rule_id} (failure domain: "
            f"{domain or 'unknown'})"
            for name, rule_id, domain in bad
        )
        sys.exit(
            "ERROR: this script assumes every pool's CRUSH failure domain is "
            "'host' (see module docstring), but the following pool(s) do "
            f"not:\n{lines}"
        )


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


def _is_real_osd(osd_id) -> bool:
    return osd_id not in (CRUSH_ITEM_NONE, -1, None)


def _slot(osd_list: list, index: int) -> "int | None":
    """Return the real OSD id at a position, or None for a missing/empty slot."""
    if index >= len(osd_list):
        return None
    osd_id = osd_list[index]
    return osd_id if _is_real_osd(osd_id) else None


def raw_crush_osds(up: list, upmap_pairs: list[dict]) -> set[int]:
    """Reconstruct the OSDs CRUSH itself chose, by undoing the PG's upmaps.

    pg_upmap_items rewrites the CRUSH result: a pair {'from': X, 'to': Y}
    means CRUSH picked X and Y is used instead. Substituting each active
    pair's 'to' back to its 'from' recovers the raw mapping, whose members
    must not be proposed as targets (see module docstring).
    """
    raw = [osd_id for osd_id in up]
    for pair in upmap_pairs:
        if pair["to"] in raw:
            raw[raw.index(pair["to"])] = pair["from"]
    return {osd_id for osd_id in raw if _is_real_osd(osd_id)}


class DivertedShard(NamedTuple):
    """A shard that CRUSH re-placed onto the full source host."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    arriving_osd: int  # OSD on the source host now receiving the shard;
    # this is the 'from' of the upmap that would divert it
    vacated_osd: "int | None"  # OSD that left this slot, or None when the
    # slot reads as CRUSH_ITEM_NONE (the usual out-OSD case)
    up: list  # the PG's full up set, for host exclusions and for
    # reconstructing the raw CRUSH mapping


def find_diverted_shards(
    pg: dict, source_host: str, osd_host: dict[int, str], is_ec: bool
) -> list[DivertedShard]:
    """Return the shards of one PG that are newly arriving on the source host."""
    pgid = pg["pgid"]
    up = pg["up"]
    acting = pg["acting"]
    found = []

    if is_ec:
        # EC: shard identity is positional, so diff index by index. This is
        # what lets the shard index be reported, and keeps two unrelated
        # shard moves in one PG from being conflated.
        for i in range(max(len(up), len(acting))):
            arriving = _slot(up, i)
            vacated = _slot(acting, i)
            if arriving is None or arriving == vacated:
                continue
            if osd_host.get(arriving) != source_host:
                continue
            found.append(DivertedShard(pgid, i, arriving, vacated, up))
    else:
        # Replicated: replicas are interchangeable, so position means nothing
        # and only the set difference is real movement.
        up_set = {o for o in up if _is_real_osd(o)}
        acting_set = {o for o in acting if _is_real_osd(o)}
        vacated_osds = sorted(acting_set - up_set)
        for arriving in sorted(up_set - acting_set):
            if osd_host.get(arriving) != source_host:
                continue
            # A replicated PG can have several arriving/vacated replicas at
            # once with no way to pair them up; report one vacated OSD only
            # when the pairing is unambiguous.
            vacated = vacated_osds[0] if len(vacated_osds) == 1 else None
            found.append(DivertedShard(pgid, "-", arriving, vacated, up))

    return found


def pgid_sort_key(pgid: str) -> tuple[int, int]:
    """Sort PG IDs numerically: pool id (decimal), then pg id (hex)."""
    pool_str, pg_hex = pgid.split(".")
    return (int(pool_str), int(pg_hex, 16))


# ---------------------------------------------------------------------------
# Target selection
# ---------------------------------------------------------------------------


def build_candidate_osds(
    osd_df: dict[int, dict], device_class: str, source_osd: int
) -> list[int]:
    """Return usable target OSD ids of the given class, least-utilized first.

    Excludes OSDs that are down, out (reweight 0) or have no CRUSH weight,
    and the source OSD itself. Without this an out OSD sorts to the very
    front: 'ceph osd df' reports it at 0% utilization.
    """
    usable = [
        node
        for osd_id, node in osd_df.items()
        if osd_id != source_osd
        and node.get("device_class") == device_class
        and node.get("status") == "up"
        and node.get("reweight", 0) > 0
        and node.get("crush_weight", 0) > 0
    ]
    # OSD id as secondary key: utilizations tie constantly on a uniformly
    # full cluster, and the operator will re-run this.
    usable.sort(key=lambda n: (n["utilization"], n["id"]))
    return [n["id"] for n in usable]


class Proposal(NamedTuple):
    shard: DivertedShard
    target_osd: int
    target_host: str
    target_utilization: float


def assign_targets(
    shards: list[DivertedShard],
    candidates: list[int],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    upmap_items: dict[str, list[dict]],
) -> tuple[list[Proposal], list[DivertedShard], list[DivertedShard]]:
    """Greedily give each diverted shard the least-utilized legal target.

    Returns (proposals, unplaceable shards, unappliable shards). Each target
    OSD is consumed, so no two shards are sent to the same OSD.
    """
    available = list(candidates)
    # Hosts already spoken for per PG: seeded from the up set, then extended
    # as each of the PG's shards is placed, so a PG with two diverted shards
    # cannot be given two targets on one host.
    blocked_hosts: dict[str, set[str]] = {}
    proposals = []
    unplaceable = []
    unappliable = []

    for shard in shards:
        forbidden_hosts = blocked_hosts.setdefault(
            shard.pgid,
            {osd_host.get(o) for o in shard.up if _is_real_osd(o)},
        )
        raw = raw_crush_osds(shard.up, upmap_items.get(shard.pgid, []))
        # 'up' alone is not enough: an OSD displaced by an existing upmap is
        # absent from 'up' but still in the raw mapping, and re-proposing it
        # would put the same OSD in the mapping twice — which Ceph's upmap
        # validation drops silently (see module docstring).
        forbidden_osds = raw | {o for o in shard.up if _is_real_osd(o)}

        # The arriving OSD becomes the 'from' of the diverting upmap pair, and
        # 'from' must be an OSD CRUSH itself chose. If the arriving OSD is
        # absent from the raw mapping it is itself the product of an existing
        # upmap (the balancer places these, and it is active on this cluster),
        # so diverting it means rewriting that pair's 'to' rather than adding
        # a new pair. Emitting a row here would produce a command that Ceph
        # accepts and then silently ignores, so report it instead.
        if shard.arriving_osd not in raw:
            unappliable.append(shard)
            continue

        for candidate in available:
            if osd_host.get(candidate) in forbidden_hosts:
                continue
            if candidate in forbidden_osds:
                continue
            available.remove(candidate)
            forbidden_hosts.add(osd_host.get(candidate))
            proposals.append(
                Proposal(
                    shard,
                    candidate,
                    osd_host.get(candidate, "?"),
                    osd_df[candidate]["utilization"],
                )
            )
            break
        else:
            unplaceable.append(shard)

    return proposals, unplaceable, unappliable


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

COLUMNS = [
    "PGID",
    "SHARD",
    "FROM_OSD",
    "TARGET_OSD",
    "TARGET_HOST",
    "TGT_UTIL",
    "VACATED",
    "EXISTING_UPMAPS",
]


def format_row(
    proposal: Proposal,
    upmap_items: dict[str, list[dict]],
) -> list[str]:
    shard = proposal.shard
    pairs = upmap_items.get(shard.pgid, [])

    return [
        shard.pgid,
        str(shard.shard),
        f"osd.{shard.arriving_osd}",
        f"osd.{proposal.target_osd}",
        proposal.target_host,
        f"{proposal.target_utilization:.1f}%",
        f"osd.{shard.vacated_osd}" if shard.vacated_osd is not None else "none",
        ",".join(f"{p['from']}->{p['to']}" for p in pairs) if pairs else "-",
    ]


def print_table(rows: list[list[str]]) -> None:
    widths = [
        max(len(header), *(len(row[i]) for row in rows))
        for i, header in enumerate(COLUMNS)
    ]

    # Last column is variable-width and rightmost; leave it unpadded.
    def emit(cells):
        print(
            "  ".join(
                cell.ljust(widths[i]) if i < len(cells) - 1 else cell
                for i, cell in enumerate(cells)
            )
        )

    emit(COLUMNS)
    for row in rows:
        emit(row)


def print_pgremapper(proposals: list[Proposal]) -> None:
    """Print one '<pgid> <from osd> <target osd>' line per proposal.

    These are the positional arguments of 'pgremapper remap', in order and
    with nothing else on the line, so the output can be fed to it directly.
    (What the table calls FROM_OSD is pgremapper's "source osd" argument —
    not to be confused with this script's source_osd, the OSD that went out.)
    OSD ids are bare integers: pgremapper parses them with strconv.Atoi and
    rejects the 'osd.N' form the table uses.
    """
    for proposal in proposals:
        print(
            f"{proposal.shard.pgid} {proposal.shard.arriving_osd} {proposal.target_osd}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    source_osd = args.source_osd

    osd_host = fetch_osd_hosts()
    osd_df = fetch_osd_df()

    if source_osd not in osd_df:
        sys.exit(f"ERROR: osd.{source_osd} not found in 'ceph osd df' output.")
    source_host = osd_host.get(source_osd)
    if source_host is None:
        sys.exit(
            f"ERROR: could not determine the host of osd.{source_osd} from "
            f"'ceph osd tree' (is it still in the CRUSH map?)."
        )
    device_class = osd_df[source_osd].get("device_class")

    upmap_items = fetch_upmap_items()
    pools = fetch_pool_details()
    ec_pool_ids = ec_pool_ids_from(pools)
    toofull_pgs = fetch_backfill_toofull_pgs()

    toofull_pool_ids = {int(pg["pgid"].split(".")[0]) for pg in toofull_pgs}
    pools_by_id = {p["pool_id"]: p for p in pools}
    check_host_failure_domain(
        [pools_by_id[i] for i in toofull_pool_ids if i in pools_by_id],
        fetch_crush_rules(),
    )

    shards = []
    for pg in toofull_pgs:
        is_ec = int(pg["pgid"].split(".")[0]) in ec_pool_ids
        shards.extend(find_diverted_shards(pg, source_host, osd_host, is_ec))
    shards.sort(
        key=lambda s: (
            pgid_sort_key(s.pgid),
            s.shard if isinstance(s.shard, int) else -1,
        )
    )

    candidates = build_candidate_osds(osd_df, device_class, source_osd)
    proposals, unplaceable, unappliable = assign_targets(
        shards, candidates, osd_host, osd_df, upmap_items
    )

    # Everything informational goes to stderr so stdout stays parseable.
    print(
        f"source: osd.{source_osd} on {source_host} (class {device_class}); "
        f"{len(toofull_pgs)} backfill_toofull PGs cluster-wide, "
        f"{len(shards)} shard(s) of them arriving on {source_host}; "
        f"{len(candidates)} candidate target OSDs",
        file=sys.stderr,
    )

    if proposals:
        if args.pgremapper:
            print_pgremapper(proposals)
        else:
            print_table([format_row(p, upmap_items) for p in proposals])

    for shard in unplaceable:
        print(
            f"WARNING: no legal target left for {shard.pgid} shard "
            f"{shard.shard} (arriving on osd.{shard.arriving_osd}) — every "
            f"candidate OSD is on a host already in the PG's up set, already "
            f"in its CRUSH mapping, or already used by another PG",
            file=sys.stderr,
        )

    for shard in unappliable:
        print(
            f"WARNING: skipped {shard.pgid} shard {shard.shard}: the arriving "
            f"osd.{shard.arriving_osd} is not in the PG's raw CRUSH mapping, "
            f"so it was placed there by an existing upmap. Divert it by "
            f"rewriting that pair's 'to', not by adding a new pair",
            file=sys.stderr,
        )

    print(
        f"proposed {len(proposals)} remap(s), {len(unplaceable)} unplaceable, "
        f"{len(unappliable)} unappliable",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
