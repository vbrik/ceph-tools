# SPDX-License-Identifier: MIT
"""Code shared by the scripts in this directory.

Not a script itself: the others import it (`from shared import ...`), so it
has to sit next to them. Four groups of things live here:

* OSD-slot helpers and PG arithmetic (progress, copies in flight, shard size);
* reading cluster state, live or from a saved snapshot (`SnapshotStore`), the
  global `--load-state` flag built on it, and the anonymizer (used by the
  `save-state` subcommand) that makes a saved snapshot safe to share;
* `fetch_*` helpers that turn snapshot keys into lookup tables;
* the two-line grouped table (`print_table`) and its cell formatters.
"""

import argparse
import copy
import hashlib
import json
import math
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Callable
from itertools import groupby
from pathlib import Path

# Sentinel used by CRUSH/Ceph for "no OSD in this slot" (crush/crush.h).
# 'ceph pg' JSON uses this value, not -1, to mark unfilled up/acting slots.
CRUSH_ITEM_NONE = 0x7FFFFFFF

POOL_TYPE_ERASURE = 3

# 'ceph osd df' reports sizes in KiB.
KIB = 1024

# The pg_stat.stat_sum counters pg_progress_pct reads.
PROGRESS_COUNTERS = ("num_objects", "num_objects_misplaced", "num_objects_degraded")

# Printed for a value that does not apply (an empty slot's UTIL/HOST, a PG
# that is not moving). Distinct from '?', which means the thing exists but its
# data is unavailable.
NOT_APPLICABLE = "-"

# Between table columns of one group, and between columns of different groups.
COLUMN_SEP = "  "
GROUP_SEP = "    "

# One table column: (group, label). An empty group means no group line.
Columns = list[tuple[str, str]]


# ---------------------------------------------------------------------------
# OSD slots, PGs and pools
# ---------------------------------------------------------------------------


def is_real_osd(osd_id) -> bool:
    """True for an actual OSD id, False for the placeholders -1/CRUSH_ITEM_NONE/None."""
    return osd_id not in (CRUSH_ITEM_NONE, -1, None)


def slot(osd_list: list, index: int) -> int | None:
    """Return the real OSD at a position of an up/acting array, else None."""
    if index >= len(osd_list):
        return None
    osd_id = osd_list[index]
    return osd_id if is_real_osd(osd_id) else None


def real_osd_set(osd_list: list) -> set[int]:
    """Return the set of real (non-placeholder) OSD ids in an up/acting array."""
    return {o for o in osd_list if is_real_osd(o)}


def pgid_pool_id(pgid: str) -> int:
    """Return the pool id (decimal) of a PG id like '19.2a1'."""
    return int(pgid.split(".")[0])


def pgid_sort_key(pgid: str) -> tuple[int, int]:
    """Sort PG ids numerically: pool id (decimal), then pg id (hex)."""
    pool_str, pg_hex = pgid.split(".")
    return (int(pool_str), int(pg_hex, 16))


def is_erasure(pool: dict | None) -> bool:
    """True if the pool (an entry of 'ceph osd pool ls detail') is erasure-coded."""
    return pool is not None and pool.get("type") == POOL_TYPE_ERASURE


def rule_failure_domain(rule: dict | None) -> str | None:
    """Return the bucket type CRUSH spreads shards over for redundancy.

    The 'type' of the rule's first choose*/chooseleaf* step: for the common EC
    shape ('choose indep 0 type host' then 'chooseleaf indep 1 type osd') that
    is the outer step, the inner osd pick being only the leaf within it.
    """
    for step in (rule or {}).get("steps", []):
        if step.get("op", "").startswith("choose"):
            return step.get("type")
    return None


def shard_size_bytes(pg: dict, pool: dict, ec_profiles: dict[str, dict]) -> int | None:
    """Estimate the bytes one shard of a PG occupies, or None if unknown.

    A replica holds all of the PG's logical size ('num_bytes'), an EC shard
    1/k of it (rounded up). Omap, metadata and stripe padding are not counted,
    so this slightly underestimates. None when the pool's erasure code profile
    is missing from ec_profiles or has no usable 'k'.
    """
    num_bytes = pg["stat_sum"]["num_bytes"]
    if not is_erasure(pool):
        return num_bytes
    try:
        k = int(ec_profiles[pool["erasure_code_profile"]]["k"])
    except (KeyError, ValueError):
        return None
    return -(-num_bytes // k)


def ec_shard_moves(up: list, acting: list) -> list[tuple[int, int | None, int]]:
    """Return (shard, source, destination) for each EC shard being moved.

    source is None when the acting slot is empty (degraded). Shards that
    stay put (clean or in-place recovery) and shards whose up slot is empty
    (cluster still waiting for an OSD) are omitted.
    """
    moves = []
    for i in range(max(len(up), len(acting))):
        source, destination = slot(acting, i), slot(up, i)
        if destination is not None and source != destination:
            moves.append((i, source, destination))
    return moves


def ec_unassigned_shards(up: list, acting: list) -> int:
    """Count EC shards with no OSD in either up or acting.

    Nothing can move for these yet, but Ceph still counts their objects as
    degraded, so they belong in the progress denominator.
    """
    return sum(
        1
        for i in range(max(len(up), len(acting)))
        if slot(up, i) is None and slot(acting, i) is None
    )


def replicated_unassigned_copies(up_set: set, acting_set: set, size: int) -> int:
    """Count replicas that are degraded but have no destination OSD yet.

    Ceph counts size - len(acting) copies per object as degraded. Each
    destination (an up OSD outside acting) fills one of those, and any
    remainder is waiting for an OSD to appear, so it is extra work that isn't
    visible in up/acting. Replicated slots carry no identity, so this is a
    count difference rather than the per-slot check used for EC.
    """
    return max(0, size - len(acting_set) - len(up_set - acting_set))


def copies_moving(up: list, acting: list, is_ec: bool, pool_size: int) -> int:
    """Count the shard/replica copies a PG has to place.

    Ceph's misplaced/degraded object counters are in copy units, so this is
    the multiplier of num_objects in the progress denominator: slots whose up
    OSD differs from acting, plus slots with no OSD anywhere yet. Fixed until
    the PG finishes (acting only switches to up at the end), so it is a stable
    denominator. pool_size (replica count) is only used for replicated pools.
    """
    if is_ec:
        return len(ec_shard_moves(up, acting)) + ec_unassigned_shards(up, acting)
    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    return len(up_set - acting_set) + replicated_unassigned_copies(
        up_set, acting_set, pool_size
    )


def pg_progress_pct(pg: dict, n_copies: int) -> float | None:
    """Estimate % of a PG's data already at its target location.

    num_objects_misplaced (backfill) and num_objects_degraded (recovery) both
    count down to 0 as movement completes, unlike the lifetime counters
    (num_objects_recovered, ...) that only ever increase. Both are in *copy*
    units, so the PG's total work is num_objects * n_copies (see copies_moving);
    dividing by num_objects alone would read 0% until more than 1/n_copies of
    the work was done. An object-count approximation, not byte-exact: it
    assumes objects are of similar size. None if the PG has no objects.

    Can read 100% while the PG is still listed as moving (up != acting):
    these counters are Ceph's own estimate and can hit zero before the
    backfill scan itself actually finishes, especially on a very large or
    contended PG. See PROGRESS_100_NOTE.
    """
    stat_sum = pg.get("stat_sum", {})
    total = stat_sum.get("num_objects", 0) * n_copies
    if total <= 0:
        return None
    # Misplaced and degraded are not disjoint (a PG can be both at once), so
    # an object counted in both could make remaining > total without the clamp.
    remaining = stat_sum.get("num_objects_misplaced", 0) + stat_sum.get(
        "num_objects_degraded", 0
    )
    return max(0.0, min(100.0, 100.0 * (1 - remaining / total)))


def progress_reads_100(pct: float | None) -> bool:
    """True when pg_progress_pct's return value displays as '100%'.

    Centralized so every script that prints PROGRESS decides, the same way,
    whether to print PROGRESS_100_NOTE.
    """
    return pct == 100.0


# Printed once by a subcommand when any row's PROGRESS reads 100% (see
# progress_reads_100). Kept in one place so the wording can't drift between
# pg-movements, show-pg-osds and stop-backfills-into-osd. No leading
# newline/hard-wrapping: each caller adds its own paragraph spacing and either
# prints this as-is (fixed-width footnote style) or hands it to a wrapper that
# reflows it (textwrap.fill treats the embedded newlines as plain whitespace).
PROGRESS_100_NOTE = (
    "PROGRESS reads 100% once Ceph's own misplaced/degraded object counters "
    "hit zero\nfor the PG — not proof it has actually finished (it's still "
    "listed here because\nup != acting). On a very large or heavily contended "
    "PG those counters can read\ncomplete well before the backfill scan itself "
    "finishes, so 100% can persist for a\nwhile; it does not by itself mean "
    "anything is stuck."
)


# ---------------------------------------------------------------------------
# Cluster state: live or from a snapshot
# ---------------------------------------------------------------------------


def ceph_json(cmd: list[str]) -> object:
    """Run a ceph command and return its parsed JSON, exiting with a message on failure."""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        sys.exit(f"ERROR: ceph command failed: {' '.join(cmd)}\n{exc.stderr.strip()}")
    except FileNotFoundError:
        sys.exit("ERROR: 'ceph' binary not found in PATH.")
    return json.loads(proc.stdout)


class SnapshotStore:
    """Source of a script's cluster state: the live ceph CLI, or a saved copy.

    `commands` maps each snapshot key to the 'ceph ... --format json' command
    that produces it; the key is also the '<key>.json' filename it is saved
    as and loaded from. With load_dir set, `json(key)` reads that file and
    never runs ceph. Either way each key is read at most once (the parsed
    result is cached), even when several fetch_* helpers ask for it.

    With save_dir set, `save()` writes an anonymized copy of every key in
    `commands`, so the capture is complete by construction and a later
    --load-state of it needs no ceph. `anonymize` is applied to a deep copy of
    the full {key: parsed JSON} dict; the copy the run itself uses is left
    alone, so --save-state never changes what a run reports.
    """

    def __init__(
        self,
        commands: dict[str, list[str]],
        *,
        load_dir: Path | None = None,
        save_dir: Path | None = None,
        anonymize: "Callable[[dict[str, object]], None] | None" = None,
    ):
        self.commands = commands
        self.load_dir = load_dir
        self.save_dir = save_dir
        self.anonymize = anonymize or anonymize_snapshots
        self._cache: dict[str, object] = {}

    @classmethod
    def from_args(
        cls, args: argparse.Namespace, commands: dict[str, list[str]], **kwargs
    ) -> "SnapshotStore":
        """Build a store from --load-state, validating the directory."""
        load_dir = None
        if args.load_state:
            load_dir = Path(args.load_state)
            if not load_dir.is_dir():
                sys.exit(f"ERROR: --load-state directory not found: {load_dir}")
        return cls(commands, load_dir=load_dir, **kwargs)

    def json(self, key: str) -> object:
        """Return the parsed JSON for one of the store's keys."""
        if key in self._cache:
            return self._cache[key]
        if self.load_dir is not None:
            path = self.load_dir / f"{key}.json"
            try:
                data = json.loads(path.read_text())
            except FileNotFoundError:
                cmd = " ".join(self.commands[key])
                sys.exit(
                    f"ERROR: --load-state directory is missing {path} (the "
                    f"output of '{cmd}')."
                )
        else:
            data = ceph_json(self.commands[key])
        self._cache[key] = data
        return data

    def save(self) -> None:
        """Write an anonymized copy of every key under save_dir (a no-op without one)."""
        if self.save_dir is None:
            return
        snapshots = copy.deepcopy({key: self.json(key) for key in self.commands})
        self.anonymize(snapshots)
        for key, obj in snapshots.items():
            (self.save_dir / f"{key}.json").write_text(
                json.dumps(obj, separators=(",", ":"))
            )


def resolve_save_dir(path: str) -> Path:
    """Validate and prepare a directory for 'backfillctl save-state' to write into.

    Created if missing; must be empty (or not yet exist) so a capture is
    never partially overwritten by an unrelated one.
    """
    save_dir = Path(path)
    save_dir.mkdir(parents=True, exist_ok=True)
    if any(save_dir.iterdir()):
        sys.exit(f"ERROR: directory is not empty: {save_dir}")
    return save_dir


def add_load_state_arg(parser: argparse.ArgumentParser):
    """Add --load-state, to analyze a captured cluster state instead of a live one.

    backfillctl adds it once, to its top-level parser, so it is global: it
    goes before the subcommand name and reaches every subcommand's args.
    """
    parser.add_argument(
        "--load-state",
        metavar="DIR",
        help="Analyze a saved cluster state instead of a live cluster. DIR "
        "must be a directory as produced by 'backfillctl save-state' (or "
        "matching the layout of the fixtures under "
        "tests/pg-osd/test-data/). No 'ceph' commands are run. Not "
        "accepted by save-state.",
    )


def extract_pg_stats(raw, source: str) -> list[dict]:
    """Pull the pg_stat list out of the several shapes ceph releases return.

    source names the command in error messages, e.g. 'ceph pg ls remapped'.
    """
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        if raw.get("pg_ready") is False:
            # Reading this as "no PGs" would answer "nothing is moving" wrongly.
            sys.exit(
                f"ERROR: '{source}' reports the PG stats are not ready (the "
                "mgr has just started or failed over?); retry in a moment."
            )
        if "pg_stats" in raw:
            return raw["pg_stats"]
        pg_map = raw.get("pg_map", {})
        if "pg_stats" in pg_map:
            return pg_map["pg_stats"]
        # Last resort: any list whose first element looks like a pg_stat.
        for val in raw.values():
            if (
                isinstance(val, list)
                and val
                and isinstance(val[0], dict)
                and "pgid" in val[0]
            ):
                return val
        # With no matching PGs 'ceph pg ls' omits 'pg_stats' and returns just
        # {"pg_ready": true}.
        if "pg_ready" in raw:
            return []
    raise SystemExit(
        f"ERROR: unrecognised JSON structure from '{source}'.\n"
        f"Top-level type: {type(raw).__name__}"
        + (f", keys: {list(raw.keys())}" if isinstance(raw, dict) else "")
    )


def fetch_pg_stats(store: SnapshotStore, key: str) -> list[dict]:
    """Return the pg_stat dicts from the PG listing stored under key."""
    cmd = store.commands[key]
    source = " ".join(cmd[: cmd.index("--format")] if "--format" in cmd else cmd)
    return extract_pg_stats(store.json(key), source)


def _osd_nodes(tree_or_df: dict) -> list[dict]:
    """Return the nodes of 'ceph osd tree'/'osd df', including 'stray' (unplaced) OSDs."""
    return tree_or_df.get("nodes", []) + tree_or_df.get("stray", [])


def fetch_osd_hosts(store: SnapshotStore, key: str = "osd_tree") -> dict[int, str]:
    """Return {osd_id: short_hostname} from 'ceph osd tree'."""
    nodes = _osd_nodes(store.json(key))
    by_id = {n["id"]: n for n in nodes}
    result = {}
    for n in nodes:
        if n.get("type") == "host":
            short = n["name"].split(".")[0]
            for child_id in n.get("children", []):
                if by_id.get(child_id, {}).get("type") == "osd":
                    result[child_id] = short
    return result


def fetch_osd_df(store: SnapshotStore, key: str = "osd_df") -> dict[int, dict]:
    """Return {osd_id: node} from 'ceph osd df'.

    Each node carries device_class, utilization, status, reweight and
    crush_weight. Down OSDs may lack a utilization.
    """
    return {n["id"]: n for n in _osd_nodes(store.json(key))}


def fetch_pools(store: SnapshotStore, key: str = "pool_ls_detail") -> dict[int, dict]:
    """Return {pool_id: pool} from 'ceph osd pool ls detail'."""
    return {p["pool_id"]: p for p in store.json(key)}


def fetch_ec_profiles(store: SnapshotStore, key: str = "osd_dump") -> dict[str, dict]:
    """Return {profile_name: profile} from 'ceph osd dump'."""
    return store.json(key).get("erasure_code_profiles", {})


def fetch_crush_rules(
    store: SnapshotStore, key: str = "crush_rule_dump"
) -> dict[int, dict]:
    """Return {rule_id: rule} from 'ceph osd crush rule dump'."""
    return {r["rule_id"]: r for r in store.json(key)}


def fetch_upmap_items(
    store: SnapshotStore, key: str = "osd_dump"
) -> dict[str, list[dict]]:
    """Return {pgid: [{'from': osd, 'to': osd}, ...]} from 'ceph osd dump'."""
    return {e["pgid"]: e["mappings"] for e in store.json(key).get("pg_upmap_items", [])}


# ---------------------------------------------------------------------------
# Anonymization for --save-state
# ---------------------------------------------------------------------------

# A reserved-for-documentation range (RFC 5737 TEST-NET-2): guaranteed not
# to be a real routable address, so a saved capture can't be mistaken for
# one and can't leak the real network's layout.
_FAKE_IP_PREFIX = "198.51.100."
_ADDR_IP_RE = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
_TRAILING_NUM_RE = re.compile(r"(\d+)$")
_FAKE_HASH_NAME_RE = re.compile(r"host-[0-9a-f]{8}")

FAKE_FSID = "00000000-0000-0000-0000-000000000000"


def _fake_ip(real_ip: str) -> str:
    """Map a real IP to a deterministic, non-routable stand-in.

    Keyed off the real address's own last octet, so the same real IP always
    anonymizes to the same fake one with no lookup table required. Distinct
    real IPs sharing a last octet collide onto the same fake one; that is
    harmless since no script parses these descriptive fields.
    """
    last_octet = int(real_ip.rsplit(".", 1)[-1])
    return f"{_FAKE_IP_PREFIX}{max(1, min(254, last_octet))}"


def _anonymize_addr_string(addr: str) -> str:
    """Replace the IP inside an 'IP:PORT' or 'IP:PORT/NONCE' address string."""
    return _ADDR_IP_RE.sub(lambda m: _fake_ip(m.group()), addr)


def _fake_uuid(osd_id: int) -> str:
    # '1's rather than '0's so osd.0's fake uuid can't collide with FAKE_FSID.
    return f"11111111-1111-1111-1111-{osd_id:012d}"


def _fake_hostname(real_name: str) -> str:
    """Map a real hostname to a deterministic stand-in.

    Keyed off the hostname's trailing number (e.g. 'ceph2-11' -> 'host11'), or,
    with none, a hash of the whole name, so the same real host always maps to
    the same fake one with no shared state.
    """
    if _FAKE_HASH_NAME_RE.fullmatch(real_name):
        return real_name  # already a stand-in: keep anonymization idempotent
    m = _TRAILING_NUM_RE.search(real_name)
    if m:
        return f"host{int(m.group(1)):02d}"
    return "host-" + hashlib.sha256(real_name.encode()).hexdigest()[:8]


def _fake_hostnames(real_names: set[str]) -> dict[str, str]:
    """Map every real hostname to a fake one, never two to the same.

    The analysis depends on which OSDs share a host, so two hosts must not
    become one (ceph1-5 and ceph2-5 would both be 'host05'). Names that collide
    under _fake_hostname all take the hash form instead. Idempotent: the fake
    names it produces map to themselves.
    """
    fakes = {name: _fake_hostname(name) for name in real_names}
    counts = Counter(fakes.values())
    for name, fake in fakes.items():
        if counts[fake] > 1:
            fakes[name] = "host-" + hashlib.sha256(name.encode()).hexdigest()[:8]
    if len(set(fakes.values())) != len(fakes):
        raise RuntimeError("cannot anonymize the hostnames without merging two hosts")
    return fakes


def anonymize_snapshots(snapshots: dict[str, object]) -> None:
    """Anonymize parsed snapshots in place, whichever of the known keys are present.

    Replaces the cluster fsid, OSD IP addresses, OSD uuids, hostnames and
    pool/CRUSH-rule names with deterministic fake values: everything that could
    fingerprint the real cluster or site. PG ids, OSD ids, utilizations,
    weights, device classes and the topology are untouched, since the analysis
    (and any --load-state replay) depends on them. Pool and rule names are
    display-only in the scripts (lookups are by id), so renaming them to
    'pool<id>'/'rule<id>' needs no cross-reference fixups.

    Idempotent: every substitution is keyed off an id, or off the real value
    itself, so a second pass changes nothing and independent passes over
    related captures agree without sharing state.
    """
    if "osd_tree" in snapshots:
        osd_tree = snapshots["osd_tree"]
        hosts = [n for n in _osd_nodes(osd_tree) if n.get("type") == "host"]
        fakes = _fake_hostnames({node["name"] for node in hosts})
        for node in hosts:
            node["name"] = fakes[node["name"]]

    if "osd_dump" in snapshots:
        osd_dump = snapshots["osd_dump"]
        osd_dump["fsid"] = FAKE_FSID
        # 'ceph osd dump' embeds its own copy of each pool's name, keyed by
        # 'pool' rather than 'pool_id'.
        for pool in osd_dump.get("pools", []):
            pool["pool_name"] = f"pool{pool['pool']}"
        for osd in osd_dump.get("osds", []):
            osd["uuid"] = _fake_uuid(osd["osd"])
            for key in (
                "public_addr",
                "cluster_addr",
                "heartbeat_back_addr",
                "heartbeat_front_addr",
            ):
                if key in osd:
                    osd[key] = _anonymize_addr_string(osd[key])
            for key in (
                "public_addrs",
                "cluster_addrs",
                "heartbeat_back_addrs",
                "heartbeat_front_addrs",
            ):
                for entry in osd.get(key, {}).get("addrvec", []):
                    entry["addr"] = _anonymize_addr_string(entry["addr"])

    for pool in snapshots.get("pool_ls_detail", []):
        pool["pool_name"] = f"pool{pool['pool_id']}"
    for rule in snapshots.get("crush_rule_dump", []):
        rule["rule_name"] = f"rule{rule['rule_id']}"


# ---------------------------------------------------------------------------
# Table output
# ---------------------------------------------------------------------------


def format_utilization(osd_df: dict[int, dict], osd_id: int | None) -> str:
    """Format an OSD's utilization as 'NN.N%'.

    '-' for an empty slot (osd_id None), '?' if 'ceph osd df' has no figure.
    """
    if osd_id is None:
        return NOT_APPLICABLE
    util = osd_df.get(osd_id, {}).get("utilization")
    return f"{util:.1f}%" if util is not None else "?"


def format_progress(pct: float | None) -> str:
    """Format a progress percentage, floored so a PG still moving never reads '100%'."""
    return NOT_APPLICABLE if pct is None else f"{math.floor(pct)}%"


# Short forms of PG state flags for table cells; unknown flags pass through.
_STATE_ABBREVS = {
    "active": "act",
    "clean": "cln",
    "degraded": "deg",
    "undersized": "undsz",
    "remapped": "remap",
    "recovering": "rcvr",
    "recovery_wait": "rcvr_wt",
    "recovery_toofull": "rcvr_tf",
    "forced_recovery": "frc_rcvr",
    "backfilling": "bkfl",
    "backfill_wait": "bkfl_wt",
    "backfill_toofull": "bkfl_tf",
    "forced_backfill": "frc_bkfl",
    "peering": "prng",
    "peered": "prd",
    "scrubbing": "scrb",
    "deep": "dp",
    "repair": "rep",
    "inconsistent": "incon",
    "incomplete": "incomp",
    "stale": "stl",
    "down": "dn",
    "creating": "crt",
    "snaptrim": "snptrim",
    "snaptrim_wait": "snptrim_wt",
    "snaptrim_error": "snptrim_err",
    "wait": "wt",
}


def abbreviate_state(state: str) -> str:
    """Abbreviate each flag of a '+'-joined PG state, e.g. 'act+remap+bkfl_wt'."""
    return "+".join(_STATE_ABBREVS.get(flag, flag) for flag in state.split("+"))


def osd_cells(
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    osd_id: int | None,
    primary: int | None = None,
    *,
    bare_id: bool = False,
) -> list[str]:
    """Return the [OSD, UTIL, HOST] cells for one slot.

    The OSD reads 'osd.N', or just 'N' with bare_id. An empty slot (osd_id
    None) reads 'none', '-', '-'. The OSD that is the PG's `primary` gets a
    trailing '*'.
    """
    if osd_id is None:
        return ["none", NOT_APPLICABLE, NOT_APPLICABLE]
    star = "*" if osd_id == primary else ""
    return [
        f"{osd_id if bare_id else f'osd.{osd_id}'}{star}",
        format_utilization(osd_df, osd_id),
        osd_host.get(osd_id, "?"),
    ]


def print_table(columns: Columns, rows: list[list[str]]) -> None:
    """Print rows under a two-line header: group spans, then column labels.

    A group's name is centered in dashes across the full width of its
    columns, so it visibly covers all of them. The span is always wider than
    the name (the labels under it alone are wider), so no fitting is needed.
    Columns of different groups are separated by the wider GROUP_SEP, on every
    line, to set the groups visually apart. The final column is left unpadded.
    """
    # A list, not max(a, *b): with no rows the star-args form degrades to
    # max(int) and raises.
    widths = [
        max([len(label), *(len(row[i]) for row in rows)])
        for i, (_, label) in enumerate(columns)
    ]
    # seps[i] is what precedes column i.
    seps = [""] + [
        COLUMN_SEP if columns[i][0] == columns[i - 1][0] else GROUP_SEP
        for i in range(1, len(columns))
    ]

    group_line = ""
    for group, indexes in groupby(range(len(columns)), key=lambda i: columns[i][0]):
        cols = list(indexes)
        span = sum(widths[i] for i in cols) + sum(len(seps[i]) for i in cols[1:])
        group_line += seps[cols[0]]
        group_line += f" {group} ".center(span, "-") if group else " " * span
    print(group_line.rstrip())

    def emit(cells: list[str]) -> None:
        last = len(cells) - 1
        print(
            "".join(
                sep + (cell.ljust(widths[i]) if i < last else cell)
                for i, (sep, cell) in enumerate(zip(seps, cells))
            )
        )

    emit([label for _, label in columns])
    for row in rows:
        emit(row)
