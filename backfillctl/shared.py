# SPDX-License-Identifier: MIT
"""Code shared by the subcommands.

* OSD-slot helpers and PG arithmetic: progress, copies in flight, shard size,
  backfill positions.
* Cluster state, live or from a capture (SnapshotStore, --load-state), and
  the anonymizer used by save-state.
* fetch_* helpers that turn snapshots into lookup tables.
* CLI helpers, the grouped table (print_table) and its cell formatters.
* Pinning moving shards back (close_pins and friends), shared by
  cancel-backfill and cancel-uphill.

Message text more than one command prints is in messages.py.
"""

import argparse
import copy
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import textwrap
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from itertools import groupby
from pathlib import Path
from typing import NamedTuple

from messages import (
    blocking_reason,
    companion_note,
    format_bytes,
    osd_list,
    stderr_para,
)

# Sentinel used by CRUSH/Ceph for "no OSD in this slot" (crush/crush.h).
# 'ceph pg' JSON uses this value, not -1, to mark unfilled up/acting slots.
CRUSH_ITEM_NONE = 0x7FFFFFFF

POOL_TYPE_ERASURE = 3

# 'ceph osd df' reports sizes in KiB.
KIB = 1024

# The pg_stat.stat_sum counters pg_progress_pct reads.
PROGRESS_COUNTERS = ("num_objects", "num_objects_misplaced", "num_objects_degraded")

# Cell for a value that does not apply. '?' means applicable but unknown.
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


def slot(osds: list, index: int) -> int | None:
    """Return the real OSD at a position of an up/acting array, else None."""
    if index >= len(osds):
        return None
    osd_id = osds[index]
    return osd_id if is_real_osd(osd_id) else None


def real_osd_set(osds: list) -> set[int]:
    """Return the set of real (non-placeholder) OSD ids in an up/acting array."""
    return {o for o in osds if is_real_osd(o)}


def pgid_pool_id(pgid: str) -> int:
    """Return the pool id (decimal) of a PG id like '19.2a1'."""
    return int(pgid.split(".")[0])


def pgid_sort_key(pgid: str) -> tuple[int, int]:
    """Sort PG ids numerically: pool id (decimal), then pg id (hex)."""
    pool_str, pg_hex = pgid.split(".")
    return (int(pool_str), int(pg_hex, 16))


class PgidFilter(NamedTuple):
    """What a list of PG ids given on the command line (e.g. --pgs) matched."""

    given: int
    matched: int
    unmatched: list[str]  # sorted; often typos

    @classmethod
    def of(cls, given: Iterable[str], present: Iterable[str]) -> "PgidFilter":
        """Return what the given ids matched among the present ones."""
        given = set(given)
        matched = given & set(present)
        return cls(len(given), len(matched), sorted(given - matched))


def is_erasure(pool: dict | None) -> bool:
    """True if the pool (an entry of 'ceph osd pool ls detail') is erasure-coded."""
    return pool is not None and pool.get("type") == POOL_TYPE_ERASURE


def rule_failure_domain(rule: dict | None) -> str | None:
    """Return the rule's failure domain: the type of its first choose* step.

    The first step, because in the common EC shape ('choose indep 0 type
    host', then 'chooseleaf indep 1 type osd') the inner step only picks a
    leaf within the host.
    """
    for step in (rule or {}).get("steps", []):
        if step.get("op", "").startswith("choose"):
            return step.get("type")
    return None


def check_known_pools(pgids: Iterable[str], pools: dict[int, dict], which: str) -> None:
    """Exit unless 'ceph osd pool ls detail' lists the pool of every PG in pgids.

    which names the PGs in the message, e.g. 'backfill_toofull PGs'. An
    unknown pool's EC shards would be diffed as replicas: plausible but
    wrong output.
    """
    unknown = sorted({pgid_pool_id(p) for p in pgids} - pools.keys())
    if unknown:
        sys.exit(
            f"ERROR: {which} belong to pool id(s) {', '.join(map(str, unknown))}, "
            "which 'ceph osd pool ls detail' does not list, so their shards "
            "cannot be analyzed."
        )


def check_host_failure_domain(
    pgids: Iterable[str],
    pools: dict[int, dict],
    crush_rules: dict[int, dict],
    which: str,
) -> None:
    """Exit unless the pool of every PG in pgids has CRUSH failure domain host.

    The pools must be known (check_known_pools). which names the PGs, as
    there. Every offending pool is listed. The remapping subcommands keep a
    PG's shards on distinct hosts, which is only right for host.
    """
    bad = []
    for pool_id in sorted({pgid_pool_id(p) for p in pgids}):
        pool = pools[pool_id]
        domain = rule_failure_domain(crush_rules.get(pool.get("crush_rule")))
        if domain != "host":
            bad.append(
                f"  pool {pool_id} ({pool.get('pool_name', '?')}): crush rule "
                f"{pool.get('crush_rule')}, failure domain {domain or 'unknown'}"
            )
    if bad:
        sys.exit(
            "ERROR: this subcommand assumes CRUSH failure domain 'host', but the "
            f"pools of these {which} use another:\n" + "\n".join(bad)
        )


def shard_size_bytes(pg: dict, pool: dict, ec_profiles: dict[str, dict]) -> int | None:
    """Estimate the bytes one shard of a PG occupies, or None if unknown.

    A replica holds the PG's num_bytes, an EC shard 1/k of it. Ignores omap,
    metadata and stripe padding, so it runs slightly low. None if the pool's
    EC profile or its 'k' is missing.
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

    source is None for an empty acting slot. Shards that stay put or have an
    empty up slot are omitted.
    """
    moves = []
    for i in range(max(len(up), len(acting))):
        source, destination = slot(acting, i), slot(up, i)
        if destination is not None and source != destination:
            moves.append((i, source, destination))
    return moves


def ec_unassigned_shards(up: list, acting: list) -> int:
    """Count EC shards with no OSD in either up or acting.

    Ceph counts their objects as degraded, so they belong in the progress
    denominator.
    """
    return sum(
        1
        for i in range(max(len(up), len(acting)))
        if slot(up, i) is None and slot(acting, i) is None
    )


def replicated_unassigned_copies(up_set: set, acting_set: set, size: int) -> int:
    """Count replicas that are degraded but have no destination OSD yet.

    Ceph counts size - len(acting) copies per object as degraded. Each
    destination fills one; the rest wait for an OSD to appear.
    """
    return max(0, size - len(acting_set) - len(up_set - acting_set))


def copies_moving(up: list, acting: list, is_ec: bool, pool_size: int) -> int:
    """Count the shard/replica copies a PG has to place.

    Ceph's misplaced/degraded counters are in copy units, so this multiplies
    num_objects in the progress denominator. It stays fixed until the PG
    finishes. pool_size is used only for replicated pools.
    """
    if is_ec:
        return len(ec_shard_moves(up, acting)) + ec_unassigned_shards(up, acting)
    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
    return len(up_set - acting_set) + replicated_unassigned_copies(
        up_set, acting_set, pool_size
    )


def pg_progress_pct(pg: dict, n_copies: int) -> float | None:
    """Estimate % of a PG's data at its target, from Ceph's object counters.

    num_objects_misplaced and num_objects_degraded count down to 0, in copy
    units, so the total is num_objects * n_copies (see copies_moving). Counts
    objects, not bytes. None if the PG has no objects.

    Unreliable after re-peering (see messages.PROGRESS_APPROX_NOTE); only the fallback
    for backfill positions.
    """
    stat_sum = pg.get("stat_sum", {})
    total = stat_sum.get("num_objects", 0) * n_copies
    if total <= 0:
        return None
    # Clamped: an object can be both misplaced and degraded.
    remaining = stat_sum.get("num_objects_misplaced", 0) + stat_sum.get(
        "num_objects_degraded", 0
    )
    return max(0.0, min(100.0, 100.0 * (1 - remaining / total)))


# Backfill position
# -----------------
# Backfill copies objects in hobject order, keyed by the bit-reversed 32-bit
# name hash. A target's 'last_backfill' ('ceph pg query' peer_info) is its
# position in that order: 'POOL:KEY:...', or MIN/MAX. Hashes are uniform, so
# the share of the PG's key range behind the position is the share of objects
# copied. Unlike the counters, it survives re-peering.


def pg_hash_bits(seed: int, pg_num: int) -> int:
    """Return how many low bits of an object's hash are fixed for PG seed.

    Mirrors ceph_stable_mod(). With n = bit length of pg_num - 1, a PG fixes
    n bits, except a PG below 2^(n-1) whose sibling seed + 2^(n-1) does not
    exist: it also takes the sibling's hashes, so it fixes n - 1.
    """
    n = (pg_num - 1).bit_length()
    half = 1 << (n - 1) if n else 0
    if n and seed < half and seed + half >= pg_num:
        return n - 1
    return n


def normalize_last_backfill(last_backfill: str) -> str | None:
    """Reduce a 'last_backfill' hobject to 'MIN', 'MAX' or its hex sort key.

    Drops the object name, which also keeps names out of save-state
    captures. None if unparsable.
    """
    if last_backfill in ("MIN", "MAX"):
        return last_backfill
    parts = last_backfill.split(":")
    if len(parts) < 2 or not re.fullmatch(r"[0-9A-Fa-f]{1,8}", parts[1]):
        return None
    return parts[1].lower()


def backfill_fraction(position: str, seed: int, pg_num: int) -> float | None:
    """Return the share (0..1) of PG seed's objects before a backfill position.

    position is normalize_last_backfill's output. None if the key is not in
    the PG's range (e.g. after a pg_num change).
    """
    if position == "MIN":
        return 0.0
    if position == "MAX":
        return 1.0
    key = int(position, 16)
    bits = pg_hash_bits(seed, pg_num)
    # The key's top `bits` bits are the hash's fixed low bits, reversed; the
    # rest is the position within the PG.
    hash_low = int(f"{key >> (32 - bits):0{bits}b}"[::-1], 2) if bits else 0
    if hash_low != seed & ((1 << bits) - 1):
        return None
    span = 1 << (32 - bits)
    return (key & (span - 1)) / span


def extract_backfill_positions(query: dict) -> dict[str, str]:
    """Return {peer: position} for the backfill targets in a 'ceph pg query'.

    Positions are normalized (normalize_last_backfill). Stray peers from
    earlier mappings are dropped.
    """
    up, acting = query.get("up", []), query.get("acting", [])
    positions = {}
    for info in query.get("peer_info", []):
        peer = str(info.get("peer", ""))
        match = re.fullmatch(r"(\d+)(?:\((\d+)\))?", peer)
        if match is None:
            continue
        osd = int(match[1])
        if match[2] is not None:
            shard = int(match[2])
            is_target = slot(up, shard) == osd and slot(acting, shard) != osd
        else:
            is_target = osd in real_osd_set(up) - real_osd_set(acting)
        position = normalize_last_backfill(info.get("last_backfill", ""))
        if is_target and position is not None:
            positions[peer] = position
    return positions


def target_peer(osd: int, shard: "int | str") -> str:
    """Name a backfill target as 'ceph pg query' does: 'OSD(SHARD)' for EC, 'OSD' for a replica."""
    return f"{osd}({shard})" if isinstance(shard, int) else str(osd)


def target_progress_pct(
    pgid: str, pool: dict | None, position: str | None
) -> float | None:
    """Return % of a PG's objects one backfill target has received, or None if unknown."""
    pg_num = pool.get("pg_num") if pool else None
    if not pg_num or position is None:
        return None
    fraction = backfill_fraction(position, int(pgid.split(".")[1], 16), pg_num)
    return None if fraction is None else 100.0 * fraction


class Progress(NamedTuple):
    """Movement progress, and where it came from."""

    pct: float | None  # None: PG has no objects, or pool unknown
    exact: bool  # from backfill positions; False: from Ceph's counters


def counter_progress(pg: dict, pool: dict | None) -> Progress:
    """Return the PG's progress by Ceph's counters: per PG, not per shard."""
    n_copies = copies_moving(
        pg["up"], pg["acting"], is_erasure(pool), pool.get("size", 0) if pool else 0
    )
    return Progress(pg_progress_pct(pg, n_copies), False)


def copy_progress(
    pg: dict, pool: dict | None, positions: dict[str, str], peer: str
) -> Progress:
    """Return the progress of one moving copy (EC shard or replica).

    peer names the copy's target (target_peer); positions is the PG's
    {peer: position}. Falls back on counter_progress.
    """
    pct = target_progress_pct(pg["pgid"], pool, positions.get(peer))
    return counter_progress(pg, pool) if pct is None else Progress(pct, True)


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
    """Cluster state from the live ceph CLI, or from a capture.

    commands maps each key to its 'ceph ... --format json' command; the key
    is also the capture's '<key>.json' filename. With load_dir, json() reads
    the file instead of running ceph. Results are cached.

    With save_dir, save() writes every key, anonymized. Anonymization works on
    a copy, so the run's own data is unaffected.
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


# {pgid: {peer: position}} of every remapped PG, in a capture.
BACKFILL_POSITIONS_FILE = "backfill_positions.json"

# Seconds per 'pg query'; a PG that times out falls back on the counters.
PG_QUERY_TIMEOUT = 30

# Concurrent 'pg query' requests over one librados connection (latency-bound).
RADOS_QUERY_THREADS = 16


def _positions_from_output(output: str | bytes) -> dict[str, str] | None:
    """Parse one 'ceph pg query' output into positions; None if it isn't one."""
    try:
        query = json.loads(output)
    except ValueError:  # JSONDecodeError, or undecodable bytes
        return None
    return extract_backfill_positions(query) if isinstance(query, dict) else None


def _query_positions_rados(rados, pgids: list[str]) -> dict[str, dict[str, str] | None]:
    """Query PGs concurrently over one librados connection.

    rados is the 'rados' module. Returns {pgid: positions, or None on
    failure}. Raises rados.Error if the cluster can't be reached.
    """
    cluster = rados.Rados(conffile="")  # "": ceph's default config search
    cluster.conf_parse_env()  # honor CEPH_ARGS, like the ceph CLI
    cluster.connect(timeout=PG_QUERY_TIMEOUT)
    try:

        def query(pgid: str) -> dict[str, str] | None:
            cmd = json.dumps({"prefix": "query", "pgid": pgid, "format": "json"})
            try:
                ret, out, _ = cluster.pg_command(pgid, cmd, b"", PG_QUERY_TIMEOUT)
            except rados.Error:
                return None
            return _positions_from_output(out) if ret == 0 else None

        with ThreadPoolExecutor(RADOS_QUERY_THREADS) as pool:
            return dict(zip(pgids, pool.map(query, pgids)))
    finally:
        cluster.shutdown()


def _query_positions_cli(pgids: list[str]) -> dict[str, dict[str, str] | None]:
    """Query PGs with parallel 'ceph pg <pgid> query' processes.

    Much slower and more CPU-hungry than librados, but needs only the CLI.
    """

    def query(pgid: str) -> dict[str, str] | None:
        cmd = ["ceph", "pg", pgid, "query", "--format", "json"]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=PG_QUERY_TIMEOUT,
                check=False,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None
        if proc.returncode != 0:
            return None
        return _positions_from_output(proc.stdout)

    with ThreadPoolExecutor(max(8, 2 * (os.cpu_count() or 1))) as pool:
        return dict(zip(pgids, pool.map(query, pgids)))


def query_backfill_positions(pgids: Iterable[str]) -> dict[str, dict[str, str]]:
    """Return {pgid: {peer: position}} for pgids, queried from the live cluster.

    Uses librados if importable, else the CLI. PGs whose query fails are left
    out (and counted on stderr).
    """
    pgids = sorted(set(pgids), key=pgid_sort_key)
    if not pgids:
        return {}
    try:
        import rados  # optional: the CLI fallback needs nothing extra
    except ImportError:
        rados = None
    results = None
    if rados is not None:
        try:
            results = _query_positions_rados(rados, pgids)
        except rados.Error:
            pass  # e.g. no keyring readable by librados; try the CLI
    if results is None:
        results = _query_positions_cli(pgids)
    failed = [pgid for pgid, positions in results.items() if positions is None]
    if failed:
        stderr_para(
            f"NOTE: 'ceph pg query' failed for {len(failed)} of {len(pgids)} "
            f"PG(s) ({', '.join(failed[:5])}{', ...' if len(failed) > 5 else ''}); "
            "their PROGRESS comes from Ceph's counters (marked '~')."
        )
    return {pgid: p for pgid, p in results.items() if p is not None}


def fetch_backfill_positions(
    store: "SnapshotStore", pgids: Iterable[str]
) -> dict[str, dict[str, str]]:
    """Return {pgid: {peer: position}} for pgids, live or from a snapshot.

    A capture without BACKFILL_POSITIONS_FILE yields {}.
    """
    if store.load_dir is None:
        return query_backfill_positions(pgids)
    try:
        saved = json.loads((Path(store.load_dir) / BACKFILL_POSITIONS_FILE).read_text())
    except FileNotFoundError:
        return {}
    return {pgid: saved[pgid] for pgid in pgids if pgid in saved}


def resolve_save_dir(path: str) -> Path:
    """Create save-state's output directory, exiting unless it is empty."""
    save_dir = Path(path)
    save_dir.mkdir(parents=True, exist_ok=True)
    if any(save_dir.iterdir()):
        sys.exit(f"ERROR: directory is not empty: {save_dir}")
    return save_dir


def parse_osd(text: str) -> int:
    """argparse type: an OSD id, given as '682' or 'osd.682'."""
    match = re.fullmatch(r"(?:osd\.)?(\d+)", text)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"expected an OSD id like 682 or osd.682, got {text!r}"
        )
    return int(match[1])


def check_osds_exist(option: str, osds: Iterable[int], osd_df: dict[int, dict]) -> None:
    """Exit naming the OSDs given with option that 'ceph osd df' does not list."""
    unknown = sorted(set(osds) - osd_df.keys())
    if unknown:
        sys.exit(f"ERROR: {option}: not in 'ceph osd df': " + osd_list(unknown))


def parse_pgid(text: str) -> str:
    """argparse type: a PG id like '19.2a1', normalized as Ceph prints it.

    Lowercase hex, no leading zeros, so '19.092E' matches '19.92e'.
    """
    match = re.fullmatch(r"(\d+)\.([0-9a-fA-F]+)", text)
    if match is None:
        raise argparse.ArgumentTypeError(f"expected a PG id like 19.2a1, got {text!r}")
    return f"{int(match[1])}.{int(match[2], 16):x}"


def percentage_points(text: str) -> float:
    """argparse type: a number from 0 to 100, e.g. a utilization difference."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number, got {text!r}") from None
    if not 0 <= value <= 100:  # also rejects nan
        raise argparse.ArgumentTypeError(f"must be from 0 to 100, got {text}")
    return value


def utilization_pct(text: str) -> float:
    """argparse type: a utilization threshold in percent, 0 or from 1 to 100.

    Rejects 0 < value <= 1: almost surely a ratio (0.85, as in Ceph's
    *_ratio settings) typed for a percentage. Taken as a percentage it
    would select everything or nothing, and the output would look plausible.
    """
    value = percentage_points(text)
    if 0 < value <= 1:
        raise argparse.ArgumentTypeError(
            f"expected a percentage like 85, not a ratio like 0.85, got {text}"
        )
    return value


class HelpFormatter(argparse.HelpFormatter):
    """Reflow each paragraph of a description separately.

    Paragraphs are separated by blank lines. '- ' list items get a hanging
    indent; indented blocks (examples) are kept as written.
    """

    def __init__(self, prog: str, **kwargs):
        kwargs.setdefault("width", min(100, shutil.get_terminal_size().columns - 2))
        super().__init__(prog, **kwargs)

    def _fill_text(self, text: str, width: int, indent: str) -> str:
        def fill(item: str, hang: str = "") -> str:
            return textwrap.fill(
                " ".join(item.split()),
                width,
                initial_indent=indent,
                subsequent_indent=indent + hang,
                break_long_words=False,
                break_on_hyphens=False,
            )

        paragraphs = []
        for para in textwrap.dedent(text).strip().split("\n\n"):
            if para.startswith(" "):
                paragraphs.append(textwrap.indent(para, indent))
            elif para.startswith("- "):
                items = re.split(r"\n(?=- )", para)
                paragraphs.append("\n".join(fill(item, "  ") for item in items))
            else:
                paragraphs.append(fill(para))
        return "\n\n".join(paragraphs)


# Where a --load-state given after the subcommand is stored, until
# resolve_load_state folds it into load_state.
SUB_LOAD_STATE = "sub_load_state"


def add_load_state_arg(
    parser: argparse.ArgumentParser, *, after_command: bool = False
) -> None:
    """Add --load-state: the global one, or with after_command a subcommand's.

    Both exist so the option works before or after the subcommand name.
    """
    parser.add_argument(
        "--load-state",
        metavar="DIR",
        dest=SUB_LOAD_STATE if after_command else "load_state",
        help="Read cluster state from a 'save-state' capture instead of the "
        "live cluster.",
    )


def resolve_load_state(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Fold a --load-state given after the subcommand into args.load_state.

    Exits through parser.error if the two places name different directories.
    """
    sub = vars(args).pop(SUB_LOAD_STATE, None)
    if sub is None:
        return
    if args.load_state is not None and args.load_state != sub:
        parser.error(
            f"--load-state given twice, with different directories: "
            f"{args.load_state} and {sub}"
        )
    args.load_state = sub


def add_pgremapper_mappings_arg(parser: argparse.ArgumentParser):
    """Add --pgremapper-mappings."""
    parser.add_argument(
        "--pgremapper-mappings",
        action="store_true",
        help="Print JSON for 'pgremapper import-mappings' instead of the table.",
    )


def add_exclude_pgs_arg(parser: argparse.ArgumentParser):
    """Add --exclude-pgs."""
    parser.add_argument(
        "--exclude-pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="Leave these PGs alone.",
    )


def extract_pg_stats(raw, source: str) -> list[dict]:
    """Extract the pg_stat list from any of the shapes ceph releases return.

    source names the command in error messages.
    """
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        if raw.get("pg_ready") is False:
            # Not "no PGs": that would wrongly report nothing moving.
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
        # 'ceph pg ls' with no matching PGs returns just {"pg_ready": true}.
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
    """Return {osd_id: node} from 'ceph osd df'. Down OSDs may lack utilization."""
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
# Anonymization for save-state
# ---------------------------------------------------------------------------

# RFC 5737 TEST-NET-2: documentation-only, never routable.
_FAKE_IP_PREFIX = "198.51.100."
_ADDR_IP_RE = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
_TRAILING_NUM_RE = re.compile(r"(\d+)$")
_FAKE_HASH_NAME_RE = re.compile(r"host-[0-9a-f]{8}")

FAKE_FSID = "00000000-0000-0000-0000-000000000000"


def _fake_ip(real_ip: str) -> str:
    """Map a real IP to a fake one with the same last octet.

    Collisions are harmless: nothing reads these fields.
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
    """Map a hostname to 'hostNN' by its trailing number, else to a hash of it."""
    if _FAKE_HASH_NAME_RE.fullmatch(real_name):
        return real_name  # already a stand-in: keep anonymization idempotent
    m = _TRAILING_NUM_RE.search(real_name)
    if m:
        return f"host{int(m.group(1)):02d}"
    return "host-" + hashlib.sha256(real_name.encode()).hexdigest()[:8]


def _fake_hostnames(real_names: set[str]) -> dict[str, str]:
    """Map every hostname to a distinct fake one; idempotent.

    Names that would collide under _fake_hostname (ceph1-5, ceph2-5) take the
    hash form instead: merging two hosts would change the analysis.
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
    """Anonymize parsed snapshots in place.

    Replaces the fsid, OSD addresses and uuids, hostnames, and pool and rule
    names with deterministic fakes. Ids, utilizations, weights, device
    classes and topology are kept. Idempotent, and consistent across
    captures.
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
        # 'osd dump' has its own copy of pool names, keyed by 'pool'.
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
    """Format an OSD's utilization as 'NN.N%', '-' for no OSD, '?' if unknown."""
    if osd_id is None:
        return NOT_APPLICABLE
    util = osd_df.get(osd_id, {}).get("utilization")
    return f"{util:.1f}%" if util is not None else "?"


def format_progress(pct: float | None, exact: bool = True) -> str:
    """Format a progress percentage, floored so only a finished copy reads 100%.

    Counter-based figures (not exact) get a '~' prefix.
    """
    if pct is None:
        return NOT_APPLICABLE
    return f"{'' if exact else '~'}{math.floor(pct)}%"


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


def osd_columns(group: str) -> Columns:
    """Return the (group, label) columns osd_cells fills: OSD, UTIL, HOST."""
    return [(group, label) for label in ("OSD", "UTIL", "HOST")]


def osd_cells(
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    osd_id: int | None,
    primary: int | None = None,
) -> list[str]:
    """Return the [OSD, UTIL, HOST] cells for one slot.

    An empty slot reads 'none', '-', '-'; primary gets a trailing '*'.
    """
    if osd_id is None:
        return ["none", NOT_APPLICABLE, NOT_APPLICABLE]
    star = "*" if osd_id == primary else ""
    return [
        f"{osd_id}{star}",
        format_utilization(osd_df, osd_id),
        osd_host.get(osd_id, "?"),
    ]


def print_table(columns: Columns, rows: list[list[str]]) -> None:
    """Print rows under a two-line header: group names, then column labels.

    Each group name is centered in dashes across its columns; groups are
    separated by GROUP_SEP. Without any group, the header is the label line
    alone. The last column is unpadded.
    """
    # A list: max(a, *b) raises when there are no rows.
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
    if group_line.strip():  # no groups: no group line
        print(group_line.rstrip())

    def emit(cells: list[str]) -> None:
        last = len(cells) - 1
        line = "".join(
            sep + (cell.ljust(widths[i]) if i < last else cell)
            for i, (sep, cell) in enumerate(zip(seps, cells))
        )
        # rstrip: an empty final cell (e.g. a blank NOTE) leaves its separator.
        print(line.rstrip())

    emit([label for _, label in columns])
    for row in rows:
        emit(row)


# ---------------------------------------------------------------------------
# Cancelling backfills: pins, companions, chains, output
#
# Shared by cancel-backfill and cancel-uphill. Each picks which shards to pin
# back; this turns them into a valid, ordered set of upmap pairs and prints it.
# ---------------------------------------------------------------------------


def fetch_remapped_pg_stats(store: SnapshotStore) -> list[dict]:
    """Return the remapped PGs (up != acting).

    Live: 'pg_ls_remapped'. From a capture: 'pg_dump_pgs', filtered.
    """
    if store.load_dir is None:
        return fetch_pg_stats(store, "pg_ls_remapped")
    return [
        pg
        for pg in fetch_pg_stats(store, "pg_dump_pgs")
        if "remapped" in pg["state"].split("+")
    ]


# Why a pin or move is proposed: the NOTE column's gist, and the 'role' key
# of the JSON entries (upmap_entry), so a filter can keep units together.
ROLE_REQUESTED = "requested"  # what the command was asked for
ROLE_COMPANION = "companion"  # keeps a requested pin valid (close_pins)
ROLE_BLOCKER = "blocker"  # unblocks the PG's requested ones; so do its companions


class Cancellation(NamedTuple):
    """One shard pinned back from the OSD it was moving to onto its acting OSD."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # where CRUSH is sending it: the 'from' of the upmap pair
    acting_osd: int  # where it is now: the 'to'
    size_bytes: int | None  # estimated, None if unknown
    state: str
    progress_pct: float | None  # of this shard's move (see copy_progress)
    companion_of: "int | str | None" = None  # shard this one goes with: the
    # requested one it keeps valid or blocks, or with of_blocker the blocker
    # whose companion it is (see close_pins); None if requested directly
    blocker_util: float | None = None  # for a blocker (--pin-blockers): its
    # target's projected utilization
    progress_exact: bool = False  # from backfill positions, not counters
    of_blocker: bool = False  # companion_of is a blocker, not a requested shard

    @property
    def role(self) -> str:
        """Return why the shard is pinned: one of the ROLE_* constants."""
        if self.blocker_util is not None or self.of_blocker:
            return ROLE_BLOCKER
        return ROLE_REQUESTED if self.companion_of is None else ROLE_COMPANION


def with_exact_progress(
    store: SnapshotStore,
    cancellations: list[Cancellation],
    pg_stats: list[dict],
    pools: dict[int, dict],
) -> list[Cancellation]:
    """Replace counter-based progress with each shard's own (copy_progress).

    Queries backfill positions only for the proposed PGs.
    """
    pgids = {c.pgid for c in cancellations}
    pgs = {pg["pgid"]: pg for pg in pg_stats if pg["pgid"] in pgids}
    positions = fetch_backfill_positions(store, pgs)
    result = []
    for c in cancellations:
        progress = copy_progress(
            pgs[c.pgid],
            pools.get(pgid_pool_id(c.pgid)),
            positions.get(c.pgid, {}),
            target_peer(c.up_osd, c.shard),
        )
        result.append(
            c._replace(progress_pct=progress.pct, progress_exact=progress.exact)
        )
    return result


class Skipped(NamedTuple):
    """A shard that could not be pinned, and why."""

    pgid: str
    shard: "int | str"
    reason: str


class PinTotals(NamedTuple):
    """What a cancel command's summary counts (see messages.print_pin_summary)."""

    requested: int  # shards pinned for themselves: not companions or blockers
    pgs: int  # PGs with any pin
    others: int  # companions and blockers
    blockers: int
    size_bytes: int  # of the requested shards of known size
    unknown_size: int  # requested shards of unknown size

    @classmethod
    def of(cls, cancellations: list[Cancellation]) -> "PinTotals":
        requested = [c for c in cancellations if c.companion_of is None]
        known = [c.size_bytes for c in requested if c.size_bytes is not None]
        return cls(
            requested=len(requested),
            pgs=len({c.pgid for c in cancellations}),
            others=len(cancellations) - len(requested),
            blockers=sum(c.blocker_util is not None for c in cancellations),
            size_bytes=sum(known),
            unknown_size=len(requested) - len(known),
        )


def same_place(a: int, b: int, osd_host: dict[int, str]) -> bool:
    """True if two OSDs are one and the same or on one host (host known)."""
    host = osd_host.get(a)
    return a == b or (host is not None and host == osd_host.get(b))


def close_pins(
    up: list, acting: list, pins: dict[int, int], osd_host: dict[int, str]
) -> tuple[dict[int, int], str | None]:
    """Extend pins ({shard: acting_osd}) until the resulting up set is valid.

    Ceph silently drops an upmap that puts two shards of a PG on one host or
    OSD. Pinning a shard back to its acting OSD does that if another shard is
    moving onto the same host, so that shard is pinned back too (a
    "companion"), which may in turn need its own.

    A shard merely moving onto a host that holds another shard of its PG is
    fine: Ceph checks the up set, and acting switches to up only once all of
    the PG's backfills finish.

    Returns (pins, None), given pins first, or ({}, reason) if a clashing
    shard cannot be pinned (not moving, or no acting OSD).
    """
    pins = dict(pins)
    while True:
        new_up = [pins.get(i, osd) for i, osd in enumerate(up)]
        added = {}
        for i in pins:
            for j, other in enumerate(new_up):
                if j == i or j in pins or not is_real_osd(other):
                    continue
                if not same_place(new_up[i], other, osd_host):
                    continue
                companion = slot(acting, j)
                if companion is None or companion == up[j]:
                    why = "has no acting OSD" if companion is None else "is not moving"
                    return {}, (
                        f"acting osd.{new_up[i]} shares a host with shard {j} "
                        f"(osd.{other}), which {why} so cannot be pinned too"
                    )
                added[j] = companion
        if not added:
            return pins, None
        pins.update(added)


def pin_replica(
    up: list, osd: int, acting_osd: int, osd_host: dict[int, str]
) -> str | None:
    """Return why replacing osd with acting_osd in up is invalid, or None.

    Only for a PG with a single replica moving (the caller's check), so a
    clash cannot be resolved with a companion.
    """
    for other in up:
        if (
            other != osd
            and is_real_osd(other)
            and same_place(acting_osd, other, osd_host)
        ):
            return (
                f"acting osd.{acting_osd} shares a host with replica "
                f"osd.{other}, which is not moving"
            )
    return None


def order_moves(
    moves: list[tuple[int, int, int]],
) -> tuple[list[tuple[int, int, int]], str | None]:
    """Order (shard, from_osd, to_osd) moves so Ceph applies all of them.

    Ceph applies an entry's pairs in order and skips one whose 'to' is still
    in the mapping, so A->B must follow B->C. Otherwise shard order is kept.
    A cycle (A->B, B->A) cannot be expressed: returns ([], reason).
    """
    remaining = sorted(moves, key=lambda move: move[0])
    ordered = []
    while remaining:
        for move in remaining:
            if not any(other[1] == move[2] for other in remaining if other is not move):
                break
        else:
            ring = format_pairs((f, t) for _, f, t in remaining)
            return [], f"the pins form a cycle ({ring}), which upmaps cannot express"
        ordered.append(move)
        remaining.remove(move)
    return ordered, None


def skipped_sort_key(item: Skipped) -> tuple:
    """Order Skipped by PG, then shard ('-' first)."""
    return (*pgid_sort_key(item.pgid), item.shard if item.shard != "-" else -1)


Pair = tuple[int, int]  # an upmap pair: (from_osd, to_osd)


class ChainResolution(NamedTuple):
    """What avoid_chains() kept, dropped and would apply in full."""

    cancellations: list[Cancellation]  # the pgremapper-safe pins, order kept
    skipped: list[Skipped]  # pins left out, and why
    chained: dict[str, list[Pair]]  # pgid: full upmap entry (see avoid_chains)


def fold_pins(
    existing: list[Pair], pins: list[Cancellation]
) -> tuple[list[Pair], list[Pair]]:
    """Return (untouched existing pairs, each pin's effective pair).

    Like pgremapper, a pin X->Y where an existing pair A->X put X in 'up'
    rewrites that pair to A->Y: the same up set, and no chain.
    """
    rest = list(existing)
    effective = []
    for c in pins:
        folded = next((p for p in rest if p[1] == c.up_osd), None)
        if folded is None:
            effective.append((c.up_osd, c.acting_osd))
        else:
            rest.remove(folded)
            effective.append((folded[0], c.acting_osd))
    return rest, effective


def chain_link(pairs: list[Pair]) -> tuple[Pair, Pair] | None:
    """Return some (A->B, B->C) in pairs, or None. A->A pairs are no-ops."""
    real = [p for p in pairs if p[0] != p[1]]
    by_from = {p[0]: p for p in real}
    for p in real:
        if p[1] in by_from:
            return p, by_from[p[1]]
    return None


def chain_heads(
    existing: list[Pair], pins: list[Cancellation]
) -> list[tuple[Cancellation, Pair]]:
    """Return (pin, the pair it chains into) for pins whose 'to' is another pair's 'from'.

    Pairs are compared after folding (fold_pins).
    """
    rest, effective = fold_pins(existing, pins)
    by_from = {f: (f, t) for f, t in rest + effective if f != t}
    return [
        (pin, by_from[t])
        for pin, (f, t) in zip(pins, effective)
        if f != t and t in by_from
    ]


def format_pairs(pairs: Iterable[Pair]) -> str:
    """Format upmap pairs the way every command prints them: '890->414, 414->341'."""
    return ", ".join(f"{f}->{t}" for f, t in pairs)


def format_link(first: Pair, second: Pair) -> str:
    """Format two chained pairs (format_pairs)."""
    return format_pairs([first, second])


def host_clash(
    up: list, pins: list[Cancellation], osd_host: dict[int, str]
) -> str | None:
    """Return why applying pins to up puts two shards on one host, or None."""
    new_up, pinned = list(up), {}
    for c in pins:
        i = c.shard if isinstance(c.shard, int) else new_up.index(c.up_osd)
        new_up[i] = c.acting_osd
        pinned[i] = c.acting_osd
    for i, osd in pinned.items():
        for j, other in enumerate(new_up):
            if j != i and is_real_osd(other) and same_place(osd, other, osd_host):
                return f"acting osd.{osd} would share a host with osd.{other}"
    return None


def avoid_chains(
    cancellations: list[Cancellation],
    upmap_items: dict[str, list[dict]],
    pg_stats: list[dict],
    osd_host: dict[int, str],
    partial: bool,
) -> ChainResolution:
    """Drop the pins that pgremapper cannot apply because they chain.

    Dry runs of pgremapper 1.0.0 import-mappings: a PG's pairs, existing and
    new, must not chain (A->B, B->C), except for a pin folded into an
    existing pair (fold_pins). Otherwise it panics, aborting the whole
    import, or folds the chain into a wrong pair. Changing a PG whose
    existing pairs chain, it removes one link as stale.

    A pin whose 'to' is another pair's 'from' is dropped, leaving its backfill
    running; the chain's last pin, valid on its own, stays. The whole PG is
    left out instead if its existing pairs chain, a dropped pin is a
    requested one (companion_of None) and not partial, or the remaining pins
    clash on a host (the dropped pin was a companion).

    chained holds, per affected PG, the upmap entry that applies every pin:
    existing pairs (the up set's source; stale ones left out), then the pins
    in apply order.
    """
    ups = {pg["pgid"]: pg["up"] for pg in pg_stats}
    by_pg: dict[str, list[Cancellation]] = {}
    for c in cancellations:
        by_pg.setdefault(c.pgid, []).append(c)

    kept, skipped, chained = [], [], {}
    for pgid, cs in by_pg.items():
        existing = [(m["from"], m["to"]) for m in upmap_items.get(pgid, [])]
        pins, dropped, why = cs, [], None
        if link := chain_link(existing):
            why = (
                f"its existing upmap pairs chain ({format_link(*link)}), "
                "which pgremapper would break"
            )
        else:
            # A pair whose 'from' is still in 'up' is stale: Ceph skips it,
            # and pgremapper removes it before adding pins.
            existing = [p for p in existing if p[0] not in ups[pgid]]
        # One pass drops every head; the loop only guards against folds
        # changing as pins go.
        while why is None and (heads := chain_heads(existing, pins)):
            for pin, successor in heads:
                reason = (
                    f"{format_link((pin.up_osd, pin.acting_osd), successor)} "
                    "would chain, which pgremapper cannot apply"
                )
                if pin.companion_of is None and not partial:
                    why = reason
                    break
                dropped.append((pin, reason))
            pins = [c for c in pins if c not in {pin for pin, _ in heads}]
        if why is None and dropped and (clash := host_clash(ups[pgid], pins, osd_host)):
            why = f"leaving a chained pin out: {clash}"
        if not dropped and why is None:
            kept.extend(cs)
            continue

        chained[pgid] = existing + [(c.up_osd, c.acting_osd) for c in cs]
        if why is not None:
            skipped.extend(
                Skipped(pgid, c.shard, why) for c in cs if c.companion_of is None
            )
            continue
        kept.extend(pins)
        for pin, reason in dropped:
            prefix = "blocker: " if pin.blocker_util is not None else ""
            skipped.append(Skipped(pgid, pin.shard, prefix + reason))
    return ChainResolution(kept, skipped, chained)


def format_note(c: Cancellation) -> str:
    """Return the NOTE cell: why a shard not chosen directly is pinned."""
    if c.blocker_util is not None:
        return (
            f"blocks shard {c.companion_of}: target "
            f"{blocking_reason(c.up_osd, c.blocker_util)}"
        )
    if c.companion_of is None:
        return ""
    return companion_note(c.companion_of, of_blocker=c.of_blocker)


# (group, label). ACTING is where the data is (the pair's 'to'), UP where
# CRUSH wants it (the 'from').
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    *osd_columns("ACTING"),
    *osd_columns("UP"),
    ("", "SIZE"),
    ("", "PROGRESS"),
    ("", "STATE"),
    ("", "NOTE"),
]


def format_row(
    c: Cancellation, osd_df: dict[int, dict], osd_host: dict[int, str]
) -> list[str]:
    return [
        c.pgid,
        str(c.shard),
        *osd_cells(osd_df, osd_host, c.acting_osd),
        *osd_cells(osd_df, osd_host, c.up_osd),
        format_bytes(c.size_bytes),
        format_progress(c.progress_pct, c.progress_exact),
        abbreviate_state(c.state),
        format_note(c),
    ]


def print_pgremapper_mappings(cancellations: list[Cancellation]) -> None:
    """Print the cancellations as JSON for 'pgremapper import-mappings'.

    Each entry carries its table row's SHARD, role and NOTE.
    """
    print_upmap_entries(
        upmap_entry(
            c.pgid,
            c.up_osd,
            c.acting_osd,
            shard=c.shard,
            role=c.role,
            note=format_note(c),
        )
        for c in cancellations
    )


def upmap_entry(pgid: str, from_osd: int, to_osd: int, **extra) -> dict:
    """Return one 'pgremapper import-mappings' entry, with extra keys after mapping.

    pgremapper 1.0.0 ignores keys it does not know (checked by dry run), so
    extra carries what a user filtering the file needs, e.g. role.
    """
    return {"pgid": pgid, "mapping": {"from": from_osd, "to": to_osd}, **extra}


def print_upmap_pairs(pairs: Iterable[tuple[str, int, int]]) -> None:
    """Print (pgid, from, to) pairs as JSON for 'pgremapper import-mappings'."""
    print_upmap_entries(upmap_entry(*pair) for pair in pairs)


def print_upmap_entries(entries: Iterable[dict]) -> None:
    """Print upmap_entry dicts as a JSON array, one entry per line."""
    lines = [json.dumps(entry) for entry in entries]
    if not lines:
        print("[]")
        return
    print("[\n" + ",\n".join(f"  {line}" for line in lines) + "\n]")
