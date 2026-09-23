# SPDX-License-Identifier: MIT
"""Code shared by the scripts in this directory.

Not a script itself: the others import it (`from shared import ...`), so it
has to sit next to them. Five groups of things live here:

* OSD-slot helpers and PG arithmetic (progress, copies in flight, shard size),
  including reading a backfill's position out of 'ceph pg query';
* reading cluster state, live or from a saved snapshot (`SnapshotStore`), the
  global `--load-state` flag built on it, and the anonymizer (used by the
  `save-state` subcommand) that makes a saved snapshot safe to share;
* `fetch_*` helpers that turn snapshot keys into lookup tables;
* the two-line grouped table (`print_table`) and its cell formatters;
* turning a chosen shard into a valid, orderable upmap proposal --
  `close_pins` and friends, `Cancellation`/`Skipped`, chain detection and the
  cancellation-proposal table/JSON output -- shared by `cancel-backfill` and
  `cancel-uphill`, the two subcommands that pin moving shards back.
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


class PgidFilter(NamedTuple):
    """What a list of PG ids given on the command line (e.g. --pgs) matched."""

    given: int
    matched: int
    unmatched: list[str]  # sorted; usually typos


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

    Unreliable for backfills: see PROGRESS_APPROX_NOTE. pg_progress prefers
    the backfill position (backfill_progress_pct) and falls back on this.
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


# Backfill position
# -----------------
# Backfill copies a PG's objects in hobject sort order, which starts with a
# 32-bit key: the bit-reversed object-name hash. Each backfill target's
# 'last_backfill' ('ceph pg <pgid> query' -> peer_info[]) is how far along
# that order it is, printed as 'POOL:KEY:...' (MIN before it starts, MAX
# once done). Hashes are uniform, so the share of the PG's key range behind
# the position is the share of its objects already copied. Unlike the
# misplaced counter it can't be thrown off by what the target reports about
# itself after re-peering (see PROGRESS_APPROX_NOTE).


def pg_hash_bits(seed: int, pg_num: int) -> int:
    """Return how many low bits of an object's hash are fixed for PG seed.

    Mirrors ceph_stable_mod(hash, pg_num, mask), mask = 2^n - 1 with n the
    bit length of pg_num - 1: a hash maps to (hash & mask) if that is below
    pg_num, else to (hash & mask >> 1). With pg_num a power of two every PG
    fixes all n bits. Otherwise a PG below 2^(n-1) whose sibling
    seed + 2^(n-1) does not exist also takes that sibling's hashes, so it
    fixes only n - 1 bits and covers twice the key range.
    """
    n = (pg_num - 1).bit_length()
    half = 1 << (n - 1) if n else 0
    if n and seed < half and seed + half >= pg_num:
        return n - 1
    return n


def normalize_last_backfill(last_backfill: str) -> str | None:
    """Reduce a 'last_backfill' hobject to 'MIN', 'MAX' or its hex sort key.

    The object name is dropped: the key is all backfill_fraction needs, and
    it keeps object names out of 'save-state' captures. None if unparsable.
    """
    if last_backfill in ("MIN", "MAX"):
        return last_backfill
    parts = last_backfill.split(":")
    if len(parts) < 2 or not re.fullmatch(r"[0-9A-Fa-f]{1,8}", parts[1]):
        return None
    return parts[1].lower()


def backfill_fraction(position: str, seed: int, pg_num: int) -> float | None:
    """Return the share (0..1) of PG seed's objects before a backfill position.

    position is normalize_last_backfill's output. None when the key does not
    belong to the PG (a pg_num change since, or a malformed value), so a
    caller never shows a figure computed against the wrong range.
    """
    if position == "MIN":
        return 0.0
    if position == "MAX":
        return 1.0
    key = int(position, 16)
    bits = pg_hash_bits(seed, pg_num)
    # The key is the hash bit-reversed, so its top `bits` bits are the
    # hash's fixed low bits, reversed; the rest is the position in the PG.
    hash_low = int(f"{key >> (32 - bits):0{bits}b}"[::-1], 2) if bits else 0
    if hash_low != seed & ((1 << bits) - 1):
        return None
    span = 1 << (32 - bits)
    return (key & (span - 1)) / span


def backfill_target_peers(up: list, acting: list, is_ec: bool) -> list[str]:
    """Return the backfill targets of a PG as 'ceph pg query' names its peers.

    'OSD(SHARD)' for each EC shard moving to a new OSD, plain 'OSD' for each
    OSD a replicated PG is moving to.
    """
    if is_ec:
        return [f"{dst}({i})" for i, _, dst in ec_shard_moves(up, acting)]
    return [str(o) for o in sorted(real_osd_set(up) - real_osd_set(acting))]


def extract_backfill_positions(query: dict) -> dict[str, str]:
    """Return {peer: position} for the backfill targets in a 'ceph pg query'.

    Peers are named as in backfill_target_peers, positions normalized (see
    normalize_last_backfill). Only the query's own backfill targets are kept:
    peer_info also lists stray OSDs from earlier mappings.
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
    """Name a backfill target as 'ceph pg query' names the peer.

    'OSD(SHARD)' for an EC shard (shard an int), plain 'OSD' for a replica
    (shard '-', as rows of replicated pools have it).
    """
    return f"{osd}({shard})" if isinstance(shard, int) else str(osd)


def target_progress_pct(
    pgid: str, pool: dict | None, position: str | None
) -> float | None:
    """Return % of a PG's objects one backfill target has already been sent.

    None if the position is missing or doesn't fit the PG (see
    backfill_fraction), or the pool or its pg_num is unknown.
    """
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
    """Return the PG's progress by Ceph's misplaced/degraded counters.

    A per-PG figure (all moving copies together): the counters have no
    per-shard breakdown.
    """
    n_copies = copies_moving(
        pg["up"], pg["acting"], is_erasure(pool), pool.get("size", 0) if pool else 0
    )
    return Progress(pg_progress_pct(pg, n_copies), False)


def copy_progress(
    pg: dict, pool: dict | None, positions: dict[str, str], peer: str
) -> Progress:
    """Return how far one moving copy (EC shard or replica) of a PG is.

    peer is the copy's backfill target (see target_peer), positions the PG's
    {peer: position} (see extract_backfill_positions), empty if unknown.
    Without a usable position for peer, falls back on the PG's counters
    (counter_progress), which are per PG, not per copy.
    """
    pct = target_progress_pct(pg["pgid"], pool, positions.get(peer))
    return counter_progress(pg, pool) if pct is None else Progress(pct, True)


def pg_progress(pg: dict, pool: dict | None, positions: dict[str, str]) -> Progress:
    """Return how far a PG's movement is as a whole, from its backfill positions if possible.

    The targets' progress (target_progress_pct) averaged, each counting equally:
    the same copy units as pg_progress_pct's. Used when every moving copy is a
    backfill target with a usable position. Otherwise, e.g. with an EC shard
    that has no OSD to go to yet, a failed query or an old --load-state
    capture, falls back on the counters (counter_progress).
    """
    is_ec = is_erasure(pool)
    up, acting = pg["up"], pg["acting"]
    n_copies = copies_moving(up, acting, is_ec, pool.get("size", 0) if pool else 0)
    targets = backfill_target_peers(up, acting, is_ec)
    if targets and len(targets) == n_copies:
        pcts = [
            target_progress_pct(pg["pgid"], pool, positions.get(t)) for t in targets
        ]
        if None not in pcts:
            return Progress(sum(pcts) / len(pcts), True)
    return counter_progress(pg, pool)


# Printed once by a subcommand when any PROGRESS figure comes from Ceph's
# counters (a Progress with exact False, shown with a '~' by format_progress).
# Kept in one place so the wording can't drift between subcommands. No
# leading newline/hard-wrapping: each caller adds its own paragraph spacing
# and either prints this as-is (fixed-width footnote style) or hands it to a
# wrapper that reflows it (textwrap.fill treats the embedded newlines as
# plain whitespace).
PROGRESS_APPROX_NOTE = (
    "~ marks PROGRESS from Ceph's misplaced/degraded object counters, used "
    "where a PG's\nbackfill positions could not be read ('ceph pg query' "
    "failed, or a --load-state\ncapture without backfill_positions.json) or "
    "do not cover all of its moving\ncopies (e.g. an EC shard with no OSD to "
    "go to yet). Those counters miss most of\nthe work left on a backfill "
    "that resumed after re-peering, so a ~ figure can\nread far too high, "
    "even ~100% for a backfill that is only a third done. It\nis per PG: "
    "every moving shard of the PG shows the same ~ figure."
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


# Where 'save-state' writes, and --load-state reads, {pgid: {peer: position}}
# (see extract_backfill_positions) for every remapped PG.
BACKFILL_POSITIONS_FILE = "backfill_positions.json"

# Per-PG 'pg query' timeout (seconds): an unresponsive primary must not hang
# the run; that PG just falls back on the counters.
PG_QUERY_TIMEOUT = 30

# Concurrent 'pg query' requests over one librados connection. Each is a
# round trip to the PG's primary, so this is about latency, not CPU.
RADOS_QUERY_THREADS = 16


def _positions_from_output(output: str | bytes) -> dict[str, str] | None:
    """Parse one 'ceph pg query' output into positions; None if it isn't one."""
    try:
        query = json.loads(output)
    except ValueError:  # JSONDecodeError, or undecodable bytes
        return None
    return extract_backfill_positions(query) if isinstance(query, dict) else None


def _query_positions_rados(rados, pgids: list[str]) -> dict[str, dict[str, str] | None]:
    """Query PGs over one librados connection, a few at a time.

    rados is the 'rados' module. Returns {pgid: positions, or None if that
    PG's query failed}. Raises rados.Error if the cluster can't be reached
    this way, for the caller to fall back on the CLI.
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
    """Query PGs with one 'ceph pg <pgid> query' process each, run in parallel.

    Slower and far more CPU-hungry than librados (each process pays the
    ceph CLI's startup), but needs nothing beyond the ceph binary.
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

    Uses librados (the 'rados' Python module) when available, else parallel
    ceph CLI calls. A PG whose query fails is left out, so its progress falls
    back on the counters; how many failed is noted on stderr.
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

    With --load-state, read from BACKFILL_POSITIONS_FILE; a capture made
    before 'save-state' wrote it yields {} (every PG on the counters).
    """
    if store.load_dir is None:
        return query_backfill_positions(pgids)
    try:
        saved = json.loads((Path(store.load_dir) / BACKFILL_POSITIONS_FILE).read_text())
    except FileNotFoundError:
        return {}
    return {pgid: saved[pgid] for pgid in pgids if pgid in saved}


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


def parse_osd(text: str) -> int:
    """argparse type: an OSD id, given as '682' or 'osd.682'."""
    match = re.fullmatch(r"(?:osd\.)?(\d+)", text)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"expected an OSD id like 682 or osd.682, got {text!r}"
        )
    return int(match[1])


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


def format_progress(pct: float | None, exact: bool = True) -> str:
    """Format a progress percentage, floored so only finished copying reads '100%'.

    A figure from Ceph's counters (exact False, see Progress) gets a '~'
    prefix, explained by PROGRESS_APPROX_NOTE.
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


def osd_cells(
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    osd_id: int | None,
    primary: int | None = None,
) -> list[str]:
    """Return the [OSD, UTIL, HOST] cells for one slot.

    The OSD reads as its bare number ('N', not 'osd.N'). An empty slot (osd_id
    None) reads 'none', '-', '-'. The OSD that is the PG's `primary` gets a
    trailing '*'.
    """
    if osd_id is None:
        return ["none", NOT_APPLICABLE, NOT_APPLICABLE]
    star = "*" if osd_id == primary else ""
    return [
        f"{osd_id}{star}",
        format_utilization(osd_df, osd_id),
        osd_host.get(osd_id, "?"),
    ]


def wrap_text(text: str, indent: str = "") -> str:
    """Wrap a stderr paragraph or list item to a readable width.

    Capped at 100 columns (and no narrower than 40) so a long NOTE/WARNING/
    ERROR stays readable on a wide terminal instead of stretching edge to
    edge; 'indent' (e.g. "  " for a list item under a paragraph) is repeated
    on wrapped lines plus two more spaces, so the continuation hangs under
    the item's own text rather than the margin.
    """
    width = min(100, max(40, shutil.get_terminal_size().columns))
    return textwrap.fill(
        text,
        width=width,
        initial_indent=indent,
        subsequent_indent=indent + "  ",
        break_long_words=False,
        break_on_hyphens=False,
    )


def stderr_para(text: str) -> None:
    """Print a wrapped stderr paragraph, blank-line-separated from the last one.

    Without the blank line, a run's several NOTE/WARNING messages read as one
    undifferentiated block once each has wrapped across multiple terminal
    lines; this makes each message its own visually distinct paragraph.
    """
    if stderr_para.printed:
        print(file=sys.stderr)
    print(wrap_text(text), file=sys.stderr)
    stderr_para.printed = True


stderr_para.printed = False


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
# Shared by cancel-backfill and cancel-uphill, both of which propose upmaps
# that pin a moving shard back to its acting OSD. Picking *which* shard to
# pin is each subcommand's own job (by target OSD for cancel-backfill, by
# utilization direction for cancel-uphill); everything here is about turning
# one or more chosen shards into a valid, orderable set of upmap pairs and
# printing the result the same way in both.
# ---------------------------------------------------------------------------


def fetch_remapped_pg_stats(store: SnapshotStore) -> list[dict]:
    """Return the PGs with up != acting ('remapped' state flag), live or from a snapshot.

    Live reads 'pg_ls_remapped' (a small fraction of the full PG population on
    a big cluster); --load-state instead filters the 'pg_dump_pgs' snapshot
    client-side by the same state flag. Both keys must be present in the
    caller's SNAPSHOT_COMMANDS.
    """
    if store.load_dir is None:
        return fetch_pg_stats(store, "pg_ls_remapped")
    return [
        pg
        for pg in fetch_pg_stats(store, "pg_dump_pgs")
        if "remapped" in pg["state"].split("+")
    ]


class Cancellation(NamedTuple):
    """One shard pinned back from the OSD it was moving to onto its acting OSD."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # where CRUSH is sending it: the 'from' of the upmap pair
    acting_osd: int  # where it is now: the 'to'
    size_bytes: int | None  # estimated, None if unknown
    state: str
    progress_pct: float | None  # of this shard's move (see copy_progress)
    companion_of: "int | str | None" = None  # the requested shard this one is
    # pinned along with (see close_pins), None if requested directly
    blocker_util: float | None = None  # cancel-backfill's --pin-blockers only:
    # set when this shard's target OSD would reach backfillfull_ratio (percent
    # it would be at), i.e. it is a blocker. Always None for cancel-uphill.
    progress_exact: bool = False  # progress_pct is from backfill positions,
    # not Ceph's counters (see with_exact_progress)


def with_exact_progress(
    store: SnapshotStore,
    cancellations: list[Cancellation],
    pg_stats: list[dict],
    pools: dict[int, dict],
) -> list[Cancellation]:
    """Return cancellations with each shard's own progress where possible.

    Planning fills progress_pct from Ceph's per-PG counters (pg_progress_pct),
    which needs no queries; this then queries the backfill positions of only
    the PGs actually proposed and replaces each shard's figure with its own
    (copy_progress), keeping the counters where a position is missing.
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


def same_place(a: int, b: int, osd_host: dict[int, str]) -> bool:
    """True if two OSDs are one and the same or on one host (host known)."""
    host = osd_host.get(a)
    return a == b or (host is not None and host == osd_host.get(b))


def close_pins(
    up: list, acting: list, pins: dict[int, int], osd_host: dict[int, str]
) -> tuple[dict[int, int], str | None]:
    """Extend pins ({shard: acting_osd}) until the resulting mapping is valid.

    Pinning a shard back puts its acting OSD into the up set, and Ceph drops an
    upmap whose result puts two shards on one host (the pool's failure domain)
    or the same OSD twice. That happens whenever another shard of the PG is
    moving too and its destination shares a host with the pinned shard's acting
    OSD: CRUSH re-placed the two together. The way out is to pin that other
    shard back as well ("companion"), which can in turn clash with a third, and
    so on until the mapping is valid.

    Returns (pins, None) with the given pins first, or ({}, reason) when a
    clashing shard cannot be pinned back (it is not moving, or has no acting
    OSD), so that nothing partial is proposed.
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


def pin_with_companions(
    up: list, acting: list, slot: int, osd_host: dict[int, str]
) -> tuple[dict[int, int], str | None]:
    """Pin EC shard 'slot' to its acting OSD, plus whatever that requires.

    Returns ({shard: acting_osd}, None) with the requested shard first, or
    ({}, reason); see close_pins.
    """
    return close_pins(up, acting, {slot: acting[slot]}, osd_host)


def pin_replica(
    up: list, osd: int, acting_osd: int, osd_host: dict[int, str]
) -> str | None:
    """Return why a replicated PG's replica cannot be pinned, or None if it can.

    The replica swaps osd for acting_osd; the same-host clash with the other
    replicas is checked as for EC. Replicas have no identity, so a clashing
    replica cannot be pinned too: a PG with a second replica moving is
    already ambiguous to pair and is the caller's job to refuse before
    calling this, so it is never seen here.
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

    Ceph applies the pairs of a pg_upmap_items entry in order and skips a pair
    whose 'to' OSD is still in the mapping. So a pair may only come after the
    pair that moves its 'to' OSD away (the one whose 'from' it is). Shard order
    is kept wherever nothing depends on anything else. Pairs that depend on each
    other in a ring (osd.A -> B and B -> A) cannot be expressed as upmaps at
    all: returns ([], reason) for those.
    """
    remaining = sorted(moves, key=lambda move: move[0])
    ordered = []
    while remaining:
        for move in remaining:
            if not any(other[1] == move[2] for other in remaining if other is not move):
                break
        else:
            ring = ", ".join(f"osd.{f}->osd.{t}" for _, f, t in remaining)
            return [], f"the pins form a cycle ({ring}), which upmaps cannot express"
        ordered.append(move)
        remaining.remove(move)
    return ordered, None


def chained_pgs(cancellations: list[Cancellation]) -> dict[str, list[Cancellation]]:
    """Return {pgid: its cancellations}, in PG order, for PGs whose pairs chain.

    Pairs chain when one's target OSD is another's source (osd.A -> B and
    B -> C), which order_moves puts in the order Ceph needs. pgremapper cannot
    apply those, see warn_chained_pgs.
    """
    by_pg: dict[str, list[Cancellation]] = {}
    for c in cancellations:
        by_pg.setdefault(c.pgid, []).append(c)
    return {
        pgid: cs
        for pgid, cs in by_pg.items()
        if any(
            c.acting_osd == other.up_osd for c in cs for other in cs if other is not c
        )
    }


def warn_chained_pgs(chained: dict[str, list[Cancellation]], left_out: bool) -> None:
    """Warn on stderr about PGs with chained pairs, with how to apply them.

    Ceph applies the pairs of an entry in order and skips a pair whose target is
    still in the mapping, so a chain has to go in the order order_moves gives.
    Dry runs of pgremapper (1.0.0) on a real chain showed it cannot: in that
    order 'import-mappings' aborts with a panic ("conflicting mapping"), which
    would take a whole batch down with it, and in the reverse order it silently
    folds the chain into one different pair. So in the machine formats these
    PGs are left out (left_out) and the pairs are given here as commands
    instead.
    """
    stderr_para(
        f"WARNING: {len(chained)} PG(s) have chained pairs (one pair's target is "
        f"another's source, e.g. osd.A->B and osd.B->C): {', '.join(chained)}. "
        "Ceph applies an entry's pairs in order and skips one whose target is "
        "still in the mapping, so they must be given in the order below. "
        "pgremapper cannot apply them: import-mappings aborts with a panic on "
        "this order, and in the other order rewrites the chain into a different "
        "mapping (seen in dry runs)."
        + (" They are therefore left out of this output." if left_out else "")
        + " Apply each with 'ceph osd pg-upmap-items', which replaces the PG's "
        "whole upmap entry, so add the PG's existing pairs from 'ceph osd dump' "
        "first (this has not been tried on your cluster):"
    )
    for pgid, cs in chained.items():
        pairs = " ".join(f"{c.up_osd} {c.acting_osd}" for c in cs)
        # Not wrapped: these are meant to be copy-pasted as shell commands.
        print(f"  ceph osd pg-upmap-items {pgid} {pairs}", file=sys.stderr)


def format_bytes(num: int | None) -> str:
    """Format a byte count in binary units, or '?' if unknown."""
    if num is None:
        return "?"
    value = float(num)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def format_note(c: Cancellation) -> str:
    """Say why a shard is in the proposal, if not because it was chosen directly."""
    if c.blocker_util is not None:
        return (
            f"blocks shard {c.companion_of}: target osd.{c.up_osd} "
            f"would be at {c.blocker_util:.1f}%, over backfillfull"
        )
    if c.companion_of is not None:
        return f"companion of shard {c.companion_of}"
    return ""


# Each entry is (group, label); the header is printed on two lines, the group
# name spanning its columns above their labels, and an empty group means the
# column has no group line. The ACTING group is where the shard's data is now
# (the 'to' of the upmap pair), UP where CRUSH wants it (the 'from'). OSDs are
# bare ids, as 'pgremapper' takes them. print_table leaves the final column
# unpadded.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "HOST"),
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

    One {pgid, mapping: {from, to}} entry per pair, in a JSON array with one
    entry per line, so it is easy to read and to prune with jq ("[]" when there
    are none, so the output is always valid JSON). import-mappings
    reads the cluster's upmaps once and applies all pairs of a PG together, so
    unlike separate 'pgremapper remap' runs the pairs of a PG cannot overwrite
    each other.
    """
    if not cancellations:
        print("[]")
        return
    print("[")
    for i, c in enumerate(cancellations):
        entry = {"pgid": c.pgid, "mapping": {"from": c.up_osd, "to": c.acting_osd}}
        print(f"  {json.dumps(entry)}{',' if i < len(cancellations) - 1 else ''}")
    print("]")
