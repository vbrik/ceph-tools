#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Propose upmaps that cancel every backfill into a given OSD.

Why
---
Ceph refuses to start a backfill when the target OSD's *projected* usage would
exceed backfillfull_ratio, and every other backfill queued or running into that
OSD counts towards the projection. On a nearly full cluster, stopping the
backfills into one OSD therefore frees the room for the ones you care about
(e.g. moving PGs off the fullest OSD onto it).

How
---
A backfill into OSD X is stopped by pinning the shard to where its data is
now: an upmap pair '<X> -> <acting OSD>' makes 'up' equal 'acting' for that
shard, so nothing moves. That is 'pgremapper remap <pgid> <X> <acting osd>'.

This script lists those remaps for every PG shard that is arriving on the
OSD, and whatever else must be pinned for them to work: the output is the
complete set of pins that stops ALL backfills into the OSD, which includes pins
that stop backfills into OTHER OSDs (see "Companion pins" and "Blockers"). It
changes nothing. It does not decide which backfills to keep: review the output
and delete the entries for the ones you want to let proceed.

Cancelling a backfill that is already running throws away its progress; the
PROGRESS and STATE columns are there to help judge that. The script cannot
tell whether the backfills you keep will then fit under backfillfull_ratio;
it only reports the OSD's utilization and how much data is arriving.

(pgremapper's own 'cancel-backfill --include-osds N --target' does the same
job at OSD/pool granularity; this script exists to review and pick per shard.)

'up'/'acting' are diffed differently per pool type, as in pg-movements.py:
EC shards are identified by position, so index i is diffed against index i.
Replicated replicas are interchangeable, so the sets are diffed and SHARD is
'-'; a replica can only be paired with the acting OSD it replaces when
exactly one replica is arriving and one is leaving.

Companion pins
--------------
A pin is only accepted if the resulting mapping is valid, and Ceph drops an
upmap that puts two shards of a PG on one host (the pool's failure domain,
which must be 'host') or one OSD twice. That happens when another shard of the
PG is moving too and its destination shares a host with the acting OSD being
pinned back to: CRUSH re-placed the two together, so pinning only one leaves
the clash. The other shard is then pinned back as well, listed as a "companion
of shard N" (which can chain, if its acting OSD clashes with a third shard).
Companions are backfills into *other* OSDs that you did not ask about: check
the NOTE column, and note that dropping a companion entry invalidates the pin
it goes with. (A companion whose target is also over backfillfull_ratio is
shown as a blocker, which is the more useful description.)

Blockers
--------
backfill_toofull is a property of the PG, not of a shard: while any backfill
target of a PG refuses its reservation, the whole PG waits, including a shard
heading for a nearly empty OSD. So stopping the backfills into an OSD is not
always enough to let the ones you keep run. For every PG with a pinned shard
the tool also pins back each other shard that is moving to an OSD whose
utilization, once the shard lands, would reach the cluster's backfillfull_ratio
(from 'ceph osd dump'), listed as "blocks shard N" in the NOTE column with that
projected utilization. The projection adds only this shard, to what
'ceph osd df' reports, so it is a lower bound of what Ceph will see.

To keep a backfill that these entries would cancel, delete its own entry, but
keep the entries of its blockers, since those are what let it start. A blocker
is only cancelled if it can be pinned validly: one that cannot is listed on
stderr and the requested pin is kept regardless. Without a backfillfull_ratio
(an older --load-state capture) no blockers are looked for, and a note says so.

Shards that cannot be pinned are listed on stderr, never dropped silently:
  - the acting slot is empty (degraded): there is no OSD to pin the shard to;
  - a replicated PG has several replicas moving and the pairing is ambiguous;
  - a same-host clash cannot be resolved because the clashing shard is not
    moving or has no acting OSD, so it cannot be pinned back too.

Testing against saved cluster state
-----------------------------------
By default every run calls the live 'ceph' CLI (see SNAPSHOT_COMMANDS for the
six commands and their JSON output). --save-state DIR also writes that JSON,
one '<key>.json' file per command, into DIR (empty or not yet existing) as a
side effect of an otherwise normal run. The copy is anonymized (see
anonymize_snapshots), so it can be shared or committed. --load-state DIR reads
such a directory back instead of calling 'ceph', so a captured state can be
replayed offline with no cluster access. The two options are mutually
exclusive. tests/pg-osd/test-data/stop-backfills-into-osd-*/ hold captures for use
as --load-state arguments, each with a README.txt describing the scenario.

Applying the output
-------------------
--import-mappings prints a JSON array for 'pgremapper import-mappings', one
{pgid, mapping: {from, to}} entry per line (all other output goes to stderr):

    stop-backfills-into-osd.py --import-mappings 682 > mappings.json
    # drop the entry into the OSD for each backfill you want to keep, but not
    # its blockers (other entries of the same PG), e.g. keep 19.92e's 896->231:
    jq 'map(select(.pgid != "19.92e" or .mapping.from != 896))' mappings.json \\
        > pruned.json
    pgremapper import-mappings pruned.json

Give it the file path, not stdin, or its confirmation prompt reads EOF.
import-mappings takes all the pairs in one run. Dry runs (pgremapper 1.0.0)
showed that it plans one combined change per PG, and that it keeps a PG's
existing pairs and adds the new ones to them, which is why pgremapper is used
rather than 'ceph osd pg-upmap-items' (that replaces the whole entry). What it
sends to the mons when actually applying was not observed.

--pgremapper prints bare '<pgid> <up osd> <acting osd>' lines instead, for
'pgremapper remap', but a PG that needs several lines (a companion pin always
does) is not safe to apply that way: run as separate commands, a later run can
overwrite the pair an earlier one just added, and a lone pair can be invalid (two
shards on one host) and is then silently dropped by the mons. Both were seen
on a live cluster, so the option warns whenever it prints such a PG.

Chained pairs
-------------
A few PGs (13 of 688 on the cluster the tests were captured from) have an OSD
that CRUSH wants in one shard slot while it holds another shard of the PG now,
so the pins chain: osd.A -> B and B -> C. Ceph applies the pairs of an upmap
entry in order and skips a pair whose target is still in the mapping, so the
pair that moves B away (B -> C) has to come first, and a ring (A -> B, B -> A)
cannot be expressed at all. The pairs of a PG are printed in a valid order and a
ring is reported on stderr as unpinnable.

pgremapper cannot apply a chain, in either order: import-mappings aborts with a
panic ("conflicting mapping ... found when trying to map") on the valid order,
which would take a whole batch with it, and in the reverse order it silently
turns the chain into one different pair. So --import-mappings and --pgremapper
leave such PGs out and print 'ceph osd pg-upmap-items <pgid> <pairs>' commands
for them on stderr (not tried on a live cluster; that command replaces the PG's
whole entry). The table shows them.

Consider 'ceph balancer off' while the cancelled PGs are pinned: the upmap
balancer may otherwise undo them.
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
from itertools import groupby
from pathlib import Path
from typing import NamedTuple

# Sentinel used by CRUSH/Ceph for "no OSD in this slot" (crush/crush.h).
CRUSH_ITEM_NONE = 0x7FFFFFFF

POOL_TYPE_ERASURE = 3

# 'ceph osd df' reports sizes in KiB.
KIB = 1024

# Maps each snapshot to the 'ceph ... --format json' command that produces it
# and the '<key>.json' filename it is saved/loaded as under --save-state/
# --load-state.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    # Exactly the PGs whose up != acting, filtered by the mons: a small fraction
    # of what 'ceph pg dump pgs' would return on a big cluster.
    "pg_ls_remapped": ["ceph", "pg", "ls", "remapped", "--format", "json"],
}

# Set from args at the top of main(): None means "call the live ceph CLI as
# normal"; a Path means "read '<key>.json' from this directory instead of
# running SNAPSHOT_COMMANDS[key]" (see --load-state).
LOAD_STATE_DIR: Path | None = None

# Set from args at the top of main(): None means "don't save"; a Path means
# "once all six snapshots are collected, write an anonymized copy of each to
# '<dir>/<key>.json'" (see --save-state and anonymize_snapshots).
SAVE_STATE_DIR: Path | None = None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_osd(text: str) -> int:
    """argparse type: an OSD id, given as '682' or 'osd.682'."""
    match = re.fullmatch(r"(?:osd\.)?(\d+)", text)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"expected an OSD id like 682 or osd.682, got {text!r}"
        )
    return int(match[1])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Propose the upmaps needed to stop ALL backfills into an "
        "OSD. Each shard arriving on it is pinned to the OSD it is on now. "
        "That can also mean pinning back shards heading for OTHER OSDs, so "
        "the output lists every pin required, not only those into the given "
        "OSD: a companion is a shard of the same PG that would otherwise share "
        "a host with a pinned shard (Ceph drops such upmaps), and a blocker is "
        "a shard whose target OSD would reach backfillfull_ratio and so holds "
        "the whole PG in backfill_toofull. The NOTE column says which is "
        "which. Prints the proposals only; nothing is changed. Deciding what "
        "to keep is up to you: delete the entries for the backfills you want "
        "to let proceed, but keep the blockers of any shard you keep and drop "
        "companions together with the entry they belong to.",
        epilog="See the docstring at the top of this script for companions, "
        "blockers, what cannot be pinned and how to apply the output.",
    )
    parser.add_argument("osd", type=parse_osd, help="OSD id, e.g. 682 or osd.682")
    fmt = parser.add_mutually_exclusive_group()
    fmt.add_argument(
        "--import-mappings",
        action="store_true",
        help="Print a JSON array for 'pgremapper import-mappings' instead of "
        "the table, one {pgid, mapping} entry per line. This is the reliable "
        "way to apply the proposals: all pairs of a PG go in together.",
    )
    fmt.add_argument(
        "--pgremapper",
        action="store_true",
        help="Print '<pgid> <up osd> <acting osd>' lines with no header "
        "instead of the table, for 'pgremapper remap', one run per line. "
        "Warns if a PG needs several lines, since separate runs can "
        "overwrite each other; prefer --import-mappings.",
    )
    state_group = parser.add_mutually_exclusive_group()
    state_group.add_argument(
        "--load-state",
        metavar="DIR",
        help="Analyze a saved cluster state instead of a live cluster. DIR "
        "must contain the six '<key>.json' files listed in SNAPSHOT_COMMANDS "
        "(what --save-state produces, and the layout of the fixtures under "
        "tests/pg-osd/test-data/). No 'ceph' commands are run.",
    )
    state_group.add_argument(
        "--save-state",
        metavar="DIR",
        help="Also save the live cluster state this run collects into DIR, as "
        "the six '<key>.json' files --load-state reads back (created if "
        "missing; must be empty or not exist). The normal analysis and output "
        "proceed as usual. The saved copy is anonymized (hostnames, pool and "
        "CRUSH rule names replaced; 'osd dump' reduced to what is used), so it "
        "is safe to share.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


# Populated lazily by _ceph_json, keyed by SNAPSHOT_COMMANDS key. main() fetches
# every key before saving, so the cache then holds the complete set, which is
# what write_anonymized_state relies on.
_SNAPSHOT_CACHE: dict[str, object] = {}


def _ceph_json(key: str) -> object:
    """Return parsed JSON for one of SNAPSHOT_COMMANDS's keys.

    Read from '<LOAD_STATE_DIR>/<key>.json' if --load-state was given,
    otherwise run the live ceph command. Cached after the first call.
    """
    if key in _SNAPSHOT_CACHE:
        return _SNAPSHOT_CACHE[key]

    if LOAD_STATE_DIR is not None:
        path = LOAD_STATE_DIR / f"{key}.json"
        try:
            text = path.read_text()
        except FileNotFoundError:
            cmd = " ".join(SNAPSHOT_COMMANDS[key])
            sys.exit(
                f"ERROR: --load-state directory is missing {path} (the "
                f"output of '{cmd}')."
            )
    else:
        try:
            proc = subprocess.run(
                SNAPSHOT_COMMANDS[key], capture_output=True, text=True, check=True
            )
        except subprocess.CalledProcessError as exc:
            sys.exit(f"ERROR: ceph command failed:\n{exc.stderr.strip()}")
        except FileNotFoundError:
            sys.exit("ERROR: 'ceph' binary not found in PATH.")
        text = proc.stdout

    _SNAPSHOT_CACHE[key] = json.loads(text)
    return _SNAPSHOT_CACHE[key]


def fetch_osd_df() -> dict[int, dict]:
    """Return {osd_id: node} from 'ceph osd df'."""
    data = _ceph_json("osd_df")
    return {n["id"]: n for n in data.get("nodes", []) + data.get("stray", [])}


def fetch_osd_hosts() -> dict[int, str]:
    """Return {osd_id: short_hostname} from 'ceph osd tree'."""
    data = _ceph_json("osd_tree")
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


def fetch_backfillfull_pct() -> float | None:
    """Return the cluster's backfillfull_ratio as a percentage, or None if absent."""
    ratio = _ceph_json("osd_dump").get("backfillfull_ratio")
    return None if ratio is None else ratio * 100


def fetch_pools() -> dict[int, dict]:
    """Return {pool_id: pool} from 'ceph osd pool ls detail'."""
    data = _ceph_json("pool_ls_detail")
    return {p["pool_id"]: p for p in data}


def fetch_ec_profiles() -> dict[str, dict]:
    """Return {profile_name: profile} from 'ceph osd dump'."""
    data = _ceph_json("osd_dump")
    return data.get("erasure_code_profiles", {})


def fetch_pg_stats() -> list[dict]:
    """Return pg_stat dicts for the remapped PGs, from 'ceph pg ls remapped'."""
    raw = _ceph_json("pg_ls_remapped")
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        if raw.get("pg_ready") is False:
            # Reading this as "no PGs" would answer "no backfills" wrongly.
            sys.exit(
                "ERROR: 'ceph pg ls remapped' reports the PG stats are not "
                "ready (the mgr has just started or failed over?); retry in a "
                "moment."
            )
        if "pg_stats" in raw:
            return raw["pg_stats"]
        if "pg_stats" in raw.get("pg_map", {}):
            return raw["pg_map"]["pg_stats"]
        # With no matching PGs 'ceph pg ls' omits 'pg_stats' and returns
        # just {"pg_ready": true}.
        if "pg_ready" in raw:
            return []
    raise SystemExit(
        f"ERROR: unrecognised JSON structure from 'ceph pg ls remapped'.\n"
        f"Top-level type: {type(raw).__name__}"
        + (f", keys: {list(raw.keys())}" if isinstance(raw, dict) else "")
    )


def fetch_crush_rules() -> dict[int, dict]:
    """Return {rule_id: rule} from 'ceph osd crush rule dump'."""
    data = _ceph_json("crush_rule_dump")
    return {r["rule_id"]: r for r in data}


# ---------------------------------------------------------------------------
# Anonymization for --save-state
# ---------------------------------------------------------------------------

# The parts of 'ceph osd dump' besides the erasure code profiles that the
# analysis reads, and so that --save-state keeps.
KEPT_OSD_DUMP_RATIOS = ("full_ratio", "backfillfull_ratio", "nearfull_ratio")

_TRAILING_NUM_RE = re.compile(r"(\d+)$")
_FAKE_HASH_NAME_RE = re.compile(r"host-[0-9a-f]{8}")


def _fake_hostname(real_name: str) -> str:
    """Map a real hostname to a deterministic stand-in.

    Keyed off the hostname's trailing number (e.g. 'ceph2-11' -> 'host11'), or,
    with none, a hash of the whole name, so the same real host always maps to
    the same fake one with no shared state. Same scheme as
    divert-toofull-backfills.py, so captures from both agree.
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
    """Anonymize a complete set of parsed snapshots in place.

    Hostnames, pool names and CRUSH rule names are replaced with deterministic
    fake values; PG ids, OSD ids, utilizations and the topology, which the
    analysis depends on, are untouched. 'ceph osd dump' is cut down to the
    erasure code profiles and the full ratios, the only parts this script
    reads, which also keeps the cluster fsid, OSD addresses and uuids out of
    the capture.

    Idempotent, since every substitution is a function of an id or the real
    value already present, so a second pass changes nothing.
    """
    osd_tree = snapshots["osd_tree"]
    hosts = [
        node
        for node in osd_tree.get("nodes", []) + osd_tree.get("stray", [])
        if node.get("type") == "host"
    ]
    fakes = _fake_hostnames({node["name"] for node in hosts})
    for node in hosts:
        node["name"] = fakes[node["name"]]

    dump = snapshots["osd_dump"]
    snapshots["osd_dump"] = {
        "erasure_code_profiles": dump.get("erasure_code_profiles", {}),
        **{k: dump[k] for k in KEPT_OSD_DUMP_RATIOS if k in dump},
    }
    for pool in snapshots["pool_ls_detail"]:
        pool["pool_name"] = f"pool{pool['pool_id']}"
    for rule in snapshots["crush_rule_dump"]:
        rule["rule_name"] = f"rule{rule['rule_id']}"


def write_anonymized_state(dir_: Path, snapshots: dict[str, object]) -> None:
    """Write an anonymized copy of every collected snapshot under dir_.

    Works on a deep copy: the cache that feeds the run's own analysis is left
    untouched, so --save-state never changes what the run itself reports.
    """
    anonymized = copy.deepcopy(snapshots)
    anonymize_snapshots(anonymized)
    for key, obj in anonymized.items():
        (dir_ / f"{key}.json").write_text(json.dumps(obj, separators=(",", ":")))


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


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


def _is_real_osd(osd_id) -> bool:
    return osd_id not in (CRUSH_ITEM_NONE, -1, None)


def _slot(osd_list: list, index: int) -> int | None:
    """Return the real OSD id at a position, or None for a missing/empty slot."""
    if index >= len(osd_list):
        return None
    osd_id = osd_list[index]
    return osd_id if _is_real_osd(osd_id) else None


def find_arrivals(
    up: list, acting: list, osd: int, is_ec: bool
) -> tuple[list[tuple["int | str", int]], list[tuple["int | str", str]]]:
    """Split the shards arriving on osd into those with an OSD to pin to and not.

    Returns (pins, skipped): pins holds (shard, acting_osd) for each shard
    that could be pinned back to the OSD it is on now, skipped holds (shard,
    reason) for those that have none. The shard is the EC shard index, or '-'
    for replicated pools. Whether a pin is actually valid (see
    pin_with_companions) is a separate question.
    """
    pins, skipped = [], []
    if is_ec:
        for i in range(len(up)):
            if _slot(up, i) != osd:
                continue
            acting_osd = _slot(acting, i)
            if acting_osd == osd:
                continue
            if acting_osd is None:
                skipped.append((i, "no acting OSD for this shard (degraded)"))
            else:
                pins.append((i, acting_osd))
        return pins, skipped

    up_set = {o for o in up if _is_real_osd(o)}
    acting_set = {o for o in acting if _is_real_osd(o)}
    if osd not in up_set or osd in acting_set:
        return pins, skipped
    arriving = up_set - acting_set
    departing = acting_set - up_set
    if len(arriving) == 1 and len(departing) == 1:
        pins.append(("-", next(iter(departing))))
    elif not departing:
        skipped.append(("-", "no acting OSD to pin to (missing replica)"))
    else:
        skipped.append(("-", "several replicas moving, pairing is ambiguous"))
    return pins, skipped


def copies_moving(up: list, acting: list, is_ec: bool, pool_size: int) -> int:
    """Count the shard/replica copies a PG has to place (see pg-movements.py).

    Ceph's misplaced/degraded object counters are in copy units, so this is
    the multiplier of num_objects in the progress denominator: slots whose up
    OSD differs from acting, plus slots with no OSD anywhere yet.
    """
    if is_ec:
        count = 0
        for i in range(max(len(up), len(acting))):
            up_osd, acting_osd = _slot(up, i), _slot(acting, i)
            if up_osd is None:
                count += acting_osd is None
            else:
                count += up_osd != acting_osd
        return count
    up_set = {o for o in up if _is_real_osd(o)}
    acting_set = {o for o in acting if _is_real_osd(o)}
    arriving = len(up_set - acting_set)
    return arriving + max(0, pool_size - len(acting_set) - arriving)


def pg_progress_pct(pg: dict, n_copies: int) -> float | None:
    """Estimate % of a PG's data already at its target location.

    From the misplaced/degraded object counters, which count down to 0 as
    movement completes; None if the PG has no objects. An approximation that
    assumes objects are of similar size.
    """
    stat_sum = pg.get("stat_sum", {})
    total = stat_sum.get("num_objects", 0) * n_copies
    if total <= 0:
        return None
    remaining = stat_sum.get("num_objects_misplaced", 0) + stat_sum.get(
        "num_objects_degraded", 0
    )
    return max(0.0, min(100.0, 100.0 * (1 - remaining / total)))


def shard_size_bytes(pg: dict, pool: dict, ec_profiles: dict[str, dict]) -> int | None:
    """Estimate the bytes one shard of a PG occupies, or None if unknown.

    A replica holds all of the PG's logical size ('num_bytes'), an EC shard
    1/k of it (rounded up). Omap, metadata and stripe padding are not counted.
    """
    num_bytes = pg["stat_sum"]["num_bytes"]
    if pool.get("type") != POOL_TYPE_ERASURE:
        return num_bytes
    try:
        k = int(ec_profiles[pool["erasure_code_profile"]]["k"])
    except (KeyError, ValueError):
        return None
    return -(-num_bytes // k)


def _same_place(a: int, b: int, osd_host: dict[int, str]) -> bool:
    """True if two OSDs are one and the same or on one host (host known)."""
    host = osd_host.get(a)
    return a == b or (host is not None and host == osd_host.get(b))


def _close_pins(
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
                if j == i or j in pins or not _is_real_osd(other):
                    continue
                if not _same_place(new_up[i], other, osd_host):
                    continue
                companion = _slot(acting, j)
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
    ({}, reason); see _close_pins.
    """
    return _close_pins(up, acting, {slot: acting[slot]}, osd_host)


def projected_utilization(
    osd_df: dict[int, dict], osd_id: int, size_bytes: int | None
) -> float | None:
    """Return an OSD's utilization (percent) once one more shard has landed.

    'ceph osd df' usage plus the shard's estimated size, over the OSD's
    capacity; None if 'ceph osd df' has no capacity for it. Only this shard is
    added, not others arriving on the OSD, so it is a lower bound of what Ceph
    will see when it decides whether to reserve the backfill.
    """
    node = osd_df.get(osd_id)
    if not node or not node.get("kb"):
        return None
    return (node["kb_used"] * KIB + (size_bytes or 0)) / (node["kb"] * KIB) * 100


def find_blockers(
    up: list,
    acting: list,
    pinned: dict[int, int],
    osd_df: dict[int, dict],
    backfillfull_pct: float,
    size_bytes: int | None,
) -> list[int]:
    """Return the EC shards, in shard order, that would hold the PG in backfill_toofull.

    backfill_toofull is a property of the PG: while any backfill target of a PG
    refuses the reservation, the whole PG waits, including a shard heading for
    a perfectly empty OSD. So a shard that is not pinned, is moving to an OSD
    whose utilization once the shard lands reaches backfillfull_ratio, and has
    an acting OSD to go back to, is a blocker for the pinned ones.
    """
    blockers = []
    for j, target in enumerate(up):
        if (
            j in pinned
            or not _is_real_osd(target)
            or _slot(acting, j) in (None, target)
        ):
            continue
        projected = projected_utilization(osd_df, target, size_bytes)
        if projected is not None and projected >= backfillfull_pct:
            blockers.append(j)
    return blockers


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


def pin_replica(
    up: list, osd: int, acting_osd: int, osd_host: dict[int, str]
) -> str | None:
    """Return why a replicated PG's replica cannot be pinned, or None if it can.

    The replica swaps osd for acting_osd; the same-host clash with the other
    replicas is checked as for EC. Replicas have no identity, so a clashing
    replica cannot be pinned too: any PG with a second replica moving is already
    ambiguous (see find_arrivals) and never gets here.
    """
    for other in up:
        if (
            other != osd
            and _is_real_osd(other)
            and _same_place(acting_osd, other, osd_host)
        ):
            return (
                f"acting osd.{acting_osd} shares a host with replica "
                f"osd.{other}, which is not moving"
            )
    return None


class Cancellation(NamedTuple):
    """One shard pinned back from the OSD it was moving to onto its acting OSD."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # where CRUSH is sending it: the 'from' of the upmap pair
    acting_osd: int  # where it is now: the 'to'
    size_bytes: int | None  # estimated, None if unknown
    state: str
    progress_pct: float | None  # of the whole PG's movement, not this shard's
    companion_of: "int | str | None" = None  # the requested shard this one is
    # pinned along with (see _close_pins, find_blockers), None if requested
    blocker_util: float | None = None  # set when this shard's target OSD would
    # reach backfillfull_ratio (percent it would be at), i.e. it is a blocker


class Skipped(NamedTuple):
    """A shard arriving on the OSD that cannot be pinned, and why."""

    pgid: str
    shard: "int | str"
    reason: str


def pgid_sort_key(pgid: str) -> tuple[int, int]:
    """Sort PG IDs numerically: pool id (decimal), then pg id (hex)."""
    pool_str, pg_hex = pgid.split(".")
    return (int(pool_str), int(pg_hex, 16))


def plan_cancellations(
    pg_stats: list[dict],
    pools: dict[int, dict],
    ec_profiles: dict[str, dict],
    osd: int,
    osd_host: dict[int, str],
    crush_rules: dict[int, dict],
    osd_df: dict[int, dict] | None = None,
    backfillfull_pct: float | None = None,
) -> tuple[list[Cancellation], list[Skipped]]:
    """Return (cancellations, skipped) for all backfills into osd, in PG order.

    Every shard arriving on osd yields a cancellation, plus cancellations for
    the other shards of its PG that go with it: companions that must be pinned
    back to keep the mapping valid (_close_pins), and, when osd_df and
    backfillfull_pct are given, blockers whose target OSD would reach
    backfillfull_pct and hold the whole PG in backfill_toofull (find_blockers).
    A blocker that cannot be pinned is skipped and reported; the requested
    shard's own pin is kept. Exits with an error if such a PG's pool does not
    fail over at host, since the clash check assumes it.
    """
    cancellations, skipped = [], []
    for pg in pg_stats:
        up, acting = pg["up"], pg["acting"]
        if osd not in up:
            continue
        pgid = pg["pgid"]
        pool = pools.get(int(pgid.split(".")[0]))
        if pool is None:
            # Without the pool type EC shards would be diffed as replicas.
            sys.exit(
                f"ERROR: PG {pgid} belongs to a pool that 'ceph osd pool ls "
                "detail' does not list, so its shards cannot be analyzed."
            )
        domain = rule_failure_domain(crush_rules.get(pool.get("crush_rule")))
        if domain != "host":
            sys.exit(
                f"ERROR: pool {pool['pool_id']} (PG {pgid}) has CRUSH failure "
                f"domain {domain or 'unknown'}; this script only checks for "
                "same-host clashes, so it cannot tell which pins are valid."
            )
        is_ec = pool.get("type") == POOL_TYPE_ERASURE
        pins, unpinnable = find_arrivals(up, acting, osd, is_ec)
        skipped.extend(Skipped(pgid, shard, why) for shard, why in unpinnable)
        if not pins:
            continue
        size = shard_size_bytes(pg, pool, ec_profiles)
        progress = pg_progress_pct(
            pg, copies_moving(up, acting, is_ec, pool.get("size", 0))
        )
        for shard, acting_osd in pins:
            if is_ec:
                resolved, why = pin_with_companions(up, acting, shard, osd_host)
                if why is None and osd_df is not None and backfillfull_pct is not None:
                    for blocker in find_blockers(
                        up, acting, resolved, osd_df, backfillfull_pct, size
                    ):
                        if blocker in resolved:  # already pulled in as a companion
                            continue
                        trial = {**resolved, blocker: acting[blocker]}
                        closed, blocker_why = _close_pins(up, acting, trial, osd_host)
                        if blocker_why is None:
                            _, blocker_why = order_moves(
                                [(s, up[s], a) for s, a in closed.items()]
                            )
                        if blocker_why is None:
                            resolved = closed
                        else:
                            skipped.append(
                                Skipped(pgid, blocker, f"blocker: {blocker_why}")
                            )
                moves, ring = order_moves([(s, up[s], a) for s, a in resolved.items()])
                why = why or ring
            else:
                why = pin_replica(up, osd, acting_osd, osd_host)
                moves = [("-", osd, acting_osd)]
            if why is not None:
                skipped.append(Skipped(pgid, shard, why))
                continue
            for s, from_osd, to_osd in moves:
                projected = None
                if s != shard and osd_df is not None and backfillfull_pct is not None:
                    projected = projected_utilization(osd_df, from_osd, size)
                    if projected is not None and projected < backfillfull_pct:
                        projected = None
                cancellations.append(
                    Cancellation(
                        pgid,
                        s,
                        from_osd,
                        to_osd,
                        size,
                        pg["state"],
                        progress,
                        None if s == shard else shard,
                        projected,
                    )
                )

    def skipped_order(item: Skipped) -> tuple:
        return (*pgid_sort_key(item.pgid), item.shard if item.shard != "-" else -1)

    # Stable, by PG only: within a PG the order is the one to apply the pairs in.
    return (
        sorted(cancellations, key=lambda c: pgid_sort_key(c.pgid)),
        sorted(skipped, key=skipped_order),
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# Each entry is (group, label); the header is printed on two lines, the group
# name spanning its columns above their labels, and an empty group means the
# column has no group line. The UP group is where CRUSH wants the shard (the
# 'from' of the upmap pair), ACTING where its data is now (the 'to').
# print_table leaves the final column unpadded.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "HOST"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("", "SIZE"),
    ("", "PROGRESS"),
    ("", "STATE"),
    ("", "NOTE"),
]

# Between table columns of one group, and between columns of different groups.
COLUMN_SEP = "  "
GROUP_SEP = "    "


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


def format_utilization(osd_df: dict[int, dict], osd_id: int) -> str:
    """Format an OSD's utilization as 'NN.N%', or '?' if 'ceph osd df' lacks it."""
    util = osd_df.get(osd_id, {}).get("utilization")
    return f"{util:.1f}%" if util is not None else "?"


def format_note(c: Cancellation) -> str:
    """Say why a shard is in the proposal, if not because it arrives on the OSD."""
    if c.blocker_util is not None:
        return (
            f"blocks shard {c.companion_of}: target osd.{c.up_osd} "
            f"would be at {c.blocker_util:.1f}%, over backfillfull"
        )
    if c.companion_of is not None:
        return f"companion of shard {c.companion_of}"
    return ""


def format_row(
    c: Cancellation, osd_df: dict[int, dict], osd_host: dict[int, str]
) -> list[str]:
    return [
        c.pgid,
        str(c.shard),
        f"osd.{c.up_osd}",
        format_utilization(osd_df, c.up_osd),
        osd_host.get(c.up_osd, "?"),
        f"osd.{c.acting_osd}",
        format_utilization(osd_df, c.acting_osd),
        osd_host.get(c.acting_osd, "?"),
        format_bytes(c.size_bytes),
        # floor, so a PG that is still moving never reads "100%"
        "-" if c.progress_pct is None else f"{math.floor(c.progress_pct)}%",
        c.state,
        format_note(c),
    ]


def print_table(rows: list[list[str]]) -> None:
    """Print rows under a two-line header: group spans, then column labels.

    A group's name is centered in dashes across the full width of its
    columns, so it visibly covers all of them. The span is always wider than
    the name (the labels under it alone are wider), so no fitting is needed.
    Columns of different groups are separated by the wider GROUP_SEP, on every
    line, to set the groups visually apart.
    """
    # A list, not max(a, *b): with no rows the star-args form degrades to
    # max(int) and raises.
    widths = [
        max([len(label), *(len(row[i]) for row in rows)])
        for i, (_, label) in enumerate(COLUMNS)
    ]
    # seps[i] is what precedes column i.
    seps = [""] + [
        COLUMN_SEP if COLUMNS[i][0] == COLUMNS[i - 1][0] else GROUP_SEP
        for i in range(1, len(COLUMNS))
    ]

    group_line = ""
    for group, indexes in groupby(range(len(COLUMNS)), key=lambda i: COLUMNS[i][0]):
        cols = list(indexes)
        span = sum(widths[i] for i in cols) + sum(len(seps[i]) for i in cols[1:])
        group_line += seps[cols[0]]
        group_line += f" {group} ".center(span, "-") if group else " " * span
    print(group_line.rstrip())

    # Last column is variable-width and rightmost; leave it unpadded.
    def emit(cells: list[str]) -> None:
        last = len(cells) - 1
        print(
            "".join(
                sep + (cell.ljust(widths[i]) if i < last else cell)
                for i, (sep, cell) in enumerate(zip(seps, cells))
            )
        )

    emit([label for _, label in COLUMNS])
    for row in rows:
        emit(row)


def print_pgremapper(cancellations: list[Cancellation]) -> None:
    """Print '<pgid> <up osd> <acting osd>' per cancellation.

    These are 'pgremapper remap's positional arguments; OSD ids must be bare
    integers, it rejects the 'osd.N' form.
    """
    for c in cancellations:
        print(f"{c.pgid} {c.up_osd} {c.acting_osd}")


def print_import_mappings(cancellations: list[Cancellation]) -> None:
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
    lines = [
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
    ]
    for pgid, cs in chained.items():
        pairs = " ".join(f"{c.up_osd} {c.acting_osd}" for c in cs)
        lines.append(f"  ceph osd pg-upmap-items {pgid} {pairs}")
    print("\n".join(lines), file=sys.stderr)


def pgs_needing_several_pins(cancellations: list[Cancellation]) -> list[str]:
    """Return the PGs (in PG order) that have more than one cancellation."""
    counts = Counter(c.pgid for c in cancellations)
    return sorted((pgid for pgid, n in counts.items() if n > 1), key=pgid_sort_key)


def warn_separate_remaps(pgids: list[str]) -> None:
    """Warn on stderr that these PGs are not safe to apply with 'remap' lines.

    Each 'pgremapper remap' run adds one pair to a PG's upmap entry, and
    'ceph osd pg-upmap-items' replaces the whole entry. On a live cluster the
    run after the first was seen to send only its own pair, undoing the one
    before; and a pair left alone can be invalid (two shards on one host) and
    is then silently dropped by the mons, so the PG never changes.
    """
    shown = ", ".join(pgids[:8]) + (
        f", ... ({len(pgids)} in all)" if len(pgids) > 8 else ""
    )
    print(
        f"WARNING: {len(pgids)} PG(s) need more than one remap ({shown}). Running "
        "these lines as separate 'pgremapper remap' commands (e.g. xargs -L1) "
        "can silently lose pairs: a later run may overwrite the pair an earlier "
        "one just added, and a lone pair can be invalid and dropped by the "
        "mons. Use --import-mappings instead, which applies all pairs of a PG "
        "together.",
        file=sys.stderr,
    )


def print_summary(
    osd: int,
    osd_df: dict[int, dict],
    osd_host: dict[int, str],
    cancellations: list[Cancellation],
    skipped: list[Skipped],
) -> None:
    """Report on stderr the OSD's fill level and what the proposal covers."""
    node = osd_df[osd]
    arriving = [c for c in cancellations if c.companion_of is None]
    others = len(cancellations) - len(arriving)
    blockers = sum(c.blocker_util is not None for c in cancellations)
    known = [c.size_bytes for c in arriving if c.size_bytes is not None]
    total = sum(known)
    capacity = node.get("kb", 0) * KIB
    share = f" ({total / capacity * 100:.1f}% of its capacity)" if capacity else ""
    unknown = len(arriving) - len(known)
    print(
        f"osd.{osd} ({osd_host.get(osd, '?')}) is at {node['utilization']:.1f}%. "
        f"{len(arriving)} arriving shard(s) can be pinned back, "
        f"~{format_bytes(total)}{share} of data"
        + (f" (+{unknown} of unknown size)" if unknown else "")
        + f"; {len(skipped)} cannot be pinned."
        + (
            f" {others} more shard(s) of those PGs, moving to other OSDs, are "
            "pinned back too"
            + (
                f" ({blockers} because their target would be over "
                "backfillfull_ratio and hold the PG in backfill_toofull)"
                if blockers
                else ""
            )
            + "."
            if others
            else ""
        ),
        file=sys.stderr,
    )
    for s in skipped:
        print(f"  cannot pin {s.pgid} shard {s.shard}: {s.reason}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    osd = args.osd

    global LOAD_STATE_DIR, SAVE_STATE_DIR
    if args.load_state:
        LOAD_STATE_DIR = Path(args.load_state)
        if not LOAD_STATE_DIR.is_dir():
            sys.exit(f"ERROR: --load-state directory not found: {LOAD_STATE_DIR}")
    if args.save_state:
        SAVE_STATE_DIR = Path(args.save_state)
        SAVE_STATE_DIR.mkdir(parents=True, exist_ok=True)
        if any(SAVE_STATE_DIR.iterdir()):
            sys.exit(f"ERROR: --save-state directory is not empty: {SAVE_STATE_DIR}")

    osd_df = fetch_osd_df()
    osd_host = fetch_osd_hosts()
    pg_stats = fetch_pg_stats()
    pools = fetch_pools()
    ec_profiles = fetch_ec_profiles()
    crush_rules = fetch_crush_rules()

    # Every SNAPSHOT_COMMANDS key is now cached, so the capture is complete.
    # Saved before the checks below, which can exit: a cluster that trips them
    # is just the kind of state worth having captured.
    if SAVE_STATE_DIR is not None:
        write_anonymized_state(SAVE_STATE_DIR, _SNAPSHOT_CACHE)

    if osd not in osd_df:
        sys.exit(f"ERROR: osd.{osd} not found in 'ceph osd df'.")
    backfillfull_pct = fetch_backfillfull_pct()
    if backfillfull_pct is None:
        print(
            "NOTE: 'osd dump' has no backfillfull_ratio (an older --load-state "
            "capture?), so shards blocking their PG are not looked for.",
            file=sys.stderr,
        )
    cancellations, skipped = plan_cancellations(
        pg_stats,
        pools,
        ec_profiles,
        osd,
        osd_host,
        crush_rules,
        osd_df,
        backfillfull_pct,
    )
    if not cancellations and not skipped:
        print(f"No backfills into osd.{osd}.", file=sys.stderr)
        if args.import_mappings:
            print_import_mappings([])
        return

    print_summary(osd, osd_df, osd_host, cancellations, skipped)
    chained = chained_pgs(cancellations)
    machine_format = args.import_mappings or args.pgremapper
    # pgremapper cannot apply chained pairs (see warn_chained_pgs), so they are
    # kept out of what is meant to be fed to it.
    printable = (
        [c for c in cancellations if c.pgid not in chained]
        if machine_format
        else cancellations
    )
    if args.import_mappings:
        print_import_mappings(printable)
    elif args.pgremapper:
        print_pgremapper(printable)
        if multi := pgs_needing_several_pins(printable):
            warn_separate_remaps(multi)
    elif cancellations:
        print_table([format_row(c, osd_df, osd_host) for c in cancellations])
    if chained:
        warn_chained_pgs(chained, left_out=machine_format)
    print(
        "NOTE: cancelling a running backfill discards its progress. Consider "
        "'ceph balancer off' while these are pinned.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
