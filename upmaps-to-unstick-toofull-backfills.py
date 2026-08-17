#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Propose upmap re-targets that divert stuck backfill_toofull PGs to emptier OSDs.

The situation this solves
-------------------------
The motivating case is a single OSD going out/down. CRUSH does *not* spread
its PGs across the cluster: for a rule of the form "choose a host bucket, then
a leaf inside it" (chooseleaf_firstn/indep type host), the failed leaf is
retried *within the same host bucket* — so every PG that lived on the dead OSD
is re-placed onto one of its 20-or-so same-host siblings. On a cluster that is
already uniformly full, those siblings blow past backfillfull_ratio and the
PGs wedge in backfill_toofull, while the rest of the cluster sits several
percent emptier and idle.

That is the shape of the problem, not a precondition of the code: the script
takes no arguments and works from whatever is in backfill_toofull right now,
whichever hosts are absorbing the shards and however they got that way.

The strategy does rely on the relevant pools' CRUSH rules failing over at the
host bucket type, so this is checked at startup (see check_host_failure_domain)
and the script exits with an error if it does not hold.

For each stuck shard the script picks an OSD elsewhere in the cluster that the
shard could legally be moved to instead. It only prints the proposed
re-targets; it changes nothing. The output is meant to be fed to another
script (or eyeballed) to actually apply the remaps.

How PGs are identified
----------------------
Every PG in backfill_toofull is examined, and a shard of it is selected when
that shard is newly arriving — in the PG's 'up' set but not its 'acting' set.
The host it is arriving on is by definition one that cannot take it, since
that is what backfill_toofull means, so no further filtering is needed. A
backfill_toofull PG with no newly-arriving shard yields nothing; it is counted
on stderr so the silence is not ambiguous.

Note the vacated OSD often cannot be identified. Once an OSD is out, the slot
it vacated in 'acting' reads as CRUSH_ITEM_NONE, not as its OSD id, so there
is no way to prove from the PG map where the shard came from. That does not
matter here — the arriving side is what is out of space, and it is always
observable. VACATED is reported when it happens to be recoverable and 'none'
otherwise.

'up'/'acting' are diffed differently per pool type, for the same reason as in
pg-movements.py: EC shards are identified by position, so index i is diffed
against index i and the shard index is reported. Replicated replicas are
interchangeable, so position carries no identity (a same-OSD-set reorder from
primary-affinity or pg-upmap-items is not movement) and the sets are diffed
instead, with SHARD shown as '-'.

How targets are chosen
----------------------
Candidates are OSDs that are up, in (reweight > 0), have a non-zero CRUSH
weight and have a known device class, grouped by that class and sorted by
utilization ascending within each class (OSD id breaks ties, so re-runs are
reproducible). Down and out OSDs are excluded by those filters, which also
keeps an out OSD from sorting *first* — 'ceph osd df' reports one at 0%
utilization.

A shard is only offered candidates of its *own* device class, that of the OSD
it is arriving on. Pools' CRUSH rules are typically class-constrained, so an
hdd shard sent to an ssd OSD would be an illegal placement.

For each shard the least-utilized such candidate is taken whose host is not
already used by the PG's 'up' set. The arriving OSD is itself a member of
'up', so the host being diverted away from is always excluded — that is the
whole point of the tool. A candidate is also rejected if it already appears in
the PG's *raw* CRUSH mapping (see below).

Each chosen OSD is removed from its class's candidate pool, so no two shards
are sent to the same OSD. If a PG has several diverted shards, each target
host is added to that PG's exclusion set before its next shard is placed.

That one-target-per-OSD rule can genuinely run out: a cluster-wide run may
have more stuck shards than there are usable OSDs in a class, in which case
the tail is reported as unplaceable rather than doubled up. Shards are
processed in PG id order, so the lowest pool ids get the emptiest targets; the
ordering is fixed rather than fair, which is what makes re-runs reproducible.

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

The same reconstruction also flags a subtler case. The arriving OSD becomes
the 'from' of the new upmap pair, and 'from' must be an OSD CRUSH itself
chose. An arriving OSD that is *absent* from the raw mapping is one an
existing upmap already put there (the balancer places these), so diverting
it means rewriting that pair's 'to' rather than adding a new pair — Ceph's
upmap validation silently drops a 'from' that CRUSH did not itself pick.
Such rows are still proposed — FROM_OSD is marked with a trailing '*', and a
note on stderr points at EXISTING_UPMAPS — on the assumption that whoever
applies this by hand will rewrite that pair's 'to' rather than paste the row
in as a new one.

Testing against saved cluster state
------------------------------------
By default every run calls the live 'ceph' CLI (see SNAPSHOT_COMMANDS for
the six commands and their JSON output). --save-state DIR captures that
same JSON, one '<key>.json' file per command, into DIR as a side effect of
an otherwise normal run — analysis and output proceed as usual against the
real data, so the run's own proposals are unaffected by the save. DIR must
be empty or not yet exist.

The saved copy is anonymized (see anonymize_snapshots): cluster fsid, OSD
IP addresses, OSD uuids, hostnames and pool/CRUSH-rule names are replaced
with deterministic fake values before writing, so a --save-state capture is
safe to hand to someone outside the cluster (or commit to a public repo)
without hand-editing it first. Every substitution is a pure function of an
id already in the same record (OSD id, pool id, rule id) or of the real
value itself, so the same real entity always anonymizes to the same fake
one — including across separate runs against the same cluster, with no
shared state needed. PG ids, OSD ids, utilizations and the overall topology
are left untouched, since those are what the analysis (and a replay via
--load-state) actually depends on.

--load-state DIR reads those six files back instead of calling 'ceph', so a
captured state — anonymized or not — can be replayed offline with no
cluster access. The two flags are mutually exclusive. test-data/
upmaps-toofull-*/ hold sample captures (each with a README describing the
scenario and the exact output the script should reproduce from it) usable
directly as --load-state arguments.

Applying the output
-------------------
Two output formats are available. The default is a human-readable table.
--pgremapper instead emits one bare '<pgid> <from osd> <target osd>' line per
remap — the table's PGID, FROM_OSD and TARGET_OSD columns, which are exactly
the positional arguments of 'pgremapper remap' (which calls FROM_OSD the
"source osd"). Only the rows go to stdout in either mode — everything else is
on stderr — so the output stays parseable.

Caveats for whatever consumes this:

  - 'ceph osd pg-upmap-items' *replaces* a PG's entire upmap entry rather than
    adding to it. PGs here frequently already carry unrelated upmap pairs, so
    a command that states only the new pair silently discards the others and
    triggers fresh remapping. The EXISTING_UPMAPS column reports each PG's
    current pairs so they can be restated. 'pgremapper remap' merges into the
    existing entry rather than replacing it, so it needs no such restatement —
    which is why --pgremapper omits that column.

  - For the same reason, rows are not independent when a PG has more than one
    diverted shard. Such a PG gets one row per shard, each repeating that PG's
    same EXISTING_UPMAPS, so running one 'pg-upmap-items' command per row makes
    the last command replace the entry the earlier ones wrote and only the
    final diversion survives. All rows sharing a PGID must be folded into a
    single command listing EXISTING_UPMAPS plus every one of that PG's new
    pairs. ('pgremapper remap' is per-pair and merges, so it is unaffected.)

  - If the upmap balancer is active ('ceph balancer status'), it may undo
    manually placed upmap entries. Consider 'ceph balancer off' while the
    diverted backfills drain.

  - A FROM_OSD suffixed '*' is itself the 'to' of one of that row's
    EXISTING_UPMAPS pairs (see "Why the raw CRUSH mapping matters" above).
    Apply such a row by rewriting that pair's 'to' to TARGET_OSD, not by
    adding 'FROM_OSD->TARGET_OSD' as a new pair — Ceph accepts a new pair
    like that and then silently drops it.

Review the proposals before applying them. To hand them to pgremapper:

    upmaps-to-unstick-toofull-backfills.py --pgremapper > remaps.txt
    xargs -a remaps.txt -L1 pgremapper-v1.0.0-linux-amd64 remap

Use 'xargs -a', not '< remaps.txt': with a redirect, xargs points each child's
stdin at /dev/null, so pgremapper's per-remap confirmation prompt reads EOF
instead of an answer. '-a' leaves stdin on the terminal. (Add '--yes' to
pgremapper to skip the prompt and its dry-run entirely.)
"""

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

# Sentinel used by CRUSH/Ceph for "no OSD in this slot" (crush/crush.h).
# 'ceph pg ls'/'ceph pg dump' JSON uses this value, not -1, for empty slots.
CRUSH_ITEM_NONE = 0x7FFFFFFF

POOL_TYPE_ERASURE = 3

# Maps each snapshot to the 'ceph ... --format json' command that produces
# it and the '<key>.json' filename it is saved/loaded as under --save-state/
# --load-state. Keys match the fixtures under test-data/upmaps-toofull-*/
# verbatim, so those directories can be passed straight to --load-state.
SNAPSHOT_COMMANDS: dict[str, list[str]] = {
    "osd_tree": ["ceph", "osd", "tree", "--format", "json"],
    "osd_df": ["ceph", "osd", "df", "--format", "json"],
    "osd_dump": ["ceph", "osd", "dump", "--format", "json"],
    "pool_ls_detail": ["ceph", "osd", "pool", "ls", "detail", "--format", "json"],
    "crush_rule_dump": ["ceph", "osd", "crush", "rule", "dump", "--format", "json"],
    "pg_ls_backfill_toofull": [
        "ceph",
        "pg",
        "ls",
        "backfill_toofull",
        "--format",
        "json",
    ],
}

# Set from args at the top of main(): None means "call the live ceph CLI as
# normal"; a Path means "read '<key>.json' from this directory instead of
# running SNAPSHOT_COMMANDS[key]" (see --load-state).
LOAD_STATE_DIR: "Path | None" = None

# Set from args at the top of main(): None means "don't save"; a Path means
# "once all six snapshots are collected, write an anonymized copy of each to
# '<dir>/<key>.json'" (see --save-state and anonymize_snapshots).
SAVE_STATE_DIR: "Path | None" = None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Propose upmap re-targets that divert stuck "
        "backfill_toofull PGs to emptier OSDs. Every backfill_toofull PG in "
        "the cluster is examined, and each shard newly arriving on a host too "
        "full to take it is offered the least-utilized "
        "OSD of its own device class that it could legally be moved to "
        "instead. Only the proposals are printed; nothing is changed. Assumes "
        "the affected pools' CRUSH failure domain is 'host', and exits with an "
        "error if it is not.",
        epilog="See the docstring at the top of this script for how targets "
        "are chosen, which shards get skipped and why, and the caveats that "
        "apply when turning these rows into 'ceph osd pg-upmap-items' or "
        "'pgremapper remap' commands.",
    )
    parser.add_argument(
        "--pgremapper",
        action="store_true",
        help="Print '<pgid> <from osd> <target osd>' lines with no header "
        "instead of the table, so each line can be passed as the arguments "
        "of 'pgremapper remap' (the script's docstring has the xargs form).",
    )
    state_group = parser.add_mutually_exclusive_group()
    state_group.add_argument(
        "--load-state",
        metavar="DIR",
        help="Analyze a saved cluster state instead of a live cluster. DIR "
        "must contain the six '<key>.json' files documented at "
        "SNAPSHOT_COMMANDS (osd_tree.json, osd_df.json, osd_dump.json, "
        "pool_ls_detail.json, crush_rule_dump.json and "
        "pg_ls_backfill_toofull.json) — the same layout as the fixtures "
        "under test-data/upmaps-toofull-*/, and what --save-state produces. "
        "No 'ceph' commands are run.",
    )
    state_group.add_argument(
        "--save-state",
        metavar="DIR",
        help="Also save the live cluster state this run collects into DIR, "
        "as the six '<key>.json' files --load-state reads back (created if "
        "missing; must be empty or not exist, so a snapshot is never "
        "partially overwritten). Analysis and normal output proceed as "
        "usual against the real data — only the saved copy is anonymized "
        "(cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule names "
        "replaced with deterministic fake values; see anonymize_snapshots), "
        "so it is safe to share outside the cluster without hand-editing.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


# Populated lazily by _ceph_json, keyed by SNAPSHOT_COMMANDS key. main()
# ends up fetching all six keys unconditionally, so by the time a
# --save-state write happens the cache always holds the complete set — that
# completeness (not just laziness) is what write_anonymized_state relies on.
_SNAPSHOT_CACHE: dict[str, object] = {}


def _ceph_json(key: str) -> object:
    """Return parsed JSON for one of SNAPSHOT_COMMANDS's keys.

    Read from '<LOAD_STATE_DIR>/<key>.json' if --load-state was given,
    otherwise run the live ceph command. Cached after the first call, so
    each key is read/run at most once per process even though the fetch_*
    functions are called from a few different places in main().
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
        cmd = SNAPSHOT_COMMANDS[key]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as exc:
            sys.exit(f"ERROR: ceph command failed:\n{exc.stderr.strip()}")
        except FileNotFoundError:
            sys.exit("ERROR: 'ceph' binary not found in PATH.")
        text = proc.stdout

    _SNAPSHOT_CACHE[key] = json.loads(text)
    return _SNAPSHOT_CACHE[key]


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


def fetch_osd_df() -> dict[int, dict]:
    """Return {osd_id: node} from 'ceph osd df'.

    Each node carries device_class, utilization, status, reweight and
    crush_weight, which together are everything needed to decide whether an
    OSD is a usable backfill target — no separate 'ceph osd dump' pass.
    """
    data = _ceph_json("osd_df")
    nodes = data.get("nodes", []) + data.get("stray", [])
    return {n["id"]: n for n in nodes}


def fetch_upmap_items() -> dict[str, list[dict]]:
    """Return {pgid: [{'from': osd, 'to': osd}, ...]} from 'ceph osd dump'."""
    data = _ceph_json("osd_dump")
    return {e["pgid"]: e["mappings"] for e in data.get("pg_upmap_items", [])}


def fetch_pool_details() -> list[dict]:
    """Return the list of pool dicts from 'ceph osd pool ls detail'."""
    return _ceph_json("pool_ls_detail")


def ec_pool_ids_from(pools: list[dict]) -> set[int]:
    """Return the set of pool ids that are erasure-coded (type == 3)."""
    return {p["pool_id"] for p in pools if p.get("type") == POOL_TYPE_ERASURE}


def fetch_crush_rules() -> dict[int, dict]:
    """Return {rule_id: rule} from 'ceph osd crush rule dump'."""
    data = _ceph_json("crush_rule_dump")
    return {r["rule_id"]: r for r in data}


def fetch_backfill_toofull_pgs() -> list[dict]:
    """Return pg_stat dicts for PGs in backfill_toofull.

    Filtered server-side by 'ceph pg ls', which is dramatically cheaper than
    dumping every PG in the cluster and filtering here.
    """
    raw = _ceph_json("pg_ls_backfill_toofull")
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
# Anonymization for --save-state
# ---------------------------------------------------------------------------

# A reserved-for-documentation range (RFC 5737 TEST-NET-2): guaranteed not
# to be a real routable address, so a saved capture can't be mistaken for
# one and can't leak the real network's layout.
_FAKE_IP_PREFIX = "198.51.100."

_ADDR_IP_RE = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
_TRAILING_NUM_RE = re.compile(r"(\d+)$")

FAKE_FSID = "00000000-0000-0000-0000-000000000000"


def _fake_ip(real_ip: str) -> str:
    """Map a real IP to a deterministic, non-routable stand-in.

    Keyed off the real address's own last octet, so the same real IP always
    anonymizes to the same fake one with no lookup table required. Distinct
    real IPs that happen to share a last octet collide onto the same fake
    one; that's harmless here since the script never parses these fields —
    they're descriptive only (see fetch_osd_df et al., none of which read
    any *_addr* key).
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

    Most ceph hostnames end in a distinguishing number (e.g. 'ceph2-11');
    keying off that number, rather than the encounter order of any one run,
    is what lets independent runs against the same cluster (or independent
    anonymization passes over related fixture directories) agree on the
    same fake name for the same real host with no shared state. A hostname
    with no trailing number falls back to a hash of the whole name, which
    is still deterministic, just not as readable.
    """
    m = _TRAILING_NUM_RE.search(real_name)
    if m:
        return f"host{int(m.group(1)):02d}"
    return "host-" + hashlib.sha256(real_name.encode()).hexdigest()[:8]


def anonymize_snapshots(snapshots: dict[str, object]) -> None:
    """Anonymize a complete set of six parsed snapshots in place.

    Replaces the cluster fsid, OSD IP addresses, OSD uuids, hostnames and
    pool/CRUSH-rule names with deterministic fake values (see _fake_ip,
    _fake_uuid, _fake_hostname above) — everything in these snapshots that
    could fingerprint the real cluster or site. PG ids, OSD ids,
    utilizations, weights and device classes are left untouched: they carry
    no site-identifying information and are exactly what the analysis (and
    any --load-state replay) depends on.

    pool_name and rule_name are display-only in this script (used solely in
    an error message in check_host_failure_domain; every lookup elsewhere
    is by pool_id/rule_id/crush_rule id), so renaming them to 'pool<id>'/
    'rule<id>' is safe and needs no cross-reference fixups.

    snapshots must hold parsed JSON for all six SNAPSHOT_COMMANDS keys
    together (not a subset) — hostnames live only in osd_tree, but the IPs
    and uuids they'd otherwise help identify live in osd_dump, so partial
    input would anonymize inconsistently.

    Idempotent: every substitution is keyed off a value already present in
    the record (osd id, pool id, rule id) or, for IPs and hostnames, off the
    real value itself — re-running this on already-anonymized snapshots
    reproduces the same fake values rather than mangling them further. That
    is what lets independent anonymization passes (e.g. over several
    related fixture directories, or a second pass after this function
    changes) agree without needing to share state.
    """
    osd_tree = snapshots["osd_tree"]
    for node in osd_tree.get("nodes", []) + osd_tree.get("stray", []):
        if node.get("type") == "host":
            node["name"] = _fake_hostname(node["name"])

    osd_dump = snapshots["osd_dump"]
    osd_dump["fsid"] = FAKE_FSID
    # 'ceph osd dump' embeds its own copy of each pool's name (separate from
    # pool_ls_detail's), keyed by 'pool' rather than 'pool_id' here.
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

    for pool in snapshots["pool_ls_detail"]:
        pool["pool_name"] = f"pool{pool['pool_id']}"

    for rule in snapshots["crush_rule_dump"]:
        rule["rule_name"] = f"rule{rule['rule_id']}"


def write_anonymized_state(dir_: Path, snapshots: dict[str, object]) -> None:
    """Write an anonymized copy of every collected snapshot under dir_.

    Called once, after all six snapshots have been collected (see
    _SNAPSHOT_CACHE), so anonymize_snapshots sees the complete set it
    requires. Operates on a deep copy — the cache that fed the run's own
    analysis and output is left untouched, so --save-state never changes
    what a run itself reports.
    """
    anonymized = copy.deepcopy(snapshots)
    anonymize_snapshots(anonymized)
    for key, obj in anonymized.items():
        (dir_ / f"{key}.json").write_text(json.dumps(obj, separators=(",", ":")))


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
    bucket, since that is what concentrates the re-placed shards onto one
    already-full host and makes moving them to a host outside the PG's up
    set the fix. If a pool's rule fails over at some other bucket type,
    excluding hosts is neither the constraint CRUSH enforces for it nor the
    one that would unstick it.
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
    """A shard newly arriving on a host that is too full to take it."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    arriving_osd: int  # OSD now receiving the shard; this is the 'from' of
    # the upmap that would divert it, and its host is
    # the one the shard is diverted away from
    vacated_osd: "int | None"  # OSD that left this slot, or None when the
    # slot reads as CRUSH_ITEM_NONE (the usual out-OSD case)
    up: list  # the PG's full up set, for host exclusions and for
    # reconstructing the raw CRUSH mapping


def find_diverted_shards(pg: dict, is_ec: bool) -> list[DivertedShard]:
    """Return the shards of one PG that are newly arriving on their up OSD."""
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
            found.append(DivertedShard(pgid, i, arriving, vacated, up))
    else:
        # Replicated: replicas are interchangeable, so position means nothing
        # and only the set difference is real movement.
        up_set = {o for o in up if _is_real_osd(o)}
        acting_set = {o for o in acting if _is_real_osd(o)}
        vacated_osds = sorted(acting_set - up_set)
        for arriving in sorted(up_set - acting_set):
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


def osd_class(osd_df: dict[int, dict], osd_id: int) -> "str | None":
    """Return an OSD's device class, or None if it is unknown."""
    return osd_df.get(osd_id, {}).get("device_class")


def build_candidate_osds(osd_df: dict[int, dict]) -> dict[str, list[int]]:
    """Return usable target OSD ids per device class, least-utilized first.

    Excludes OSDs that are down, out (reweight 0) or have no CRUSH weight.
    That also covers the OSD whose failure caused the pile-up in the first
    place: without it an out OSD would sort to the very front, since
    'ceph osd df' reports one at 0% utilization.

    Keyed by device class because a shard may only be diverted to an OSD of
    its own class (see module docstring); each class's pool is drawn down
    independently.
    """
    usable = [
        node
        for node in osd_df.values()
        if node.get("status") == "up"
        and node.get("reweight", 0) > 0
        and node.get("crush_weight", 0) > 0
        and node.get("device_class")
    ]
    # OSD id as secondary key: utilizations tie constantly on a uniformly
    # full cluster, and the operator will re-run this.
    usable.sort(key=lambda n: (n["utilization"], n["id"]))
    by_class: dict[str, list[int]] = {}
    for node in usable:
        by_class.setdefault(node["device_class"], []).append(node["id"])
    return by_class


class Proposal(NamedTuple):
    shard: DivertedShard
    target_osd: int
    target_host: str
    target_utilization: float
    via_existing_upmap: bool  # arriving_osd is absent from the raw CRUSH
    # mapping, i.e. it is itself the 'to' of an existing pair (see module
    # docstring); applying this row means rewriting that pair's 'to', not
    # adding a new pair


def assign_targets(
    shards: list[DivertedShard],
    candidates: dict[str, list[int]],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    upmap_items: dict[str, list[dict]],
) -> tuple[list[Proposal], list[DivertedShard]]:
    """Greedily give each diverted shard the least-utilized legal target.

    Returns (proposals, unplaceable shards). Each target OSD is consumed
    from its device class's pool, so no two shards are sent to the same OSD.
    """
    available = {cls: list(osds) for cls, osds in candidates.items()}
    # Hosts already spoken for per PG: seeded from the up set, then extended
    # as each of the PG's shards is placed, so a PG with two diverted shards
    # cannot be given two targets on one host.
    blocked_hosts: dict[str, set[str]] = {}
    proposals = []
    unplaceable = []

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

        # The arriving OSD becomes the 'from' of the diverting upmap pair,
        # and 'from' must be an OSD CRUSH itself chose. If the arriving OSD
        # is absent from the raw mapping it is itself the product of an
        # existing upmap (the balancer places these), so applying this row
        # as printed means rewriting that pair's 'to' rather than adding a
        # new pair — flagged rather than skipped (see module docstring).
        via_existing_upmap = shard.arriving_osd not in raw

        # Only OSDs of the arriving OSD's own class are legal targets. An
        # unknown class yields an empty pool, so the shard falls through to
        # unplaceable rather than being sent somewhere CRUSH would reject.
        pool = available.get(osd_class(osd_df, shard.arriving_osd), [])
        for candidate in pool:
            if osd_host.get(candidate) in forbidden_hosts:
                continue
            if candidate in forbidden_osds:
                continue
            pool.remove(candidate)
            forbidden_hosts.add(osd_host.get(candidate))
            proposals.append(
                Proposal(
                    shard,
                    candidate,
                    osd_host.get(candidate, "?"),
                    osd_df[candidate]["utilization"],
                    via_existing_upmap,
                )
            )
            break
        else:
            unplaceable.append(shard)

    return proposals, unplaceable


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

COLUMNS = [
    "PGID",
    "SHARD",
    "FROM_OSD",
    "FROM_HOST",
    "TARGET_OSD",
    "TARGET_HOST",
    "TGT_UTIL",
    "VACATED",
    "EXISTING_UPMAPS",
]


def format_row(
    proposal: Proposal,
    osd_host: dict[int, str],
    upmap_items: dict[str, list[dict]],
) -> list[str]:
    shard = proposal.shard
    pairs = upmap_items.get(shard.pgid, [])

    # '*' means arriving_osd is itself the 'to' of one of this row's
    # EXISTING_UPMAPS pairs; applying the row means rewriting that pair's
    # 'to' rather than adding 'FROM_OSD->TARGET_OSD' as a new pair (see
    # module docstring).
    from_osd = f"osd.{shard.arriving_osd}" + (
        "*" if proposal.via_existing_upmap else ""
    )

    return [
        shard.pgid,
        str(shard.shard),
        from_osd,
        # Varies per row now that the whole cluster is scanned, so unlike
        # the single-host version it cannot live in the stderr header.
        osd_host.get(shard.arriving_osd, "?"),
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
    (What the table calls FROM_OSD is pgremapper's "source osd" argument.)
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

    global LOAD_STATE_DIR, SAVE_STATE_DIR
    if args.load_state:
        LOAD_STATE_DIR = Path(args.load_state)
        if not LOAD_STATE_DIR.is_dir():
            sys.exit(f"ERROR: --load-state directory not found: {LOAD_STATE_DIR}")
    if args.save_state:
        SAVE_STATE_DIR = Path(args.save_state)
        SAVE_STATE_DIR.mkdir(parents=True, exist_ok=True)
        existing = list(SAVE_STATE_DIR.iterdir())
        if existing:
            sys.exit(
                f"ERROR: --save-state directory is not empty: {SAVE_STATE_DIR}"
            )

    osd_host = fetch_osd_hosts()
    osd_df = fetch_osd_df()
    upmap_items = fetch_upmap_items()
    pools = fetch_pool_details()
    ec_pool_ids = ec_pool_ids_from(pools)
    toofull_pgs = fetch_backfill_toofull_pgs()

    crush_rules = fetch_crush_rules()

    # All six SNAPSHOT_COMMANDS keys are now in _SNAPSHOT_CACHE (every
    # fetch_* above has run), so this is the earliest point an anonymized
    # save can be written. Deliberately done before check_host_failure_domain,
    # which can sys.exit: a cluster that fails that check is exactly the kind
    # of surprising state worth having captured, so the save must not be
    # skipped just because the rest of the analysis can't proceed.
    if SAVE_STATE_DIR is not None:
        write_anonymized_state(SAVE_STATE_DIR, _SNAPSHOT_CACHE)

    toofull_pool_ids = {int(pg["pgid"].split(".")[0]) for pg in toofull_pgs}
    pools_by_id = {p["pool_id"]: p for p in pools}
    check_host_failure_domain(
        [pools_by_id[i] for i in toofull_pool_ids if i in pools_by_id],
        crush_rules,
    )

    shards = []
    pgs_with_shards = 0
    for pg in toofull_pgs:
        is_ec = int(pg["pgid"].split(".")[0]) in ec_pool_ids
        found = find_diverted_shards(pg, is_ec)
        shards.extend(found)
        pgs_with_shards += bool(found)
    shards.sort(
        key=lambda s: (
            pgid_sort_key(s.pgid),
            s.shard if isinstance(s.shard, int) else -1,
        )
    )

    candidates = build_candidate_osds(osd_df)
    proposals, unplaceable = assign_targets(
        shards, candidates, osd_host, osd_df, upmap_items
    )

    # Everything informational goes to stderr so stdout stays parseable.
    by_class = ", ".join(
        f"{cls}={len(osds)}" for cls, osds in sorted(candidates.items())
    )
    print(
        f"{len(toofull_pgs)} backfill_toofull PG(s) cluster-wide, "
        f"{pgs_with_shards} with newly-arriving shard(s); "
        f"{len(shards)} shard(s) to divert; "
        f"candidate target OSDs: {by_class or 'none'}",
        file=sys.stderr,
    )

    if proposals:
        if args.pgremapper:
            print_pgremapper(proposals)
        else:
            print_table([format_row(p, osd_host, upmap_items) for p in proposals])

    flagged = [p for p in proposals if p.via_existing_upmap]
    if flagged:
        print(
            f"\nNOTE: {len(flagged)} proposal(s) have a FROM_OSD marked '*': "
            "that OSD is itself the 'to' of an existing upmap pair, so "
            "apply the row by rewriting that pair's 'to' to TARGET_OSD, not "
            "by adding 'FROM_OSD->TARGET_OSD' as a new pair (see "
            "EXISTING_UPMAPS).",
            file=sys.stderr,
        )

    for shard in unplaceable:
        print(
            f"WARNING: no legal target left for {shard.pgid} shard "
            f"{shard.shard} (arriving on osd.{shard.arriving_osd}, class "
            f"{osd_class(osd_df, shard.arriving_osd) or 'unknown'}) — every "
            f"candidate OSD of that class is on a host already in the PG's up "
            f"set, already in its CRUSH mapping, or already used by another PG",
            file=sys.stderr,
        )

    print(
        f"proposed {len(proposals)} remap(s), {len(unplaceable)} unplaceable",
        file=sys.stderr,
    )
    if unplaceable:
        print(
            "NOTE: each target OSD is used at most once, so a cluster-wide run "
            "with more stuck shards than usable OSDs will always leave a tail "
            "unplaceable. Apply these, let them drain, then re-run.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
