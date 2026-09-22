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
needs no argument to say where to look, and works from whatever is in
backfill_toofull right now, whichever hosts are absorbing the shards and
however they got that way.

The strategy does rely on the relevant pools' CRUSH rules failing over at the
host bucket type, so this is checked at startup (see check_host_failure_domain)
and the script exits with an error if it does not hold.

For each stuck shard the script picks an OSD elsewhere in the cluster that
the shard could legally be moved to and that has room to take it. It only
prints the proposed re-targets; it changes nothing. The output is meant to be fed to another
script (or eyeballed) to actually apply the remaps.

How PGs are identified
----------------------
Every PG in backfill_toofull is examined, and a shard of it is a candidate
when it is newly arriving — in the PG's 'up' set but not its 'acting' set.

backfill_toofull is a property of the PG, not of each of its arriving
shards. A ten-shard PG may have one shard wedged on a full host and the rest
backfilling perfectly well, so "this PG is stuck" does not license diverting
everything it is currently taking on. Diverting the healthy ones is worse
than wasted motion: the room on target OSDs is one cluster-wide supply (see
below), so a spurious diversion uses up room that a genuinely stuck shard
then cannot get.

Ceph does not report which shard was refused, so the arriving OSD's
utilization stands in for it: a candidate is kept only if that OSD is at or
above --min-up-util, which defaults to the cluster's own nearfull_ratio.
The test is deliberately not backfillfull_ratio, because Ceph refuses a
backfill on the target's *projected* usage once the shard has landed, not on
its usage today — an OSD several percent below backfillfull_ratio can still
be the one rejecting the reservation. nearfull_ratio is the loosest line
that still excludes OSDs which plainly are not the blocker.

A backfill_toofull PG with no newly-arriving shard yields nothing, and
candidates dropped by --min-up-util are counted separately; both are
reported on stderr so the silence is not ambiguous.

'up'/'acting' are diffed differently per pool type, for the same reason as in
the pg-movements subcommand: EC shards are identified by position, so index i is diffed
against index i and the shard index is reported. Replicated replicas are
interchangeable, so position carries no identity (a same-OSD-set reorder from
primary-affinity or pg-upmap-items is not movement) and the sets are diffed
instead, with SHARD shown as '-'.

--pgs PGID [PGID ...] narrows all of the above to just the given PG(s):
every other backfill_toofull PG in the cluster is treated as though it
were not stuck, so its shards compete for none of the room described
above and are not what the unknown-pool and host-failure-domain checks
(see check_host_failure_domain) validate. This is a filter on which PGs
are examined, not a change to how any one of them is analyzed — useful
for probing a handful of PGs in isolation, or re-running against just
the ones an earlier pass left unplaced. A given id that is not currently
backfill_toofull is reported on stderr, since that usually means a typo.

Which way a row points
----------------------
The table's header has two lines: a group name (ACTING, UP or TARGET) spanning
each OSD/UTIL/HOST triple (TARGET adds a PROJ column, see below), named for
the set the OSD came from. The groups are ordered along the shard's path:

  ACTING OSD  where the shard's data is right now — the backfill's source
  UP OSD      where CRUSH wants it: the arriving OSD, full enough (see
              --min-up-util) to be what is wedging the backfill
  TARGET OSD  where this script proposes it go instead

So data flows ACTING OSD -> UP OSD today and is stuck; applying a row
redirects that to ACTING OSD -> TARGET OSD. UP OSD is the 'from' of the
upmap pair that does the redirecting (and pgremapper's "source osd"), but
that is a direction in the *mapping*, not in the data: nothing is ever
copied off UP OSD.

ACTING OSD often cannot be identified. Once an OSD is out, the slot it left
in 'acting' reads as CRUSH_ITEM_NONE, not as its OSD id, so there is no way
to prove from the PG map where the shard is coming from. That does not
matter here — the arriving side is the one with the space problem, and it
is always observable. ACTING OSD is reported when it happens to be
recoverable and 'none' (with '-' for its utilization and host) otherwise.

Note that UP OSD is the arriving OSD that is *plausibly* the blocker, not
provably so: Ceph reports backfill_toofull per PG without naming the shard
whose reservation was refused, so the row asserts only that this OSD is
full enough to be a candidate for it (see "How PGs are identified").

How targets are chosen
----------------------
Candidates are OSDs that are up, in (reweight > 0), have a non-zero CRUSH
weight, a known device class and a known capacity (without one nothing can be
projected onto them), grouped by that class and sorted by utilization
ascending within each class (OSD id breaks ties, so re-runs are reproducible). Down and out OSDs are
excluded by those filters, which also keeps an out OSD from sorting *first*
— 'ceph osd df' reports one at 0% utilization.

--max-target-util PERCENT is the ceiling on a target's *projected*
utilization (see "How utilization is projected"): a shard is only placed on
a candidate that stays at or below PERCENT once the shard is on it. It
defaults to the cluster's backfillfull_ratio minus one percentage point, a
margin that absorbs the error in the shard size estimate and in the backfills
the projection cannot see. It may not exceed backfillfull_ratio, which Ceph
refuses to backfill past: the script exits with an error if it does. Set too
low it simply leaves shards unplaceable.

A shard is only offered candidates of its *own* device class, that of the OSD
it is arriving on. Pools' CRUSH rules are typically class-constrained, so an
hdd shard sent to an ssd OSD would be an illegal placement.

For each shard, of the candidates whose host is not already used by the PG's
'up' set, the one whose projected utilization is lowest is taken (see "How
utilization is projected"). The arriving OSD is itself a member of 'up', so
the host being diverted away from is always excluded — that is the whole
point of the tool. A candidate is also rejected if it already appears in the
PG's *raw* CRUSH mapping (see below), or if it is not strictly less utilized
than the arriving OSD: a redirect must move the shard somewhere emptier than
where it was headed. That comparison uses the current 'ceph osd df'
utilization of both, and an arriving OSD with no utilization figure imposes
no limit. When even the emptiest eligible candidate is at least as full as
the arriving OSD, the shard is left unplaced. If a PG has several diverted
shards, each target host is added to that PG's exclusion set before its next
shard is placed.

How utilization is projected
----------------------------
An OSD may be the target of several shards, but only up to --max-target-uses
of them (default 5), and only while it has room. Ceph refuses a backfill on
the target's *projected* usage, so each candidate's utilization is projected
as it would be if the shard were placed on it:

  - its usage in 'ceph osd df' (kb_used), plus
  - the size of every shard arriving on it, plus
  - the size of every shard already proposed onto it in this run.

The arriving shards are on their way and are not in kb_used yet. They include
the stuck shards about to be diverted, which count on their arriving OSD
until they are: one that is diverted stops counting there, and one that ends
up unplaceable never does, since it still lands where it was headed. This
makes the projection depend on the order shards are processed (an OSD whose
own stuck shard is diverted only later looks fuller to the shards placed
before that), which errs on the safe side; applying, draining and re-running
picks up what that left over.

A candidate whose projection is above --max-target-util is not eligible for
that shard, so an OSD stops being used once the next shard would fill it
that far. The projection is what the TARGET PROJ column shows for each row:
the target's utilization after this shard and all the ones above it have
completed. That column is why an OSD that appears in several rows is not
mistaken for one that appears once. Candidates are ranked by that same figure,
lowest first (OSD id breaks ties), so an OSD is reused only as the others
fill up.

A shard's size is not reported by Ceph. It is estimated from the PG's
logical size ('num_bytes'): all of it for a replicated pool, 1/k of it for an
erasure-coded one, with k taken from the pool's erasure code profile. Omap,
metadata and EC stripe padding are not counted, so the estimate is slightly
low. The projection can still be optimistic: backfills that are not in
backfill_toofull (running or waiting elsewhere) are not counted, though data
leaving an OSD is not credited either.

The cluster-wide run can still have more stuck shards than the targets have
room for, in which case the tail is left unplaced. The number of shards that
get no target is reported on stderr after the table (in --pgremapper mode
too), without a reason or a list: that the greedy pass found nothing is a
limitation of this heuristic, and a suitable OSD may well exist. Applying the
proposals, letting them drain and re-running is the usual next step.
--max-target-uses 1 gives every OSD at most one shard.

In what order shards are placed
-------------------------------
Since room is scarce, who gets it matters, and it goes to the shards whose
ACTING OSD (the one the data is being backfilled from) is fullest: those are
the OSDs it is most urgent to relieve, and the ones nearest the cluster's
full_ratio. Each turn takes the shard whose acting OSD is projected to be
fullest. A redirect does not itself take data off the acting OSD, but it is
what lets the stalled backfill finish, after which the acting OSD drops its
copy; so placing a shard lowers its acting OSD's projected utilization by the
shard's size, and that OSD's other shards then rank lower. The priority thus
rotates between acting OSDs as shards are placed, where a fixed sort would
spend all the room on the same few.

That projection is kept apart from the target-side one above and never feeds
into it: the space is only freed once the backfill has finished, while Ceph
refuses a backfill when it reserves, so counting it as room on a target would
be optimistic.

A shard whose acting OSD is unknown (the usual out-OSD case, see above) has
no such utilization and goes after every shard whose acting OSD is known. Ties,
and a run where no acting OSD is known, are placed in PG id order, so the
result is reproducible; the rows are printed in PG id order too, whatever
order they were placed in.

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

A subtler case needs no special handling here. The arriving OSD becomes the
'from' of the new upmap pair, and Ceph only honors a 'from' that CRUSH itself
chose; an arriving OSD absent from the raw mapping is one an existing upmap
already put there (the balancer places these). Diverting it means rewriting
that existing pair's 'to' rather than adding a new pair, which is exactly
what 'pgremapper remap' does when its source osd is the 'to' of an existing
pair, so such rows are proposed like any other (see "Applying the output").

Testing against saved cluster state
------------------------------------
By default every run calls the live 'ceph' CLI: the five commands in
SNAPSHOT_COMMANDS, plus the mons-filtered 'ceph pg ls backfill_toofull' (see
fetch_backfill_toofull_pg_stats) rather than a full 'pg dump pgs'.

'backfillctl save-state DIR' captures a cluster's state — anonymized, and
covering every subcommand, not just this one — into DIR, as one '<key>.json'
file per command including a full 'ceph pg dump pgs'. 'backfillctl
--load-state DIR divert-toofull-backfills ...' then reads those files back
instead of calling 'ceph', filtering pg_dump_pgs client-side for the PGs in
backfill_toofull, so a captured state can be replayed offline with no
cluster access. tests/pg-osd/test-data/divert-toofull-backfills-*/ hold
sample captures usable directly as --load-state arguments, each with a
README.txt describing the scenario and what the script should reproduce
from it — the exact table for the small fixtures, and for the cluster-sized
one the counts and invariants its test asserts instead.

Applying the output
-------------------
Three output formats are available. The default is a human-readable table
for review. --import-mappings prints a JSON array for 'pgremapper
import-mappings', one {pgid, mapping: {from, to}} entry per line (from is
UP OSD, to is TARGET OSD; all other output goes to stderr). --pgremapper
instead emits one bare '<pgid> <from osd> <target osd>' line per remap — the
table's PGID, UP OSD and TARGET OSD columns, which are exactly the positional
arguments of 'pgremapper remap' (which calls UP OSD the "source osd", meaning
the upmap's 'from'). In either machine format only the rows go to stdout —
everything else is on stderr — so it stays parseable.

Those machine formats carry no utilization, so nothing in them tells an
operator how full a proposed target is. That is why the capacity check is
built in rather than a flag to remember: by the time the output is being fed
to pgremapper, the only thing standing between the operator and a batch of
remaps that re-wedge is the projection, which never lets a target exceed
--max-target-util (itself capped at backfillfull_ratio).

Apply the proposals with pgremapper, not by hand-writing 'ceph osd
pg-upmap-items' commands. The table deliberately does not carry each PG's
existing upmap pairs, and 'pg-upmap-items' cannot be driven without them:

  - 'ceph osd pg-upmap-items' *replaces* a PG's entire upmap entry rather than
    adding to it, so a command stating only the new pair silently discards
    the PG's other pairs and triggers fresh remapping. Rows are also not
    independent when a PG has more than one diverted shard: one command per
    row makes the last replace what the earlier ones wrote.

  - When UP OSD is itself the 'to' of an existing pair (the balancer places
    these), adding the pair 'UP OSD -> TARGET OSD' is accepted and then
    silently dropped by Ceph, because UP OSD is not an OSD CRUSH chose. The
    existing pair's 'to' has to be rewritten instead.

'pgremapper remap' is per-pair, and per its source (mappingstate.go,
tryRemap) merges a single invocation's pair into the existing entry rather
than replacing it, rewriting an existing pair's 'to' instead of adding a
second one when its source osd is that pair's 'to' — so a single call handles
both cases above. Running it several times in a row for the same PG is a
different matter: a PG can have more than one diverted shard (rare — one PG
out of 51 proposed on the cluster-sized test fixture — but real), and
the stop-backfills-into-osd subcommand's own docstring records a later
'remap' run overwriting the pair an earlier one had just added, on a live
cluster. --import-mappings sidesteps that: import-mappings reads the
cluster's upmaps once and applies every pair of a PG together, so it is the
reliable way to apply the proposals. --pgremapper warns on stderr whenever a
PG needs more than one line, for the same reason stop-backfills-into-osd does.

  - If the upmap balancer is active ('ceph balancer status'), it may undo
    manually placed upmap entries. Consider 'ceph balancer off' while the
    diverted backfills drain.

Review the proposals before applying them. To hand them to pgremapper:

    backfillctl divert-toofull-backfills --import-mappings > mappings.json
    pgremapper-v1.0.0-linux-amd64 import-mappings mappings.json

Give it the file path, not stdin, or its confirmation prompt reads EOF.
--pgremapper's lines are applied differently, one 'remap' call per line:

    backfillctl divert-toofull-backfills --pgremapper > remaps.txt
    xargs -a remaps.txt -L1 pgremapper-v1.0.0-linux-amd64 remap

Use 'xargs -a', not '< remaps.txt': with a redirect, xargs points each child's
stdin at /dev/null, so pgremapper's per-remap confirmation prompt reads EOF
instead of an answer. '-a' leaves stdin on the terminal. (Add '--yes' to
pgremapper to skip the prompt and its dry-run entirely.)
"""

import argparse
import heapq
import json
import math
import sys
from collections import Counter, deque
from typing import NamedTuple

import shared
from shared import (
    KIB,
    POOL_TYPE_ERASURE,
    PgidFilter,
    SnapshotStore,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    fetch_upmap_items,
    is_real_osd,
    osd_cells,
    pgid_pool_id,
    pgid_sort_key,
    print_table,
    rule_failure_domain,
    slot,
)

# Ceph's own defaults for these ratios (OSDMap::build_simple). Used only if
# 'ceph osd dump' somehow omits them, so that --min-up-util and
# --max-target-util still get a sane cluster-independent default rather than
# silently falling back to "no threshold at all", which is the unsafe
# direction for both of them.
DEFAULT_NEARFULL_RATIO = 0.85
DEFAULT_BACKFILLFULL_RATIO = 0.90

# How many shards one OSD may be proposed as the target of (--max-target-uses).
DEFAULT_MAX_TARGET_USES = 5


# Maps each snapshot to the 'ceph ... --format json' command that produces
# it. Keys match the fixtures under tests/pg-osd/test-data/
# divert-toofull-backfills-*/ verbatim, so those directories can be passed
# straight to --load-state. pg_ls_backfill_toofull is what a live run
# actually issues (see fetch_backfill_toofull_pg_stats): a small fraction of
# the full 'pg dump pgs' on a big cluster. --load-state instead reads
# pg_dump_pgs.json -- what 'backfillctl save-state' captures, covering every
# PG -- and filters it client-side for the same PGs.
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
    "pg_dump_pgs": ["ceph", "pg", "dump", "pgs", "--format", "json"],
}


def fetch_backfill_toofull_pg_stats(store: SnapshotStore) -> list[dict]:
    """Return the PGs with 'backfill_toofull' in their state, live or from a snapshot.

    See SNAPSHOT_COMMANDS: live reads pg_ls_backfill_toofull, --load-state
    instead filters pg_dump_pgs client-side by the same state flag.
    """
    if store.load_dir is None:
        return fetch_pg_stats(store, "pg_ls_backfill_toofull")
    return [
        pg
        for pg in fetch_pg_stats(store, "pg_dump_pgs")
        if "backfill_toofull" in pg["state"].split("+")
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def positive_int(text: str) -> int:
    """argparse type: an integer of at least 1."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "divert-toofull-backfills",
        description="Propose upmap re-targets that divert stuck "
        "backfill_toofull PGs to emptier OSDs. Every backfill_toofull PG in "
        "the cluster is examined, and each shard newly arriving on an OSD "
        "full enough to be the one blocking it is offered the least-utilized "
        "OSD of its own device class that it could legally be moved to and "
        "that has room for it and is strictly emptier than the OSD it is arriving on. "
        "Shards whose ACTING OSD is fullest are placed first. "
        "An OSD may be the target of several shards, up to --max-target-uses, "
        "for as long as its projected utilization (counting the shards "
        "already sent to it and those still arriving on it) stays at or below "
        "--max-target-util. "
        "Both thresholds default to the cluster's own "
        "ratios: shards arriving below nearfull_ratio are left alone, and no "
        "target is projected within a point of backfillfull_ratio. Only "
        "the proposals are printed; nothing is changed. Assumes the affected "
        "pools' CRUSH failure domain is 'host', and exits with an error if "
        "it is not.",
        epilog="See the docstring at the top of this script for how targets "
        "are chosen, which shards get skipped and why, and the caveats that "
        "apply when turning these rows into 'pgremapper remap' commands.",
    )
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
        help="Print '<pgid> <from osd> <target osd>' lines with no header "
        "instead of the table, so each line can be passed as the arguments "
        "of 'pgremapper remap' (the script's docstring has the xargs form). "
        "Warns if a PG needs more than one line, since separate runs can "
        "overwrite each other; prefer --import-mappings.",
    )
    parser.add_argument(
        "--min-up-util",
        type=float,
        metavar="PERCENT",
        help="Ceph reports backfill_toofull per PG. It doesn't say which "
        "arriving shard's reservation was refused. A PG can have several "
        "shards arriving at once, and only one of them may be wedged. The "
        "script can't see which one, so it uses the arriving OSD's "
        "utilization as a proxy. A shard is diverted only if its arriving "
        'OSD (the "UP OSD") is at or above the threshold. The default is '
        "the cluster's nearfull_ratio, usually 85%%.",
    )
    parser.add_argument(
        "--max-target-util",
        type=float,
        metavar="PERCENT",
        help="Never let a target's projected utilization (its 'ceph osd df' "
        "usage plus the shards arriving on it and those already proposed "
        "onto it, including the shard being placed) exceed PERCENT. Defaults "
        "to the cluster's backfillfull_ratio - 1; a value above "
        "backfillfull_ratio is an error, since Ceph refuses to backfill past "
        "it. The number of shards left with no eligible target is reported "
        "on stderr.",
    )
    parser.add_argument(
        "--max-target-uses",
        type=positive_int,
        default=DEFAULT_MAX_TARGET_USES,
        metavar="N",
        help="Hard limit on how many shards may be redirected to one OSD "
        "(default: %(default)s). Independently of this limit, an OSD stops "
        "being used once receiving another shard would bring its projected "
        "utilization above --max-target-util; 1 gives every OSD at most one "
        "shard.",
    )
    parser.add_argument(
        "--pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="Restrict analysis to only these PG id(s): every other "
        "backfill_toofull PG is ignored, as if it were not stuck. "
        "Space-separated, e.g. --pgs 19.92e 20.1a3. A given id that is not "
        "currently backfill_toofull is reported on stderr, since that "
        "usually means a typo.",
    )
    return parser


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


class FullRatios(NamedTuple):
    """The cluster's fullness thresholds, as percentages.

    These are what --min-up-util and --max-target-util default to, so
    the script's two safety margins track whatever the cluster itself
    considers 'getting full' and 'too full to backfill onto' rather than
    hard-coded numbers that would be wrong on a tuned cluster.
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


def ec_pool_ids_from(pools: list[dict]) -> set[int]:
    """Return the set of pool ids that are erasure-coded (type == 3)."""
    return {p["pool_id"] for p in pools if p.get("type") == POOL_TYPE_ERASURE}


# ---------------------------------------------------------------------------
# Failure domain validation
# ---------------------------------------------------------------------------


def check_host_failure_domain(pools: list[dict], crush_rules: dict[int, dict]) -> None:
    """Exit with an error unless every given pool's CRUSH rule fails over at host.

    The diversion strategy this script implements (see module docstring)
    only makes sense if a failed leaf is retried within the same host
    bucket, since that is what concentrates the re-placed shards onto one
    already-full host and makes moving them to a host outside the PG's up
    set the fix. If a pool's rule fails over at some other bucket type,
    excluding hosts is neither the constraint CRUSH enforces for it nor the
    one that would unwedge it.
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
            "ERROR: this script assumes the CRUSH failure domain of every "
            "pool with a stuck PG is 'host' (see module docstring), but the "
            f"following pool(s) do not:\n{lines}"
        )


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


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
    return {osd_id for osd_id in raw if is_real_osd(osd_id)}


def shard_size_bytes(pg: dict, pool: dict, ec_profiles: dict[str, dict]) -> int:
    """Estimate the bytes one shard of a PG occupies on its OSD (see shared).

    Exits with an error where shared.shard_size_bytes reports "unknown": every
    diversion's utilization projection needs a size, and a silent 0 would
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


class DivertedShard(NamedTuple):
    """A shard newly arriving on a host that is too full to take it."""

    pgid: str
    shard: "int | str"  # EC shard index, or '-' for replicated pools
    up_osd: int  # OSD the shard is arriving on: in 'up', not yet in
    # 'acting'. This is the 'from' of the upmap that would
    # divert it, and its host is the one being diverted away
    # from. Despite that 'from', no data flows off it.
    acting_osd: int | None  # OSD still holding the shard, i.e. the
    # backfill's data source, or None when the slot reads as
    # CRUSH_ITEM_NONE (the usual out-OSD case)
    up_set: list  # the PG's full up set, for host exclusions and for
    # reconstructing the raw CRUSH mapping
    size_bytes: int = 0  # what the shard will occupy once backfilled, see
    # shard_size_bytes; 0 means "not known", which projects no usage


def filter_toofull_pgs(
    pgs: list[dict], wanted: set[str]
) -> tuple[list[dict], set[str]]:
    """Restrict pgs to only those whose pgid is in wanted (the --pgs filter).

    Returns (kept, matched), where matched is the subset of wanted that was
    actually found, so the caller can report the rest as likely typos.
    """
    matched = {pg["pgid"] for pg in pgs if pg["pgid"] in wanted}
    kept = [pg for pg in pgs if pg["pgid"] in wanted]
    return kept, matched


def find_diverted_shards(
    pg: dict, is_ec: bool, size_bytes: int = 0
) -> list[DivertedShard]:
    """Return the shards of one PG that are newly arriving on their up OSD.

    size_bytes is the size of each of them (all shards of a PG are the same
    size), recorded on the result for the utilization projection.
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
            found.append(DivertedShard(pgid, i, up_osd, acting_osd, up, size_bytes))
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
            found.append(DivertedShard(pgid, "-", up_osd, acting_osd, up, size_bytes))

    return found


def select_stuck_shards(
    shards: list[DivertedShard],
    osd_df: dict[int, dict],
    min_up_util: float,
) -> tuple[list[DivertedShard], list[DivertedShard]]:
    """Split arriving shards into those worth diverting and those to leave be.

    backfill_toofull is reported per PG, but a PG can have several shards
    arriving at once and only one of them refused. Ceph does not say which,
    so a shard is treated as the stuck one only when the OSD it is arriving
    on is itself at or above min_up_util (see module docstring for why
    that defaults to nearfull_ratio and not backfillfull_ratio).

    This is not merely cosmetic. The room on target OSDs is one
    cluster-wide supply, so diverting a shard that was never blocked uses up
    room that a genuinely stuck shard then cannot have.

    Returns (stuck, skipped), preserving the input order in both.
    """
    stuck, skipped = [], []
    for shard in shards:
        util = osd_df.get(shard.up_osd, {}).get("utilization")
        # An arriving OSD missing from 'ceph osd df' cannot be ruled out as
        # the blocker, so keep it rather than silently dropping the shard.
        if util is None or util >= min_up_util:
            stuck.append(shard)
        else:
            skipped.append(shard)
    return stuck, skipped


# ---------------------------------------------------------------------------
# Target selection
# ---------------------------------------------------------------------------


def osd_class(osd_df: dict[int, dict], osd_id: int) -> str | None:
    """Return an OSD's device class, or None if it is unknown."""
    return osd_df.get(osd_id, {}).get("device_class")


def build_candidate_osds(osd_df: dict[int, dict]) -> dict[str, list[int]]:
    """Return usable target OSD ids per device class, least-utilized first.

    Excludes OSDs that are down, out (reweight 0) or have no CRUSH weight.
    That also covers the OSD whose failure caused the pile-up in the first
    place: without it an out OSD would sort to the very front, since
    'ceph osd df' reports one at 0% utilization. An OSD with no utilization
    figure at all is excluded for the same reason — it cannot be ranked, and
    treating a missing value as 0% would make it the first pick.

    Keyed by device class because a shard may only be diverted to an OSD of
    its own class (see module docstring); each class's OSDs fill up
    independently.
    """
    usable = [
        node
        for node in osd_df.values()
        if node.get("status") == "up"
        and node.get("reweight", 0) > 0
        and node.get("crush_weight", 0) > 0
        and node.get("device_class")
        and node.get("kb")  # capacity unknown: nothing can be projected onto it
        and node.get("utilization") is not None
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


class SourcePressure:
    """How full each shard's ACTING OSD is projected to be, for ordering shards.

    Target room is scarce, so which shards get it matters. The pressure a shard
    is under is how full the OSD its data sits on is: relieving the fullest
    ones first is what matters most. A redirect does not move data off that
    OSD by itself, but it is what lets the stalled backfill finish, after
    which the acting OSD drops its copy; so placing a shard lowers its acting
    OSD's projected utilization by the shard's size (relieve()), and that OSD's
    other shards then rank lower.

    Deliberately separate from ProjectedUsage and never fed back into it:
    that space frees up only once the backfill has finished, whereas Ceph
    refuses a backfill at reservation time, so crediting it to a target would
    be optimistic.
    """

    def __init__(self, osd_df: dict[int, dict]):
        self._used, self._capacity = usage_and_capacity(osd_df)

    def utilization(self, shard: DivertedShard) -> float:
        """Return the shard's acting OSD's projected utilization (percent).

        -inf when it is not known (the usual out-OSD case, where the acting
        slot reads as empty), which ranks the shard behind every known one.
        """
        osd_id = shard.acting_osd
        if osd_id not in self._used:
            return -math.inf
        return self._used[osd_id] / self._capacity[osd_id] * 100

    def relieve(self, shard: DivertedShard) -> None:
        """Record that shard is placed, so its acting OSD will lose it."""
        if shard.acting_osd in self._used:
            self._used[shard.acting_osd] -= shard.size_bytes


class ProjectedUsage:
    """What each OSD will hold once the backfills already in motion complete.

    Starts from the usage 'ceph osd df' reports plus the size of every shard
    still arriving on the OSD: those bytes are not in 'kb_used' yet, and it is
    the emptiest OSDs, the ones most attractive as targets, that have the
    most in flight. That includes the stuck shards this script is about to
    divert: until one is actually diverted it is still headed for its OSD, so
    a shard that ends up unplaceable keeps counting there. redirect() then
    takes a diverted shard's size off the OSD it was headed for and puts it on
    its target.

    The counting is order-dependent: an OSD whose own stuck shard is diverted
    only later in the run looks fuller to the shards placed before that.
    That errs on the safe side, so the run leaves room for a re-run.

    Not accounted for: data leaving an OSD (never credited) and backfills that
    are not in backfill_toofull (unknown to this script), so the projection
    can still be optimistic. Ceph itself refuses a backfill on the target's
    projected usage rather than today's (see module docstring).
    """

    def __init__(self, osd_df: dict[int, dict], arriving: list[DivertedShard]):
        self._used, self._capacity = usage_and_capacity(osd_df)
        for shard in arriving:
            if shard.up_osd in self._used:
                self._used[shard.up_osd] += shard.size_bytes

    def utilization_after(self, osd_id: int, extra_bytes: int) -> float:
        """Return the OSD's projected utilization (percent) with extra_bytes more."""
        return (self._used[osd_id] + extra_bytes) / self._capacity[osd_id] * 100

    def redirect(self, shard: DivertedShard, target_osd: int) -> None:
        """Record that shard goes to target_osd instead of its UP OSD."""
        if shard.up_osd in self._used:
            self._used[shard.up_osd] -= shard.size_bytes
        self._used[target_osd] += shard.size_bytes


class Proposal(NamedTuple):
    shard: DivertedShard
    target_osd: int
    target_host: str
    target_utilization: float  # current, from 'ceph osd df'
    target_projected: float  # once this and all earlier proposals have completed


def assign_targets(
    shards: list[DivertedShard],
    candidates: dict[str, list[int]],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    upmap_items: dict[str, list[dict]],
    *,
    projection: ProjectedUsage,
    max_uses: int,
    max_target_util: float,
) -> tuple[list[Proposal], list[DivertedShard]]:
    """Greedily give each diverted shard the legal target that ends up emptiest.

    Legal means: same device class, host and OSD not already used by the PG,
    strictly less utilized than the shard's UP OSD (currently, as in
    'ceph osd df'), not already the target of max_uses shards, and at or
    below max_target_util (percent) once this shard has been added to what
    the projection says it will hold. Of the legal candidates the one with the
    lowest such projected utilization wins (OSD id breaks ties), so
    reusing an OSD only happens as the others fill up.

    The shards are not taken in the order given: each turn takes the one whose
    ACTING OSD is projected to be fullest (see SourcePressure), so the
    scarce target room goes to relieving the fullest sources, and that
    priority rotates as shards are placed. Ties, and shards whose acting OSD is
    unknown, go in the order given, so the caller's order is what makes a run
    reproducible. Returns (proposals, unplaceable shards), each in the order
    given regardless of the order they were placed in.
    """
    uses: Counter[int] = Counter()
    pressure = SourcePressure(osd_df)
    # Hosts already spoken for per PG: seeded from the up set, then extended
    # as each of the PG's shards is placed, so a PG with two diverted shards
    # cannot be given two targets on one host.
    blocked_hosts: dict[str, set[str]] = {}
    proposed: dict[int, Proposal] = {}
    unplaceable = set()

    # A shard's priority is its acting OSD's utilization, and placing a shard
    # changes that for that OSD's shards only. So shards are queued per acting
    # OSD (each queue in the order given) and it is the queues that are
    # ranked, in a heap holding exactly one entry per non-empty queue: no
    # entry ever goes stale. Ranking is fullest first, the earliest waiting
    # shard breaking ties, which is the same as picking the best shard
    # overall.
    queues: dict[int | None, deque[int]] = {}
    for i, shard in enumerate(shards):
        queues.setdefault(shard.acting_osd, deque()).append(i)
    heap = [
        (-pressure.utilization(shards[q[0]]), q[0], acting)
        for acting, q in queues.items()
    ]
    heapq.heapify(heap)

    while heap:
        _, i, acting = heapq.heappop(heap)
        queue = queues[acting]
        queue.popleft()
        shard = shards[i]

        if shard.pgid not in blocked_hosts:
            blocked_hosts[shard.pgid] = {
                osd_host.get(o) for o in shard.up_set if is_real_osd(o)
            }
        forbidden_hosts = blocked_hosts[shard.pgid]
        raw = raw_crush_osds(shard.up_set, upmap_items.get(shard.pgid, []))
        # 'up' alone is not enough: an OSD displaced by an existing upmap is
        # absent from 'up' but still in the raw mapping, and re-proposing it
        # would put the same OSD in the mapping twice — which Ceph's upmap
        # validation drops silently (see module docstring).
        forbidden_osds = raw | {o for o in shard.up_set if is_real_osd(o)}

        # Only OSDs of the arriving OSD's own class are legal targets. An
        # unknown class yields an empty pool, so the shard falls through to
        # unplaceable rather than being sent somewhere CRUSH would reject.
        pool = candidates.get(osd_class(osd_df, shard.up_osd), [])
        # A target must be strictly emptier than the OSD being diverted from,
        # or the redirect gains nothing. Unknown UP utilization cannot be
        # compared, so it imposes no limit (as in select_stuck_shards).
        max_util = osd_df.get(shard.up_osd, {}).get("utilization")
        legal = []
        for candidate in pool:
            # Pool is sorted by utilization, and a projection is never below
            # the current figure: everything from here on is over the cap.
            if osd_df[candidate]["utilization"] > max_target_util:
                break
            if osd_host.get(candidate) in forbidden_hosts:
                continue
            if candidate in forbidden_osds:
                continue
            if uses[candidate] >= max_uses:
                continue
            if max_util is not None and osd_df[candidate]["utilization"] >= max_util:
                continue
            projected = projection.utilization_after(candidate, shard.size_bytes)
            if projected > max_target_util:
                continue
            legal.append((projected, candidate))

        if legal:
            projected, target = min(legal)
            projection.redirect(shard, target)
            pressure.relieve(shard)
            uses[target] += 1
            forbidden_hosts.add(osd_host.get(target))
            proposed[i] = Proposal(
                shard,
                target,
                osd_host.get(target, "?"),
                osd_df[target]["utilization"],
                projected,
            )
        else:
            unplaceable.add(i)

        # The queue's rank may have changed (placing relieved it); requeue it.
        if queue:
            heapq.heappush(
                heap, (-pressure.utilization(shards[queue[0]]), queue[0], acting)
            )

    return (
        [proposed[i] for i in sorted(proposed)],
        [shards[i] for i in sorted(unplaceable)],
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# Ordered so each row reads along the shard's path: where its data is now
# (ACTING), where the stalled backfill is trying to put it (UP), and where
# this script proposes it go instead (TARGET), with each OSD followed by its
# utilization and host; TARGET also gets PROJ, the utilization it is projected
# to reach once this and all earlier rows have completed (see ProjectedUsage),
# which is what tells an OSD used by several rows apart from one used once.
# Each entry is (group, label); the header is printed
# on two lines, the group name spanning its columns above their labels, and
# an empty group means the column has no group line. print_table leaves the
# final column unpadded.
COLUMNS = [
    ("", "PGID"),
    ("", "SHARD"),
    ("ACTING", "OSD"),
    ("ACTING", "UTIL"),
    ("ACTING", "HOST"),
    ("UP", "OSD"),
    ("UP", "UTIL"),
    ("UP", "HOST"),
    ("TARGET", "OSD"),
    ("TARGET", "UTIL"),
    ("TARGET", "PROJ"),
    ("TARGET", "HOST"),
]


def format_row(
    proposal: Proposal,
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
) -> list[str]:
    shard = proposal.shard
    return [
        shard.pgid,
        str(shard.shard),
        # An unknown acting OSD (the usual out-OSD case) reads 'none', '-', '-'.
        *osd_cells(osd_df, osd_host, shard.acting_osd),
        # The UP host varies per row now that the whole cluster is scanned, so
        # unlike the single-host version it cannot live in the stderr header.
        *osd_cells(osd_df, osd_host, shard.up_osd),
        f"osd.{proposal.target_osd}",
        f"{proposal.target_utilization:.1f}%",
        f"{proposal.target_projected:.1f}%",
        proposal.target_host,
    ]


def print_unplaceable(count: int) -> None:
    """Report on stderr how many shards no target was found for.

    Only the count, not the shards: a big run would bury the table's tail.
    Deliberately says nothing about why: the heuristic gave up on these, but
    that does not mean no legal placement exists, only that finding one is
    beyond what this script implements.
    """
    print(
        f"{count} shard(s) could not be placed. This is a limitation of the "
        "heuristic used here, not proof that no suitable OSD exists; finding "
        "one is not implemented.",
        file=sys.stderr,
    )


def print_pgremapper(proposals: list[Proposal]) -> None:
    """Print one '<pgid> <from osd> <target osd>' line per proposal.

    These are the positional arguments of 'pgremapper remap', in order and
    with nothing else on the line, so the output can be fed to it directly.
    pgremapper's "source osd" is the upmap's 'from', i.e. the table's
    UP OSD — not ACTING OSD, which is where the data actually sits. OSD ids
    are bare integers: pgremapper parses them with strconv.Atoi and rejects
    the 'osd.N' form the table uses.
    """
    for proposal in proposals:
        print(f"{proposal.shard.pgid} {proposal.shard.up_osd} {proposal.target_osd}")


def print_import_mappings(proposals: list[Proposal]) -> None:
    """Print the proposals as JSON for 'pgremapper import-mappings'.

    One {pgid, mapping: {from, to}} entry per proposal (from is UP OSD, the
    upmap's 'from'; to is TARGET OSD), in a JSON array with one entry per
    line, so it is easy to read and to prune with jq ("[]" when there are
    none, so the output is always valid JSON). import-mappings reads the
    cluster's upmaps once and applies all pairs of a PG together, so unlike
    separate 'pgremapper remap' runs the pairs of a PG cannot overwrite each
    other (see module docstring).
    """
    if not proposals:
        print("[]")
        return
    print("[")
    for i, proposal in enumerate(proposals):
        entry = {
            "pgid": proposal.shard.pgid,
            "mapping": {"from": proposal.shard.up_osd, "to": proposal.target_osd},
        }
        print(f"  {json.dumps(entry)}{',' if i < len(proposals) - 1 else ''}")
    print("]")


def pgs_needing_several_targets(proposals: list[Proposal]) -> list[str]:
    """Return the PGs (in PG order) with more than one proposed remap."""
    counts = Counter(p.shard.pgid for p in proposals)
    return sorted((pgid for pgid, n in counts.items() if n > 1), key=pgid_sort_key)


def warn_separate_remaps(pgids: list[str]) -> None:
    """Warn on stderr that these PGs are not safe to apply with 'remap' lines.

    'pgremapper remap' merges a single invocation's pair into a PG's existing
    upmap entry (see module docstring), but running it once per line for a PG
    with several proposed remaps means several invocations against the same
    PG, and the stop-backfills-into-osd subcommand's own docstring records a
    later run overwriting the pair an earlier one had just added, on a live
    cluster.
    """
    shown = ", ".join(pgids[:8]) + (
        f", ... ({len(pgids)} in all)" if len(pgids) > 8 else ""
    )
    print(
        f"WARNING: {len(pgids)} PG(s) have more than one proposed remap "
        f"({shown}). Running these lines as separate 'pgremapper remap' "
        "commands (e.g. xargs -L1) can silently lose pairs: a later run may "
        "overwrite the pair an earlier one just added. Use --import-mappings "
        "instead, which applies all pairs of a PG together.",
        file=sys.stderr,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


class DivertResult(NamedTuple):
    """Everything a run decides, independent of how it is printed.

    plan() computes it and render() prints it, so tests of the planning can
    assert on these fields and survive changes to the output format. osd_df
    and osd_host are carried along only because the table shows them.
    """

    proposals: list[Proposal]  # in PG, then shard, order
    unplaceable: list[DivertedShard]
    toofull_pg_count: int  # backfill_toofull PGs considered (after --pgs)
    pgs_with_shards: int  # of those, the ones with a newly-arriving shard
    arriving_count: int  # arriving shards in all
    stuck_count: int  # arriving on an OSD at or above min_up_util
    left_alone_count: int  # arriving on an OSD below it: not the blocker
    candidates: dict[str, list[int]]  # see build_candidate_osds
    min_up_util: float
    max_target_util: float
    ratios: FullRatios
    pgs_filter: PgidFilter | None  # None without --pgs
    osd_df: dict[int, dict]
    osd_host: dict[int, str]


def plan(args: argparse.Namespace, store: SnapshotStore) -> DivertResult:
    """Fetch the cluster state from store and work out what to divert where.

    Exits with an error message when the state cannot be analyzed safely
    (see check_host_failure_domain and the unknown-pool check). The only output
    is the --pgs note (see print_pgs_filter), printed as soon as the filter
    runs so that the ids that matched nothing are named even if planning then
    exits: a typo can be what trips one of those errors.
    """
    osd_host = fetch_osd_hosts(store)
    osd_df = fetch_osd_df(store)
    upmap_items = fetch_upmap_items(store)
    pools_by_id = fetch_pools(store)
    pools = list(pools_by_id.values())
    ec_pool_ids = ec_pool_ids_from(pools)
    toofull_pgs = fetch_backfill_toofull_pg_stats(store)
    crush_rules = fetch_crush_rules(store)
    ec_profiles = fetch_ec_profiles(store)

    # Both thresholds track the cluster's own idea of full unless overridden.
    ratios = fetch_full_ratios(store)
    min_up_util = ratios.nearfull if args.min_up_util is None else args.min_up_util
    max_target_util = (
        ratios.backfillfull - 1
        if args.max_target_util is None
        else args.max_target_util
    )
    if not 0 < max_target_util <= ratios.backfillfull:
        sys.exit(
            f"ERROR: max target utilization {max_target_util:g}% (--max-target-util, "
            "or its backfillfull_ratio - 1 default) must be above 0 and "
            f"at most the cluster's backfillfull_ratio ({ratios.backfillfull:g}%): "
            "Ceph refuses to backfill onto an OSD past that, so a higher cap "
            "would let the script propose targets that re-wedge."
        )

    pgs_filter = None
    if args.pgs:
        wanted_pgs = set(args.pgs)
        toofull_pgs, matched_pgs = filter_toofull_pgs(toofull_pgs, wanted_pgs)
        pgs_filter = PgidFilter(
            len(wanted_pgs), len(matched_pgs), sorted(wanted_pgs - matched_pgs)
        )
        print_pgs_filter(pgs_filter)

    toofull_pool_ids = {pgid_pool_id(pg["pgid"]) for pg in toofull_pgs}
    # A pool that has stuck PGs but is absent from 'ceph osd pool ls detail'
    # would skip the failure-domain check and, not being in ec_pool_ids, have
    # its EC shards diffed as interchangeable replicas. Both failures are
    # silent and produce plausible-looking rows, so refuse instead.
    unknown_pools = sorted(toofull_pool_ids - pools_by_id.keys())
    if unknown_pools:
        sys.exit(
            "ERROR: backfill_toofull PGs belong to pool id(s) "
            f"{', '.join(map(str, unknown_pools))}, which 'ceph osd pool ls "
            "detail' does not list. Their CRUSH rule and pool type are "
            "unknown, so their shards cannot be analyzed correctly."
        )
    check_host_failure_domain(
        [pools_by_id[i] for i in sorted(toofull_pool_ids)], crush_rules
    )

    arriving = []
    pgs_with_shards = 0
    for pg in toofull_pgs:
        pool_id = pgid_pool_id(pg["pgid"])
        found = find_diverted_shards(
            pg,
            pool_id in ec_pool_ids,
            shard_size_bytes(pg, pools_by_id[pool_id], ec_profiles),
        )
        arriving.extend(found)
        pgs_with_shards += bool(found)
    shards, not_full_enough = select_stuck_shards(arriving, osd_df, min_up_util)
    shards.sort(
        key=lambda s: (
            pgid_sort_key(s.pgid),
            s.shard if isinstance(s.shard, int) else -1,
        )
    )

    candidates = build_candidate_osds(osd_df)
    # Every arriving shard counts towards its OSD until it is diverted.
    projection = ProjectedUsage(osd_df, arriving)
    proposals, unplaceable = assign_targets(
        shards,
        candidates,
        osd_host,
        osd_df,
        upmap_items,
        projection=projection,
        max_uses=args.max_target_uses,
        max_target_util=max_target_util,
    )
    return DivertResult(
        proposals=proposals,
        unplaceable=unplaceable,
        toofull_pg_count=len(toofull_pgs),
        pgs_with_shards=pgs_with_shards,
        arriving_count=len(arriving),
        stuck_count=len(shards),
        left_alone_count=len(not_full_enough),
        candidates=candidates,
        min_up_util=min_up_util,
        max_target_util=max_target_util,
        ratios=ratios,
        pgs_filter=pgs_filter,
        osd_df=osd_df,
        osd_host=osd_host,
    )


def print_pgs_filter(pgs_filter: PgidFilter) -> None:
    """Report on stderr what --pgs matched, naming the ids that matched nothing."""
    print(
        f"--pgs: {pgs_filter.matched} of {pgs_filter.given} given PG id(s) "
        "are currently backfill_toofull and will be the only ones "
        "considered"
        + (
            f"; {len(pgs_filter.unmatched)} matched nothing (check for typos): "
            + ", ".join(pgs_filter.unmatched)
            if pgs_filter.unmatched
            else ""
        ),
        file=sys.stderr,
    )


def render(result: DivertResult, args: argparse.Namespace) -> None:
    """Print result: proposals on stdout in the format args asks for, notes on stderr."""
    # Everything informational goes to stderr so stdout stays parseable.
    osd_df = result.osd_df
    max_target_util = result.max_target_util
    by_class = ", ".join(
        f"{cls}={sum(osd_df[o]['utilization'] <= max_target_util for o in osds)}"
        f"/{len(osds)}"
        for cls, osds in sorted(result.candidates.items())
    )
    print(
        f"{result.toofull_pg_count} backfill_toofull PG(s) cluster-wide, "
        f"{result.pgs_with_shards} with newly-arriving shard(s); "
        f"{result.arriving_count} arriving shard(s), of which "
        f"{result.stuck_count} on an OSD "
        f"at or above --min-up-util {result.min_up_util:g}% "
        f"({result.left_alone_count} left alone as not the blocker); "
        f"candidate target OSDs at or below the cap now / in all: "
        f"{by_class or 'none'}; each may take up to "
        f"--max-target-uses {args.max_target_uses} shard(s), while its "
        f"projected utilization stays at or below --max-target-util "
        f"{max_target_util:g}% "
        f"(backfillfull_ratio {result.ratios.backfillfull:g}%)",
        file=sys.stderr,
    )

    proposals, unplaceable = result.proposals, result.unplaceable
    if args.import_mappings:
        print_import_mappings(proposals)
    elif proposals:
        if args.pgremapper:
            print_pgremapper(proposals)
            if multi := pgs_needing_several_targets(proposals):
                warn_separate_remaps(multi)
        else:
            print_table(
                COLUMNS,
                [format_row(p, result.osd_host, osd_df) for p in proposals],
            )

    if unplaceable:
        print_unplaceable(len(unplaceable))

    print(
        f"proposed {len(proposals)} remap(s), {len(unplaceable)} unplaceable",
        file=sys.stderr,
    )
    if unplaceable:
        print(
            "NOTE: each target OSD takes at most --max-target-uses shards and "
            "none is filled past --max-target-util, so a run with more stuck "
            "shards than that leaves room for will leave a tail unplaceable. "
            "Apply these, let them drain, then re-run.",
            file=sys.stderr,
        )


def run(args: argparse.Namespace) -> None:
    render(plan(args, SnapshotStore.from_args(args, SNAPSHOT_COMMANDS)), args)
