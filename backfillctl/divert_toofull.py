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
the show-backfill subcommand: EC shards are identified by position, so index i is diffed
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
get no target is reported on stderr after the table, without a reason or a
list: that the greedy pass found nothing is a limitation of this heuristic,
and a suitable OSD may well exist. Applying the proposals, letting them drain
and re-running is the usual next step.
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
--load-state DIR divert-toofull ...' then reads those files back
instead of calling 'ceph', filtering pg_dump_pgs client-side for the PGs in
backfill_toofull, so a captured state can be replayed offline with no
cluster access. tests/pg-osd/test-data/divert-toofull-*/ hold
sample captures usable directly as --load-state arguments, each with a
README.txt describing the scenario and what the script should reproduce
from it — the exact table for the small fixtures, and for the cluster-sized
one the counts and invariants its test asserts instead.

Applying the output
-------------------
Two output formats are available. The default is a human-readable table for
review. --pgremapper-mappings prints a JSON array for 'pgremapper
import-mappings' instead, one {pgid, mapping: {from, to}} entry per line
(from is UP OSD, to is TARGET OSD; all other output goes to stderr), so only
the rows go to stdout — everything else is on stderr — and it stays
parseable.

The machine format carries no utilization, so nothing in it tells an operator
how full a proposed target is. That is why the capacity check is built in
rather than a flag to remember: by the time the output is being fed to
pgremapper, the only thing standing between the operator and a batch of
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

'pgremapper import-mappings' avoids both: dry runs (pgremapper 1.0.0) showed
that it reads a PG's existing upmap once and applies all its proposed pairs
together as one combined change, keeping the PG's existing pairs and
rewriting an existing pair's 'to' instead of adding a second one when its
source osd is that pair's 'to'. That also makes it the reliable way to handle
a PG with more than one diverted shard (rare — one PG out of 51 proposed on
the cluster-sized test fixture — but real): applied together, its pairs
cannot overwrite each other the way a sequence of separate commands could.

  - If the upmap balancer is active ('ceph balancer status'), it may undo
    manually placed upmap entries. Consider 'ceph balancer off' while the
    diverted backfills drain.

Review the proposals before applying them. To hand them to pgremapper:

    backfillctl divert-toofull --pgremapper-mappings > mappings.json
    pgremapper-v1.0.0-linux-amd64 import-mappings mappings.json

Give it the file path, not stdin, or its confirmation prompt reads EOF. (Add
'--yes' to pgremapper to skip the prompt and its dry-run entirely.)
"""

import argparse
import heapq
import json
import math
import sys
from collections import Counter, deque
from typing import NamedTuple

from placement import (
    ArrivingShard,
    FullRatios,
    ProjectedUsage,
    add_target_args,
    build_candidate_osds,
    check_host_failure_domain,
    ec_pool_ids_from,
    fetch_full_ratios,
    find_arriving_shards,
    osd_class,
    pick_target,
    raw_crush_osds,
    resolve_max_target_util,
    shard_size_bytes,
    usage_and_capacity,
)
from shared import (
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
)

# Maps each snapshot to the 'ceph ... --format json' command that produces
# it. Keys match the fixtures under tests/pg-osd/test-data/
# divert-toofull-*/ verbatim, so those directories can be passed
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


def build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "divert-toofull",
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
        "apply when applying these proposals with 'pgremapper import-mappings'.",
    )
    add_target_args(
        parser,
        max_target_util_help="Never let a target's projected utilization "
        "(its 'ceph osd df' usage plus the shards arriving on it and those "
        "already proposed onto it, including the shard being placed) exceed "
        "PERCENT. Defaults to the cluster's backfillfull_ratio - 1; a value "
        "above backfillfull_ratio is an error, since Ceph refuses to backfill "
        "past it. The number of shards left with no eligible target is "
        "reported on stderr.",
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


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


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


def select_stuck_shards(
    shards: list[ArrivingShard],
    osd_df: dict[int, dict],
    min_up_util: float,
) -> tuple[list[ArrivingShard], list[ArrivingShard]]:
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

    def utilization(self, shard: ArrivingShard) -> float:
        """Return the shard's acting OSD's projected utilization (percent).

        -inf when it is not known (the usual out-OSD case, where the acting
        slot reads as empty), which ranks the shard behind every known one.
        """
        osd_id = shard.acting_osd
        if osd_id not in self._used:
            return -math.inf
        return self._used[osd_id] / self._capacity[osd_id] * 100

    def relieve(self, shard: ArrivingShard) -> None:
        """Record that shard is placed, so its acting OSD will lose it."""
        if shard.acting_osd in self._used:
            self._used[shard.acting_osd] -= shard.size_bytes


class Proposal(NamedTuple):
    shard: ArrivingShard
    target_osd: int
    target_host: str
    target_utilization: float  # current, from 'ceph osd df'
    target_projected: float  # once this and all earlier proposals have completed


def assign_targets(
    shards: list[ArrivingShard],
    candidates: dict[str, list[int]],
    osd_host: dict[int, str],
    osd_df: dict[int, dict],
    upmap_items: dict[str, list[dict]],
    *,
    projection: ProjectedUsage,
    max_uses: int,
    max_target_util: float,
) -> tuple[list[Proposal], list[ArrivingShard]]:
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
        picked = pick_target(
            pool,
            shard.size_bytes,
            forbidden_hosts=forbidden_hosts,
            forbidden_osds=forbidden_osds,
            osd_host=osd_host,
            osd_df=osd_df,
            projection=projection,
            uses=uses,
            max_uses=max_uses,
            max_target_util=max_target_util,
            below_util=osd_df.get(shard.up_osd, {}).get("utilization"),
        )

        if picked:
            projected, target = picked
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


def print_pgremapper_mappings(proposals: list[Proposal]) -> None:
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
    unplaceable: list[ArrivingShard]
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
    max_target_util = resolve_max_target_util(args.max_target_util, ratios)

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
        [pools_by_id[i] for i in sorted(toofull_pool_ids)],
        crush_rules,
        "with a stuck PG",
    )

    arriving = []
    pgs_with_shards = 0
    for pg in toofull_pgs:
        pool_id = pgid_pool_id(pg["pgid"])
        found = find_arriving_shards(
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
    if args.pgremapper_mappings:
        print_pgremapper_mappings(proposals)
    elif proposals:
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
