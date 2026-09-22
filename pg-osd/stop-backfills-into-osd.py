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
that stop backfills into OTHER OSDs (see "Companion pins"). It changes
nothing. It does not decide which backfills to keep: review the output and
delete the entries for the ones you want to let proceed, or pass their PG ids
to --exclude-pgs to leave them out of the output (and its companions and
blockers) from the start.

That covers the script's name: by default it does exactly and only what
"stop backfills into an OSD" says. It has a second, less obvious job, off by
default and enabled with --pin-blockers: making sure that whichever backfills
you end up keeping (by deleting their entries) actually run, rather than
sitting in backfill_toofull because of some unrelated shard of the same PG
(see "Blockers"). Without --pin-blockers the tool never looks for those, so a
kept backfill can still be stuck for a reason the output never mentions.

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
Ceph checks the failure domain on the 'up' set (CRUSH output plus upmaps), not
on 'acting', which is just where the data is now. A PG's backfills run together
and 'acting' switches to 'up' only once ALL of them have finished, so a shard
that is still moving never adds a same-host shard to 'acting': in the shard 1
(acting on host H) and shard 4 (moving to host H) example, H holds a partial
copy of shard 4 that does not count as redundancy until the switch, and by then
shard 1 has left H. That co-location during the move is harmless, so it is NOT
what companions are about.

What matters is that a pin is only accepted if the resulting 'up' set is valid:
Ceph drops an upmap that puts two shards of a PG on one host (the pool's failure
domain, which must be 'host') or one OSD twice. Pinning shard 1 back to its
acting OSD on H while shard 4 is still headed for H makes exactly that 'up' set,
and permanently, not just during the move. CRUSH re-placed the two together, so
pinning only one leaves the clash and the upmap is silently dropped. The other
shard is then pinned back as well, listed as a "companion of shard N" (which
can chain, if its acting OSD clashes with a third shard). In other words: you
cannot cancel shard 1's move and keep shard 4's, because no valid 'up' set has
both.
Companions are backfills into *other* OSDs that you did not ask about: check
the NOTE column, and note that dropping a companion entry invalidates the pin
it goes with. (With --pin-blockers, a companion whose target is also over
backfillfull_ratio is shown as a blocker instead, which is the more useful
description; without it, every companion is shown as a companion regardless.)

Blockers
--------
backfill_toofull is a property of the PG, not of a shard: while any backfill
target of a PG refuses its reservation, the whole PG waits, including a shard
heading for a nearly empty OSD. So stopping the backfills into an OSD is not
always enough to let the ones you keep run: if you delete a shard's entry to
keep its backfill, but some other shard of the same PG is heading for an OSD
that is itself over backfillfull_ratio, the PG stays backfill_toofull and the
one you kept never moves either -- for a reason this script would otherwise
never mention.

--pin-blockers turns this analysis on (off by default: it is a second job
beyond stopping backfills into the named OSD, see "How", and finding it
changes what PGs the output touches, not just how it explains them). With it,
for every PG with a pinned shard the tool also pins back each other shard that
is moving to an OSD whose utilization, once the shard lands, would reach the
cluster's backfillfull_ratio (from 'ceph osd dump'), listed as "blocks shard
N" in the NOTE column with that projected utilization. The projection adds
only this shard, to what 'ceph osd df' reports, so it is a lower bound of what
Ceph will see.

To keep a backfill that these entries would cancel, delete its own entry, but
keep the entries of its blockers, since those are what let it start. A blocker
is only cancelled if it can be pinned validly: one that cannot is listed on
stderr and the requested pin is kept regardless. Without --pin-blockers, a
trailing note points at the option (unless the cluster has no
backfillfull_ratio to begin with, e.g. an older --load-state capture, in
which case there is nothing it could find anyway and no note is printed).
Given --pin-blockers with no backfillfull_ratio available, a note says so
instead.

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
anonymize_snapshots here and shared.anonymize_snapshots), so it can be shared or
committed. --load-state DIR reads
such a directory back instead of calling 'ceph', so a captured state can be
replayed offline with no cluster access. The two options are mutually
exclusive. tests/pg-osd/test-data/stop-backfills-into-osd-*/ hold captures for use
as --load-state arguments, each with a README.txt describing the scenario.

Applying the output
-------------------
--import-mappings prints a JSON array for 'pgremapper import-mappings', one
{pgid, mapping: {from, to}} entry per line (all other output goes to stderr):

    stop-backfills-into-osd.py --pin-blockers --import-mappings --osd 682 > mappings.json
    # drop the entry into the OSD for each backfill you want to keep, but not
    # its blockers (other entries of the same PG, present with --pin-blockers),
    # e.g. keep 19.92e's 896->231:
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
import json
import re
import shutil
import sys
import textwrap
from collections import Counter
from typing import NamedTuple

from shared import (
    KIB,
    POOL_TYPE_ERASURE,
    PROGRESS_100_NOTE,
    SnapshotStore,
    abbreviate_state,
    add_state_args,
    copies_moving,
    fetch_crush_rules,
    fetch_ec_profiles,
    fetch_osd_df,
    fetch_osd_hosts,
    fetch_pg_stats,
    fetch_pools,
    format_progress,
    is_real_osd,
    osd_cells,
    pg_progress_pct,
    pgid_sort_key,
    print_table,
    progress_reads_100,
    real_osd_set,
    rule_failure_domain,
    shard_size_bytes,
    slot,
)
from shared import anonymize_snapshots as anonymize_common

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
        "That can also mean pinning back a shard heading for another OSD, a "
        "companion: a shard of the same PG that would otherwise share a host "
        "with a pinned shard once that is pinned back, so the resulting 'up' "
        "set would break the host failure domain and Ceph would silently drop "
        "the upmap (a shard still moving onto a host that holds another shard "
        "of its PG is harmless in itself: a PG's 'acting' set switches to "
        "'up' only when all its backfills have finished). Prints the "
        "proposals only; nothing is changed. Deciding what to keep is up to "
        "you: delete the entries for the backfills you want to let proceed, "
        "and drop companions together with the entry they belong to. "
        "--pin-blockers additionally pins back any other shard of the same PG "
        "whose own target OSD would reach backfillfull_ratio and so hold the "
        "whole PG in backfill_toofull: without it, a backfill you decide to "
        "keep can still never run, for a reason this tool would not "
        "otherwise mention. The NOTE column says which pin is which.",
        epilog="See the docstring at the top of this script for companions, "
        "blockers, what cannot be pinned and how to apply the output.",
    )
    parser.add_argument(
        "--osd", type=parse_osd, required=True, help="OSD id, e.g. 682 or osd.682"
    )
    parser.add_argument(
        "--exclude-pgs",
        nargs="+",
        default=[],
        metavar="PGID",
        help="PG id(s) to leave alone: skip entirely (no pins, no companions, "
        "no blockers) even if they have a backfill into --osd. Space-"
        "separated, e.g. --exclude-pgs 19.92e 20.1a3. A given id that does "
        "not match a remapped PG with --osd in its 'up' set is reported on "
        "stderr, since that usually means a typo.",
    )
    parser.add_argument(
        "--pin-blockers",
        action="store_true",
        help="Also pin back any other shard of a PG whose target OSD would "
        "reach backfillfull_ratio and so hold the whole PG in "
        "backfill_toofull, marked 'blocks shard N' in the NOTE column. Off "
        "by default: finding blockers changes which PGs the output touches, "
        "not just how it explains them. Keep a blocker's entry when you keep "
        "the shard it blocks; that pin is what lets it start.",
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
        help="Print '<pgid> <up osd> <acting osd>' lines with no header "
        "instead of the table, for 'pgremapper remap', one run per line. "
        "Warns if a PG needs several lines, since separate runs can "
        "overwrite each other; prefer --import-mappings.",
    )
    add_state_args(parser, SNAPSHOT_COMMANDS)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def fetch_backfillfull_pct(store: SnapshotStore) -> float | None:
    """Return the cluster's backfillfull_ratio as a percentage, or None if absent."""
    ratio = store.json("osd_dump").get("backfillfull_ratio")
    return None if ratio is None else ratio * 100


# ---------------------------------------------------------------------------
# Anonymization for --save-state
# ---------------------------------------------------------------------------

# The parts of 'ceph osd dump' besides the erasure code profiles that the
# analysis reads, and so that --save-state keeps.
KEPT_OSD_DUMP_RATIOS = ("full_ratio", "backfillfull_ratio", "nearfull_ratio")


def anonymize_snapshots(snapshots: dict[str, object]) -> None:
    """Anonymize a complete set of parsed snapshots in place.

    shared.anonymize_snapshots does the general scrub (hostnames, pool and CRUSH
    rule names, fsid, OSD addresses and uuids). 'ceph osd dump' is then cut down
    to the erasure code profiles and the full ratios, the only parts this script
    reads, which also keeps everything else in it out of the capture.
    """
    anonymize_common(snapshots)
    dump = snapshots["osd_dump"]
    snapshots["osd_dump"] = {
        "erasure_code_profiles": dump.get("erasure_code_profiles", {}),
        **{k: dump[k] for k in KEPT_OSD_DUMP_RATIOS if k in dump},
    }


# ---------------------------------------------------------------------------
# PG analysis
# ---------------------------------------------------------------------------


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
            if slot(up, i) != osd:
                continue
            acting_osd = slot(acting, i)
            if acting_osd == osd:
                continue
            if acting_osd is None:
                skipped.append((i, "no acting OSD for this shard (degraded)"))
            else:
                pins.append((i, acting_osd))
        return pins, skipped

    up_set, acting_set = real_osd_set(up), real_osd_set(acting)
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
                if j == i or j in pins or not is_real_osd(other):
                    continue
                if not _same_place(new_up[i], other, osd_host):
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
        if j in pinned or not is_real_osd(target) or slot(acting, j) in (None, target):
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
            and is_real_osd(other)
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
    blocker_util: float | None = None  # set (only with --pin-blockers, see
    # plan_cancellations' blockers_enabled) when this shard's target OSD would
    # reach backfillfull_ratio (percent it would be at), i.e. it is a blocker


class Skipped(NamedTuple):
    """A shard arriving on the OSD that cannot be pinned, and why."""

    pgid: str
    shard: "int | str"
    reason: str


def plan_cancellations(
    pg_stats: list[dict],
    pools: dict[int, dict],
    ec_profiles: dict[str, dict],
    osd: int,
    osd_host: dict[int, str],
    crush_rules: dict[int, dict],
    osd_df: dict[int, dict] | None = None,
    backfillfull_pct: float | None = None,
    pin_blockers: bool = False,
    exclude_pgs: frozenset[str] | set[str] = frozenset(),
) -> tuple[list[Cancellation], list[Skipped]]:
    """Return (cancellations, skipped) for all backfills into osd, in PG order.

    Every shard arriving on osd yields a cancellation, plus cancellations for
    the other shards of its PG that go with it: companions that must be pinned
    back to keep the mapping valid (_close_pins, always), and, when
    pin_blockers is true and osd_df/backfillfull_pct are given, blockers whose
    target OSD would reach backfillfull_pct and hold the whole PG in
    backfill_toofull (find_blockers). A blocker that cannot be pinned is
    skipped and reported; the requested shard's own pin is kept. Exits with an
    error if such a PG's pool does not fail over at host, since the clash
    check assumes it.

    A PG whose id is in exclude_pgs is left alone entirely: no cancellation,
    companion, blocker or skipped entry, as if it were never seen.
    """
    blockers_enabled = (
        pin_blockers and osd_df is not None and backfillfull_pct is not None
    )
    cancellations, skipped = [], []
    for pg in pg_stats:
        up, acting = pg["up"], pg["acting"]
        if osd not in up:
            continue
        pgid = pg["pgid"]
        if pgid in exclude_pgs:
            continue
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
                if why is None and blockers_enabled:
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
                if s != shard and blockers_enabled:
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
        *osd_cells(osd_df, osd_host, c.acting_osd, bare_id=True),
        *osd_cells(osd_df, osd_host, c.up_osd, bare_id=True),
        format_bytes(c.size_bytes),
        format_progress(c.progress_pct),
        abbreviate_state(c.state),
        format_note(c),
    ]


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
    stderr_para(
        f"WARNING: {len(pgids)} PG(s) need more than one remap ({shown}). Running "
        "these lines as separate 'pgremapper remap' commands (e.g. xargs -L1) "
        "can silently lose pairs: a later run may overwrite the pair an earlier "
        "one just added, and a lone pair can be invalid and dropped by the "
        "mons. Use --import-mappings instead, which applies all pairs of a PG "
        "together."
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
    stderr_para(
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
        )
    )
    for s in skipped:
        print(
            wrap_text(f"cannot pin {s.pgid} shard {s.shard}: {s.reason}", indent="  "),
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    osd = args.osd
    exclude_pgs = set(args.exclude_pgs)

    store = SnapshotStore.from_args(
        args, SNAPSHOT_COMMANDS, anonymize=anonymize_snapshots
    )

    osd_df = fetch_osd_df(store)
    osd_host = fetch_osd_hosts(store)
    pg_stats = fetch_pg_stats(store, "pg_ls_remapped")
    pools = fetch_pools(store)
    ec_profiles = fetch_ec_profiles(store)
    crush_rules = fetch_crush_rules(store)

    # Saved before the checks below, which can exit: a cluster that trips them
    # is just the kind of state worth having captured.
    store.save()

    if osd not in osd_df:
        sys.exit(f"ERROR: osd.{osd} not found in 'ceph osd df'.")
    backfillfull_pct = fetch_backfillfull_pct(store)
    if backfillfull_pct is None and args.pin_blockers:
        stderr_para(
            "NOTE: 'osd dump' has no backfillfull_ratio (an older --load-state "
            "capture?), so --pin-blockers has nothing to work from and no "
            "blockers are looked for."
        )
    if exclude_pgs:
        # "matched" only means osd is somewhere in the PG's 'up' (i.e. it is
        # one of the PGs plan_cancellations would otherwise have looked at);
        # it does not mean osd itself has a backfill (see find_arrivals), so
        # the note below must not claim more than that.
        matched = {
            pg["pgid"]
            for pg in pg_stats
            if osd in pg["up"] and pg["pgid"] in exclude_pgs
        }
        unmatched = sorted(exclude_pgs - matched)
        stderr_para(
            f"NOTE: --exclude-pgs: {len(matched)} of {len(exclude_pgs)} given "
            f"PG id(s) matched a remapped PG involving osd.{osd} and were "
            "left alone"
            + (
                f"; {len(unmatched)} matched nothing (check for typos): "
                f"{', '.join(unmatched)}."
                if unmatched
                else "."
            )
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
        args.pin_blockers,
        exclude_pgs,
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
        print_table(COLUMNS, [format_row(c, osd_df, osd_host) for c in cancellations])
        if any(progress_reads_100(c.progress_pct) for c in cancellations):
            stderr_para(f"NOTE: {PROGRESS_100_NOTE}")
    if chained:
        warn_chained_pgs(chained, left_out=machine_format)
    if backfillfull_pct is not None and not args.pin_blockers:
        stderr_para(
            "NOTE: --pin-blockers was not given, so a shard of the same PG "
            "whose own target OSD is over backfillfull_ratio was not pinned "
            "back. If that leaves a PG in backfill_toofull, a backfill you "
            "decide to keep from this output will never actually run. Rerun "
            "with --pin-blockers to find and include those pins too."
        )
    stderr_para(
        "NOTE: cancelling a running backfill discards its progress. Consider "
        "'ceph balancer off' while these are pinned."
    )


if __name__ == "__main__":
    main()
