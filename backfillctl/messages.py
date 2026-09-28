# SPDX-License-Identifier: MIT
"""User-facing text that more than one call site prints, and the stderr
primitives that print it.

Text only one command prints stays in that command. A message that says what
one here says reuses it, so that the commands word it the same way.

Imports nothing from backfillctl (types only, for annotations), so that
shared.py and placement.py can use it too.
"""

import shutil
import sys
import textwrap
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared import PgidFilter, PinTotals, Skipped

# ---------------------------------------------------------------------------
# stderr
# ---------------------------------------------------------------------------


def wrap_text(text: str, indent: str = "") -> str:
    """Wrap a stderr paragraph to the terminal, 40 to 100 columns.

    Continuation lines hang two spaces deeper than indent.
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
    """Print a wrapped stderr paragraph, separated from the previous by a blank line.

    Flushes stdout first, so a table and the notes about it keep their
    order when both streams go to one pipe (2>&1).
    """
    sys.stdout.flush()
    if stderr_para.printed:
        print(file=sys.stderr)
    print(wrap_text(text), file=sys.stderr)
    stderr_para.printed = True


stderr_para.printed = False


def stderr_items(items: Iterable[str]) -> None:
    """Print items on stderr, one indented, wrapped line each.

    For the details under a stderr_para summary, e.g. each shard that could
    not be pinned or placed.
    """
    sys.stdout.flush()
    for item in items:
        print(wrap_text(item, indent="  "), file=sys.stderr)


# ---------------------------------------------------------------------------
# Values in text and table cells
# ---------------------------------------------------------------------------


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


def osd_list(osds: Iterable[int]) -> str:
    """Name OSDs in id order, e.g. 'osd.74, osd.682'."""
    return ", ".join(f"osd.{o}" for o in sorted(osds))


# ---------------------------------------------------------------------------
# NOTE cells: why a shard is moved or pinned
# ---------------------------------------------------------------------------


def blocking_reason(osd_id: int, projected: float) -> str:
    """Say why a shard headed for osd_id is a blocker, in the words every command uses.

    A blocker's target is projected, counting every shard arriving on it,
    at or over backfillfull_ratio: Ceph refuses that backfill, and
    backfill_toofull then holds back the whole PG.
    """
    return f"osd.{osd_id} projected at {projected:.1f}%, at or over backfillfull_ratio"


def companion_note(shard: "int | str", *, of_blocker: bool = False) -> str:
    """Say which shard a pinned companion keeps valid: a requested one or a blocker."""
    return f"companion of {'blocker ' if of_blocker else ''}shard {shard}"


# ---------------------------------------------------------------------------
# Notes and warnings
# ---------------------------------------------------------------------------

# Footnote for PROGRESS figures marked '~'. Pre-wrapped for printing as-is;
# stderr_para reflows it.
PROGRESS_APPROX_NOTE = (
    "~ marks PROGRESS from Ceph's misplaced/degraded counters, used where\n"
    "backfill positions are unavailable. The counters are per PG and can read\n"
    "far too high after re-peering, even ~100% for a backfill a third done."
)


def print_progress_note(progress: Iterable[tuple[float | None, bool]]) -> None:
    """Print the '~' footnote if any (pct, exact) is an estimate from counters."""
    if any(pct is not None and not exact for pct, exact in progress):
        stderr_para(f"NOTE: {PROGRESS_APPROX_NOTE}")


def print_query_failed(failed: Iterable[str], total: int, effect: str) -> None:
    """Report on stderr the PGs whose 'ceph pg query' failed, out of total queried.

    effect says what the failure costs, e.g. "their PROGRESS comes from
    Ceph's counters (marked '~')". Prints nothing if none failed.
    """
    failed = list(failed)
    if failed:
        stderr_para(
            f"NOTE: 'ceph pg query' failed for {len(failed)} of {total} PG(s) "
            f"({', '.join(failed[:5])}{', ...' if len(failed) > 5 else ''}); "
            f"{effect}."
        )


def print_movement_summary(copies: int, pgs: int) -> None:
    """Sum up a table of copy movements (show-backfill's rows) on stderr."""
    stderr_para(f"{copies} copy movement(s) across {pgs} PG(s).")


def print_no_movements(filter_options: Iterable[str]) -> None:
    """Say on stderr that no movement is shown: none at all, or none matching filter_options."""
    options = "/".join(filter_options)
    stderr_para(
        f"No PG movements match {options}." if options else "No PG movements detected."
    )


def print_pgid_filter(
    option: str, f: "PgidFilter", matched: str, unmatched: str
) -> None:
    """Report on stderr what option's PG ids matched, naming those that did not.

    matched says what a match means ('have movement'); unmatched, what else
    than a typo a non-match may be ('not moving').
    """
    stderr_para(
        f"NOTE: {option}: {f.matched} of {f.given} given PG id(s) {matched}"
        + (
            f"; {len(f.unmatched)} matched nothing ({unmatched}, or a typo): "
            + ", ".join(f.unmatched)
            if f.unmatched
            else ""
        )
        + "."
    )


def warn_chains(chained: dict[str, list[tuple[int, int]]]) -> None:
    """Warn on stderr about PGs whose pins chain, with commands that apply them all."""
    stderr_para(
        f"WARNING: the pins of {len(chained)} PG(s) chain (A->B, B->C), which "
        f"pgremapper cannot apply: {', '.join(chained)}. Pins left out are "
        "listed above. To apply them all, run the (untested) commands below; "
        "each sets the PG's whole upmap entry, existing pairs included. "
        "pgremapper removes part of such a chain as stale when it later "
        "changes the PG."
    )
    for pgid, pairs in chained.items():
        flat_pairs = " ".join(f"{f} {t}" for f, t in pairs)
        # Not wrapped: for copy-pasting.
        print(f"  ceph osd pg-upmap-items {pgid} {flat_pairs}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Pins: cancel-backfill, cancel-uphill
# ---------------------------------------------------------------------------

CANCEL_NOTE = (
    "NOTE: cancelling a running backfill discards its progress. Consider "
    "'ceph balancer off' while these are pinned."
)


def print_pin_summary(
    intro: str,
    totals: "PinTotals",
    skipped: "list[Skipped]",
    *,
    share: str = "",
    unjudged: bool = False,
) -> None:
    """Sum up a cancel command's pins on stderr, and list what cannot be pinned.

    intro names the requested shards ('12 uphill shard(s) in 9 PG(s)'); share
    follows their size (', 3.1% of its capacity'). With unjudged, skipped
    includes shards that could not be judged, not only ones that cannot be
    pinned.
    """
    t = totals
    stderr_para(
        f"{intro} can be pinned back, ~{format_bytes(t.size_bytes)} of data"
        + (f" (+{t.unknown_size} of unknown size)" if t.unknown_size else "")
        + f"{share}; {len(skipped)} cannot be "
        + ("judged or pinned." if unjudged else "pinned.")
        + (
            f" {t.others} more shard(s), moving to other OSDs, are pinned too "
            "(companions" + (f"; blockers: {t.blockers}" if t.blockers else "") + ")."
            if t.others
            else ""
        )
    )
    stderr_items(f"cannot pin {s.pgid} shard {s.shard}: {s.reason}" for s in skipped)


def print_pin_footer(
    chained: dict[str, list[tuple[int, int]]], extra_notes: Iterable[str] = ()
) -> None:
    """Print a cancel command's closing notes: chained pins, extra_notes, the cost."""
    if chained:
        warn_chains(chained)
    for note in extra_notes:
        stderr_para(note)
    stderr_para(CANCEL_NOTE)


# ---------------------------------------------------------------------------
# Moves: balance, divert-toofull, drain
# ---------------------------------------------------------------------------


def unplaceable_note(limits: str) -> str:
    """Say what to do about shards no target was found for.

    limits names what held the targets back, e.g. '--max-target-util'.
    """
    return (
        f"NOTE: targets ran out of room ({limits}), or the greedy placement "
        "missed some. Apply these, let them finish, then re-run."
    )


def targets_clause(
    max_target_util: float,
    backfillfull_pct: float,
    max_target_uses: int | None = None,
) -> str:
    """Return the limits every move target is held to, as a clause."""
    uses = (
        ""
        if max_target_uses is None
        else f"up to --max-target-uses {max_target_uses} shard(s) each, "
    )
    return (
        f"{uses}projected at or below --max-target-util {max_target_util:g}% "
        f"(backfillfull_ratio {backfillfull_pct:g}%)"
    )


def print_unplaceable(
    shards: Iterable[tuple[str, "int | str", str]],
    limits: str = "--max-target-util, --max-target-uses",
) -> None:
    """List on stderr the (pgid, shard, where) no target was found for, and what to do.

    where says where the shard is moving, e.g. '(headed for osd.31)'; limits,
    as in unplaceable_note. Prints nothing if shards is empty.
    """
    items = [
        f"cannot place {pgid} shard {shard} {where}: no legal target"
        for pgid, shard, where in shards
    ]
    if items:
        stderr_items(items)
        stderr_para(unplaceable_note(limits))


# ---------------------------------------------------------------------------
# Shedding: drain, balance
# ---------------------------------------------------------------------------

# At most this many OSDs or PGs are named in one note, fullest or first
# first; the rest are counted. An unreachable level can leave most of a
# device class above it.
MAX_NAMED = 10


def named_list(items: list[str]) -> str:
    """Join items, naming at most MAX_NAMED: 'a, b, c, and 7 more'."""
    more = len(items) - MAX_NAMED
    return ", ".join(items[:MAX_NAMED]) + (f", and {more} more" if more > 0 else "")


def level_text(level: float) -> str:
    """Format a level: '55%', '72.25%', '52.35%' (at most two decimals)."""
    return f"{level:.2f}".rstrip("0").rstrip(".") + "%"


def left_alone_clause(unsettled: int, chained: int) -> str:
    """Return a sentence counting the PGs left alone, or '' if none were."""
    if not unsettled and not chained:
        return ""
    return (
        f" PGs left alone: {unsettled} not active, or degraded, undersized, "
        "recovering or peering (re-run once they settle); "
        f"{chained} whose upmap pairs chain (A->B, B->C), which pgremapper "
        "would break."
    )


def blockers_clause(nearfull_pct: float) -> str:
    """Return a sentence saying which shards count as blockers."""
    return (
        "Blockers: other shards of a PG heading for an OSD projected at or over "
        "backfillfull_ratio or, if the PG is backfill_toofull now, onto an OSD "
        f"at or above nearfull_ratio {nearfull_pct:g}%."
    )


def print_shed_outcome(
    off: str,
    *,
    moved: int,
    moved_bytes: int,
    unplaceable: int,
    kept: int | None,
    diverted: int,
    pinned: int,
    stuck: list[str],
    unexplained: list[str],
) -> None:
    """Sum up on stderr the moves off the sources (off names them).

    kept: shards left on a source below the level; None without a level.
    stuck and unexplained: PGs that will stay backfill_toofull.
    """

    def pg_list(pgids: list[str]) -> str:
        return f"{len(pgids)} ({named_list(pgids)})" if pgids else "0"

    left = (
        "" if kept is None else f", {kept} left in place (their OSD is below the level)"
    )
    stderr_para(
        f"Proposed {moved} move(s) off {off}, {format_bytes(moved_bytes)}, "
        f"{unplaceable} unplaceable{left}; {diverted} blocking shard(s) diverted, "
        f"{pinned} pinned back. PGs that will stay backfill_toofull: "
        f"{pg_list(stuck)}; for an unidentified reason: {pg_list(unexplained)}"
        + (". Their NOTE (JSON: 'note') says why." if stuck or unexplained else ".")
    )


def print_still_above(which: str, level: float, above: list[tuple[int, float]]) -> None:
    """Name on stderr the OSDs (which, e.g. 'source(s)') projected to stay at or
    above level, fullest first; above is (OSD, utilization). Prints nothing if
    it is empty.
    """
    if above:
        fullest = sorted(above, key=lambda ou: (-ou[1], ou[0]))
        named = named_list([f"osd.{o} ({u:.1f}%)" for o, u in fullest])
        stderr_para(
            f"{len(above)} {which} projected to stay at or above the "
            f"{level_text(level)} level: {named}."
        )


def print_level_unplaceable(count: int, guards: str) -> None:
    """Say on stderr what to do about count shards no target was found for,
    with a level; guards says what a target had to end up below.
    """
    if count:
        stderr_para(
            f"NOTE: {count} shard(s) found no target at or below --max-target-util "
            f"that would end up below {guards}. Apply these, let them finish, "
            "then re-run; if little moves, the level is out of reach."
        )


def print_stalled(pgids: list[str]) -> None:
    """Name on stderr the PGs a run leaves in backfill_toofull as they were.

    Each has a shard backfilling onto a source that Ceph refuses, and no
    proposed move to hold up. Prints nothing if pgids is empty.
    """
    if pgids:
        stderr_para(
            f"NOTE: {len(pgids)} PG(s) have a shard backfilling onto a source "
            "that Ceph refuses (backfill_toofull), left as it is: "
            f"{named_list(pgids)}. See divert-toofull or cancel-backfill."
        )
