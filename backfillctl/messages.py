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

UNPLACEABLE_NOTE = (
    "NOTE: targets ran out of room (--max-target-util, --max-target-uses), or "
    "the greedy placement missed some. Apply these, let them finish, then re-run."
)


def targets_clause(
    max_target_uses: int, max_target_util: float, backfillfull_pct: float
) -> str:
    """Return the limits every move target is held to, as a clause."""
    return (
        f"up to --max-target-uses {max_target_uses} shard(s) each, projected at "
        f"or below --max-target-util {max_target_util:g}% (backfillfull_ratio "
        f"{backfillfull_pct:g}%)"
    )


def print_unplaceable(shards: Iterable[tuple[str, "int | str", str]]) -> None:
    """List on stderr the (pgid, shard, where) no target was found for, and what to do.

    where says where the shard is moving, e.g. '(headed for osd.31)'. Prints
    nothing if shards is empty.
    """
    items = [
        f"cannot place {pgid} shard {shard} {where}: no legal target"
        for pgid, shard, where in shards
    ]
    if items:
        stderr_items(items)
        stderr_para(UNPLACEABLE_NOTE)
