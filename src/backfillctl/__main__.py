# SPDX-License-Identifier: MIT
"""Dispatch to backfillctl's subcommands.

build_parser() has no side effects, so tests can import it. Its
--print-completion (from shtab) prints a completion script for it.
"""

import argparse

import shtab

from backfillctl import (
    balance,
    cancel_backfill,
    cancel_uphill,
    divert_toofull,
    drain,
    measure_rate,
    save_state,
    show_backfill,
    show_pg_osds,
)
from backfillctl.shared import HelpFormatter, add_load_state_arg, resolve_load_state

_COMMAND_MODULES = (
    show_pg_osds,
    show_backfill,
    measure_rate,
    divert_toofull,
    cancel_backfill,
    cancel_uphill,
    drain,
    balance,
    save_state,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfillctl",
        description="Ceph PG backfill and upmap tools: show what is moving and how fast, "
        "divert or cancel backfills, drain OSDs, move data off the fullest OSDs. Commands that remap PGs only "
        "print upmap proposals; they change nothing.",
        formatter_class=HelpFormatter,
    )
    add_load_state_arg(parser)
    shtab.add_argument_to(
        parser, help="Print a shell completion script and exit; see the README."
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="COMMAND",
    )
    for module in _COMMAND_MODULES:
        subparser = module.build_parser(subparsers)
        subparser.set_defaults(run=module.run)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    resolve_load_state(parser, args)
    args.run(args)


if __name__ == "__main__":
    main()
