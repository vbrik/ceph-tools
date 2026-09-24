# SPDX-License-Identifier: MIT
"""Dispatch to backfillctl's subcommands.

build_parser() has no side effects, so tests and shtab can import it:
'shtab --shell=bash backfillctl.__main__.build_parser'.
"""

import argparse

import cancel_backfill
import cancel_uphill
import divert_toofull
import drain
import save_state
import show_backfill
import show_pg_osds
from shared import HelpFormatter, add_load_state_arg

_COMMAND_MODULES = (
    show_pg_osds,
    show_backfill,
    divert_toofull,
    cancel_backfill,
    cancel_uphill,
    drain,
    save_state,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfillctl",
        description="Ceph PG backfill and upmap tools: show what is moving, "
        "divert or cancel backfills, drain OSDs. Commands that remap PGs only "
        "print upmap proposals; they change nothing.",
        formatter_class=HelpFormatter,
    )
    add_load_state_arg(parser)
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
    args.run(args)


if __name__ == "__main__":
    main()
