# SPDX-License-Identifier: MIT
"""Dispatch to backfillctl's subcommands.

Each subcommand's own options and behavior are unchanged from when it was
its own script (see the module docstring of each file in this directory);
this wires them together under one command. The one exception is
--load-state, which every subcommand used to define for itself: it is now a
global option, given before the subcommand name
('backfillctl --load-state DIR show-backfill').

build_parser() is split out from main() so it can be imported without side
effects, both by tests and by 'shtab backfillctl.__main__.build_parser' to
generate shell completions (https://docs.iterative.ai/shtab/).
"""

import argparse

import cancel_backfill
import cancel_uphill
import divert_toofull
import drain
import save_state
import show_backfill
import show_pg_osds
from shared import add_load_state_arg

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
        description="Ceph PG/OSD backfill and upmap tools: show what's "
        "moving, and propose upmaps to divert or cancel backfills and drain OSDs.",
    )
    add_load_state_arg(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)
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
