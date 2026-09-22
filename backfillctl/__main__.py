# SPDX-License-Identifier: MIT
"""Dispatch to backfillctl's subcommands.

Each subcommand's argument parsing and behavior are unchanged from when it
was its own script (see the module docstring of each file in this
directory); this only wires them together under one command.
"""

import argparse

import divert_toofull_backfills
import osds_of_pg
import pg_movements
import stop_backfills_into_osd

_COMMAND_MODULES = (
    osds_of_pg,
    pg_movements,
    divert_toofull_backfills,
    stop_backfills_into_osd,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="backfillctl",
        description="Ceph PG/OSD backfill and upmap tools: show what's "
        "moving, and propose upmaps to divert or stop backfills.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for module in _COMMAND_MODULES:
        subparser = module.build_parser(subparsers)
        subparser.set_defaults(run=module.run)

    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
