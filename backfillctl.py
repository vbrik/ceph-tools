#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Executable entry point for the backfillctl package: './backfillctl.py show-backfill'."""

import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(
        str(Path(__file__).resolve().parent / "backfillctl"), run_name="__main__"
    )
