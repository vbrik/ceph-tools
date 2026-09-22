#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Run the backfillctl package directly, e.g. './backfillctl.py show-backfill'.

Equivalent to 'python3 -m backfillctl ...' or 'python3 backfillctl ...';
this just adds a directly-executable entry point at the repo root.
"""

import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(
        str(Path(__file__).resolve().parent / "backfillctl"), run_name="__main__"
    )
