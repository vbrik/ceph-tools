# SPDX-License-Identifier: MIT
"""backfillctl: Ceph PG backfill and upmap tools.

Puts this directory on sys.path so the modules can import each other
unqualified ('from shared import ...'), however the package is run or
imported.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
