# SPDX-License-Identifier: MIT
"""One command, several subcommands, for Ceph PG/OSD backfill and upmap work.

Puts this package's own directory on sys.path so its modules can import each
other (and shared.py) with plain, unqualified imports -- the same style the
scripts used before this reorg -- whether this package is run as
'python -m backfillctl', run directly as 'python backfillctl' (Python's
directory-execution support, which runs __main__.py), or imported by tests
as 'backfillctl.<module>'.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
