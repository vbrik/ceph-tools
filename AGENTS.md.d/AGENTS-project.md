# Other
- note that there are some files in ./gitignore that are useful for testing of some scripts (don't add them to the repo and don't delete them!)
- `tests/` holds the unit tests (all of them), one subdirectory per script group (`tests/backfillctl/`, `tests/cephfs/`); `tests/backfillctl/test-data` contains cluster state snapshot data for testing
- Python tools that are more than one file are packages in `src/` (one `ceph-tools` distribution, see `pyproject.toml`), each with its own `[project.scripts]` entry. Modules import each other package-qualified (`from backfillctl.shared import ...`), never bare (`import shared`): installed, bare names would be global to the venv and clash between tools. Give packages distinctive names
- backfillctl: a message that another command also prints (a note, warning, summary clause or NOTE cell) goes in `src/backfillctl/messages.py`; check there before wording a new one
