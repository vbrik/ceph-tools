#!/usr/bin/env python

import json
import subprocess
import sys


def main():
    pg_dump = subprocess.check_output(["ceph", "pg", "dump", "-f", "json"])
    pg_dump = json.loads(pg_dump)
    pg_stats = pg_dump["pg_map"]["pg_stats"]
    for pg in pg_stats:
        summary = pg["stat_sum"]
        print(
            pg["pgid"],
            f"{round(summary['num_bytes'] / 10**9, 1)}GB",
            f"{round(summary['num_objects'] / 1000, 1)}K objects",
        )


if __name__ == "__main__":
    sys.exit(main())
