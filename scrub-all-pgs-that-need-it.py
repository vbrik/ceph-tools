#!/usr/bin/env python
import argparse
import json
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Use 'ceph tell osd...' to scrub or deep scrub all PGs that "
        "are behind on scrubbing according to 'ceph health detail'. "
        "This is a work-around for broken 'ceph pg (deep-)scrub'.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("args", nargs="*")
    parser.parse_args()

    cmd = ["ceph", "health", "detail", "-f", "json-pretty"]
    health = json.loads(subprocess.check_output(cmd))

    soft_detail = health["checks"].get("PG_NOT_SCRUBBED", {}).get("detail", [])
    soft_pgs = [m["message"].split()[1] for m in soft_detail]
    for pg in soft_pgs:
        query = json.loads(subprocess.check_output(["ceph", "pg", pg, "query"]))
        primary = query["info"]["stats"]["acting_primary"]
        cmd = ["ceph", "tell", f"osd.{primary}", "scrub", pg]
        print(cmd)
        print(subprocess.check_output(cmd))

    deep_detail = health["checks"].get("PG_NOT_DEEP_SCRUBBED", {}).get("detail", [])
    deep_pgs = [m["message"].split()[1] for m in deep_detail]
    for pg in deep_pgs:
        query = json.loads(subprocess.check_output(["ceph", "pg", pg, "query"]))
        primary = query["info"]["stats"]["acting_primary"]
        cmd = ["ceph", "tell", f"osd.{primary}", "deep_scrub", pg]
        print(cmd)
        print(subprocess.check_output(cmd))


if __name__ == "__main__":
    sys.exit(main())
