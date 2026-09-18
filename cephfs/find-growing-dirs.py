#!/usr/bin/env python3
"""
Locate the fastest-growing subtree in a CephFS mount using recursive
directory statistics (rstats), without walking the tree.

Samples the ceph.dir.rbytes extended attribute on the immediate children of a
directory at two points in time, ranks them by delta, then optionally descends
into the top grower and repeats. Cost is O(children per level), not O(files).

Requires a CephFS mount (kernel client or ceph-fuse). Read access to the
directories being sampled is sufficient; no MDS admin socket access needed.

Example:
    ./find-growing-dirs.py /mnt/cephfs --interval 60 --depth 6
"""

import argparse
import concurrent.futures
import os
import sys
import time

RBYTES = "ceph.dir.rbytes"
RFILES = "ceph.dir.rfiles"
RCTIME = "ceph.dir.rctime"


def getxattr_int(path: str, name: str) -> "int | None":
    """Return an integer-valued Ceph xattr, or None if unavailable."""
    try:
        return int(os.getxattr(path, name))
    except (OSError, ValueError):
        return None


def subdirs(path: str) -> list[str]:
    """Immediate subdirectories of path, not following symlinks."""
    try:
        with os.scandir(path) as it:
            entries = list(it)
    except OSError as exc:
        print(f"warning: cannot scan {path}: {exc}", file=sys.stderr)
        return []

    result = []
    for e in entries:
        try:
            if e.is_dir(follow_symlinks=False):
                result.append(e.path)
        except OSError as exc:
            print(f"warning: cannot stat {e.path}: {exc}", file=sys.stderr)
    return sorted(result)


def sample(
    paths: list[str], pool: concurrent.futures.ThreadPoolExecutor
) -> dict[str, int]:
    """Map path -> recursive byte count, skipping paths without rstats."""
    result = {}
    futures = {p: pool.submit(getxattr_int, p, RBYTES) for p in paths}
    for p, fut in futures.items():
        rbytes = fut.result()
        if rbytes is not None:
            result[p] = rbytes
    return result


def human(n: float) -> str:
    sign = "-" if n < 0 else ""
    n = abs(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if round(n, 2) < 1024 or unit == "PiB":
            return f"{sign}{n:.2f} {unit}"
        n /= 1024


def measure_level(
    root: str,
    interval: float,
    top: int,
    pool: concurrent.futures.ThreadPoolExecutor,
) -> "str | None":
    """
    Sample root and its children over `interval` seconds, report the deltas,
    and return the fastest-growing child (or None if there is no clear one).
    """
    targets = [root] + subdirs(root)
    t0 = time.monotonic()
    before = sample(targets, pool)
    if root not in before:
        print(
            f"error: {root} has no {RBYTES} xattr -- not a CephFS mount?",
            file=sys.stderr,
        )
        return None

    time.sleep(interval)
    after = sample(targets, pool)
    elapsed = time.monotonic() - t0

    root_after = after.get(root, before[root])
    root_delta = root_after - before[root]
    rate = f"{human(root_delta / elapsed)}/s" if elapsed > 0 else "n/a"
    print(f"\n{root}")
    print(
        f"  total {human(before[root])} -> {human(root_after)} "
        f"({human(root_delta)} in {elapsed:.0f}s, {rate})"
    )

    deltas = [(after[p] - before[p], p) for p in before if p != root and p in after]
    deltas.sort(key=lambda d: (-d[0], d[1]))

    growers = [d for d in deltas if d[0] > 0]
    if not growers:
        unaccounted = root_delta
        print(
            "  no child subtree grew; growth is in files directly under this "
            f"directory, or rstats have not propagated yet "
            f"(unaccounted: {human(unaccounted)})"
        )
        return None

    accounted = sum(d for d, _ in growers)
    shown = growers[:top]
    for delta, path in shown:
        share = 100.0 * delta / root_delta if root_delta else float("nan")
        print(f"  {human(delta):>12}  {share:5.1f}%  {path}")
    hidden = growers[top:]
    if hidden:
        hidden_total = sum(d for d, _ in hidden)
        print(
            f"  {human(hidden_total):>12}         "
            f"({len(hidden)} more growing children not shown; raise --top to see them)"
        )
    if root_delta - accounted > 0:
        print(
            f"  {human(root_delta - accounted):>12}         "
            f"(unaccounted: direct children or propagation lag)"
        )

    return growers[0][1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("root", help="directory in a CephFS mount to start from")
    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="seconds between samples at each level (default: 60)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=1,
        help="how many levels to descend into the top grower "
        "(default: 1, i.e. one level only)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="how many children to list per level (default: 10)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="concurrent xattr lookups per sample (default: 16)",
    )
    args = parser.parse_args()
    if args.interval < 0:
        parser.error("--interval must be non-negative")
    if args.top < 1:
        parser.error("--top must be at least 1")
    if args.workers < 1:
        parser.error("--workers must be at least 1")

    current = os.path.abspath(args.root)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for level in range(args.depth):
            nxt = measure_level(current, args.interval, args.top, pool)
            if nxt is None:
                break
            current = nxt

    print(f"\nfinal: {current}")
    rfiles = getxattr_int(current, RFILES)
    if rfiles is not None:
        print(f"  {RFILES}: {rfiles}")
    try:
        print(f"  {RCTIME}: {os.getxattr(current, RCTIME).decode()}")
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
