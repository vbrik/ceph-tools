#!/usr/bin/env python
import argparse
import os
import sys
from dateutil.parser import parse  # noqa
from itertools import islice, chain
from multiprocessing import Process, JoinableQueue, Manager
from multiprocessing.managers import ListProxy  # noqa


def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def rctime_checker(
    min_rctime: float,
    need_rctime_checked: JoinableQueue,
    need_ctime_checked: JoinableQueue,
    max_batch_size: int,
):
    while True:
        need_rctime_checked_buffer = []
        need_ctime_checked_buffer = []

        dirs_batch = need_rctime_checked.get()

        if dirs_batch is None:
            need_rctime_checked.task_done()
            return

        for dir_path in dirs_batch:
            try:
                entries = os.scandir(dir_path)
            except FileNotFoundError:
                continue
            for entry in entries:
                need_ctime_checked_buffer.append(entry.path)
                try:
                    if entry.is_dir(follow_symlinks=False):
                        rctime = os.getxattr(entry.path, "ceph.dir.rctime", follow_symlinks=False)
                        if float(rctime) >= min_rctime:
                            need_rctime_checked_buffer.append(entry.path)
                except FileNotFoundError:
                    continue

        if need_rctime_checked_buffer:
            for batch in batched(need_rctime_checked_buffer, max_batch_size):
                need_rctime_checked.put(batch)

        if need_ctime_checked_buffer:
            for batch in batched(need_ctime_checked_buffer, max_batch_size):
                need_ctime_checked.put(batch)

        need_rctime_checked.task_done()


def ctime_checker(min_ctime: float, need_ctime_checked: JoinableQueue, ctime_matches: ListProxy):
    while True:
        paths_batch = need_ctime_checked.get()

        if paths_batch is None:
            need_ctime_checked.task_done()
            return

        matches = []
        for path in paths_batch:
            try:
                stat = os.stat(path, follow_symlinks=False)
                if stat.st_ctime >= min_ctime:
                    matches.append(path)
            except FileNotFoundError:
                continue

        if matches:
            ctime_matches.append(matches)  # append is faster than extend

        need_ctime_checked.task_done()


def main():
    parser = argparse.ArgumentParser(
        description="Use cephfs's ceph.dir.rctime extended attribute to find "
        "files and directories whose ctime is on or after the supplied date.",
        epilog="Notes: "
        "(1) Directory paths will be printed with a trailing slash. "
        "(2) A variety of date formats can be parsed. YYYY-MM-DD is just "
        "one example. To specify Unix time, prepend the number of seconds "
        "with @. "
        "(3) Since this script is IO-bound, it makes sense to use many more "
        "threads than the number of CPUs. ",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("root_path", metavar="PATH", help="Where to look for files")
    parser.add_argument(
        "--min-ctime", metavar="DATE", required=True, help="Minimum ctime in a reasonable format²"
    )
    parser.add_argument(
        "--relative", action="store_true", help="print matching paths relative to PATH"
    )
    parser.add_argument(
        "--parents", action="store_true", help="print only parent directories of matches"
    )
    parser.add_argument(
        "--threads", metavar="NUM", type=int, default=64, help="number of threads to use³"
    )
    args = parser.parse_args()

    root_path = args.root_path.rstrip("/")

    if not os.path.isdir(root_path):
        parser.exit(1, "PATH is not a directory or doesn't exist")

    if args.min_ctime.startswith("@"):
        min_ctime = float(args.min_ctime[1:])
    else:
        min_ctime_date = parse(args.min_ctime)
        min_ctime = min_ctime_date.timestamp()

    # Work queues
    need_ctime_checked = JoinableQueue()
    need_rctime_checked = JoinableQueue()

    rctime_checkers = [
        Process(
            target=rctime_checker, args=(min_ctime, need_rctime_checked, need_ctime_checked, 100)
        )
        for _ in range(args.threads // 2)
    ]
    [p.start() for p in rctime_checkers]

    # Output structure
    ctime_matches = Manager().list()
    ctime_checkers = [
        Process(target=ctime_checker, args=(min_ctime, need_ctime_checked, ctime_matches))
        for _ in range(args.threads // 2)
    ]
    [p.start() for p in ctime_checkers]

    need_ctime_checked.put_nowait([root_path])
    need_rctime_checked.put_nowait([root_path])

    # Wait until all tasks complete, send termination signal, and join children
    need_rctime_checked.join()
    [need_rctime_checked.put(None) for _ in range(args.threads)]
    [p.join() for p in rctime_checkers]
    need_ctime_checked.join()
    [need_ctime_checked.put(None) for _ in range(args.threads)]
    [p.join() for p in ctime_checkers]

    results_iter = chain(*ctime_matches)
    if args.parents:
        results = sorted(set(os.path.dirname(path) for path in results_iter))
    else:
        results = sorted(results_iter)
    if args.relative:
        results = [path.split(root_path + "/", maxsplit=1)[-1] for path in results]
        if root_path in results:
            results = [(path if path != root_path else ".") for path in results]
    print("\n".join(results))


if __name__ == "__main__":
    sys.exit(main())
