#!/usr/bin/env python
# SPDX-License-Identifier: MIT
import argparse
import json
import re
import socket
import subprocess
import sys

INO_RE = re.compile(r"^0x[0-9a-f]+$")
INO_SOURCES = ["delegated_inos", "completed_requests", "prealloc_inos"]


def is_ino(v):
    return isinstance(v, str) and INO_RE.match(v) and v != "0x0"


def ceph_cmd(args):
    """Run a `ceph` CLI command and return its parsed JSON output."""
    try:
        result = subprocess.run(["ceph"] + args, capture_output=True, text=True)
    except OSError as e:
        print(f"error: cannot run `ceph`: {e}", file=sys.stderr)
        sys.exit(1)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"error: cannot parse `ceph` output as JSON: {e}", file=sys.stderr)
        sys.exit(1)


def get_active_ranks():
    """Return the sorted list of (target, rank) for MDS ranks currently 'in' any filesystem.

    target is what to pass as `mds.<target>` to `ceph tell`: just the rank
    number for a single-filesystem cluster, or `<fs_name>:<rank>` when more
    than one filesystem is present, since rank numbering can otherwise
    collide across filesystems.
    """
    data = ceph_cmd(["fs", "dump", "--format", "json"])
    by_fs = [
        (fs.get("mdsmap", {}).get("fs_name", ""), rank)
        for fs in data.get("filesystems", [])
        for rank in fs.get("mdsmap", {}).get("in", [])
    ]
    multi_fs = len({fs_name for fs_name, _ in by_fs}) > 1
    return sorted(
        (f"{fs_name}:{rank}" if multi_fs else str(rank), rank)
        for fs_name, rank in by_fs
    )


def load_live(rank, match_type=None, match_val=None):
    """Return [(rank, session), ...] from `ceph tell mds.RANK client ls`."""
    targets = [(str(rank), rank)] if rank is not None else get_active_ranks()
    if not targets:
        print("error: no active MDS ranks found", file=sys.stderr)
        sys.exit(1)
    ranks_shown = [r for _, r in targets]
    rank_desc = f"mds.{targets[0][0]}" if len(targets) == 1 else f"ranks {ranks_shown}"
    print(
        f"warning: querying `client ls` on {rank_desc} -- "
        f"this can be resource-intensive on a busy cluster",
        file=sys.stderr,
    )
    # Filter server-side when possible so we don't pull every session on
    # every rank just to find one client's.
    cmd_extra = [f"id={match_val}"] if match_type == "id" else []
    entries = []
    for target, r in targets:
        sessions = ceph_cmd(["tell", f"mds.{target}", "client", "ls"] + cmd_extra)
        entries.extend((r, c) for c in sessions)
    return entries


def load_file(path):
    """Return [(None, session), ...] from a JSON file (or - for stdin)."""
    if path == "-":
        data = json.load(sys.stdin)
    else:
        with open(path) as f:
            data = json.load(f)
    return [(None, c) for c in data]


def resolve_client(client_arg):
    """Return ('id', int) or ('ip', str) depending on the identifier type."""
    if client_arg.isdigit():
        return ("id", int(client_arg))
    try:
        socket.inet_aton(client_arg)
        return ("ip", client_arg)
    except OSError:
        pass
    ip = socket.gethostbyname(client_arg)
    return ("ip", ip)


def find_sessions(entries, match_type, match_val):
    """entries is a list of (rank, session) tuples; rank is None for file input."""
    results = []
    for rank, c in entries:
        if match_type == "id":
            match = c.get("id") == match_val
        else:
            addr = c.get("entity", {}).get("addr", {}).get("addr", "")
            match = addr.split(":")[0] == match_val
        if match:
            results.append((rank, c))
    return results


def extract_inos(session, source_key):
    """Return ordered list of unique non-zero inode hex strings from source_key."""
    seen = set()
    result = []
    for item in session.get(source_key, []):
        if not isinstance(item, dict):
            continue
        for v in item.values():
            if is_ino(v) and v not in seen:
                seen.add(v)
                result.append(v)
    return result


def ino_to_path(ino_hex, pools, cache):
    if ino_hex in cache:
        return cache[ino_hex]
    ino = ino_hex[2:]  # strip 0x in Python
    rados = None
    for pool in pools:
        rados = subprocess.run(
            ["rados", "-p", pool, "getxattr", f"{ino}.00000000", "parent"],
            capture_output=True,
        )
        if rados.returncode == 0:
            break
    if rados is None or rados.returncode != 0:
        cache[ino_hex] = None
        return None
    dencoder = subprocess.run(
        [
            "ceph-dencoder",
            "type",
            "inode_backtrace_t",
            "import",
            "-",
            "decode",
            "dump_json",
        ],
        input=rados.stdout,
        capture_output=True,
    )
    if dencoder.returncode != 0:
        cache[ino_hex] = None
        return None
    try:
        decoded = json.loads(dencoder.stdout)
    except json.JSONDecodeError:
        cache[ino_hex] = None
        return None
    dnames = [a["dname"] for a in decoded.get("ancestors", [])]
    dnames.reverse()
    path = "/" + "/".join(dnames)
    cache[ino_hex] = path
    return path


def print_session(rank, session, pools, cache):
    sid = session.get("id")
    addr = session.get("entity", {}).get("addr", {}).get("addr", "?")
    label = f"client {sid} ({addr})"
    if rank is not None:
        label += f" [mds.{rank}]"
    print(f"{label}:")
    for source in INO_SOURCES:
        inos = extract_inos(session, source)
        if not inos:
            continue
        print(f"  {source}:")
        for ino in inos:
            path = ino_to_path(ino, pools, cache)
            if path is not None:
                print(f"    {ino} {path}")
            else:
                print(f"    {ino} (not found)")


def main():
    parser = argparse.ArgumentParser(
        description="Show inode paths for a CephFS client session. "
        "Reads client sessions from a JSON file/stdin, or, if FILE is "
        "omitted, queries MDS rank(s) live.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--meta-pool",
        default="cephfs.default.meta",
        help="Rados metadata pool (directories)",
    )
    parser.add_argument(
        "--data-pool",
        default="cephfs.default.data",
        help="Rados data pool (files, symlinks)",
    )
    parser.add_argument(
        "-r",
        "--rank",
        type=int,
        default=None,
        metavar="RANK",
        help="Live mode only: query only this MDS rank "
        "(default: query all active ranks). Not valid with FILE.",
    )
    parser.add_argument("client", help="Client id (integer), IP address, or hostname")
    parser.add_argument(
        "file",
        nargs="?",
        default=None,
        help='JSON file from "ceph tell mds.RANK client ls", or - for stdin. '
        "If omitted, query MDS rank(s) live (see --rank) via "
        "`ceph tell mds.RANK client ls`.",
    )
    args = parser.parse_args()

    try:
        match_type, match_val = resolve_client(args.client)
    except OSError as e:
        print(f"error: cannot resolve client {args.client!r}: {e}", file=sys.stderr)
        return 1

    if args.file is None:
        entries = load_live(args.rank, match_type, match_val)
    else:
        if args.rank is not None:
            print(
                "error: --rank only applies in live mode (omit FILE to use it)",
                file=sys.stderr,
            )
            return 1
        entries = load_file(args.file)

    sessions = find_sessions(entries, match_type, match_val)
    if not sessions:
        print(f"error: no client session found for {args.client!r}", file=sys.stderr)
        return 1

    pools = [args.meta_pool, args.data_pool]
    cache = {}
    for i, (rank, session) in enumerate(sessions):
        if i:
            print()
        print_session(rank, session, pools, cache)
    return None


if __name__ == "__main__":
    sys.exit(main())
