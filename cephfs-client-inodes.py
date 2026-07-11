#!/usr/bin/env python
import argparse
import json
import re
import socket
import subprocess
import sys

INO_RE = re.compile(r'^0x[0-9a-f]+$')
INO_SOURCES = ['delegated_inos', 'completed_requests', 'prealloc_inos']


def is_ino(v):
    return isinstance(v, str) and INO_RE.match(v) and v != '0x0'


def resolve_client(client_arg):
    """Return ('id', int) or ('ip', str) depending on the identifier type."""
    if client_arg.isdigit():
        return ('id', int(client_arg))
    try:
        socket.inet_aton(client_arg)
        return ('ip', client_arg)
    except OSError:
        pass
    ip = socket.gethostbyname(client_arg)
    return ('ip', ip)


def find_sessions(data, match_type, match_val):
    results = []
    for c in data:
        if match_type == 'id':
            if c.get('id') == match_val:
                results.append(c)
        else:
            addr = c.get('entity', {}).get('addr', {}).get('addr', '')
            if addr.split(':')[0] == match_val:
                results.append(c)
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
            ['rados', '-p', pool, 'getxattr', f'{ino}.00000000', 'parent'],
            capture_output=True)
        if rados.returncode == 0:
            break
    if rados is None or rados.returncode != 0:
        cache[ino_hex] = None
        return None
    dencoder = subprocess.run(
        ['ceph-dencoder', 'type', 'inode_backtrace_t', 'import', '-', 'decode', 'dump_json'],
        input=rados.stdout, capture_output=True)
    if dencoder.returncode != 0:
        cache[ino_hex] = None
        return None
    try:
        decoded = json.loads(dencoder.stdout)
    except json.JSONDecodeError:
        cache[ino_hex] = None
        return None
    dnames = [a['dname'] for a in decoded.get('ancestors', [])]
    dnames.reverse()
    path = '/' + '/'.join(dnames)
    cache[ino_hex] = path
    return path


def print_session(session, pools, cache):
    sid = session.get('id')
    addr = session.get('entity', {}).get('addr', {}).get('addr', '?')
    print(f'client {sid} ({addr}):')
    for source in INO_SOURCES:
        inos = extract_inos(session, source)
        if not inos:
            continue
        print(f'  {source}:')
        for ino in inos:
            path = ino_to_path(ino, pools, cache)
            if path is not None:
                print(f'    {ino} {path}')
            else:
                print(f'    {ino} (not found)')


def main():
    parser = argparse.ArgumentParser(
        description='Show inode paths for a CephFS client session.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--meta-pool', default='cephfs.default.meta',
                        help='Rados metadata pool (directories)')
    parser.add_argument('--data-pool', default='cephfs.default.data',
                        help='Rados data pool (files, symlinks)')
    parser.add_argument('client',
        help='Client id (integer), IP address, or hostname')
    parser.add_argument('file',
                        help='JSON file from "ceph tell mds.RANK client ls", or - for stdin')
    args = parser.parse_args()

    if args.file == '-':
        data = json.load(sys.stdin)
    else:
        with open(args.file) as f:
            data = json.load(f)

    try:
        match_type, match_val = resolve_client(args.client)
    except OSError as e:
        print(f'error: cannot resolve client {args.client!r}: {e}', file=sys.stderr)
        return 1

    # noinspection PyUnboundLocalVariable
    sessions = find_sessions(data, match_type, match_val)
    if not sessions:
        print(f'error: no client session found for {args.client!r}', file=sys.stderr)
        return 1

    pools = [args.meta_pool, args.data_pool]
    cache = {}
    for i, session in enumerate(sessions):
        if i:
            print()
        print_session(session, pools, cache)
    return None


if __name__ == '__main__':
    sys.exit(main())
