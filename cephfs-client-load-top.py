#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Show CephFS clients with the highest load, live from the cluster.
"""
import argparse
import collections
import json
import os
import socket
import subprocess
import sys
import tempfile
import time


# A client with this hostname hasn't reported a real one (e.g. it mounted
# over loopback); fall back to a reverse DNS lookup of its IP instead.
LOCALHOST_NAMES = {'localhost', 'localhost.localdomain'}
DOMAIN_SUFFIX = '.icecube.wisc.edu'


def ceph_cmd(args):
    """Run a `ceph` CLI command and return its parsed JSON output."""
    result = subprocess.run(['ceph'] + args, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)
    return json.loads(result.stdout)


def get_active_ranks():
    """Return the sorted list of MDS ranks currently 'in' any filesystem."""
    data = ceph_cmd(['fs', 'dump', '--format', 'json'])
    ranks = set()
    for fs in data.get('filesystems', []):
        ranks.update(fs.get('mdsmap', {}).get('in', []))
    return sorted(ranks)


def get_ip(session):
    addr = session.get('entity', {}).get('addr', {}).get('addr', '')
    return addr.rsplit(':', 1)[0] if addr else None


def get_hostname(meta, ip):
    hostname = meta.get('hostname')
    if hostname in LOCALHOST_NAMES and ip:
        try:
            hostname = socket.gethostbyaddr(ip)[0]
        except socket.herror:
            hostname = ip
    if hostname and hostname.endswith(DOMAIN_SUFFIX):
        hostname = hostname[:-len(DOMAIN_SUFFIX)]
    return hostname


def get_caps_value(session, key):
    # recall_caps/release_caps are decaying counters shaped like
    # {"value": ..., "halflife": ...} on some ceph versions and plain
    # numbers on others; handle both without caring which.
    val = session.get(key)
    return val.get('value') if isinstance(val, dict) else val


def build_row(rank, session):
    """Flatten one `session ls` entry into the fixed set of display columns."""
    meta = session.get('client_metadata', {})
    ip = get_ip(session)
    return {
        'rank': rank,
        'id': session.get('id'),
        'hostname': get_hostname(meta, ip),
        'ip': ip,
        'request_load_avg': session.get('request_load_avg'),
        'num_leases': session.get('num_leases'),
        'num_caps': session.get('num_caps'),
        'requests_in_flight': session.get('requests_in_flight'),
        'num_completed_requests': session.get('num_completed_requests'),
        'num_completed_flushes': session.get('num_completed_flushes'),
        'recall_caps': get_caps_value(session, 'recall_caps'),
        'release_caps': get_caps_value(session, 'release_caps'),
        'mount_point': meta.get('root'),
    }


# --------------------------------------------------------------------------
# Column model
#
# `name` is what users type for --sort/--hide; `header` uses '\n' to wrap
# long headings across two or three lines. Order here is the default and
# only display order (rank is always first, mount_point always last, per
# design; --hide just removes columns, it never reorders them).
# --------------------------------------------------------------------------

Column = collections.namedtuple('Column', ['name', 'header', 'formatter', 'align'])


def fmt_int(value):
    return '-' if value is None else str(value)


def fmt_float(decimals):
    def _fmt(value):
        return '-' if value is None else f'{value:.{decimals}f}'
    return _fmt


def fmt_str(value):
    return value if value else '-'


DEFAULT_MOUNT_POINT_WIDTH = 80


def truncate_middle(text, width):
    """Abbreviate `text` to `width` characters by cutting out its middle."""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    keep = width - len('...')
    left, right = -(-keep // 2), keep // 2  # left gets the extra char on odd widths
    return text[:left] + '...' + (text[-right:] if right else '')


def fmt_path(width):
    """Build a mount-point formatter; width=None disables truncation."""
    def _fmt(value):
        if not value:
            return '-'
        return value if width is None else truncate_middle(value, width)
    return _fmt


def build_columns(full_mount_point=False):
    """Return the display columns in their fixed order.

    `name` is what users type for --sort/--hide; `header` uses '\\n' to wrap
    long headings across two or three lines. Order here is the default and
    only display order (mds rank is always first, mount_point always last,
    per design; --hide just removes columns, it never reorders them).
    """
    mount_point_width = None if full_mount_point else DEFAULT_MOUNT_POINT_WIDTH
    return [
        Column('rank', 'mds\nrank', fmt_int, '>'),
        Column('id', 'id', fmt_int, '>'),
        Column('hostname', 'hostname', fmt_str, '<'),
        Column('ip', 'ip', fmt_str, '<'),
        Column('request_load_avg', 'request\nload avg', fmt_float(2), '>'),
        Column('num_leases', 'num\nleases', fmt_int, '>'),
        Column('num_caps', 'num\ncaps', fmt_int, '>'),
        Column('requests_in_flight', 'requests\nin flight', fmt_int, '>'),
        Column('num_completed_requests', 'num\ncompleted\nrequests', fmt_int, '>'),
        Column('num_completed_flushes', 'num\ncompleted\nflushes', fmt_int, '>'),
        Column('recall_caps', 'recall\ncaps', fmt_float(1), '>'),
        Column('release_caps', 'release\ncaps', fmt_float(1), '>'),
        Column('mount_point', 'mount point', fmt_path(mount_point_width), '<'),
    ]


COLUMN_NAMES = [c.name for c in build_columns()]


def parse_column_list(value):
    """Split a comma-separated --sort/--hide value into column names."""
    return [item.strip() for item in value.split(',') if item.strip()]


def sort_rows(rows, sort_keys):
    """Sort rows by sort_keys, highest first, primary key first.

    Applies one stable pass per key, starting with the *least* significant
    key and finishing with the most significant one. Because list.sort() is
    stable (and stays stable under reverse=True), each later pass only
    breaks ties left by the previous one, which yields correct multi-key
    ordering without building composite sort keys -- which matters here
    since different columns mix None, numbers, and strings that don't
    compare against each other. Rows missing a given key sort after rows
    that have it, for that key.
    """
    for key in reversed(sort_keys):
        has_value = [r for r in rows if r[key] is not None]
        no_value = [r for r in rows if r[key] is None]
        has_value.sort(key=lambda r: r[key], reverse=True)
        rows = has_value + no_value
    return rows


def print_table(columns, rows):
    """Render rows as a plain-text table with (possibly multi-line) headers."""
    header_lines = [c.header.split('\n') for c in columns]
    height = max((len(h) for h in header_lines), default=1)
    # Top-pad shorter headers with blank lines so all headers bottom-align
    # against the separator line below them.
    header_lines = [[''] * (height - len(h)) + h for h in header_lines]

    formatted_rows = [[c.formatter(row[c.name]) for c in columns] for row in rows]

    widths = []
    for i, col in enumerate(columns):
        width = max(len(line) for line in header_lines[i])
        if formatted_rows:
            width = max(width, max(len(r[i]) for r in formatted_rows))
        widths.append(width)

    def render(cells):
        return '  '.join(
            text.rjust(widths[i]) if columns[i].align == '>' else text.ljust(widths[i])
            for i, text in enumerate(cells)
        )

    for line_idx in range(height):
        print(render([header_lines[i][line_idx] for i in range(len(columns))]))
    print('  '.join('-' * w for w in widths))
    for r in formatted_rows:
        print(render(r))


COLUMN_NOTES = """
Column notes:
  num_completed_requests / num_completed_flushes: how many replies (to
    requests / to cap flushes) the MDS is holding for this client because it
    hasn't been acked yet. High/growing values mean the client is slow to
    ack, which costs the MDS memory.

  recall_caps / release_caps: how hard the MDS is asking this client to give
    back capabilities vs. how many it's actually giving back. recall_caps
    high with release_caps not keeping up means the client is holding onto
    caps under MDS cache pressure, hurting the whole cluster.
""".strip('\n')


def print_column_notes(columns):
    """Print a short explanation of a few easily-confused column pairs.

    Only prints if at least one column from a given pair is visible, so the
    notes don't show up for columns the user chose to --hide.
    """
    shown = {c.name for c in columns}
    pairs = [
        {'num_completed_requests', 'num_completed_flushes'},
        {'recall_caps', 'release_caps'},
    ]
    if any(shown & pair for pair in pairs):
        print()
        print(COLUMN_NOTES)


# --------------------------------------------------------------------------
# Live query + optional caching
# --------------------------------------------------------------------------

def default_cache_path(rank):
    tag = f'rank{rank}' if rank is not None else 'all-ranks'
    return os.path.join(tempfile.gettempdir(), f'cephfs-load-top.{tag}.cache.json')


# Bump whenever the on-disk cache payload's shape changes, so a cache file
# left behind by an older (or newer) version of this script is treated as a
# miss -- refetched and overwritten -- instead of crashing or being
# silently misinterpreted.
CACHE_VERSION = 2


def get_cluster_fsid():
    """Return the fsid (unique id) of the cluster `ceph` currently targets.

    Which cluster that is can change between invocations via CEPH_CONF,
    CEPH_ARGS, CEPH_KEYRING, or other environment variables the `ceph` CLI
    honors -- nothing this script controls. A cache keyed only on rank/path
    would happily serve another cluster's stale MDS data after a switch, so
    every cache file is stamped with the fsid it was fetched from, and that
    stamp is checked against the *current* fsid before the cache is trusted.
    `ceph fsid` is a single lightweight monitor RPC, unlike `session ls`, so
    checking it doesn't undercut the point of caching.
    """
    return ceph_cmd(['fsid', '--format', 'json'])['fsid']


def query_live(rank):
    """Query `ceph tell mds.RANK session ls` for one rank or all active ranks."""
    ranks = [rank] if rank is not None else get_active_ranks()
    entries = []
    for r in ranks:
        entries.extend(
            {'rank': r, 'session': s} for s in ceph_cmd(['tell', f'mds.{r}', 'session', 'ls'])
        )
    return entries


def load_session_entries(rank, cache_ttl, cache_file):
    """Return a list of {'rank': int, 'session': dict} entries.

    Live data comes from `ceph tell mds.RANK session ls`, which can be slow
    to answer on a busy cluster. When cache_ttl > 0, a JSON snapshot at
    cache_file (or a default path derived from `rank`) is reused as long as
    it is younger than cache_ttl seconds, was written by this same version
    of the script (see CACHE_VERSION), for the same `rank` selection, and is
    stamped with the fsid of the cluster we're currently pointed at (see
    get_cluster_fsid) -- the `rank` check matters because a custom
    --cache-file can otherwise be reused across different --rank selections.
    Otherwise the MDS is queried live and the snapshot is refreshed.
    """
    if cache_ttl <= 0:
        return query_live(rank)

    path = cache_file or default_cache_path(rank)
    # Only pay for the `ceph fsid` RPC when there's actually a fresh cache
    # file to validate against; a cold or expired cache skips straight to
    # a live query without it.
    fsid = None
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < cache_ttl:
        fsid = get_cluster_fsid()
        try:
            with open(path) as f:
                cached = json.load(f)
            if (cached['version'] == CACHE_VERSION
                    and cached['rank'] == rank
                    and cached['fsid'] == fsid):
                return cached['entries']
        except (json.JSONDecodeError, KeyError, TypeError, OSError):
            pass  # incompatible, corrupt, or unreadable cache file; fall through to a live query

    entries = query_live(rank)
    if fsid is None:
        fsid = get_cluster_fsid()
    with open(path, 'w') as f:
        json.dump({'version': CACHE_VERSION, 'fsid': fsid, 'rank': rank, 'entries': entries}, f)
    return entries


# Convention: the authoritative list of supported column names lives in this
# epilog. Any --help text for an argument that takes column name(s) should
# point here ("see supported columns below") instead of repeating the list,
# so there's a single source of truth to keep in sync with build_columns().
EPILOG = (
    'Supported columns (for --sort and --hide):\n'
    '  ' + ', '.join(COLUMN_NAMES) + '\n'
)


def main():
    parser = argparse.ArgumentParser(
        description='Show CephFS clients with the highest load, live from the cluster.',
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '-r', '--rank',
        type=int,
        default=None,
        metavar='RANK',
        help='Only query this MDS rank (default: query all active ranks, per `ceph fs dump`)',
    )
    parser.add_argument(
        '-n',
        type=int,
        default=40,
        metavar='N',
        help='Show only the top N clients after sorting (default: 40; use 0 to show all)',
    )
    parser.add_argument(
        '-s', '--sort',
        default='request_load_avg',
        metavar='COLUMN[,COLUMN...]',
        help=(
            'Column(s) to sort by, highest first (default: request_load_avg). '
            'Comma-separated for primary, secondary, ... keys. '
            'See supported columns below.'
        ),
    )
    parser.add_argument(
        '--hide',
        default='',
        metavar='COLUMN[,COLUMN...]',
        help=(
            'Column(s) to hide from the output. All columns are shown by default. '
            'Comma-separated. See supported columns below.'
        ),
    )
    parser.add_argument(
        '--cache-ttl',
        type=int,
        default=0,
        metavar='SECONDS',
        help=(
            'Reuse cluster data for up to SECONDS seconds instead of always '
            'querying live (default: 0, caching disabled)'
        ),
    )
    parser.add_argument(
        '--cache-file',
        default=None,
        metavar='PATH',
        help=(
            'Cache file to use with --cache-ttl (default: a fixed path under the '
            'system temp directory, chosen based on --rank)'
        ),
    )
    parser.add_argument(
        '--full-mount-point',
        action='store_true',
        help=(
            'Show the full mount point path. By default it is abbreviated to '
            f'{DEFAULT_MOUNT_POINT_WIDTH} characters by cutting out the middle.'
        ),
    )
    args = parser.parse_args()

    sort_keys = parse_column_list(args.sort)
    hide_columns = set(parse_column_list(args.hide))
    for key in sort_keys:
        if key not in COLUMN_NAMES:
            parser.error(f"unknown --sort column '{key}'; see supported columns below")
    for key in hide_columns:
        if key not in COLUMN_NAMES:
            parser.error(f"unknown --hide column '{key}'; see supported columns below")

    entries = load_session_entries(args.rank, args.cache_ttl, args.cache_file)
    rows = [build_row(e['rank'], e['session']) for e in entries]
    rows = sort_rows(rows, sort_keys)
    if args.n > 0:
        rows = rows[:args.n]

    columns = [c for c in build_columns(args.full_mount_point) if c.name not in hide_columns]
    print_table(columns, rows)
    print_column_notes(columns)


if __name__ == '__main__':
    sys.exit(main())
