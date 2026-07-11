#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Human-friendly display of CephFS MDS operations from `ceph tell mds.X dump_*` JSON.
Reads JSON from stdin. Handles dump_blocked_ops, dump_historic_ops,
dump_historic_ops_by_duration, and dump_ops_in_flight.
"""
import argparse
from dataclasses import dataclass
import json
import re
import shutil
import socket
import subprocess
import sys

try:
    import ldap3 as _ldap3
    _LDAP3_AVAILABLE = True
except ImportError:
    _LDAP3_AVAILABLE = False

_LDAPSEARCH = shutil.which('ldapsearch')

# Parses the description field common to all MDS op types.
# Examples:
#   client_request(client.841441635:9658931 create #0x2017150963a/file.xml
#                  2026-06-22T15:00:07.343938+0000 ASYNC caller_uid=55536, caller_gid=55536{...})
#   client_request(client.X:N getattr AsXsFs #0x2008420f04a 2026-06-23T... caller_uid=...)
#   client_request(client.X:N setattr size=0 mtime=2026-06-23T... #0x20171763dce 2026-06-23T... caller_uid=...)
# The number of tokens between action and timestamp varies by op type, so we capture them as
# a single blob (args) and search for the inode within it.
DESC_RE = re.compile(
    r'^(?P<op_type>\w+)\('
    r'client\.(?P<client_id>\d+):\d+\s+'
    r'(?P<action>\S+)\s+'
    r'(?P<args>.+?)'
    r'\s+\d{4}-\d{2}-\d{2}T\S+'
    r'(?:\s+ASYNC)?'
    r'(?:\s+caller_uid=(?P<uid>\d+),\s*caller_gid=(?P<gid>\d+))?'
)

# Finds #0xINODE or #0xINODE/filename anywhere within the args blob.
INODE_ARGS_RE = re.compile(r'#(0x[0-9a-f]+)(?:/(\S+))?')

# Column widths for fixed-width fields.
_W_TIME = 12  # HH:MM:SS.mmm
_W_DUR  = 7   # e.g. 1.685s, 559ms
_W_AGE  = 7
_W_TYPE = 14  # client_request
_W_ACT  = 10  # create, lookup, ...
_W_FLAG = 20  # after stripping "submit entry: " prefix
_W_ID     = 12  # uid/gid: wide enough for a username or group name
_W_CLIENT = 40  # client_id (ip, short-hostname)
_W_INODE  = 14  # 0x + up to 12 hex digits

HEADER = (
    f'{"time":<{_W_TIME}}'
    f'  {"dur":>{_W_DUR}}'
    f'  {"age":>{_W_AGE}}'
    f'  {"type":<{_W_TYPE}}'
    f'  {"action":<{_W_ACT}}'
    f'  {"flag_point":<{_W_FLAG}}'
    f'  {"uid":<{_W_ID}}'
    f'  {"gid":<{_W_ID}}'
    f'  {"client (ip, hostname)":<{_W_CLIENT}}'
    f'  {"inode":<{_W_INODE}}'
    f'  path'
)


@dataclass
class Config:
    meta_pool: str
    resolve: bool


_CONN_FAILED = object()  # sentinel: LDAP connection permanently failed


class LdapResolver:
    """Resolves numeric UID/GID to symbolic names via LDAP, with caching.

    Uses ldap3 if available, falls back to the ldapsearch command.
    Returns None (caller shows numeric) when both are unavailable or lookup fails."""

    def __init__(self, server_url, people_base, group_base):
        self._server_url = server_url
        self._people_base = people_base
        self._group_base = group_base
        self._cache = {}
        self._conn = None  # None = not yet attempted; _CONN_FAILED = gave up

    def _ensure_conn(self):
        if self._conn is _CONN_FAILED:
            return False
        if self._conn is None:
            try:
                srv = _ldap3.Server(self._server_url, get_info=_ldap3.NONE)
                self._conn = _ldap3.Connection(srv, auto_bind=True)
            except Exception as exc:
                print(f'warning: ldap3 connect to {self._server_url} failed'
                      f'{" (will use ldapsearch)" if _LDAPSEARCH else ""}: {exc}',
                      file=sys.stderr)
                self._conn = _CONN_FAILED
                return False
        return True

    def _lookup_ldap3(self, base, ldap_filter, attr):
        if not self._ensure_conn():
            return None
        try:
            self._conn.search(base, ldap_filter, attributes=[attr])
            if self._conn.entries:
                val = self._conn.entries[0][attr].value
                return str(val) if val is not None else None
        except Exception:
            pass
        return None

    def _lookup_ldapsearch(self, base, ldap_filter, attr):
        r = subprocess.run(
            ['ldapsearch', '-LLL', '-x', '-H', self._server_url,
             '-b', base, ldap_filter, attr],
            capture_output=True, text=True)
        if r.returncode != 0:
            return None
        prefix = attr.lower() + ': '
        for line in r.stdout.splitlines():
            if line.lower().startswith(prefix):
                return line.split(': ', 1)[1].strip()
        return None

    def _lookup(self, base, ldap_filter, attr):
        if _LDAP3_AVAILABLE:
            result = self._lookup_ldap3(base, ldap_filter, attr)
            # If the connection succeeded (even with no result), don't also try
            # ldapsearch — the entry simply doesn't exist.  Only fall through
            # when the connection itself failed.
            if self._conn is not _CONN_FAILED:
                return result
        if _LDAPSEARCH:
            return self._lookup_ldapsearch(base, ldap_filter, attr)
        return None

    def resolve_uid(self, uid_num):
        key = ('uid', uid_num)
        if key not in self._cache:
            self._cache[key] = self._lookup(
                self._people_base, f'(uidNumber={uid_num})', 'uid')
        return self._cache[key]

    def resolve_gid(self, gid_num):
        key = ('gid', gid_num)
        if key not in self._cache:
            self._cache[key] = self._lookup(
                self._group_base, f'(gidNumber={gid_num})', 'cn')
        return self._cache[key]


def load_client_ls(files, mds_rank):
    """Return dict of client_id (int) -> {ip, hostname}."""
    records = []
    if files:
        for path in files:
            with open(path) as f:
                records.extend(json.load(f))
    else:
        r = subprocess.run(
            ['ceph', 'tell', f'mds.{mds_rank}', 'client', 'ls', '--format=json'],
            capture_output=True, text=True)
        if r.returncode != 0:
            print(f'warning: ceph tell mds.{mds_rank} client ls failed: {r.stderr.strip()}',
                  file=sys.stderr)
            return {}
        records = json.loads(r.stdout)

    clients = {}
    for c in records:
        cid = c.get('id')
        if cid is None:
            continue
        addr = c.get('entity', {}).get('addr', {}).get('addr', '')
        ip = addr.split(':')[0] if addr else ''
        hostname = c.get('client_metadata', {}).get('hostname', '')
        clients[cid] = {'ip': ip, 'hostname': hostname}
    return clients


def _rados_inode_path(inode_hex, meta_pool):
    """Fall back: resolve inode via rados getxattr + ceph-dencoder backtrace.
    Only queries the metadata pool: the #inode/name target in MDS op descriptions
    is always a parent directory inode, which lives exclusively in the metadata pool."""
    ino = inode_hex[2:]  # strip 0x
    r = subprocess.run(
        ['rados', '-p', meta_pool, 'getxattr', f'{ino}.00000000', 'parent'],
        capture_output=True)
    if r.returncode != 0:
        return None
    dec = subprocess.run(
        ['ceph-dencoder', 'type', 'inode_backtrace_t', 'import', '-', 'decode', 'dump_json'],
        input=r.stdout, capture_output=True)
    if dec.returncode != 0:
        return None
    try:
        decoded = json.loads(dec.stdout)
    except json.JSONDecodeError:
        return None
    dnames = [a['dname'] for a in decoded.get('ancestors', [])]
    dnames.reverse()
    return ('/' + '/'.join(dnames)) if dnames else None


def resolve_inode(inode_hex, cfg, cache):
    if inode_hex in cache:
        return cache[inode_hex]
    path = _rados_inode_path(inode_hex, cfg.meta_pool)
    cache[inode_hex] = path
    return path


def extract_ops(data):
    """Extract the ops list from any dump_* output shape."""
    if isinstance(data, list):
        return data
    if 'ops' in data:
        return data['ops']
    # Fallback: find any list of dicts with a 'description' key.
    for v in data.values():
        if isinstance(v, list) and v and isinstance(v[0], dict) and 'description' in v[0]:
            return v
    return []


def fmt_dur(secs):
    if secs >= 4294967295:  # 0xFFFFFFFF: Ceph sentinel for uninitialized age
        return '?'
    if secs < 1.0:
        return f'{secs * 1000:.0f}ms'
    if secs < 60:
        return f'{secs:.3f}s'
    return f'{int(secs // 60)}m{secs % 60:.1f}s'


def fmt_flag(flag):
    if flag.startswith('submit entry: '):
        flag = flag[len('submit entry: '):]
    return flag[:_W_FLAG]


def fmt_id(num_str, resolved):
    """Return resolved name if available, else the numeric string."""
    return resolved if resolved is not None else num_str


_rdns_cache = {}

def _short_host(ip, hostname):
    """Return short hostname: strip domain, and resolve 'localhost' via rDNS."""
    short = hostname.split('.')[0] if hostname not in ('', '?') else hostname
    if short == 'localhost':
        if ip not in _rdns_cache:
            try:
                _rdns_cache[ip] = socket.gethostbyaddr(ip)[0].split('.')[0]
            except OSError:
                _rdns_cache[ip] = short
        return _rdns_cache[ip]
    return short


def print_op(op, clients, inode_cache, cfg, ldap):
    desc = op.get('description', '')
    m = DESC_RE.match(desc)

    initiated = op.get('initiated_at', '')
    # initiated_at format: 2026-06-22T15:00:07.344809+0000; [11:23] = HH:MM:SS.mmm
    time_str = initiated[11:23] if len(initiated) >= 23 else (initiated or '?')

    dur_str = fmt_dur(op.get('duration', 0))
    age_str = fmt_dur(op.get('age', 0))

    td = op.get('type_data', {})
    flag = td.get('flag_point', '?')
    op_type = td.get('op_type', '?')

    if not m:
        print(f'{time_str:<{_W_TIME}}  {dur_str:>{_W_DUR}}  {age_str:>{_W_AGE}}'
              f'  {op_type}  {flag}  [unparseable: {desc}]')
        return

    g = m.groupdict()
    action = g['action']
    client_id = int(g['client_id'])
    uid_num = g.get('uid') or '?'
    gid_num = g.get('gid') or '?'
    args = g['args']

    ci = clients.get(client_id, {})
    ip = ci.get('ip') or '?'
    hostname = _short_host(ip, ci.get('hostname') or '?')

    uid_str = fmt_id(uid_num, ldap.resolve_uid(uid_num)) if ldap and uid_num != '?' else uid_num
    gid_str = fmt_id(gid_num, ldap.resolve_gid(gid_num)) if ldap and gid_num != '?' else gid_num

    im = INODE_ARGS_RE.search(args)
    if im:
        inode_str = im.group(1)
        filename = im.group(2)
        if filename is None:
            # space-separated token after inode (readdir style)
            rest = args[im.end():].split()
            filename = rest[0] if rest else None
        if cfg.resolve:
            dir_path = resolve_inode(inode_str, cfg, inode_cache)
        else:
            dir_path = None
        if dir_path is not None:
            path = f'{dir_path}/{filename}' if filename else dir_path
        else:
            path = f'(not persisted)/{filename}' if filename else '(not persisted)'
    else:
        inode_str = '?'
        path = args

    print(
        f'{time_str:<{_W_TIME}}'
        f'  {dur_str:>{_W_DUR}}'
        f'  {age_str:>{_W_AGE}}'
        f'  {op_type:<{_W_TYPE}}'
        f'  {action:<{_W_ACT}}'
        f'  {fmt_flag(flag):<{_W_FLAG}}'
        f'  {uid_str:<{_W_ID}}'
        f'  {gid_str:<{_W_ID}}'
        f'  {f"{client_id} ({ip}, {hostname})":<{_W_CLIENT}}'
        f'  {inode_str:<{_W_INODE}}'
        f'  {path}'
    )


def main():
    ap = argparse.ArgumentParser(
        description='Human-friendly display of CephFS MDS dump_* ops. Reads JSON from stdin.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog='The --ldap-* options only matter when the ldap3 package or the '
               'ldapsearch command is available; otherwise UID/GID are shown as '
               'plain numbers regardless of --no-ldap.')
    ap.add_argument('--client-ls', metavar='FILE', nargs='+',
                    help='JSON file(s) from "ceph tell mds.N client ls"; '
                         'if omitted, queries mds.MDS_RANK live')
    ap.add_argument('--mds-rank', type=int, default=0,
                    help='MDS rank used for live ceph tell queries (client ls and dump inode)')
    ap.add_argument('--no-resolve-inodes', action='store_true',
                    help='Skip inode-to-path resolution (faster, shows raw inode hex)')
    ap.add_argument('--meta-pool', default='cephfs.default.meta',
                    help='CephFS metadata pool (rados fallback for inode resolution)')
    ap.add_argument('--ldap-server', default='ldaps://ldap-supplier.icecube.wisc.edu',
                    help='LDAP server URL for UID/GID resolution')
    ap.add_argument('--ldap-base', default='ou=People,dc=icecube,dc=wisc,dc=edu',
                    help='LDAP search base for UID lookup (posixAccount)')
    ap.add_argument('--ldap-group-base', default=None,
                    help='LDAP search base for GID lookup (posixGroup); '
                         'defaults to --ldap-base with ou=People replaced by ou=Group')
    ap.add_argument('--no-ldap', action='store_true',
                    help='Skip LDAP UID/GID resolution; only useful when ldap3 or '
                         'ldapsearch is installed, since resolution is skipped '
                         'automatically otherwise')
    args = ap.parse_args()

    data = json.load(sys.stdin)
    ops = extract_ops(data)
    if not ops:
        print('No ops found in input.', file=sys.stderr)
        return 1

    cfg = Config(
        meta_pool=args.meta_pool,
        resolve=not args.no_resolve_inodes,
    )

    ldap = None
    if not args.no_ldap and (_LDAP3_AVAILABLE or _LDAPSEARCH):
        group_base = args.ldap_group_base or args.ldap_base.replace(
            'ou=People,', 'ou=Group,', 1)
        ldap = LdapResolver(args.ldap_server, args.ldap_base, group_base)
    elif not args.no_ldap:
        print('note: neither ldap3 nor ldapsearch is available, so UID/GID '
              'will be shown as plain numbers', file=sys.stderr)

    if ldap:
        ldap._ensure_conn()

    clients = load_client_ls(args.client_ls, args.mds_rank)
    inode_cache = {}

    print(HEADER)
    for op in ops:
        print_op(op, clients, inode_cache, cfg, ldap)
    return 0


if __name__ == '__main__':
    sys.exit(main())
