#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Human-friendly display of CephFS MDS operations from `ceph tell mds.X dump_*` JSON.
By default, queries all active MDS ranks live (auto-detected via `ceph fs
status`) for one of dump_blocked_ops, dump_historic_ops, or
dump_ops_in_flight, and tags each op with the rank it came from. Pass
--mds-rank to query a single rank instead. A JSON file (e.g. saved output of
one of those commands, or dump_historic_ops_by_duration) can be read instead
with --json-file.
"""

import argparse
import concurrent.futures
from dataclasses import dataclass
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

try:
    import ldap3 as _ldap3

    _LDAP3_AVAILABLE = True
except ImportError:
    _LDAP3_AVAILABLE = False

_LDAPSEARCH = shutil.which("ldapsearch")

# Parses the description field common to all MDS op types.
# Examples:
#   client_request(client.841441635:9658931 create #0x2017150963a/file.xml
#                  2026-06-22T15:00:07.343938+0000 ASYNC caller_uid=55536, caller_gid=55536{...})
#   client_request(client.X:N getattr AsXsFs #0x2008420f04a 2026-06-23T... caller_uid=...)
#   client_request(client.X:N setattr size=0 mtime=2026-06-23T... #0x20171763dce 2026-06-23T... caller_uid=...)
# The number of tokens between action and timestamp varies by op type, so we capture them as
# a single blob (args) and search for the inode within it.
DESC_RE = re.compile(
    r"^(?P<op_type>\w+)\("
    r"client\.(?P<client_id>\d+):\d+\s+"
    r"(?P<action>\S+)\s+"
    r"(?P<args>.+?)"
    r"\s+\d{4}-\d{2}-\d{2}T\S+"
    r"(?:\s+ASYNC)?"
    r"(?:\s+caller_uid=(?P<uid>\d+),\s*caller_gid=(?P<gid>\d+))?"
)

# Finds #0xINODE or #0xINODE/filename anywhere within the args blob.
INODE_ARGS_RE = re.compile(r"#(0x[0-9a-f]+)(?:/(\S+))?")

# Column widths for fixed-width fields.
_W_RANK = 4  # MDS rank, or "?" when the source dump's rank is unknown
_W_TIME = 12  # HH:MM:SS.mmm
_W_DUR = 7  # e.g. 1.685s, 559ms
_W_AGE = 7
_W_TYPE = 14  # client_request
_W_ACT = 10  # create, lookup, ...
_W_FLAG = 20  # after stripping "submit entry: " prefix
_W_ID = 12  # uid/gid: wide enough for a username or group name
_W_CLIENT = 40  # client_id (ip, short-hostname)
_W_INODE = 14  # 0x + up to 12 hex digits

HEADER = (
    f"{'rank':<{_W_RANK}}"
    f"  {'time':<{_W_TIME}}"
    f"  {'dur':>{_W_DUR}}"
    f"  {'age':>{_W_AGE}}"
    f"  {'type':<{_W_TYPE}}"
    f"  {'action':<{_W_ACT}}"
    f"  {'flag_point':<{_W_FLAG}}"
    f"  {'uid':<{_W_ID}}"
    f"  {'gid':<{_W_ID}}"
    f"  {'client (ip, hostname)':<{_W_CLIENT}}"
    f"  {'inode':<{_W_INODE}}"
    f"  path"
)


def default_inode_cache_dir():
    # uid-suffixed: avoids trusting/colliding with another user's files in shared /tmp.
    return os.path.join(tempfile.gettempdir(), f"cephfs-mds-ops-pretty.{os.getuid()}")


def inode_cache_file(cache_dir, fsid):
    """One file per fsid: pool/filesystem names aren't unique across clusters."""
    return os.path.join(cache_dir, f"{fsid}.json")


@dataclass
class Config:
    meta_pool: str
    resolve: bool
    cache_enabled: bool
    cache_ttl: int
    cache_dir: str


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
                print(
                    f"warning: ldap3 connect to {self._server_url} failed"
                    f"{' (will use ldapsearch)' if _LDAPSEARCH else ''}: {exc}",
                    file=sys.stderr,
                )
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
            [
                "ldapsearch",
                "-LLL",
                "-x",
                "-H",
                self._server_url,
                "-b",
                base,
                ldap_filter,
                attr,
            ],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            return None
        prefix = attr.lower() + ": "
        for line in r.stdout.splitlines():
            if line.lower().startswith(prefix):
                return line.split(": ", 1)[1].strip()
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
        key = ("uid", uid_num)
        if key not in self._cache:
            self._cache[key] = self._lookup(
                self._people_base, f"(uidNumber={uid_num})", "uid"
            )
        return self._cache[key]

    def resolve_gid(self, gid_num):
        key = ("gid", gid_num)
        if key not in self._cache:
            self._cache[key] = self._lookup(
                self._group_base, f"(gidNumber={gid_num})", "cn"
            )
        return self._cache[key]


def query_mds_dump(op_type, mds_rank, fatal=True):
    """Live `ceph tell mds.X dump_*` query. When only one rank is being queried,
    failure is fatal: there's no meaningful fallback when the ops data itself is
    unavailable. When querying multiple ranks, pass fatal=False so one rank's
    failure doesn't take down the whole run; the caller gets None back and can
    warn and continue."""
    r = subprocess.run(
        ["ceph", "tell", f"mds.{mds_rank}", op_type, "--format=json"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        print(
            f"{'error' if fatal else 'warning'}: ceph tell mds.{mds_rank} {op_type} "
            f"failed: {r.stderr.strip()}",
            file=sys.stderr,
        )
        if fatal:
            sys.exit(1)
        return None
    return json.loads(r.stdout)


def get_active_ranks():
    """Return the sorted list of currently active MDS ranks, via `ceph fs status`.

    Only "active" ranks are included (not standby, standby-replay, etc.): those
    are the ones that actually serve client requests and thus have op dumps
    worth showing."""
    r = subprocess.run(
        ["ceph", "fs", "status", "--format=json"], capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"error: ceph fs status failed: {r.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError as exc:
        print(f"error: could not parse `ceph fs status` output: {exc}", file=sys.stderr)
        sys.exit(1)
    ranks = sorted(
        entry["rank"]
        for entry in data.get("mdsmap", [])
        if entry.get("state") == "active"
    )
    if not ranks:
        print("error: no active MDS ranks found", file=sys.stderr)
        sys.exit(1)
    return ranks


def _parse_client_records(records):
    clients = {}
    for c in records:
        cid = c.get("id")
        if cid is None:
            continue
        addr = c.get("entity", {}).get("addr", {}).get("addr", "")
        ip = addr.split(":")[0] if addr else ""
        hostname = c.get("client_metadata", {}).get("hostname", "")
        clients[cid] = {"ip": ip, "hostname": hostname}
    return clients


def load_client_ls(files):
    """Return dict of client_id (int) -> {ip, hostname} from saved `client ls` JSON file(s)."""
    records = []
    for path in files:
        with open(path) as f:
            records.extend(json.load(f))
    return _parse_client_records(records)


def query_client_ls_live(mds_rank):
    """Live `ceph tell mds.X client ls` query. Returns None (with a warning)
    on failure -- unlike query_mds_dump, this is never fatal: the ops
    display just falls back to showing raw client IDs."""
    r = subprocess.run(
        ["ceph", "tell", f"mds.{mds_rank}", "client", "ls", "--format=json"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        print(
            f"warning: ceph tell mds.{mds_rank} client ls failed: {r.stderr.strip()}",
            file=sys.stderr,
        )
        return None
    return _parse_client_records(json.loads(r.stdout))


# Bump whenever the on-disk client ls cache payload's shape changes, so a
# cache file left behind by an older/newer version of this script is treated
# as a miss instead of being misinterpreted.
CLIENT_CACHE_VERSION = 1


def default_client_cache_file(cache_dir, mds_rank):
    return os.path.join(cache_dir, f"client-ls.mds{mds_rank}.json")


def _save_client_ls_cache(path, fsid, mds_rank, clients):
    dir_path = os.path.dirname(path) or "."
    tmp_path = None
    try:
        os.makedirs(dir_path, mode=0o700, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{os.path.basename(path)}.", dir=dir_path
        )
        with os.fdopen(fd, "w") as f:
            json.dump(
                {
                    "version": CLIENT_CACHE_VERSION,
                    "fsid": fsid,
                    "rank": mds_rank,
                    "clients": clients,
                },
                f,
            )
        os.replace(tmp_path, path)
    except OSError as exc:
        print(
            f"warning: failed to save client ls cache to {path}: {exc}", file=sys.stderr
        )
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def load_client_ls_cached(files, mds_rank, cache_ttl, cache_file, cache_dir):
    """Return dict of client_id (int) -> {ip, hostname}.

    Reads from `files` if given (no caching involved: the caller already has
    a static snapshot). Otherwise queries the MDS live, or -- when
    cache_ttl > 0 -- reuses a JSON snapshot at cache_file (or a default path
    derived from mds_rank under cache_dir) as long as it's younger than
    cache_ttl seconds, was written by this same version of the script, for
    the same mds_rank, and is stamped with the fsid of the cluster we're
    currently pointed at. A stale/mismatched/corrupt cache falls straight
    through to a live query, whose result then refreshes the cache file.

    Caching trades staleness for less client-ls load on the MDS: a client
    that connected after the cached snapshot was taken -- quite possibly the
    very client generating the op you're inspecting -- won't be in it and
    will show up with numeric/blank fields instead of hostname/IP. It's off
    by default (cache_ttl == 0) for that reason.
    """
    if files:
        return load_client_ls(files)
    if cache_ttl <= 0:
        return query_client_ls_live(mds_rank) or {}

    path = cache_file or default_client_cache_file(cache_dir, mds_rank)
    fsid = None
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < cache_ttl:
        fsid = get_fsid()
        try:
            with open(path) as f:
                cached = json.load(f)
            if (
                cached.get("version") == CLIENT_CACHE_VERSION
                and cached.get("rank") == mds_rank
                and cached.get("fsid") == fsid
            ):
                return {int(k): v for k, v in cached["clients"].items()}
        except (json.JSONDecodeError, KeyError, TypeError, OSError):
            pass  # incompatible, corrupt, or unreadable cache; fall through to a live query

    clients = query_client_ls_live(mds_rank)
    if clients is None:
        return {}
    if fsid is None:
        fsid = get_fsid()
    if fsid is not None:
        _save_client_ls_cache(path, fsid, mds_rank, clients)
    return clients


def _rados_inode_path(inode_hex, meta_pool):
    """Fall back: resolve inode via rados getxattr + ceph-dencoder backtrace.
    Only queries the metadata pool: the #inode/name target in MDS op descriptions
    is always a parent directory inode, which lives exclusively in the metadata pool."""
    ino = inode_hex[2:]  # strip 0x
    r = subprocess.run(
        ["rados", "-p", meta_pool, "getxattr", f"{ino}.00000000", "parent"],
        capture_output=True,
    )
    if r.returncode != 0:
        return None
    dec = subprocess.run(
        [
            "ceph-dencoder",
            "type",
            "inode_backtrace_t",
            "import",
            "-",
            "decode",
            "dump_json",
        ],
        input=r.stdout,
        capture_output=True,
    )
    if dec.returncode != 0:
        return None
    try:
        decoded = json.loads(dec.stdout)
    except json.JSONDecodeError:
        return None
    dnames = [a["dname"] for a in decoded.get("ancestors", [])]
    dnames.reverse()
    return ("/" + "/".join(dnames)) if dnames else None


def get_fsid():
    """Return the live cluster's fsid, or None if it can't be determined."""
    try:
        r = subprocess.run(["ceph", "fsid"], capture_output=True, text=True)
    except OSError:
        return None
    if r.returncode != 0:
        return None
    return r.stdout.strip() or None


def load_inode_cache(path):
    """Never raises. Returns (entries, age_seconds); age is None if no file existed."""
    try:
        with open(path) as f:
            entries = json.load(f)
        age = time.time() - os.path.getmtime(path)
    except FileNotFoundError:
        return {}, None
    except (OSError, ValueError) as exc:
        print(
            f"warning: ignoring unreadable inode cache {path}: {exc}", file=sys.stderr
        )
        return {}, None
    return (entries if isinstance(entries, dict) else {}), age


def save_inode_cache(path, cache):
    """A failed lookup may be transient, so only successful resolutions are persisted."""
    entries = {
        k: v
        for k, v in cache.items()
        if isinstance(v, dict) and v.get("path") is not None
    }
    dir_path = os.path.dirname(path) or "."
    tmp_path = None
    try:
        os.makedirs(dir_path, mode=0o700, exist_ok=True)
        # Merge with the current on-disk content (newer entry wins) so concurrent runs don't clobber each other.
        try:
            with open(path) as f:
                on_disk = json.load(f)
        except (OSError, ValueError):
            on_disk = {}
        if isinstance(on_disk, dict):
            for k, v in on_disk.items():
                if isinstance(v, dict) and v.get("path") is not None:
                    if k not in entries or v.get("ts", 0) > entries[k].get("ts", 0):
                        entries[k] = v
        # mkstemp avoids the symlink attack a predictable f'{path}.tmp' would invite in a shared /tmp.
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{os.path.basename(path)}.", dir=dir_path
        )
        with os.fdopen(fd, "w") as f:
            json.dump(entries, f)
        os.replace(tmp_path, path)
    except OSError as exc:
        print(f"warning: failed to save inode cache to {path}: {exc}", file=sys.stderr)
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def resolve_inode(inode_hex, cfg, cache, stats):
    """This run's own cache entries never expire; disk-loaded ones expire after cfg.cache_ttl (0 = never)."""
    key = f"{cfg.meta_pool}:{inode_hex}"
    entry = cache.get(key)
    if isinstance(entry, dict):
        ts = entry.get("ts")
        fresh = ts is not None and (
            ts >= stats["run_start"]
            or cfg.cache_ttl == 0
            or (time.time() - ts) < cfg.cache_ttl
        )
        if fresh:
            return entry.get("path")
    path = _rados_inode_path(inode_hex, cfg.meta_pool)
    if path is None and isinstance(entry, dict) and entry.get("path") is not None:
        # Keep serving the last-known-good path on a failed re-fetch; a later success overwrites it.
        path = entry["path"]
        cache[key] = {"path": path, "ts": time.time()}
        return path
    cache[key] = {"path": path, "ts": time.time()}
    if (
        path is not None
    ):  # None isn't persisted (see save_inode_cache), so it's not "new to disk"
        stats["dirty"] = True
    return path


def extract_ops(data):
    """Extract the ops list from any dump_* output shape."""
    if isinstance(data, list):
        return data
    if "ops" in data:
        return data["ops"]
    # Fallback: find any list of dicts with a 'description' key.
    for v in data.values():
        if (
            isinstance(v, list)
            and v
            and isinstance(v[0], dict)
            and "description" in v[0]
        ):
            return v
    return []


def fmt_dur(secs):
    if secs >= 4294967295:  # 0xFFFFFFFF: Ceph sentinel for uninitialized age
        return "?"
    if secs < 1.0:
        return f"{secs * 1000:.0f}ms"
    if secs < 60:
        return f"{secs:.3f}s"
    return f"{int(secs // 60)}m{secs % 60:.1f}s"


def fmt_cache_age(secs):
    secs = int(secs)
    mins, secs = divmod(secs, 60)
    if mins == 0:
        return f"{secs}s"
    hours, mins = divmod(mins, 60)
    if hours == 0:
        return f"{mins}m{secs}s"
    days, hours = divmod(hours, 24)
    if days == 0:
        return f"{hours}h{mins}m"
    return f"{days}d{hours}h"


def fmt_flag(flag):
    if flag.startswith("submit entry: "):
        flag = flag[len("submit entry: ") :]
    return flag[:_W_FLAG]


def fmt_id(num_str, resolved):
    """Return resolved name if available, else the numeric string."""
    return resolved if resolved is not None else num_str


_rdns_cache = {}


def _short_host(ip, hostname):
    """Return short hostname: strip domain, and resolve 'localhost' via rDNS."""
    short = hostname.split(".")[0] if hostname not in ("", "?") else hostname
    if short == "localhost":
        if ip not in _rdns_cache:
            try:
                _rdns_cache[ip] = socket.gethostbyaddr(ip)[0].split(".")[0]
            except OSError:
                _rdns_cache[ip] = short
        return _rdns_cache[ip]
    return short


def print_op(op, rank, clients, inode_cache, cfg, ldap, cache_stats):
    rank_str = str(rank)
    desc = op.get("description", "")
    m = DESC_RE.match(desc)

    initiated = op.get("initiated_at", "")
    # initiated_at format: 2026-06-22T15:00:07.344809+0000; [11:23] = HH:MM:SS.mmm
    time_str = initiated[11:23] if len(initiated) >= 23 else (initiated or "?")

    dur_str = fmt_dur(op.get("duration", 0))
    age_str = fmt_dur(op.get("age", 0))

    td = op.get("type_data", {})
    flag = td.get("flag_point", "?")
    op_type = td.get("op_type", "?")

    if not m:
        print(
            f"{rank_str:<{_W_RANK}}  {time_str:<{_W_TIME}}  {dur_str:>{_W_DUR}}  {age_str:>{_W_AGE}}"
            f"  {op_type}  {flag}  [unparseable: {desc}]"
        )
        return

    g = m.groupdict()
    action = g["action"]
    client_id = int(g["client_id"])
    uid_num = g.get("uid") or "?"
    gid_num = g.get("gid") or "?"
    args = g["args"]

    ci = clients.get(client_id, {})
    ip = ci.get("ip") or "?"
    hostname = _short_host(ip, ci.get("hostname") or "?")

    uid_str = (
        fmt_id(uid_num, ldap.resolve_uid(uid_num))
        if ldap and uid_num != "?"
        else uid_num
    )
    gid_str = (
        fmt_id(gid_num, ldap.resolve_gid(gid_num))
        if ldap and gid_num != "?"
        else gid_num
    )

    im = INODE_ARGS_RE.search(args)
    if im:
        inode_str = im.group(1)
        filename = im.group(2)
        if filename is None:
            # space-separated token after inode (readdir style)
            rest = args[im.end() :].split()
            filename = rest[0] if rest else None
        if cfg.resolve:
            dir_path = resolve_inode(inode_str, cfg, inode_cache, cache_stats)
        else:
            dir_path = None
        if dir_path is not None:
            path = f"{dir_path}/{filename}" if filename else dir_path
        else:
            path = f"(not persisted)/{filename}" if filename else "(not persisted)"
    else:
        inode_str = "?"
        path = args

    print(
        f"{rank_str:<{_W_RANK}}"
        f"  {time_str:<{_W_TIME}}"
        f"  {dur_str:>{_W_DUR}}"
        f"  {age_str:>{_W_AGE}}"
        f"  {op_type:<{_W_TYPE}}"
        f"  {action:<{_W_ACT}}"
        f"  {fmt_flag(flag):<{_W_FLAG}}"
        f"  {uid_str:<{_W_ID}}"
        f"  {gid_str:<{_W_ID}}"
        f"  {f'{client_id} ({ip}, {hostname})':<{_W_CLIENT}}"
        f"  {inode_str:<{_W_INODE}}"
        f"  {path}"
    )


def main():
    ap = argparse.ArgumentParser(
        description="Human-friendly display of CephFS MDS dump_* ops. "
        "By default, queries all active MDS ranks live.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="LDAP UID/GID resolution is off by default: it only activates "
        "when both --ldap-server and --ldap-base are given, and even then "
        "only if the ldap3 package or the ldapsearch command is available; "
        "otherwise UID/GID are shown as plain numbers.",
    )
    ap.add_argument(
        "op_type",
        nargs="?",
        choices=["dump_blocked_ops", "dump_historic_ops", "dump_ops_in_flight"],
        default=None,
        help="Which MDS op dump to query live; required unless --json-file is given",
    )
    ap.add_argument(
        "--json-file",
        metavar="FILE",
        help="Read ops JSON from this file (e.g. saved output of a dump_* "
        "command) instead of querying the MDS live",
    )
    ap.add_argument(
        "--client-ls",
        metavar="FILE",
        nargs="+",
        help='JSON file(s) from "ceph tell mds.N client ls"; '
        "if omitted, queries mds.MDS_RANK live",
    )
    ap.add_argument(
        "--mds-rank",
        type=int,
        default=None,
        metavar="RANK",
        help="Query only this MDS rank instead of all active ranks. Default: "
        "auto-detect and query every active rank live (via `ceph fs status`), "
        "tagging each op with its rank. Ignored (an unavailable ops dump falls "
        "back to a single rank, defaulting to 0) when --json-file is given",
    )
    ap.add_argument(
        "--client-cache-ttl",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Reuse the last `client ls` result for up to SECONDS seconds "
        "instead of querying the MDS live every run; trades staleness "
        "(a client that connected after the snapshot was taken -- possibly "
        "the one generating the op you're inspecting -- shows up with "
        "numeric/blank fields) for less client-ls load on the MDS. "
        "Default: 0, disabled (always query live). Ignored when "
        "--client-ls is given",
    )
    ap.add_argument(
        "--client-cache-file",
        default=None,
        metavar="PATH",
        help="Cache file to use with --client-cache-ttl (default: a fixed "
        "path under --inode-cache-dir, chosen based on --mds-rank)",
    )
    ap.add_argument(
        "--no-resolve-inodes",
        action="store_true",
        help="Skip inode-to-path resolution (faster, shows raw inode hex)",
    )
    ap.add_argument(
        "--meta-pool",
        default="cephfs.default.meta",
        help="CephFS metadata pool (rados fallback for inode resolution)",
    )
    ap.add_argument(
        "--inode-cache-ttl",
        type=int,
        default=3600,
        metavar="SECONDS",
        help="How long a cached inode-to-path lookup stays valid "
        "before being re-fetched from the metadata pool; "
        "0 = never expires",
    )
    ap.add_argument(
        "--no-inode-cache",
        action="store_true",
        help="Do not read or write the on-disk inode-to-path cache; "
        "always query the metadata pool (still avoids "
        "querying the same inode twice within one run)",
    )
    ap.add_argument(
        "--inode-cache-dir",
        metavar="DIR",
        default=default_inode_cache_dir(),
        help="Directory for on-disk inode-to-path cache files; one file "
        "per cluster (named by the cluster's fsid), so it's always "
        "safe to share this directory across clusters",
    )
    ap.add_argument(
        "--ldap-server",
        default=None,
        metavar="URL",
        help="LDAP server URL for UID/GID resolution (e.g. "
        "ldaps://ldap.example.com); UID/GID resolution is disabled unless "
        "this and --ldap-base are both given",
    )
    ap.add_argument(
        "--ldap-base",
        default=None,
        metavar="DN",
        help="LDAP search base for UID lookup (posixAccount), e.g. "
        "ou=People,dc=example,dc=com; UID/GID resolution is disabled "
        "unless this and --ldap-server are both given",
    )
    ap.add_argument(
        "--ldap-group-base",
        default=None,
        metavar="DN",
        help="LDAP search base for GID lookup (posixGroup); "
        "defaults to --ldap-base with ou=People replaced by ou=Group",
    )
    args = ap.parse_args()

    data = None
    if args.json_file:
        if args.op_type:
            ap.error("op_type is ignored when --json-file is given")
        with open(args.json_file) as f:
            data = json.load(f)
    elif not args.op_type:
        ap.error("op_type is required unless --json-file is given")

    if data is not None:
        # A file's dump has no discoverable rank of origin; args.mds_rank (or
        # its default) is only needed to target live client-ls/inode lookups.
        ranks = [args.mds_rank if args.mds_rank is not None else 0]
        dump_rank_known = False
    else:
        ranks = [args.mds_rank] if args.mds_rank is not None else get_active_ranks()
        dump_rank_known = True

    if args.client_cache_file and len(ranks) > 1:
        ap.error("--client-cache-file requires a single MDS rank (--mds-rank)")

    cfg = Config(
        meta_pool=args.meta_pool,
        resolve=not args.no_resolve_inodes,
        cache_enabled=not args.no_inode_cache,
        cache_ttl=args.inode_cache_ttl,
        cache_dir=args.inode_cache_dir,
    )

    ldap = None
    if args.ldap_server and args.ldap_base:
        if _LDAP3_AVAILABLE or _LDAPSEARCH:
            group_base = args.ldap_group_base or args.ldap_base.replace(
                "ou=People,", "ou=Group,", 1
            )
            ldap = LdapResolver(args.ldap_server, args.ldap_base, group_base)
        else:
            print(
                "note: neither ldap3 nor ldapsearch is available, so UID/GID "
                "will be shown as plain numbers",
                file=sys.stderr,
            )

    # The ops dump(s) (if live), client ls (one query per rank -- sessions are
    # per-rank), `ceph fsid`, and the LDAP bind are independent I/O calls; run
    # them concurrently, not back-to-back.
    with concurrent.futures.ThreadPoolExecutor() as pool:
        ops_futures = (
            {
                r: pool.submit(query_mds_dump, args.op_type, r, len(ranks) == 1)
                for r in ranks
            }
            if data is None
            else {}
        )
        if args.client_ls:
            clients_futures = {
                None: pool.submit(
                    load_client_ls_cached,
                    args.client_ls,
                    ranks[0],
                    args.client_cache_ttl,
                    args.client_cache_file,
                    cfg.cache_dir,
                )
            }
        else:
            clients_futures = {
                r: pool.submit(
                    load_client_ls_cached,
                    None,
                    r,
                    args.client_cache_ttl,
                    args.client_cache_file,
                    cfg.cache_dir,
                )
                for r in ranks
            }
        fsid_future = (
            pool.submit(get_fsid) if cfg.resolve and cfg.cache_enabled else None
        )
        if ldap:
            pool.submit(ldap._ensure_conn)

        # Grouped by rank in `ranks` order (ascending); a rank that failed
        # (multi-rank live query only -- single-rank failures are fatal) is
        # simply missing, not interleaved with the others' ops.
        dumps_by_rank = {}
        if data is None:
            for r, fut in ops_futures.items():
                result = fut.result()
                if result is not None:
                    dumps_by_rank[r] = result
            if not dumps_by_rank:
                print("error: failed to query ops from all MDS ranks", file=sys.stderr)
                return 1
        else:
            dumps_by_rank[ranks[0]] = data

        clients = {}
        for fut in clients_futures.values():
            clients.update(fut.result())

        cache_file = None
        inode_cache = {}
        cache_age = None
        if fsid_future is not None:
            fsid = fsid_future.result()
            if fsid is None:
                print(
                    "warning: could not determine cluster fsid (`ceph fsid` failed); "
                    "not using the on-disk inode cache for this run",
                    file=sys.stderr,
                )
            else:
                cache_file = inode_cache_file(cfg.cache_dir, fsid)
                inode_cache, cache_age = load_inode_cache(cache_file)

    ops = [
        (r if dump_rank_known else "?", op)
        for r, dump in dumps_by_rank.items()
        for op in extract_ops(dump)
    ]
    if not ops:
        print("No ops found in input.", file=sys.stderr)
        return 1

    cache_stats = {"dirty": False, "run_start": time.time()}
    print(HEADER)
    for rank, op in ops:
        print_op(op, rank, clients, inode_cache, cfg, ldap, cache_stats)

    if cache_file is not None and cache_stats["dirty"]:
        save_inode_cache(cache_file, inode_cache)
    if cache_age is not None:
        print(
            f"(inode cache: {cache_file}, age {fmt_cache_age(cache_age)})",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
