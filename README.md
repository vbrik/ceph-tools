# ceph-tools

Command-line tools for Ceph and CephFS cluster administration, debugging,
and troubleshooting: PG movement/remapping, OSD/PG lookups, MDS ops
inspection, CephFS client load and inode-to-path resolution. Each tool
wraps `ceph` CLI / `rados` output (mostly JSON) into something more
directly useful — grouping, resolving IDs to names, diffing, sorting — for
questions that come up repeatedly during cluster operation but aren't
answered directly by a single `ceph` subcommand.

Every script is standalone and can be copied out and run on its own; there
is no shared library or install step beyond the requirements below.

## Requirements

- A working `ceph` CLI (and `rados`, `ceph-dencoder` for a couple of tools)
  pointed at the target cluster.
- Python 3 for the `.py` scripts (`cephfs-client-inodes.py` runs under the
  `python` shebang, everything else under `python3`). Stdlib only, except:
  - `cephfs-mds-ops-pretty.py` optionally uses the `ldap3` package for
    UID/GID resolution, falling back to the `ldapsearch` CLI if it isn't
    installed, and skipping resolution automatically (showing plain numbers)
    if neither is present. `--no-ldap` forces the same behavior even when
    LDAP tooling is available.
- `jq` for the `.sh` scripts.
- `getfattr` (from `attr`/`acl` packages) for `cephfs-du`.

Some scripts hard-code environment-specific defaults (e.g. pool names
`cephfs.default.meta`/`cephfs.default.data`, or the IceCube LDAP server in
`cephfs-mds-ops-pretty.py`) that were written for a specific cluster.
Check `--help` and adjust flags/defaults as needed for other environments.

## Tools

### RADOS / OSD

- **`ceph-show-osds-of-pg`** — Show the `up` and `acting` OSD sets for a
  given PG, with each OSD's host.
  `ceph-show-osds-of-pg <pgid>`

- **`ceph-show-pg-movements.py`** — For every PG where `up` != `acting`,
  print source/destination OSDs, movement type, and PG state. Handles EC
  (per-shard) and replicated (set-diff) pools differently; see
  `--help` for the full explanation of the diffing logic and edge cases.
  `ceph-show-pg-movements.py [--sort-by {pgid,from-osd,to-osd}]`

- **`ceph-show-upmaps-of-osd.sh`** — Show `pg_upmap_items` entries where a
  given OSD is a source or destination.
  `ceph-show-upmaps-of-osd.sh <osd>`

- **`cephfs-find-large-omap-objects.sh`** — List PGs with objects flagged
  for having large omap entries.

- **`pgremapper`** (git submodule, [digitalocean/pgremapper](https://github.com/digitalocean/pgremapper)) —
  Third-party tool for controlling PG backfill/remapping without CRUSH map
  changes. Run `git submodule update --init` and build per its own README.

### CephFS clients and MDS

- **`cephfs-client-id-to-host`** — Resolve a CephFS client session ID to
  hostname and IP.
  `cephfs-client-id-to-host <client-id>`

- **`cephfs-client-inodes.py`** — Show filesystem paths for the inodes
  (delegated/completed-request/preallocated) held by a client session.
  Reads client sessions from a `client ls` JSON file/stdin, or, if the file
  argument is omitted, queries MDS rank(s) live via
  `ceph tell mds.RANK client ls` (all active ranks by default, or one rank
  via `--rank`); live queries print a warning since `client ls` can be
  resource-intensive on a busy MDS.
  `cephfs-client-inodes.py [--meta-pool POOL] [--data-pool POOL] [--rank RANK] <client> [file|-]`

- **`cephfs-client-load-top.py`** — `top`-style live view of CephFS client
  load across MDS ranks (request rate, caps, leases, in-flight requests,
  etc.), sortable and filterable by column, with optional result caching.
  `cephfs-client-load-top.py [-r RANK] [-n N] [-s COLUMNS] [--hide COLUMNS] [--cache-ttl SECONDS] [--cache-file PATH] [--full-mount-point]`

- **`cephfs-mds-ops-pretty.py`** — Human-friendly rendering of
  `ceph tell mds.X dump_{blocked,historic,ops_in_flight}` JSON (read from
  stdin): resolves inodes to paths and client IDs to hostnames/users.
  Inode-to-path lookups are cached on disk across runs by default (see
  `--inode-cache-ttl`/`--no-inode-cache`/`--inode-cache-dir` in `--help`).
  `ceph tell mds.0 dump_ops_in_flight | cephfs-mds-ops-pretty.py [options]`

- **`cephfs-dir-tree-pins.sh`** — List directories pinned (exported) to
  each MDS rank.

- **`cephfs-inode-to-path`** — Resolve a hex inode number to its filesystem
  path via the metadata/data pool backtrace xattr.
  `cephfs-inode-to-path <inode-hex>`

- **`cephfs-du`** — Report size (`ceph.dir.rbytes` for directories, file
  size otherwise) of paths on a mounted CephFS, in human-readable units.
  `cephfs-du <path> [path...]`

## License

MIT (see `LICENSE`). `pgremapper` (submodule) carries its own Apache 2.0
license.
