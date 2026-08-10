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
  - `cephfs-mds-ops-pretty.py` can optionally resolve UID/GID to names via
    LDAP, using the `ldap3` package if installed, falling back to the
    `ldapsearch` CLI otherwise. This is off by default and only activates
    when both `--ldap-server` and `--ldap-base` are given (see `--help`).
- `jq` for the `.sh` scripts.
- `getfattr` (from `attr`/`acl` packages) for `cephfs-du`.

Some scripts hard-code environment-specific defaults (e.g. pool names
`cephfs.default.meta`/`cephfs.default.data`) that were written for a
specific cluster. Check `--help` and adjust flags/defaults as needed for
other environments.

## Tools

### RADOS / OSD

- **`osds-of-pg`** — Show the `up` and `acting` OSD sets for a
  given PG, with each OSD's host.
  `osds-of-pg <pgid>`

- **`pg-movements.py`** — For every PG where `up` != `acting`,
  print source/destination OSDs, movement type, and PG state. Handles EC
  (per-shard) and replicated (set-diff) pools differently; see
  `--help` for the full explanation of the diffing logic and edge cases.
  `pg-movements.py [--sort-by {pgid,from-osd,to-osd}]`

- **`upmaps-of-osd.sh`** — Show `pg_upmap_items` entries where a
  given OSD is a source or destination.
  `upmaps-of-osd.sh <osd>`

- **`upmaps-to-unstick-toofull-backfills.py`** —
  Propose upmap re-targets that unstick PGs wedged in `backfill_toofull`
  on a full host. When an OSD goes out, a `chooseleaf ... type host` CRUSH
  rule retries *inside the same host bucket*, so the dead OSD's PGs pile
  onto its same-host siblings instead of spreading across the cluster; on
  an already-full cluster those siblings cross `backfillfull_ratio` and the
  backfills stall. Given the OSD that went out, this finds the
  `backfill_toofull` PGs with a shard newly landing on its host and, for
  each, picks the least-utilized OSD of the same device class on a host not
  already in the PG's `up` set — a destination that satisfies the
  fault domain. Prints the proposed remaps (including each PG's existing
  `pg_upmap_items`, since `ceph osd pg-upmap-items` replaces rather than
  adds to an entry) for another script to apply; changes nothing itself.
  Handles EC pools per-shard and replicated pools by set difference. See
  `--help` for the full explanation and caveats.
  `upmaps-to-unstick-toofull-backfills.py <osd>`

- **`find-large-omap-objects.sh`** — List PGs with objects flagged
  for having large omap entries.

- **`pgremapper-v1.0.0-linux-amd64`** (prebuilt binary,
  [digitalocean/pgremapper](https://github.com/digitalocean/pgremapper)) —
  Third-party tool for controlling PG backfill/remapping without CRUSH map
  changes. Vendored as a static Linux amd64 binary; see its own project for
  source and other platforms.

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
  `ceph tell mds.X dump_{blocked,historic,ops_in_flight}` JSON. By default,
  auto-detects and queries every active MDS rank live, tagging each op with
  its rank (`--mds-rank` restricts to one); a saved JSON file can be used
  instead via `--json-file`. Resolves inodes to paths and client IDs to
  hostnames/users.
  Inode-to-path lookups are cached on disk across runs by default (see
  `--inode-cache-ttl`/`--no-inode-cache`/`--inode-cache-dir` in `--help`).
  `client ls` results are cached the same way for a short time by default
  (10 minutes), since a stale cache can hide the very client generating the
  op you're inspecting (see `--client-cache-ttl`/`--client-cache-file`).
  `cephfs-mds-ops-pretty.py dump_ops_in_flight [options]`

- **`cephfs-dir-tree-pins.sh`** — List directories pinned (exported) to
  each MDS rank.

- **`cephfs-inode-to-path`** — Resolve a hex inode number to its filesystem
  path via the metadata/data pool backtrace xattr.
  `cephfs-inode-to-path <inode-hex>`

- **`cephfs-du`** — Report size (`ceph.dir.rbytes` for directories, file
  size otherwise) of paths on a mounted CephFS, in human-readable units.
  `cephfs-du <path> [path...]`

## License

MIT (see `LICENSE`). `pgremapper-v1.0.0-linux-amd64` (vendored binary)
carries its own Apache 2.0 license.
