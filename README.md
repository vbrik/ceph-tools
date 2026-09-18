# ceph-tools

Command-line tools for Ceph and CephFS cluster administration, debugging,
and troubleshooting: PG movement/remapping, upmap manipulation, scrub
scheduling, OSD/PG lookups, MDS ops inspection, CephFS client load and
inode-to-path resolution, and finding large, wide, or fast-growing
directories on a mounted CephFS. Most tools wrap `ceph` CLI / `rados`
output (mostly JSON) into something more directly useful — grouping,
resolving IDs to names, diffing, sorting; the rest read CephFS recursive
statistics (`ceph.dir.*` extended attributes) directly off a mount. They
answer questions that come up repeatedly during cluster operation but
aren't answered directly by a single `ceph` subcommand.

Every script is standalone and can be copied out and run on its own; there
is no shared library or install step beyond the requirements below.

## Requirements

- A working `ceph` CLI (and `rados`, `ceph-dencoder` for a couple of tools)
  pointed at the target cluster.
- Python 3 for the `.py` scripts. Most run under the `python3` shebang;
  `cephfs-client-inodes.py`, `find-cephfs-rctime.py` and
  `scrub-all-pgs-that-need-it.py` use `python`. Stdlib only, except:
  - `find-cephfs-rctime.py` requires `python-dateutil` for its flexible
    `--min-ctime` date parsing.
  - `mds-ops-pretty.py` can optionally resolve UID/GID to names via
    LDAP, using the `ldap3` package if installed, falling back to the
    `ldapsearch` CLI otherwise. This is off by default and only activates
    when both `--ldap-server` and `--ldap-base` are given (see `--help`).
  - `upmap-remapped.py` uses the `rados` Python bindings if importable and
    otherwise falls back to shelling out to `ceph ... | jq`.
- `jq` for the `.sh` scripts and for `upmap-remapped.py`'s fallback path.
- A mounted CephFS (kernel client or ceph-fuse) for `cephfs-du`,
  `cephfs-growth.py`, `find-cephfs-rctime.py` and `cephfs-find-wide-dirs`;
  plus `getfattr` (from `attr`/`acl` packages) for `cephfs-du`.

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
  Propose upmap re-targets that unstick PGs wedged in `backfill_toofull` on
  full hosts. When an OSD goes out, a `chooseleaf ... type host` CRUSH rule
  retries *inside the same host bucket*, so the dead OSD's PGs pile onto its
  same-host siblings instead of spreading across the cluster; on an
  already-full cluster those siblings cross `backfillfull_ratio` and the
  backfills stall. It needs no arguments to say where to look: it scans every
  `backfill_toofull` PG cluster-wide, finds each shard newly landing on a host
  that cannot take it (in `up` but not `acting`), and picks the least-utilized
  OSD of that shard's own device class on a host not already in the PG's `up`
  set — a destination that satisfies the fault domain. Prints the proposed
  remaps
  (including each PG's existing `pg_upmap_items`, since
  `ceph osd pg-upmap-items` replaces rather than adds to an entry) for
  another script to apply; changes nothing itself. `--pgremapper` switches
  the output to headerless `<pgid> <from osd> <target osd>` lines, ready to
  feed to `pgremapper remap` (via `xargs -a remaps.txt -L1 …`). Each target
  OSD is used at most once, so a large run may report a tail as unplaceable;
  apply, drain, re-run. Handles EC pools per-shard and replicated pools by
  set difference. See the script's module docstring for the full explanation
  and caveats (`--help` summarizes and points there).
  `upmaps-to-unstick-toofull-backfills.py [--pgremapper]`

- **`scrub-all-pgs-that-need-it.py`** — Scrub and deep-scrub every PG that
  `ceph health detail` reports under `PG_NOT_SCRUBBED` /
  `PG_NOT_DEEP_SCRUBBED`. For each such PG it looks up the acting primary
  via `ceph pg <pgid> query` and issues `ceph tell osd.<primary> scrub` /
  `deep_scrub`, which is a work-around for a broken `ceph pg (deep-)scrub`.
  Acts on the cluster immediately — it has no dry-run mode.
  `scrub-all-pgs-that-need-it.py`

- **`find-large-omap-objects.sh`** — List PGs with objects flagged
  for having large omap entries.

- **`upmap-remapped.py`** (third-party, from
  [cernceph/ceph-scripts](https://github.com/cernceph/ceph-scripts/blob/master/tools/upmap/upmap-remapped.py))
  — Print `ceph osd pg-upmap-items` / `rm-pg-upmap-items` commands that make all
  currently remapped PGs immediately `active+clean`, pinning data where it
  already is so the mgr `upmap` balancer can unwind the movement gradually.
  The intended use is around a disruptive change (new hosts, CRUSH rule or
  tunable changes) with `norebalance` set: apply, let the cluster go clean,
  then unset the flag. Prints to stdout and changes nothing until piped to a
  shell. `--ignore-backfilling` leaves PGs that are already backfilling
  alone. Handles EC and replicated pools differently; see the header comment
  for the full procedure and its disclaimers. The copy here predates the
  current upstream version, which has since fixed error handling on the
  no-`rados` fallback path.
  `upmap-remapped.py [--ignore-backfilling]`

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

- **`mds-ops-pretty.py`** — Human-friendly rendering of
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
  `mds-ops-pretty.py dump_ops_in_flight [options]`

- **`cephfs-dir-tree-pins.sh`** — List directories pinned (exported) to
  each MDS rank.

- **`cephfs-inode-to-path`** — Resolve a hex inode number to its filesystem
  path via the metadata/data pool backtrace xattr.
  `cephfs-inode-to-path <inode-hex>`

### CephFS trees (on a mounted filesystem)

These read CephFS recursive statistics (`ceph.dir.rbytes`,
`ceph.dir.rfiles`, `ceph.dir.rctime`, …) off a mount, and need no cluster
credentials or MDS admin access — only read access to the directories
being examined.

- **`cephfs-du`** — Report size (`ceph.dir.rbytes` for directories, file
  size otherwise) of paths on a mounted CephFS, in human-readable units.
  `cephfs-du <path> [path...]`

- **`cephfs-growth.py`** — Locate the fastest-growing subtree without
  walking the tree. Samples `ceph.dir.rbytes` on a directory's immediate
  children twice, ranks children by delta, then descends into the top
  grower and repeats. Cost is O(children per level), not O(files).
  `cephfs-growth.py [--interval SECONDS] [--depth N] [--top N] [--workers N] <root>`

- **`find-cephfs-rctime.py`** — Find files and directories whose `ctime` is
  at or after a given date, using `ceph.dir.rctime` to prune subtrees that
  cannot contain a match — much faster than `find -newer` on a large tree.
  Accepts a variety of date formats, or Unix time as `@SECONDS`. `--parents`
  prints only the parent directories of matches. Being IO-bound, it defaults
  to far more workers than there are CPUs.
  `find-cephfs-rctime.py --min-ctime DATE [--relative] [--parents] [--threads NUM] <path>`

- **`cephfs-find-wide-dirs`** — Quickly find directories holding many
  files, using the `ceph.dir.files` / `ceph.dir.rfiles` xattrs.
  `--min-num-files 0` skips the xattr reads and matches every directory.
  Vendored here as a prebuilt x86-64 Linux binary; the Rust source lives in
  [vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs).
  `cephfs-find-wide-dirs --min-num-files NUMBER [--threads NUMBER] <path>`

## License

MIT (see `LICENSE`), except for the vendored third-party tools:

- `pgremapper-v1.0.0-linux-amd64` carries its own Apache 2.0 license.
- `upmap-remapped.py` comes from
  [cernceph/ceph-scripts](https://github.com/cernceph/ceph-scripts), which
  is GPL-2.0 licensed; the file itself credits Dan van der Ster (CERN) and
  carries a no-warranty disclaimer.

`cephfs-find-wide-dirs` is a build of
[vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs),
which states no license of its own.
