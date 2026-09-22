# ceph-tools

Command-line tools for Ceph and CephFS cluster administration, debugging,
and troubleshooting: PG movement/remapping, upmap manipulation, stopping and diverting backfills, scrub
scheduling, OSD/PG lookups, MDS ops inspection, CephFS client load and
inode-to-path resolution, and finding large, wide, or fast-growing
directories on a mounted CephFS. Most tools wrap `ceph` CLI / `rados`
output (mostly JSON) into something more directly useful — grouping,
resolving IDs to names, diffing, sorting; the rest read CephFS recursive
statistics (`ceph.dir.*` extended attributes) directly off a mount. They
answer questions that come up repeatedly during cluster operation but
aren't answered directly by a single `ceph` subcommand.

Every script is standalone and can be copied out and run on its own, with
one exception: `backfillctl` (see below) is a small package, not a single
file, because its four subcommands share code and more subcommands are
coming; copy the whole `backfillctl/` directory (symlinks to it work), not
individual files out of it. There is no install step beyond the requirements
below. CephFS tools live in the `cephfs/` directory, PG/OSD tools in
`pg-osd/`, and `backfillctl` at the top level next to them, since — like
those directories — it stands on its own; everything else is at the top
level too.

## Requirements

- A working `ceph` CLI (and `rados`, `ceph-dencoder` for a couple of tools)
  pointed at the target cluster.
- Python 3 for the `.py` scripts and for `backfillctl` (run as
  `python3 backfillctl <subcommand>` or `python3 -m backfillctl <subcommand>`
  from this repo's root; it's a package, not a single executable file, so it
  has no shebang of its own). Most `.py` scripts run under the `python3`
  shebang; `cephfs/client-inodes.py`, `cephfs/find-recent-rctime.py` and
  `pg-osd/scrub-all-pgs-that-need-it.py` use `python`. Stdlib only, except:
  - `cephfs/find-recent-rctime.py` requires `python-dateutil` for its
    flexible `--min-ctime` date parsing.
  - `cephfs/mds-ops-pretty.py` can optionally resolve UID/GID to names via
    LDAP, using the `ldap3` package if installed, falling back to the
    `ldapsearch` CLI otherwise. This is off by default and only activates
    when both `--ldap-server` and `--ldap-base` are given (see `--help`).
  - `external/upmap-remapped.py` uses the `rados` Python bindings if importable and
    otherwise falls back to shelling out to `ceph ... | jq`.
- `jq` for the `.sh` scripts and for `external/upmap-remapped.py`'s fallback path.
- A mounted CephFS (kernel client or ceph-fuse) for `cephfs/du`,
  `cephfs/find-growing-dirs.py`, `cephfs/find-recent-rctime.py` and
  `cephfs/cephfs-find-wide-dirs`; plus `getfattr` (from `attr`/`acl` packages) for `cephfs/du`.

Some scripts hard-code environment-specific defaults (e.g. pool names
`cephfs.default.meta`/`cephfs.default.data`) that were written for a
specific cluster. Check `--help` and adjust flags/defaults as needed for
other environments.

## Tools

### RADOS / OSD

`backfillctl` is a small package (see Requirements above for how to run it),
not a set of standalone scripts, because its subcommands share code and more
are coming that will be variations on the existing ones. Run
`python3 backfillctl <subcommand> --help` (or
`python3 -m backfillctl <subcommand> --help`) for any of the five below.
Four analyze a cluster; the fifth, `save-state`, captures one so the other
four can replay it offline.

- **`backfillctl save-state`** — Capture the live cluster state every other
  subcommand needs into `DIR` (created if missing, must be empty), as one
  anonymized `<key>.json` file per `ceph ... --format json` command — including
  a full `ceph pg dump pgs`, covering every PG, not just the remapped or
  `backfill_toofull` ones the other subcommands ask for live. Each of them
  then reads `DIR` with `--load-state DIR`, filtering `pg_dump_pgs.json`
  itself for the PGs it cares about, so one capture serves all four.
  `backfillctl save-state DIR`

- **`backfillctl osds-of-pg`** — Show a PG's `acting` and `up` OSDs, one row per
  shard, with each OSD's utilization and host, the PG's primaries
  marked `*`, remap PROGRESS for shards that are moving (same estimate as
  `backfillctl pg-movements`, per PG), and the PG's `pg_upmap_items` pairs that touch
  each row (UPMAPS). Same grouped ACTING/UP table style as
  `backfillctl divert-toofull-backfills`. `--load-state DIR` replays a
  `backfillctl save-state` capture instead of querying the live cluster.
  `backfillctl osds-of-pg [--load-state DIR] <pgid>`

- **`backfillctl pg-movements`** — For every PG where `up` != `acting`,
  print source/destination OSDs, movement type, per-PG progress, and PG
  state. Progress is derived from the misplaced/degraded object counters,
  which count copies, so it is scaled by the number of shards/replicas
  moving. Those counters can hit zero before the PG actually finishes
  (a known gap, seen on large/contended PGs), so a run where any row reads
  100% prints a note explaining that; `osds-of-pg` and
  `stop-backfills-into-osd` do the same. Handles EC (per-shard) and
  replicated (set-diff) pools differently; see
  `--help` for the full explanation of the diffing logic and edge cases.
  `--load-state DIR` replays a `backfillctl save-state` capture instead of
  querying the live cluster.
  `backfillctl pg-movements [--sort-by {pgid,from-osd,to-osd}] [--load-state DIR]`

- **`pg-osd/upmaps-of-osd.sh`** — Show `pg_upmap_items` entries where a
  given OSD is a source or destination.
  `pg-osd/upmaps-of-osd.sh <osd>`

- **`backfillctl divert-toofull-backfills`** —
  Propose upmap re-targets that unwedge PGs stuck in `backfill_toofull` on
  full hosts. When an OSD goes out, a `chooseleaf ... type host` CRUSH rule
  retries *inside the same host bucket*, so the dead OSD's PGs pile onto its
  same-host siblings instead of spreading across the cluster; on an
  already-full cluster those siblings cross `backfillfull_ratio` and the
  backfills stall. It needs no arguments to say where to look: it scans every
  `backfill_toofull` PG cluster-wide, finds each shard newly landing on an OSD
  full enough to be what is blocking it (in `up` but not `acting`), and picks
  the least-utilized OSD of that shard's own device class on a host not
  already in the PG's `up` set and strictly emptier than the OSD the shard was
  arriving on — a destination that satisfies the fault domain and has room for
  the shard. Prints the proposed remaps; changes nothing itself. Each row
  follows one shard's path, giving the OSD, utilization and host at each step,
  under a two-line header whose first line spans each group: `ACTING` where
  its data sits now, `UP` the too-full OSD the stalled backfill is aimed at,
  `TARGET` the proposed replacement. `--import-mappings` prints a JSON array
  for `pgremapper import-mappings` instead (prune it with `jq`, then
  `pgremapper import-mappings file.json`), which applies all of a PG's pairs
  in one call — the reliable way to apply the proposals, since a PG can (rarely)
  have more than one diverted shard. `--pgremapper` instead prints headerless
  `<pgid> <from osd> <target osd>` lines for `pgremapper remap` (via
  `xargs -a remaps.txt -L1 …`); apply with it rather than by hand with
  `ceph osd pg-upmap-items`, which replaces the whole entry. A single `remap`
  call merges into a PG's existing `pg_upmap_items`, but separate `remap` runs
  against the same PG can overwrite each other's pairs (seen on a live
  cluster with the same tool in `stop-backfills-into-osd`), so
  `--pgremapper` warns on stderr whenever a PG needs more than one line. An
  OSD can be the
  target of several shards: each shard's size is estimated from its PG
  (`num_bytes`, divided by `k` for EC pools) and projected onto the target —
  together with the shards already sent to it and every shard still arriving
  there, stuck ones included until they are diverted — and an OSD stops being
  used once that projection would exceed `--max-target-util`, or after
  `--max-target-uses N` shards (default 5; 1 gives every OSD at most one). The
  `TARGET PROJ` column shows that projection. Shards are placed
  fullest-`ACTING`-OSD first, re-ranked as each placement relieves its source,
  so the scarce room goes to the OSDs most urgent to relieve (shards with no
  known acting OSD go last, and rows are printed in PG order regardless). A
  large run may still leave a tail unplaced; how many is reported on stderr,
  in `--pgremapper` mode too (a limitation of the heuristic, not proof that no
  OSD would do); apply, drain, re-run. Two safety thresholds default to the
  cluster's own ratios and can be overridden. `--min-up-util PERCENT` (default
  `nearfull_ratio`) only diverts a shard whose arriving OSD is that full:
  `backfill_toofull` is a property of the PG, not of each shard arriving on
  it, so without this a PG with one wedged shard has all its healthy arrivals
  diverted too, spending target OSDs that genuinely stuck shards then cannot
  get. `--max-target-util PERCENT` (default `backfillfull_ratio` minus 1) caps
  a target's *projected* utilization: an OSD stops being used once the next
  shard would take it above that, so a proposal never re-wedges. It cannot
  exceed `backfillfull_ratio`, and the script exits with an error if it is
  set higher (`100` no longer disables it). `--pgs PGID [PGID ...]` restricts
  the run to just the given PG(s), as if every other `backfill_toofull` PG
  were not stuck; a given id that isn't currently `backfill_toofull` is
  reported on stderr, since that usually means a typo.
  `--load-state DIR` replays a `backfillctl save-state` capture offline
  instead of querying the live cluster. Handles EC pools per-shard and
  replicated pools by set difference. See the subcommand's module docstring
  for the full explanation and caveats (`--help` summarizes and points there).
  `backfillctl divert-toofull-backfills [--import-mappings | --pgremapper]
  [--min-up-util PERCENT] [--max-target-util PERCENT] [--max-target-uses N]
  [--pgs PGID [PGID ...]] [--load-state DIR]`

- **`backfillctl stop-backfills-into-osd`** — List the upmaps needed to stop *all*
  backfills into a given OSD, by pinning each arriving shard to the OSD that
  holds it now. Ceph refuses a backfill when the target's *projected* usage
  would pass `backfillfull_ratio`, and every other backfill headed for that OSD
  counts towards it, so stopping those frees room for the ones you want (e.g.
  draining the fullest OSD). Prints proposals only; changes nothing. The list
  is everything required, which can include pins that stop backfills into
  **other** OSDs, marked in the `NOTE` column:
  - a *companion*: another shard of the same PG that is moving onto the host
    of a shard you pin back. Ceph checks the failure domain on the `up` set, so
    pinning only one of the two would leave two shards of the PG on one host
    there and Ceph silently drops the upmap; you cannot keep one move and
    cancel the other. (A shard moving onto a host that holds another shard of
    its PG is harmless by itself: a PG's backfills run together and `acting`
    switches to `up` only once all of them have finished.)
  - a *blocker*, only with `--pin-blockers` (off by default): another shard of
    the PG whose target OSD would reach `backfillfull_ratio`.
    `backfill_toofull` is a per-PG state, so it holds the whole PG back,
    including the shard you want to keep (e.g. `99 -> 337` blocking
    `231 -> 896`). Without `--pin-blockers` the tool does exactly what its name
    says and nothing more, so a backfill you decide to keep from its output
    can still be stuck in `backfill_toofull` for a reason it never mentions; a
    trailing NOTE says so and points at the option.

  It does not pick which backfills to keep: the table shows each shard's acting and
  up OSD (bare ids, with utilization and host, under a two-line header whose
  first line spans each of the `ACTING` and `UP` groups), size, PG progress and
  abbreviated state (cancelling a running backfill discards its progress). You drop the entries
  for the ones to let proceed; with `--pin-blockers`, keep the blockers of any
  shard you keep, and always drop companions with the entry they belong to.
  `--exclude-pgs PGID [PGID ...]` does this up front instead: those PGs (and
  their companions/blockers) never appear in the output. A given id that
  doesn't match a remapped PG with `--osd` in its `up` set is reported on
  stderr, since that usually means a typo.
  `--import-mappings` prints a JSON array for `pgremapper import-mappings`
  (prune it with `jq`, then `pgremapper import-mappings file.json`), which
  takes all pairs in one run and, as dry runs showed, keeps a PG's existing
  pairs; this is the way to apply the output. PGs whose pairs chain (an OSD
  that moves between shard slots, about 2% of PGs on the test cluster) are left
  out, since pgremapper cannot apply them in either order; the tool prints
  `ceph osd pg-upmap-items` commands for them on stderr.
  `--pgremapper` prints bare `<pgid> <up osd> <acting osd>` lines for
  `pgremapper remap` instead, but separate `remap` runs on one PG can overwrite
  each other's pairs (seen on a live cluster), so it warns on stderr whenever a
  PG needs more than one line. Shards that cannot be pinned (no acting OSD,
  ambiguous replicated pairing, a clash that no companion can resolve) are
  listed on stderr. Assumes the pools' CRUSH failure domain is `host`.
  pgremapper's `cancel-backfill --include-osds N --target` does the same at
  OSD/pool granularity. `--load-state DIR` replays a `backfillctl save-state`
  capture offline instead of querying the live cluster.
  `backfillctl stop-backfills-into-osd --osd OSD [--exclude-pgs PGID [PGID ...]] [--pin-blockers] [--import-mappings | --pgremapper] [--load-state DIR]`

- **`pg-osd/scrub-all-pgs-that-need-it.py`** — Scrub and deep-scrub every PG that
  `ceph health detail` reports under `PG_NOT_SCRUBBED` /
  `PG_NOT_DEEP_SCRUBBED`. For each such PG it looks up the acting primary
  via `ceph pg <pgid> query` and issues `ceph tell osd.<primary> scrub` /
  `deep_scrub`, which is a work-around for a broken `ceph pg (deep-)scrub`.
  Acts on the cluster immediately — it has no dry-run mode.
  `pg-osd/scrub-all-pgs-that-need-it.py`

- **`find-large-omap-objects.sh`** — List PGs with objects flagged
  for having large omap entries.

- **`external/upmap-remapped.py`** (third-party, from
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
  `external/upmap-remapped.py [--ignore-backfilling]`

- **`external/pgremapper-v1.0.0-linux-amd64`** (prebuilt binary,
  [digitalocean/pgremapper](https://github.com/digitalocean/pgremapper)) —
  Third-party tool for controlling PG backfill/remapping without CRUSH map
  changes. Vendored as a static Linux amd64 binary; see its own project for
  source and other platforms.

### CephFS clients and MDS

- **`cephfs/client-id-to-host`** — Resolve a CephFS client session ID to
  hostname and IP.
  `cephfs/client-id-to-host <client-id>`

- **`cephfs/client-inodes.py`** — Show filesystem paths for the inodes
  (delegated/completed-request/preallocated) held by a client session.
  Reads client sessions from a `client ls` JSON file/stdin, or, if the file
  argument is omitted, queries MDS rank(s) live via
  `ceph tell mds.RANK client ls` (all active ranks by default, or one rank
  via `--rank`); live queries print a warning since `client ls` can be
  resource-intensive on a busy MDS.
  `cephfs/client-inodes.py [--meta-pool POOL] [--data-pool POOL] [--rank RANK] <client> [file|-]`

- **`cephfs/top.py`** — `top`-style live view of CephFS client
  load across MDS ranks (request rate, caps, leases, in-flight requests,
  etc.), sortable and filterable by column, with optional result caching.
  `cephfs/top.py [-r RANK] [-n N] [-s COLUMNS] [--hide COLUMNS] [--cache-ttl SECONDS] [--cache-file PATH] [--full-mount-point]`

- **`cephfs/mds-ops-pretty.py`** — Human-friendly rendering of
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
  `cephfs/mds-ops-pretty.py dump_ops_in_flight [options]`

- **`cephfs/dir-tree-pins.sh`** — List directories pinned (exported) to
  each MDS rank.

- **`cephfs/inode-to-path`** — Resolve a hex inode number to its filesystem
  path via the metadata/data pool backtrace xattr.
  `cephfs/inode-to-path <inode-hex>`

### CephFS trees (on a mounted filesystem)

These read CephFS recursive statistics (`ceph.dir.rbytes`,
`ceph.dir.rfiles`, `ceph.dir.rctime`, …) off a mount, and need no cluster
credentials or MDS admin access — only read access to the directories
being examined.

- **`cephfs/du`** — Report size (`ceph.dir.rbytes` for directories, file
  size otherwise) of paths on a mounted CephFS, in human-readable units.
  `cephfs/du <path> [path...]`

- **`cephfs/find-growing-dirs.py`** — Locate the fastest-growing subtree
  without walking the tree. Samples `ceph.dir.rbytes` on a directory's immediate
  children twice, ranks children by delta, then descends into the top
  grower and repeats. Cost is O(children per level), not O(files).
  `cephfs/find-growing-dirs.py [--interval SECONDS] [--depth N] [--top N] [--workers N] <root>`

- **`cephfs/find-recent-rctime.py`** — Find files and directories whose `ctime` is
  at or after a given date, using `ceph.dir.rctime` to prune subtrees that
  cannot contain a match — much faster than `find -newer` on a large tree.
  Accepts a variety of date formats, or Unix time as `@SECONDS`. `--parents`
  prints only the parent directories of matches. Being IO-bound, it defaults
  to far more workers than there are CPUs.
  `cephfs/find-recent-rctime.py --min-ctime DATE [--relative] [--parents] [--threads NUM] <path>`

- **`cephfs/cephfs-find-wide-dirs`** — Quickly find directories holding many
  files, using the `ceph.dir.files` / `ceph.dir.rfiles` xattrs.
  `--min-num-files 0` skips the xattr reads and matches every directory.
  Vendored here as a prebuilt x86-64 Linux binary; the Rust source lives in
  [vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs).
  `cephfs/cephfs-find-wide-dirs --min-num-files NUMBER [--threads NUMBER] <path>`

## Tests

Stdlib `unittest`, no dependencies. All tests live in `tests/`, in one
subdirectory per script group (`tests/pg-osd/`, `tests/cephfs/`); `pytest tests`
works too. `unittest discover` does not descend into the hyphenated
directories, so run it once per group:

```
python3 -m unittest discover -s tests/pg-osd
python3 -m unittest discover -s tests/cephfs
```

The tests import `backfillctl`'s modules through `tests/pg-osd/_support.py`,
which puts this repo's root on `sys.path` so `from backfillctl import ...`
resolves. `tests/pg-osd/test_shared.py` covers `backfillctl/shared.py`: the progress
arithmetic (EC and replicated copy counting), PG/pool helpers, the shared table
printer and cell formatters, and the `--load-state` snapshot layer (its
counterpart, `backfillctl save-state`, is covered in `test_save_state.py`,
including the general anonymizer: idempotent, keeps two hosts distinct,
scrubs fsid, addresses, uuids and names). `test_save_state.py` also replays
one real, already-committed fixture through two different subcommands'
client-side PG filters to check they actually discriminate (not just pass
everything through) against a directory that mixes matching and
non-matching PGs.

The `backfillctl divert-toofull-backfills` tests replay the
cluster-state snapshots under `tests/pg-osd/test-data/` via `--load-state` and check the
output against what each fixture's `README.txt` documents, so fixture and
code cannot drift apart: the exact table for the small fixtures, and for the
cluster-sized one (808 stuck PGs, 1513 arriving shards) the counts plus the
invariants that matter — no target projected above `--max-target-util`
(`backfillfull_ratio` minus 1 by default), no target used more than
`--max-target-uses` times, no shard diverted off an OSD below `nearfull_ratio`.

The `backfillctl stop-backfills-into-osd` tests do the same with two real-cluster
snapshots in `tests/pg-osd/test-data/stop-backfills-into-osd-*/` (688 remapped PGs;
one where stopping the backfills into an OSD needs companion pins for 5 of 6
arriving PGs, and `--pin-blockers` adds blocker pins for those same 5 plus the
6th, whose own backfill would otherwise be held up by an unrelated shard in
its PG; and one where `--pin-blockers` is the only thing standing between a
single wanted backfill and the blocker in its own PG that is holding it up).
Besides the exact `--pgremapper` output documented in each `README.txt`, they
check independently that applying the proposed pins leaves
every PG with no repeated host or OSD, and that a `--load-state` directory
replays to the same result with no `ceph` available.

## License

MIT (see `LICENSE`), except for the vendored third-party tools in `external/`
(unmodified copies from their upstream repos):

- `external/pgremapper-v1.0.0-linux-amd64` carries its own Apache 2.0 license.
- `external/upmap-remapped.py` comes from
  [cernceph/ceph-scripts](https://github.com/cernceph/ceph-scripts), which
  is GPL-2.0 licensed; the file itself credits Dan van der Ster (CERN) and
  carries a no-warranty disclaimer.

`cephfs/cephfs-find-wide-dirs` is a build of
[vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs),
which states no license of its own.
