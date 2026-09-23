# ceph-tools

Command-line tools for Ceph and CephFS cluster administration, debugging,
and troubleshooting: PG movement/remapping, upmap manipulation, cancelling and diverting backfills, draining OSDs, scrub
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
file, because its seven subcommands share code and more subcommands are
coming; copy the whole `backfillctl/` directory (symlinks to it work), not
individual files out of it. `backfillctl.py` at the top level is an optional
executable shim to `backfillctl/`; copy it alongside `backfillctl/` if you
want `./backfillctl.py <subcommand>`, otherwise it's not needed. There is no
install step beyond the requirements below. CephFS tools live in the
`cephfs/` directory, PG/OSD tools in `pg-osd/`, and `backfillctl` at the top
level next to them, since — like those directories — it stands on its own;
everything else is at the top level too.

## Requirements

- A working `ceph` CLI (and `rados`, `ceph-dencoder` for a couple of tools)
  pointed at the target cluster.
- Python 3 for the `.py` scripts and for `backfillctl`, run as
  `python3 backfillctl <subcommand>` or `python3 -m backfillctl <subcommand>`
  from this repo's root (it's a package, not a single executable file, so it
  has no shebang of its own), or as `./backfillctl.py <subcommand>` from
  anywhere, via the executable shim described above. Most `.py` scripts run
  under the `python3` shebang; `cephfs/client-inodes.py`,
  `cephfs/find-recent-rctime.py` and
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
`python3 -m backfillctl <subcommand> --help`) for any of the seven below.
Six analyze a cluster; the seventh, `save-state`, captures one so the other
six can replay it offline with the global `--load-state DIR` option, given
before the subcommand name: `backfillctl --load-state DIR <subcommand> ...`
(`save-state` itself rejects it).

Shell tab completion is available via [`shtab`](https://docs.iterative.ai/shtab/)
(not a `backfillctl` dependency; install it separately to generate
completions): `shtab --shell=bash backfillctl.__main__.build_parser >
completions.bash` from this repo's root, then source `completions.bash`
(similarly for `--shell=zsh`). That registers completion only for the literal
command name `backfillctl`; if you invoke it as `backfillctl.py` (see above),
also add `complete -F _shtab_backfillctl backfillctl backfillctl.py` after
sourcing `completions.bash`, to cover both names with the one generated
script.

- **`backfillctl save-state`** — Capture the live cluster state every other
  subcommand needs into `DIR` (created if missing, must be empty), as one
  anonymized `<key>.json` file per `ceph ... --format json` command — including
  a full `ceph pg dump pgs`, covering every PG, not just the remapped or
  `backfill_toofull` ones the other subcommands ask for live. Each of them
  then reads `DIR` via `backfillctl --load-state DIR`, filtering `pg_dump_pgs.json`
  itself for the PGs it cares about, so one capture serves all six.
  `backfillctl save-state DIR`

- **`backfillctl show-pg-osds`** — Show one or more PGs' `acting` and `up` OSDs,
  a table per PG with one row per shard, with each OSD's utilization and
  host, the PG's primaries marked `*`, remap PROGRESS for shards that are moving (same estimate as
  `backfillctl show-backfill`, per PG), and the PG's `pg_upmap_items` pairs that touch
  each row (UPMAPS). Same grouped ACTING/UP table style as
  `backfillctl divert-toofull`. `--load-state DIR` replays a
  `backfillctl save-state` capture instead of querying the live cluster.
  `backfillctl [--load-state DIR] show-pg-osds <pgid> [<pgid> ...]`

- **`backfillctl show-backfill`** — Show backfills: for every PG where
  `up` != `acting`, print source/destination OSDs, movement type, per-PG progress, and PG
  state. Progress is derived from the misplaced/degraded object counters,
  which count copies, so it is scaled by the number of shards/replicas
  moving. Those counters can hit zero before the PG actually finishes
  (a known gap, seen on large/contended PGs), so a run where any row reads
  100% prints a note explaining that; `show-pg-osds` and
  `cancel-backfill` do the same. Handles EC (per-shard) and
  replicated (set-diff) pools differently; see
  `--help` for the full explanation of the diffing logic and edge cases.
  `--osds` narrows the output to rows involving any of the given OSDs (as
  source, destination or `*`-marked recovering primary), `--pgs` to the
  given PGs; together, a row must match both.
  `--load-state DIR` replays a `backfillctl save-state` capture instead of
  querying the live cluster.
  `backfillctl [--load-state DIR] show-backfill [--sort-by {pgid,from-osd,to-osd}] [--osds OSD ...] [--pgs PGID ...]`

- **`backfillctl divert-toofull`** —
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
  `TARGET` the proposed replacement. `--pgremapper-mappings` prints a JSON
  array for `pgremapper import-mappings` instead (prune it with `jq`, then
  `pgremapper import-mappings file.json`); apply with it rather than by hand
  with `ceph osd pg-upmap-items`, which replaces the whole entry. Dry runs
  showed `import-mappings` reads a PG's existing upmap once and applies all
  of a PG's proposed pairs together as one combined change, keeping the PG's
  existing pairs — the reliable way to apply the proposals, since a PG can
  (rarely) have more than one diverted shard. An OSD can be the
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
  large run may still leave a tail unplaced; how many is reported on stderr
  (a limitation of the heuristic, not proof that no OSD would do); apply,
  drain, re-run. Two safety thresholds default to the
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
  `backfillctl [--load-state DIR] divert-toofull
  [--pgremapper-mappings] [--min-up-util PERCENT]
  [--max-target-util PERCENT] [--max-target-uses N] [--pgs PGID [PGID ...]]`

- **`backfillctl cancel-backfill`** — List the upmaps needed to cancel
  backfills by pinning each moving shard to the OSD that holds it now: by
  default *every* backfill in the cluster (freeze data movement, then let
  through only what you want), or with `--osd` all backfills into one OSD.
  Ceph refuses a backfill when the target's *projected* usage would pass
  `backfillfull_ratio`, and every other backfill headed for that OSD counts
  towards it, so stopping those frees room for the ones you want (e.g.
  draining the fullest OSD). Prints proposals only; changes nothing. Without
  `--osd`, a PG with every moving shard pinned back is simply its `acting` set
  again, and replicated PGs with several replicas moving are paired in sorted
  order (any pairing restores `acting`). With `--osd`, the list is everything
  required, which can include pins that stop backfills into **other** OSDs,
  marked in the `NOTE` column:
  - a *companion*: another shard of the same PG that is moving onto the host
    of a shard you pin back. Ceph checks the failure domain on the `up` set, so
    pinning only one of the two would leave two shards of the PG on one host
    there and Ceph silently drops the upmap; you cannot keep one move and
    cancel the other. (A shard moving onto a host that holds another shard of
    its PG is harmless by itself: a PG's backfills run together and `acting`
    switches to `up` only once all of them have finished.)
  - a *blocker*, only with `--pin-blockers` (off by default; requires `--osd`,
    since without it no moving shard is left unpinned): another shard of
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
  doesn't match a remapped PG (with `--osd` in its `up` set, if given) is
  reported on stderr, since that usually means a typo.
  `--pgremapper-mappings` prints a JSON array for `pgremapper import-mappings`
  (prune it with `jq`, then `pgremapper import-mappings file.json`), which
  takes all pairs in one run and, as dry runs showed, keeps a PG's existing
  pairs; this is the way to apply the output. PGs whose pairs chain (an OSD
  that moves between shard slots, about 2% of PGs on the test cluster) are left
  out, since pgremapper cannot apply them in either order; the tool prints
  `ceph osd pg-upmap-items` commands for them on stderr. Shards that cannot be
  pinned (no acting OSD, ambiguous replicated pairing with `--osd`, a clash
  that no companion can resolve) are
  listed on stderr. Assumes the pools' CRUSH failure domain is `host`.
  pgremapper's own `cancel-backfill` (optionally `--include-osds N --target`)
  does the same at cluster/OSD/pool granularity. `--load-state DIR` replays a `backfillctl save-state`
  capture offline instead of querying the live cluster.
  `backfillctl [--load-state DIR] cancel-backfill [--osd OSD [--pin-blockers]] [--exclude-pgs PGID [PGID ...]] [--pgremapper-mappings]`

- **`backfillctl cancel-uphill`** — List the upmaps needed to cancel
  backfills that move data "uphill": from a less-utilized OSD to a
  more-utilized one, the opposite of what backfill is supposed to
  accomplish (this can happen as a side effect of a manual CRUSH/OSD
  change). Every moving shard of every
  remapped PG is compared by `ceph osd df` utilization; a replicated PG is
  only judged when exactly one replica is arriving and one departing, and a
  shard with no utilization figure on either end (e.g. a down OSD) is left
  alone rather than guessed at — both reported on stderr, distinct from the
  (usually much larger) set of shards that are simply not uphill, which are
  not reported at all. A destination OSD's reported utilization already
  includes whatever this backfill has copied so far, while the source keeps
  its full copy until the PG goes clean, so a move that actually started
  downhill can read as uphill once it is partway done; `--min-delta PERCENT`
  (default `1.0`) only counts a shard as uphill when the destination is at
  least that many percentage points more utilized than the source, to filter
  out deltas small enough to plausibly be that artifact. Uses the same
  pinning, companion, chain and output machinery as `cancel-backfill` (see
  its entry above and its module docstring), so the two behave identically
  once a shard is selected; when a PG has more than one uphill shard, all of
  them are pinned together as one unit, so their companions and chain order
  stay consistent — if that combined pin is not valid, none of the PG's
  uphill shards are proposed, not just the one that clashed. Prints
  proposals only; changes nothing.
  `--exclude-pgs PGID [PGID ...]` and `--pgremapper-mappings` work the same
  way as in `cancel-backfill`. `--load-state DIR` replays a
  `backfillctl save-state` capture offline instead of querying the live
  cluster.
  `backfillctl [--load-state DIR] cancel-uphill [--min-delta PERCENT] [--exclude-pgs PGID [PGID ...]] [--pgremapper-mappings]`

- **`backfillctl drain`** — Propose upmaps that move every PG shard
  mapped to the given OSDs (`--osds`), or to every OSD of the given hosts
  (`--hosts`, short or fully qualified names as in `ceph osd tree`) —
  resident there or still backfilling onto it — to the least-utilized OSDs
  cluster-wide, instead of letting CRUSH pile them
  onto the same host's siblings as marking the OSD `out` would. Targets are
  chosen as in `divert-toofull` (same device class, a host no other shard of
  the PG uses, not in the PG's `up` set or raw CRUSH mapping, projected
  utilization at or below `--max-target-util`, at most `--max-target-uses`
  shards each), except that a drained OSD is never a target, the evacuee's own
  host is allowed, the projection counts every remapped PG's arriving shards,
  and the largest shards are placed first. Because `backfill_toofull` is a
  per-PG state, an evacuee would wait on any other shard of its PG heading for
  an OSD projected above `--max-target-util` — or, for a PG that is
  `backfill_toofull` now, arriving on an OSD at or above `--min-up-util`
  (default `nearfull_ratio`, the same guess `divert-toofull` makes about which
  shard is refused); such a *blocker* is diverted too
  if there is room, otherwise pinned back to its acting OSD (with companions,
  as in `cancel-backfill`), otherwise the evacuee is proposed anyway with a
  `NOTE` saying the PG will stay `backfill_toofull` and why. A diverted or
  pinned blocker's `NOTE` gives the threshold it crossed and the evacuees
  it would have held up. The table has
  `ACTING`/`UP`/`TARGET` groups plus `NOTE`; `--pgremapper-mappings` prints the
  JSON for `pgremapper import-mappings`. Evacuees with no room anywhere are
  counted on stderr (apply, let drain, re-run); a PG that is
  `backfill_toofull` now with no identifiable blocker is flagged in `NOTE`.
  Keep the OSD up and `in` until it is empty: an upmap's `from` must be an
  OSD CRUSH chose, so marking it out early voids these pairs (afterwards,
  `external/upmap-remapped.py` pins what CRUSH remaps to where the data
  already is). Assumes the pools' CRUSH failure domain is `host`. Prints
  proposals only; changes nothing.
  `backfillctl [--load-state DIR] drain (--osds OSD [OSD ...] | --hosts HOST [HOST ...]) [--min-up-util PERCENT] [--pgremapper-mappings] [--max-target-util PERCENT] [--max-target-uses N]`

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
non-matching PGs. `test_backfillctl.py` runs the dispatcher itself, checking
that `--load-state` is global (accepted only before the subcommand name) and
that `save-state` rejects it.

Each `backfillctl` subcommand is split into `plan()`, which returns what the
run decided as a typed result (e.g. `DivertResult`, `StopResult`), and
`render()`, which prints it (only notes on what the run was given, such as
`--pgs` ids that matched nothing, are printed by `plan()`, so that they still
show when planning exits with an error). Tests of the logic assert on the result
(`_support.plan_from_state` runs `plan()` on a `--load-state` directory);
tests of the output format feed results to `render()` and its helpers, plus
a few subprocess runs that check the two are wired together. A change of
output format, like reordering columns, should therefore break only
rendering tests.

The `backfillctl divert-toofull` tests replay the
cluster-state snapshots under `tests/pg-osd/test-data/` via `--load-state` and check the
plan against what each fixture's `README.txt` documents, so fixture and
code cannot drift apart: the exact proposals (the README's machine-checked
"Expected proposals" block) for the small fixtures, and for the
cluster-sized one (808 stuck PGs, 1513 arriving shards) the counts plus the
invariants that matter — no target projected above `--max-target-util`
(`backfillfull_ratio` minus 1 by default), no target used more than
`--max-target-uses` times, no shard diverted off an OSD below `nearfull_ratio`.

The `backfillctl cancel-backfill` tests do the same with two real-cluster
snapshots in `tests/pg-osd/test-data/cancel-backfill-*/` (688 remapped PGs;
one where stopping the backfills into an OSD needs companion pins for 5 of 6
arriving PGs, and `--pin-blockers` adds blocker pins for those same 5 plus the
6th, whose own backfill would otherwise be held up by an unrelated shard in
its PG; and one where `--pin-blockers` is the only thing standing between a
single wanted backfill and the blocker in its own PG that is holding it up).
Without `--osd`, they check that the first snapshot's 688 PGs are all
covered with nothing unpinnable, and that applying each PG's pins in order
turns its `up` back into its `acting`.
Besides the exact pins documented in each `README.txt`, they
check independently that applying the proposed pins leaves
every PG with no repeated host or OSD, and that a `--load-state` directory
replays to the same result with no `ceph` available.

`cancel-backfill` and `cancel-uphill` share the pinning, companion, chain
and output code (`shared.close_pins` and friends) that turns a chosen shard
into a valid, orderable upmap proposal; it is tested once, through
`cancel-backfill`'s suite (that code used to live in `cancel_backfill.py`
itself, private to it; it moved to `shared.py`, importable under its own
name, when `cancel-uphill` needed it too, and `cancel_backfill.py` now
imports it back rather than defining it). `test_cancel_uphill.py` only
tests what is actually new: `find_uphill_shards` (EC and replicated,
`--min-delta` thresholding, unknown utilization, ambiguous/missing-replica
pairing, several uphill shards in one PG) and that `plan_cancellations`
wires selection into a pin correctly, including a companion needed by two
independently-uphill shards and the all-or-nothing skip when a PG's
combined pin is not valid. A replay of the real-cluster fixture also used
by `cancel-backfill`'s tests checks two invariants against live data
(every directly-selected shard's delta actually exceeds `--min-delta`,
and applying the proposed pins never repeats a host or OSD in any PG's
`up`), and a subprocess run through the real `backfillctl` entry point
checks `cancel-uphill` is actually registered in `__main__.py`, not just
reachable through the test's own throwaway parser.

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
