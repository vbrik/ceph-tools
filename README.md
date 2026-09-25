# ceph-tools

Command-line tools for Ceph and CephFS administration and troubleshooting:
PG backfill and remapping, upmaps, cancelling and diverting backfills,
draining OSDs, relieving the fullest OSDs (utilization balancing), scrub
scheduling, OSD/PG lookups, MDS ops inspection, CephFS client load,
inode-to-path resolution, and finding large, wide or fast-growing
directories on CephFS. Most tools turn `ceph` and `rados` JSON
output into something more useful; the CephFS tree tools read recursive
statistics (`ceph.dir.*` xattrs) off a mount.

Every script is standalone; there is no install step. The exception is
`backfillctl`, a package: copy the whole `backfillctl/` directory, plus
`backfillctl.py` if you want to run it as `./backfillctl.py`.

## Requirements

- A `ceph` CLI configured for the cluster (`rados` and `ceph-dencoder` for a
  couple of tools).
- Python 3, standard library only, except (`pip install -r requirements.txt`):
  - `cephfs/find-recent-rctime.py` needs `python-dateutil`.
  - `cephfs/mds-ops-pretty.py` can resolve UIDs/GIDs through LDAP (with
    `--ldap-server` and `--ldap-base`), using `ldap3` or the `ldapsearch` CLI.
  - `backfillctl` and `external/upmap-remapped.py` use the `rados` Python
    bindings if available, and fall back to the `ceph` CLI otherwise
    (slower; `upmap-remapped.py` then also needs `jq`). These bindings come
    from your distro's Ceph packages, not PyPI, so they're not in
    `requirements.txt`.
- `jq` for the `.sh` scripts.
- A mounted CephFS for the CephFS tree tools, and `getfattr` for `cephfs/du`.

`cephfs/client-inodes.py`, `cephfs/find-recent-rctime.py` and
`scrub-all-pgs-that-need-it.py` use a `python` shebang rather than `python3`.
Some scripts default to site-specific pool names (`cephfs.default.meta`,
`cephfs.default.data`); check `--help`.

## Tools

### RADOS / OSD

#### backfillctl

Commands for inspecting and steering PG backfills. Run it as
`./backfillctl.py <command>`, `python3 backfillctl <command>` or
`python3 -m backfillctl <command>`, and see `<command> --help` for details.

Commands that remap PGs only print upmap proposals; they change nothing.
Apply the proposals with `--pgremapper-mappings` and
[pgremapper](https://github.com/digitalocean/pgremapper)'s `import-mappings`,
which adds to a PG's existing upmap pairs. `ceph osd pg-upmap-items` replaces
them all. The remapping commands assume the CRUSH failure domain is `host`.
Entries from the cancel commands and `drain` also carry the table's `shard`,
`role` (requested, companion, blocker) and `note`, for pruning with `jq`;
pgremapper ignores them.

pgremapper cannot apply chained pairs (A->B, B->C), which cancelling some EC
backfills needs. The cancel commands leave such backfills running, and print
`ceph osd pg-upmap-items` commands that would cancel them too.

| Command | Purpose |
|---|---|
| `show-backfill` | What is moving: acting and up OSDs, type, progress and state, per moving EC shard or replica. Filter with `--osds`, `--pgs` and `--hosts`. |
| `measure-rate [--interval SECONDS]` | How fast backfill destinations (UP OSDs) receive data: each copy's RATE (objects/s, MiB/s) and ETA, from two samples at least `--interval` (30) seconds apart; then the rates per destination OSD and host, and the total. Tables sort by destination host (`ceph1-2` before `ceph1-10`), OSD, PG and shard; `--sort-by` `obj/s` or `mib/s` puts the fastest first in all three, `progress` or `eta` the furthest along or soonest done copies. `--osds`/`--hosts` match the destination; `--pgs` as in `show-backfill`. `--save-state DIR` saves both samples for `--load-state DIR`. |
| `show-pg-osds PGID...` | Acting and up OSDs of given PGs, per shard, with utilization, host, progress and upmap pairs. |
| `divert-toofull` | Re-target shards stuck in `backfill_toofull` to the least-utilized legal OSDs, e.g. after an OSD failure piles its data onto its host's other OSDs. |
| `drain --osds OSD... \| --hosts HOST...` | Move every shard off OSDs or hosts, spread across the cluster rather than onto the same host. Also diverts or pins back shards that would hold the moved ones in `backfill_toofull`. Keep the OSDs up and in until empty: marking them out voids the upmaps. |
| `balance [--class CLASS] [--osds OSD... \| --min-source-util PCT]` | Lower a device class's highest OSD utilization by moving shards off the fullest OSDs onto the emptiest, without filling any target past its source. Stops once the maximum can't go lower (not a full balancer); `--max-moves` limits the batch. Turn off the upmap balancer while the backfills run. |
| `cancel-backfill [--osds OSD... [--pin-blockers]]` | Cancel backfills by pinning shards to where their data is: all of them, or those into given full OSDs to make room for others. |
| `cancel-uphill` | Cancel backfills that move data to a more-utilized OSD. |
| `save-state DIR` | Capture the cluster state the other commands read, anonymized, for replay with `--load-state DIR`, before or after the command. `measure-rate` saves its own. |

PROGRESS is computed from each backfill target's position (`last_backfill` in
`ceph pg query`), one query per PG shown. Ceph's misplaced/degraded counters
are only a fallback, marked `~`: after re-peering they can read ~100% for a
backfill a third done.

Shell completion via [shtab](https://docs.iterative.ai/shtab/), from the repo
root:

```
shtab --shell=bash backfillctl.__main__.build_parser > completions.bash
source completions.bash
complete -F _shtab_backfillctl backfillctl backfillctl.py  # also for backfillctl.py
```

#### Other

- **`scrub-all-pgs-that-need-it.py`**: Scrub and deep-scrub every PG that
  `ceph health detail` lists under `PG_NOT_SCRUBBED` / `PG_NOT_DEEP_SCRUBBED`,
  via `ceph tell osd.<primary>`. This works around a broken
  `ceph pg (deep-)scrub`. It acts immediately; there is no dry run.

- **`find-large-omap-objects.sh`**: List PGs with objects flagged for large
  omap.

- **`external/upmap-remapped.py`** (third-party, from
  [cernceph/ceph-scripts](https://github.com/cernceph/ceph-scripts/blob/master/tools/upmap/upmap-remapped.py)):
  Print upmap commands that make all remapped PGs `active+clean` by pinning
  data where it is, so the `upmap` balancer can unwind the movement
  gradually. Use it around disruptive changes (new hosts, CRUSH changes)
  with `norebalance` set. `--ignore-backfilling` skips PGs already
  backfilling. This copy predates upstream fixes to its no-`rados` fallback.
  `external/upmap-remapped.py [--ignore-backfilling]`

- **`external/pgremapper-v1.0.0-linux-amd64`** (prebuilt,
  [digitalocean/pgremapper](https://github.com/digitalocean/pgremapper)):
  Control PG backfill and remapping with upmaps, without CRUSH changes.

### CephFS clients and MDS

- **`cephfs/client-id-to-host`**: Resolve a CephFS client session ID to
  hostname and IP.
  `cephfs/client-id-to-host <client-id>`

- **`cephfs/client-inodes.py`**: Show the paths of the inodes (delegated,
  completed-request, preallocated) held by a client session. Reads
  `client ls` JSON from a file or stdin, or queries MDS ranks live (all
  active ranks, or `--rank`).
  `cephfs/client-inodes.py [--meta-pool POOL] [--data-pool POOL] [--rank RANK] <client> [file|-]`

- **`cephfs/top.py`**: `top`-style live view of CephFS client load across MDS
  ranks (request rate, caps, leases, in-flight requests), sortable and
  filterable by column.
  `cephfs/top.py [-r RANK] [-n N] [-s COLUMNS] [--hide COLUMNS] [--cache-ttl SECONDS] [--cache-file PATH] [--full-mount-point]`

- **`cephfs/mds-ops-pretty.py`**: Readable rendering of
  `ceph tell mds.X dump_{blocked,historic,ops_in_flight}`, queried live from
  every active rank (or `--mds-rank`), or read with `--json-file`. Resolves
  inodes to paths and client IDs to hosts and users, caching lookups on disk
  (see `--help`).
  `cephfs/mds-ops-pretty.py dump_ops_in_flight [options]`

- **`cephfs/dir-tree-pins.sh`**: List directories pinned to each MDS rank.

- **`cephfs/inode-to-path`**: Resolve a hex inode number to its path via the
  backtrace xattr.
  `cephfs/inode-to-path <inode-hex>`

### CephFS trees (on a mounted filesystem)

These read CephFS recursive statistics (`ceph.dir.rbytes`, `ceph.dir.rfiles`,
`ceph.dir.rctime`, …) off a mount. They need read access to the directories,
not cluster credentials.

- **`cephfs/du`**: Size of paths (`ceph.dir.rbytes` for directories), in
  human-readable units.
  `cephfs/du <path> [path...]`

- **`cephfs/find-growing-dirs.py`**: Find the fastest-growing subtree without
  walking the tree: sample the children's `ceph.dir.rbytes` twice, descend
  into the top grower, repeat.
  `cephfs/find-growing-dirs.py [--interval SECONDS] [--depth N] [--top N] [--workers N] <root>`

- **`cephfs/find-recent-rctime.py`**: Find files and directories with `ctime`
  at or after a date, pruning subtrees by `ceph.dir.rctime`. Much faster
  than `find -newer`. `--parents` prints only the matches' parent
  directories.
  `cephfs/find-recent-rctime.py --min-ctime DATE [--relative] [--parents] [--threads NUM] <path>`

- **`cephfs/cephfs-find-wide-dirs`**: Find directories holding many files,
  using `ceph.dir.files` / `ceph.dir.rfiles`. Prebuilt x86-64 Linux binary;
  source at
  [vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs).
  `cephfs/cephfs-find-wide-dirs --min-num-files NUMBER [--threads NUMBER] <path>`

## Tests

Standard library `unittest`; `pytest tests` also works. Neither test group
has an `__init__.py`, so `unittest discover` won't find them from `tests/`
itself; run each group directly:

```
python3 -m unittest discover -s tests/backfillctl
python3 -m unittest discover -s tests/cephfs
```

No test needs a cluster. Each `backfillctl` command is split into `plan()`,
which returns a typed result, and `render()`, which prints it. Logic tests
assert on the result, and output tests on the rendering. Text that more
than one command prints is in `backfillctl/messages.py`. Many tests replay
cluster captures in `tests/backfillctl/test-data/` (mostly real, anonymized)
through `--load-state`. Each capture's `README.txt` documents its scenario
and the expected result, which the tests check.

### Coverage

With [coverage.py](https://coverage.readthedocs.io/) 7.10 or later, run
each group under `coverage`, then merge the results:

```
coverage run -m unittest discover -s tests/backfillctl
coverage run -m unittest discover -s tests/cephfs
coverage combine && coverage report    # or: coverage html
```

`.coveragerc` measures branches and the commands tests run as subprocesses.
Scripts without tests count at 0% in the total. `external/` is not measured.

## License

MIT (see `LICENSE`), except the unmodified third-party tools in `external/`:

- `external/pgremapper-v1.0.0-linux-amd64`: Apache 2.0.
- `external/upmap-remapped.py`: from
  [cernceph/ceph-scripts](https://github.com/cernceph/ceph-scripts)
  (GPL-2.0); the file credits Dan van der Ster (CERN) and carries a
  no-warranty disclaimer.

`cephfs/cephfs-find-wide-dirs` is a build of
[vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs),
which states no license.
