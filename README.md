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

- A `ceph` CLI configured for the cluster.
- Python 3; `pip install -r requirements.txt` for the few extras some
  scripts use. The `rados` Python bindings (from your distro's Ceph
  packages) make `backfillctl` faster.
- `jq` for the shell scripts.
- A mounted CephFS for the CephFS tree tools.

Some scripts default to site-specific pool names; check `--help`.

## Tools

### PGs, backfill and OSDs

#### backfillctl

Commands for inspecting and steering PG backfills. Run it as
`./backfillctl.py <command>`, `python3 backfillctl <command>` or
`python3 -m backfillctl <command>`, and see `<command> --help` for details.

**Inspect**

- `show-backfill`: what is moving, with the progress and state of each
  moving shard or replica, and the projected utilization of its target OSD.
- `measure-rate`: how fast backfill destinations receive data, with ETAs.
- `show-pg-osds`: where given PGs live, with hosts, utilization and upmaps.

**Remap**

- `drain`: empty OSDs or hosts (or only down to a utilization).
- `balance`: move data off a device class's fullest OSDs onto its emptiest.
- `divert-toofull`: re-target shards stuck in `backfill_toofull`.

**Cancel**

- `cancel-backfill`: pin shards back where their data is, for all backfills
  or only those into given full OSDs.
- `cancel-uphill`: cancel backfills that move data to a more-utilized OSD.

**Replay**

- `save-state`: capture the cluster state, anonymized, for `--load-state`.

The Remap and Cancel commands change nothing; they print upmap proposals.
Apply them with [pgremapper](https://github.com/digitalocean/pgremapper)'s
`import-mappings` (see `--pgremapper-mappings`), which, unlike
`ceph osd pg-upmap-items`, keeps a PG's existing upmap pairs. Turn off the
upmap balancer (`ceph balancer off`) for as long as the upmaps should hold,
and keep drained OSDs up and in: marking them out voids the upmaps. The
remapping commands assume the CRUSH failure domain is `host`. `balance`
works in rounds: apply, let the backfills finish, and re-run.

Shell completion via [shtab](https://docs.iterative.ai/shtab/), from the repo
root:

```
shtab --shell=bash backfillctl.__main__.build_parser > completions.bash
source completions.bash
complete -F _shtab_backfillctl backfillctl backfillctl.py  # also for backfillctl.py
```

#### Other

- **`scrub-all-pgs-that-need-it.py`**: Scrub and deep-scrub every PG that
  `ceph health detail` reports as overdue, working around a broken
  `ceph pg (deep-)scrub`. It acts immediately; there is no dry run.
- **`find-large-omap-objects.sh`**: List PGs with objects flagged for large
  omap.
- **`external/upmap-remapped.py`** (third-party,
  [cernceph/ceph-scripts](https://github.com/cernceph/ceph-scripts/blob/master/tools/upmap/upmap-remapped.py)):
  Pin all remapped PGs where their data is, so the `upmap` balancer can
  unwind the movement gradually. Handy around new hosts or CRUSH changes.
  This copy predates upstream fixes to its no-`rados` fallback.
- **`external/pgremapper-v1.0.0-linux-amd64`** (prebuilt,
  [digitalocean/pgremapper](https://github.com/digitalocean/pgremapper)):
  Control PG backfill and remapping with upmaps, without CRUSH changes.

### CephFS clients and MDS

- **`cephfs/top.py`**: `top`-style live view of CephFS client load across MDS
  ranks: request rate, caps, leases, in-flight requests.
- **`cephfs/mds-ops-pretty.py`**: Readable MDS ops (`dump_ops_in_flight`,
  `dump_blocked`, `dump_historic`), with inodes resolved to paths and clients
  to hosts and users.
- **`cephfs/client-inodes.py`**: Paths of the inodes a client session holds.
- **`cephfs/client-id-to-host`**: Resolve a client session ID to hostname and
  IP.
- **`cephfs/inode-to-path`**: Resolve a hex inode number to its path.
- **`cephfs/dir-tree-pins.sh`**: List directories pinned to each MDS rank.

### CephFS trees (on a mounted filesystem)

These read CephFS recursive statistics (`ceph.dir.*` xattrs) off a mount,
so they are fast on huge trees and need only read access, not cluster
credentials.

- **`cephfs/du`**: Size of files and directories, without walking the tree.
- **`cephfs/scan-growing-dirs.py`**: Find fast-growing directories on
  CephFS across every branch under one or more roots at once.
- **`cephfs/find-growing-dirs.py`**: Follow the fastest-growing subtree down,
  one level per sampling interval.
- **`cephfs/find-recent-rctime.py`**: Find files and directories changed
  since a date; much faster than `find -newer`.
- **`cephfs/cephfs-find-wide-dirs`**: Find directories holding many files.
  Prebuilt x86-64 Linux binary; source at
  [vbrik/cephfs-find-wide-dirs](https://github.com/vbrik/cephfs-find-wide-dirs).

## Tests

No test needs a cluster; many replay anonymized cluster captures from
`tests/backfillctl/test-data/`. Run each group separately (`pytest tests`
also works):

```
python3 -m unittest discover -s tests/backfillctl
python3 -m unittest discover -s tests/cephfs
```

For coverage, with [coverage.py](https://coverage.readthedocs.io/) 7.10+:

```
coverage run -m unittest discover -s tests/backfillctl
coverage run -m unittest discover -s tests/cephfs
coverage combine && coverage report    # or: coverage html
```

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
