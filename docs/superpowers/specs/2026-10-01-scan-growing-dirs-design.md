# scan-growing-dirs.py — design

Date: 2026-10-01. Status: approved 2026-10-01; amended during planning
(see `docs/superpowers/plans/2026-10-01-scan-growing-dirs.md`).

## Purpose

Find where a CephFS tree is growing, across many branches at once, in one
sampling interval. The tool reads two `ceph.dir.rbytes` samples per
directory and uses `ceph.dir.rctime` to skip idle subtrees, so it never
walks files.

It complements `cephfs/find-growing-dirs.py`, which follows only the single
top grower and spends one full interval per level. Both tools stay.

Success means the output names either the directories whose own files are
growing, or the unexplored subtrees worth a closer look, each with a rate.

## Evidence from `/mnt/ceph2` (2026-10-01, read-only mount)

- **Every `ceph.dir.*` getxattr appears to be an MDS round trip.** About
  74 ms serially, consistent with a forced getattr per call.
  Throughput scales with threads: on ~1000 unique dirs it was 2.9 ms/call
  at 8 threads, 0.8 ms at 32, and 0.5 ms at 64.
- **rctime can be ahead of the local clock.** `exp/IceCube` read 1.1 s in
  the future, because ctime comes from the writing client's clock.
- **A parent's rctime lags its children's.** The root's rctime trailed
  `exp`'s, and it advanced in steps of about 5 s, which matches the MDS
  rstat propagation tick.
- **Live lag check, with a scratch build of the tool** (`--delay 20
  --depth 4` on `/mnt/ceph2`, about 10 MiB/s total): every listed dir's
  rate matched its children's sum to within 15 KiB/s, so the 1 MiB/s
  default leaves wide headroom. The wake-up re-check flagged a subdir of
  the root that started changing during the sleep.
- **`ceph.dir.*` getxattr needs read permission on the dir itself.**
  `exp/ingest` (`dr-xr-x--x`) gave EACCES even though it can be traversed.
  An unreadable dir can be neither listed nor measured.

## Terms

- **Tracked dir:** a root, or a subdir whose rctime shows recent activity.
  Every tracked dir is sampled.
- **Idle subdir:** a subdir whose rctime is older than the cutoff. It is
  not tracked and not descended into, and its growth is assumed to be 0.
- **Listed dir:** a tracked dir whose subdirs were enumerated and probed.
- **Leaf:** a tracked dir with no tracked children.
- **Probe:** one rctime getxattr on a subdir of a listed dir, made to
  decide whether it is idle.

## CLI

```
scan-growing-dirs.py [options] ROOT [ROOT ...]
scan-growing-dirs.py --load FILE [--threshold RATE] [--sort KEY] [--json]

  --delay SECONDS     60       minimum interval between a dir's two samples
  --depth N           5        levels to descend below each root (root = 0)
  --threshold RATE    1MiB/s   rows below it are hidden
  --max-dirs N        1000     error out if more dirs are active
  --max-entries N     10000    don't descend into dirs with more entries
  --threads N         32       parallel getxattr/readdir calls
  --sort {depth,rate} depth
  --json                       rows as JSON on stdout
  --save FILE                  write the sampled state after the run
  --load FILE                  report from a saved state; no sampling
```

- **`--threshold`** has the form `<number><unit>/<duration>`, e.g.
  `500MB/min` or `10GiB/h`.
  - Units: `B`, `kB` … `PB` (powers of 1000), and `KiB` … `PiB` (powers
    of 1024). Units are case-insensitive.
  - Durations: `s`, `min`, `h`, `d`.
  - The rate must be > 0 and finite. Bare `M`/`G` are rejected as
    ambiguous.
- **Numeric limits:** 0 < `--delay` ≤ 86400 (`MAX_DELAY`; more is a typo,
  and extreme values overflow the sleep), `--depth` ≥ 0, and `--threads`,
  `--max-dirs` and `--max-entries` ≥ 1.
- **`--save FILE`** is checked before any sampling, so a typo doesn't waste
  a long run. A usage error (exit 2) results if FILE is empty or a
  directory, or if its directory is missing or not writable.
- **`--load` conflicts:** combining it with a ROOT, `--delay`, `--depth`,
  `--max-dirs`, `--max-entries`, `--threads` or `--save` is an error that
  names the option. Sampling options therefore default to `None` and get
  their real defaults after validation, so the code can tell what the
  user gave.
- **`--help`** covers what the tool does, when to use it, how to act on
  each row kind, and the limitations below.

## Pipeline

1. **Preflight.** Each ROOT must exist and be a directory, and its
   `ceph.dir.rbytes`, `rctime` and `entries` must be readable. Roots must
   be distinct and not nested. Every failing root is named as typed, then
   the tool exits with `ERROR:`. Roots are resolved (made absolute, with
   symlinks followed) and shown resolved. Below the roots, symlinks are
   never followed: not by listings, and not by getxattr, so a subdir
   swapped for a link mid-run isn't read through it.

2. **Build**, breadth-first and in parallel. The idle cutoff is
   `build_start_wall − delay − IDLE_GRACE`, with `IDLE_GRACE = 30` s. For
   each tracked dir at depth < `--depth`:
   - **Idle root:** if it is a root whose rctime is older than the cutoff,
     it becomes a leaf with reason `IDLE`. Nothing below it can be active,
     so it isn't listed.
   - **Wide dir:** if `ceph.dir.entries` > `--max-entries`, it becomes a
     leaf with reason `WIDE`, and the entry count is recorded. It is never
     read with readdir.
   - **Otherwise** its subdirs are listed with `scandir`, using `d_type`
     and not following symlinks; each subdir's inode comes from the listing
     too. If listing fails, it becomes a leaf with reason `UNLISTABLE`;
     if the dir is gone (ENOENT or ENOTDIR), it is marked vanished.
   - **Each subdir is probed** for its rctime:
     - rctime ≥ cutoff: the subdir becomes tracked.
     - rctime < cutoff: it is idle; its rctime is recorded for the wake-up
       re-check.
     - EACCES, EPERM or another error: it is unreadable and added to the
       permission warning.
     - ENODATA or ENOTSUP: it is not CephFS (a mount point), so it is
       skipped with a warning. Its bytes aren't in the parent's rbytes
       anyway.
     - ENOENT or ENOTDIR: it vanished and is ignored.

   All three stages (build, sampling, re-check) classify errors the same
   way: vanished, not CephFS, or unreadable.

   Dirs at depth = `--depth` become leaves with reason `DEPTH`. As soon as
   the tracked count (roots included) exceeds `--max-dirs`, the build stops,
   pending work is cancelled, and the tool exits with `ERROR:`. The message
   names the count, the cutoff age and the depth reached, and suggests
   lowering `--depth`, narrowing the roots, or raising `--max-dirs`.

3. **Sample 1.** One parallel pass reads `rbytes` of every tracked dir, in
   build (BFS) order. Each sample is timestamped at the `time.monotonic()`
   midpoint of its getxattr call. A dir that is gone (ENOENT or ENOTDIR)
   is marked vanished: its bytes left before measuring began, so it
   contributes nothing and isn't a read problem. Any other failure makes
   the dir **unsampled**: it gets no rows, and its parent counts it as
   unmeasured.

4. **Sleep** for `--delay` seconds, starting after pass 1 has finished.

5. **Sample 2.** The same pass, in the same order. Each dir's interval is
   therefore ≥ `--delay`: its pass-1 midpoint is before the sleep, and its
   pass-2 midpoint is after it.
   - ENOENT: the dir counts as shrinking to 0 bytes (`rbytes2 = 0`), which
     keeps its parent's own-files figure right. One note gives the count.
   - Any other error makes the dir unsampled and adds it to the
     permission warning.

6. **Follow renames.** A tracked dir that vanished in sample 2, while its
   parent now lists a subdir with the **same inode** under another name,
   was renamed within its parent. It moves to the new path with its tracked
   subtree, and the moved dirs are sampled a second time there through the
   same `sample_all` as step 5. A failed re-read therefore makes a dir
   unsampled, exactly as in step 5. This way a rename isn't counted as the
   dir shrinking to 0 plus a new dir appearing, and a note lists each
   rename. The cost is one readdir per parent of a dir that vanished,
   usually none. Moves to another parent aren't followed; they show as
   growth at the destination, as documented.

7. **Re-check.** For every sampled listed dir with `own(n) ≠ 0`, whatever
   the threshold (that keeps `--load` with a different threshold
   consistent), the tool works from the paths and rates as corrected by
   step 6. The gate is `≠ 0` rather than `> 0`, because a subdir's growth
   can be hidden by the dir's own files shrinking and still count in an
   ancestor's `spread` row.
   - It re-probes the rctime of the dir's idle subdirs. A subdir whose
     rctime changed **woke up**. One that has vanished is ignored, and an
     unreadable one counts as unreadable. The cost is bounded by the
     build's probe count.
   - It lists the dir again. A subdir that wasn't there at build time (not
     tracked, idle, unreadable or non-CephFS) was created during sampling,
     and also counts as woken. A dir that can no longer be listed is
     skipped. The cost is one readdir per such dir.

   Either way, the subdir's growth is in `own(n)` but wasn't measured on
   its own. A job that writes into a fresh output dir each run is the
   common case for new subdirs.

8. **Analyze**, then **render** a table or JSON. `--save` writes the state
   after step 7. `--load` replaces steps 1–7.

Progress goes to stderr. On a TTY it is a single updating line (phase,
depth, tracked count, probes done); otherwise there is one line per phase.
Ctrl-C exits with code 130 at once (`os._exit` after flushing output),
because a normal exit would join worker threads that can hang in getxattr
on a stuck MDS.

## Measurement model

- **`R(n)`** = (rbytes2 − rbytes1) / (t2 − t1), using dir *n*'s own
  timestamps, in bytes/s. It is signed and covers *n*'s whole subtree.
- **`own(n)`**, for a listed dir, is `R(n) − Σ M(c)` over its tracked
  children.
  - **`M(c)`** is `R(c)` if *c* was sampled. Otherwise it is `Σ M` over
    *c*'s own tracked children (0 if it has none). This way, rows found
    below an unsampled dir aren't counted a second time in `own(n)`.
  - Idle subdirs contribute 0.
  - Unreadable subdirs, woken subdirs, and the own files of unsampled
    subdirs are **unmeasured**: their growth lands in `own(n)`. A subdir
    that vanished is not unmeasured: its `M` is exact (0 if it vanished
    before sample 1, −rbytes1/Δt if before sample 2).
- **Signs:** rates stay signed through all arithmetic, and only rows ≥ the
  threshold are reported. Clamping a shrinking child to 0 before the
  subtraction would hide the growth of the parent's own files.
- **Why wall-clock time, not rctime.** rctime is the latest ctime in the
  subtree, not the time the rbytes value was true. For bursty writers,
  Δrbytes/Δrctime blows up. rctime also carries writer clock skew, and it
  moves on chmod, rename and delete.
- **Why a separate sampling pass.** Building the tree is slow and uneven.
  Sampling all dirs in two quick passes aligns their windows, which the
  `own` subtraction relies on.
- **Why no leaf-first order.** Propagation lag shifts a parent's window
  later but doesn't shorten it, so for steady growth it cancels. What
  matters is the same order in both passes, and subtracting rates rather
  than deltas.

## Rows

Each dir is evaluated on its own. There is no threshold pruning of
subtrees: every tracked dir is sampled regardless, so pruning could not
save I/O, and it would hide a growing child whose shrinking sibling pulls
the parent's net rate below the threshold.

| Kind | Emitted for | Rate | NOTE |
|---|---|---|---|
| `files` | listed dir, `own(n)` ≥ T | `own(n)` | unmeasured subdirs, if any |
| `tree` | leaf with reason `DEPTH`, `WIDE`, `UNLISTABLE` or `IDLE`, `R(n)` ≥ T | `R(n)` | `depth limit`, `<N> entries`, `unlistable`, `idle at start` |
| `spread` | listed non-leaf, uncovered growth ≥ T | `R(n) − covered(n)` | unmeasured subdirs, if any |

- **`spread` is computed bottom-up**, in reverse BFS order.
  - `covered(n)` is the sum of the rates of rows already emitted in *n*'s
    subtree, including *n*'s own `files` row.
  - If `R(n) − covered(n)` ≥ T, a `spread` row is emitted, and `covered(n)`
    becomes `R(n)`.
  - Diffuse growth is therefore reported once, at the lowest dir where it
    adds up to the threshold.
  - A dir can have both a `files` row and a `spread` row. Both are printed.
- **`~` mark.** It goes on a `files` or `spread` rate that may include
  unmeasured subdirs.
  - For `files`, that means *n* has unmeasured subdirs.
  - For `spread`, it means some dir in the uncovered part of *n*'s
    subtree has them.
  - `tree` rates are exact, because rbytes covers the whole subtree.
- **NOTE for `~` rows:** counts joined by `; `, e.g. `2 unreadable
  subdirs; 1 subdir became active`. Unsampled tracked subdirs count as
  unreadable. For `spread`, the counts are summed over the uncovered part
  of the subtree.
- **Unsampled dirs** get no rows. Rows below them are still reported.

Worked example (T = 1 MiB/s, rates in MiB/s):

```
/r          R=55.3  own = 55.3 − (5 + 50) = 0.3   → none (uncovered 0.3)
├─ a        R=5     leaf, DEPTH                    → tree 5.0
├─ b        R=50    own = 50 − 100×0.5 = 0         → spread 50.0
│  └─ b1…b100      R=0.5 each, leaves              → none
└─ c        idle
```

If `/r/x` grows at +5 and `/r/y` shrinks at −4.8, then `/r/x` is reported
and `/r` is not: its uncovered rate is 0.2 − 5 < 0.

**Sort order.**
- `depth` (the default): depth, then path, then kind (`files` before
  `spread`).
- `rate`: rate descending, then depth, then path, then kind.
- Depth is measured from each row's root.

## Output

**Table** on stdout:

```
      RATE  KIND    PATH                              NOTE
52.1 MiB/s  files   /mnt/ceph2/exp/IceCube/2026/raw
48.0 MiB/s  spread  /mnt/ceph2/sim/IceCube/b
12.4 MiB/s  tree    /mnt/ceph2/sim/a/b/c/d/e          depth limit
 3.0 MiB/s  tree    /mnt/ceph2/exp/foo/wide           12345 entries
 2.2 MiB/s~ files   /mnt/ceph2/exp/bar                2 unreadable subdirs
```

- RATE uses binary units with one decimal and is right-aligned. `~` or a
  space follows the rate, so the digits line up.
- Paths are never truncated.

**stderr.** Only the footnotes and the "Nothing grew" line are specific to
the table; everything else is printed with `--json` too.

- **Footnotes** (table only): one line per kind and mark present in the
  shown rows.
  - `files`: growth of files directly in the dir.
  - `spread`: growth in the dir's subtree that no row below it shows.
  - `tree`: growth of a subtree that wasn't explored (see NOTE).
  - `~`: may include growth in subdirs that couldn't be measured (see
    NOTE).
- **Summary:** e.g. `Tracked 412 dirs under 2 roots (probed 9310
  subdirs); sample intervals 61.0-61.9 s.` (ASCII hyphen)
- **No rows** (table only): `Nothing grew at 1.0 MiB/s or more; a lower
  --threshold shows slower growth.` The exit code is still 0. If some dirs
  couldn't be read, the line says `Nothing that could be read grew …`.
- **Nothing sampled:** if no dir could be sampled (e.g. permissions changed
  mid-run), the run ends with `ERROR: no dir could be sampled, so nothing
  was measured` and exits 1, in both modes. `--save` still writes the
  state, for a bug report.
- **Permission warning:** one `WARNING:` paragraph covers unreadable,
  unlistable and unsampled dirs, up to 10 paths plus `+N more`. Each dir
  is listed and counted once, with all its problems. The full list is in
  `--json` and `--save`. It then states only the effects that apply to
  this run:
  - an unreadable or unsampled subdir is counted in its parent's rate,
    marked `~`;
  - a root that couldn't be sampled gets no rows of its own;
  - an unlistable dir is measured as a whole subtree.
- Separate notes cover non-CephFS subdirs, dirs renamed during sampling
  (`old -> new`), and dirs that vanished (each counts as 0 bytes from then
  on).

**`--json`** prints one object on stdout:

```json
{"version": 1, "threshold": 1048576.0,
 "rows": [{"path": "...", "root": "...", "depth": 2, "kind": "files",
           "rate": 54629376.0, "approx": false, "note": "",
           "unreadable": [], "woke_up": []}],
 "unreadable": [{"path": "...", "problem": "unreadable: Permission denied"}],
 "not_cephfs": ["..."],
 "summary": {"roots": 2, "tracked": 412, "probed": 9310,
             "interval_min": 61.0, "interval_max": 61.9}}
```

Rates are in bytes/s. Rows are in the chosen sort order. `interval_min`
and `interval_max` are null if no dir was sampled.

**`--save` / `--load` state** is a JSON object with:

- `"format": "scan-growing-dirs-state"` and `"version": 1`;
- `created`, the run's arguments, the roots, the probe count, and the
  non-CephFS subdirs;
- `nodes` in BFS order, each with:
  - path, root path, depth, and parent index;
  - leaf reason, entries (for `WIDE`), and the listing error;
  - the unreadable subdirs (path → error) and the woken subdir paths;
  - both samples (`[rbytes, t]`, or null), the sampling error, and the
    vanished flag;
  - the inode (null for roots) and, after a rename, the old path.

The report is printed before the state is saved, so a failed save doesn't
lose it; the failure still exits 1. The save goes to a temporary file next
to the target (symlinks are written through), which then replaces it, so
a failed or interrupted save leaves any earlier file intact.

`--load` checks `format`, `version` and the values it relies on: parent
indexes, field types, positive sample intervals, and no NaN or Infinity.
It exits with `ERROR:` on any mismatch, and on an unreadable file. An
empty `--save` or `--load` path is an error, not a no-op. A loaded state renders exactly as a live run
would with the same threshold and sort. The state contains real paths and
is not anonymized.

## Errors and exit codes

| Code | Meaning |
|---|---|
| 0 | Success, with or without rows |
| 1 | `ERROR:`: bad root, overlapping roots, `--max-dirs` exceeded, no dir could be sampled, bad `--load` file, failed `--save` |
| 2 | argparse usage error, including a bad `--threshold`, numeric limits, `--load` conflicts and a bad `--save` path |
| 130 | Ctrl-C (exits at once, without joining worker threads) |
| 141 | stdout's reader went away (e.g. `\| head`); the state is still saved |

## Code structure

A single standalone file, `cephfs/scan-growing-dirs.py`, like the other
cephfs tools. It needs Python 3.11 or later (`enum.StrEnum`, `datetime.UTC`)
and nothing outside the standard library. Deciding and printing are
separate.

- **Named constants:** `IDLE_GRACE`, `PROBE_BATCH` (16 subdirs per probe
  task), `MAX_LISTED_PATHS = 10`, `PROGRESS_INTERVAL`, the vxattr names,
  and the state and JSON format versions.
- **`parse_rate(text) -> float`** returns bytes/s and raises `ValueError`
  with the accepted forms. It is wrapped as an argparse `type`.
- **`Fs`** wraps `getxattr` (never following a symlink), subdir listing
  (`subdirs()` returns path → inode), `exists`, `isdir`, `realpath`, and
  the clocks: `clock()` is monotonic, `now()` is wall time, and `sleep()`.
  Tests substitute a fake with a scripted clock and tree.
- **Dataclasses:**
  - `Sample(rbytes, t)`.
  - `Node`: path, root, depth, parent, children, reason, entries,
    list_error, idle (path → rctime), unreadable (path → error), woke_up,
    s1, s2, sample_error, vanished, ino, renamed_from. `Node.rate` is R(n);
    `measured()` is M(n) and `own_rate()` is own(n).
  - `Run`: the roots, the nodes in BFS order, the sampling limits,
    `created`, `probed`, `not_cephfs`. It is what `--save` writes.
  - `Row`: path, root, depth, kind, rate, approx, note, unreadable,
    woke_up.
- **I/O functions** take an `Fs` and, except `preflight`, an executor:
  - `preflight(paths, fs, pool)` checks the roots in parallel (a glob such
    as `/home/*` can give hundreds) and returns the root nodes, or raises
    `FatalError` with one argument per problem.
  - `build(run, fs, pool, progress)` grows `run.nodes` in BFS order, and
    raises `FatalError` past `--max-dirs`.
  - `sample_all(nodes, fs, pool, second=...)` fills `s1` or `s2`.
  - `follow_renames(run, fs, pool)` moves renamed subtrees and samples
    them again through `sample_all`.
  - `recheck_subdirs(run, fs, pool)` fills `woke_up`.
  - Probing goes through `_probe_batches()`, which keeps at most
    `IN_FLIGHT_BATCHES` (1024) tasks of `PROBE_BATCH` subdirs in flight.
    That bounds memory for millions of probes, and it lets the
    `--max-dirs` check stop the build early. Both the build and the
    re-check use it.
  - `sample_run(args, fs)` runs them all, in order, with one pool.
- **Pure functions:**
  - `analyze(nodes, threshold) -> list[Row]`, `sort_rows(rows, key)`;
  - `render_table(rows)`, `footnotes(rows)`, `summary(run)`, `notes(run)`
    and `render_json(run, rows, threshold)`, which all return `str` (or
    `list[str]`).
- **State:** `save_state(path, run)` and `load_state(path) -> Run` share
  one table per record (`NODE_FIELDS`, `RUN_FIELDS`). Each entry says how
  a field is written to JSON and how it is checked and read back, so a new
  field is added in one place and every field is type-checked.
- **Text tables:** `KIND_TEXT` (row kinds, for footnotes and `--help`) and
  `REASON_TEXT` (each tree NOTE and its advice in `--help`). `Kind`'s
  member order is the sort and legend order.
- **`report()`** only prints. `main()` decides the exit status: 141 on a
  closed pipe, and an `ERROR:` if `sampled(run)` is false.
- **`main(argv, fs)`** parses and validates the arguments, runs or loads,
  reports, then saves. It maps `FatalError` to `ERROR:` lines and exit 1,
  Ctrl-C to 130, and a closed stdout pipe to 141 (after saving). It
  reconfigures stdout with `surrogateescape`, so directory names that
  aren't UTF-8 print as their original bytes instead of crashing the
  table. `run_cli()`, the entry point, exits with `os._exit` after Ctrl-C.

Threads are used because getxattr, scandir and sleep release the GIL. One
`ThreadPoolExecutor(--threads)` serves every phase.

## Testing

`tests/cephfs/test_scan_growing_dirs.py`, using unittest, and loading the
script with `importlib` like the other cephfs tests. No cluster is needed;
the fake `Fs` provides a scripted tree, xattrs that change over fake time,
and a fake clock and sleep.

- **`parse_rate`:** each unit and duration, case-insensitivity, decimals,
  and rejection of zero, negative, missing-unit, bare `M` and unknown
  inputs.
- **`analyze`:**
  - the worked example;
  - a shrinking sibling;
  - a dir with both `files` and `spread` rows;
  - each `tree` reason and its NOTE;
  - `~` from unreadable, woken and unsampled subdirs, including summing
    them for `spread`;
  - a rate exactly at the threshold;
  - an all-negative tree, and no rows at all;
  - vanished dirs (counted as 0);
  - an unsampled dir with rows below it, whose growth must not be
    counted twice in the parent's `own`;
  - multiple roots.
- **`build`:**
  - the cutoff boundary, including the grace margin;
  - an idle root, which is not listed;
  - the depth limit, with depth 0 tracking only the roots;
  - a wide dir is never listed (the fake checks that scandir wasn't
    called);
  - `--max-dirs` exactly at the limit vs. one over;
  - unreadable, non-CephFS and vanished subdirs, and unlistable dirs;
  - symlinks are not followed.
- **Sampling:** every interval is ≥ the delay under the fake clock; both
  passes use the same order; ENOENT in pass 2 counts as 0; an error in
  pass 1 makes the dir unsampled.
- **`recheck_subdirs`:**
  - it runs only for dirs with `own > 0`;
  - changed rctime means woke up, and so does a subdir created during
    sampling;
  - known subdirs (tracked, idle, unreadable, non-CephFS) are not new;
  - vanished, unreadable and no-longer-listable cases;
  - end to end, a subdir created during the sleep marks its parent's
    `files` row `~`.
- **Preflight:** missing root, a root that isn't a dir, missing xattrs,
  permission denied, duplicate and nested roots, and all bad roots
  reported together.
- **CLI:**
  - argument validation;
  - each `--load` conflict;
  - a save → load round trip gives identical stdout;
  - the JSON shape;
  - exit codes.
- **Rendering:**
  - column alignment and the `~` column;
  - footnotes only for the kinds present;
  - both sort orders and their tie-breaks;
  - the permission-warning cap.
- **Live smoke run** on `/mnt/ceph2` with a short `--delay`. The mount is
  read-only, so this only exercises the tool against whatever activity
  happens to be going on.
  - The fake `Fs` can't reproduce parent/child propagation lag. So on
    whatever subtree is active, compare each ancestor's `R` with the sum
    of its children's `R`, and record how large the spurious `files` and
    `spread` residuals on busy upper-level dirs are.
  - That tells us whether the 1 MiB/s default is usable on this cluster.

`ruff format` and `ruff check` must both pass on the script and the
tests.

## Docs

- Add a README entry under "CephFS trees", next to `find-growing-dirs.py`.
- Say how the two tools differ: this one reports every growing branch in
  one interval, while `find-growing-dirs.py` drills down into the single
  top grower. Use search-friendly wording ("fast-growing directories",
  "CephFS", "rbytes").
- Check the README's intro sentence, which mentions "fast-growing
  directories", and keep it accurate.

## Limitations (for `--help`)

- **Noisy own-files figures.** rstats propagate lazily (seconds, and
  longer across MDS ranks and pinned subtrees), and clients flush file
  sizes late. `files` and `spread` rates are differences of rates, so
  they are noisy for bursty writers. `tree` rates are not.
- **Moves count as growth.** `mv` into a tracked dir from another dir
  shows as growth there, and the matching shrinkage at the source isn't
  reported. A tracked dir renamed within its parent is followed by its
  inode, so that isn't growth.
- **Idle pruning trusts writer clocks.** It assumes client clocks are
  within `IDLE_GRACE` of the local clock.
- **Late starters are missed.** A subdir that is created, or becomes
  active, only during the sleep isn't measured on its own: it shows up as
  a `~` on its parent's row.
- **Writers with lagging clocks can hide growth.** A subdir written by a
  client whose clock is more than `IDLE_GRACE` behind looks idle, and its
  rctime may not move. Its growth then lands in the parent's `files`
  rate without a `~`.

## Decisions

| Decision | Reason |
|---|---|
| Timestamps from monotonic wall clock, not rctime | rctime isn't an "as of" time; it is skewed by writer clocks and moves on non-growth changes |
| A separate sampling pass after the build | Aligned windows for the `own` subtraction; interval ≥ delay is guaranteed |
| Same order in both passes, not leaf-first | Lag shifts windows rather than shrinking them; leaf-first buys little |
| Signed arithmetic, positive-only filter at report time | Clamping children first misattributes growth |
| Per-dir report filtering, no threshold pruning | Pruning saves no I/O after sampling, and it hides growing children of net-flat parents |
| `spread` rows | Without them, diffuse growth (many dirs below T each) is invisible |
| `~` for unmeasured subdirs | Their growth silently lands in the parent's `files` figure otherwise |
| Re-list dirs after sample 2; new subdirs count as woken | Added during implementation, at the user's request: a subdir created during sampling was invisible, and its growth looked like the parent's own files |
| Follow renames within a parent by inode | Code review: `job.tmp → job` during the sleep showed the dir's whole size as the parent's file growth |
| Re-check gate `own ≠ 0`, not `> 0` | Code review: a new subdir hidden by shrinking own files was missing its `~` on the ancestor's `spread` row |
| ENOENT/ENOTDIR mean vanished at every stage | Code review: short-lived scratch dirs produced bogus permission warnings and `~` marks |
| getxattr doesn't follow symlinks; roots are resolved | Code review: a subdir swapped for a link mid-run was read through it |
| Save on a closed pipe (exit 141); hard exit after Ctrl-C | Code review: `\| head` lost `--save`; a hung MDS blocked exit |
| `IDLE_GRACE` = 30 s | Absorbs writer clock skew and late size and rstat updates |
| No probe limit; progress on stderr | The expensive case (many active dirs with thousands of idle subdirs) is rare |
| `ceph.dir.entries` decides `WIDE` | Avoids listing huge dirs just to find out they are too big |
| Threads rather than processes | The work is I/O; one round trip per call; parallelism scales about 90× |
| New tool next to `find-growing-dirs.py` | The user chose to keep the old tool |
| `--json` and `--save`/`--load` | Project CLI guidelines; replay re-renders without re-sampling |
| `files` and `spread` both printed for the same dir; NOTE column | User choice |

## Out of scope

- Continuous or repeated monitoring (a `top`-like mode).
- Per-file attribution inside a `files` dir.
- Telling renames apart from writes.
- A `--max-probes` limit (the user chose progress output instead).
- Anonymizing saved state.
