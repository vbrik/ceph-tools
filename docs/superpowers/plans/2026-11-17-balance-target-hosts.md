# balance --target-hosts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `backfillctl balance --target-hosts HOST ...` moves data off the fullest OSDs onto the given hosts only, sending a pinned shard back to the OSD CRUSH maps it to when that is legal.

**Architecture:** The restriction lives in `shed.Planner`: its candidate order is built from target-host OSDs only, so every `place()` call (moves, blocker diversions, `try_pin_elsewhere`) obeys it. With target hosts set, `place()` first tries the shard's "unpin target" (N' of a live existing pair N'→A, A = shard's up OSD), exempting N' from the raw-CRUSH ban for that shard only. `balance.py` adds the option, validates it, and reports it.

**Tech Stack:** Python 3.14, argparse, unittest (`tests/backfillctl/_support.SyntheticCluster`).

**Spec:** `docs/superpowers/specs/2026-11-17-balance-target-hosts-design.md`

## Global Constraints

- Generic `balance` (no `--target-hosts`), `drain` and `divert-toofull` behave exactly as before; all existing tests pass unchanged.
- Unpin preference applies only when target hosts are given.
- Source side unchanged: fullest source first, its largest shard first; level = class mean (all class OSDs, incl. new hosts) + `--max-deviation`, or `--until-util`.
- Output for an unpin is the ordinary pair A→N' (pgremapper `tryRemap` removes the opposite pair N'→A).
- Messages another command could print go in `src/backfillctl/messages.py`; wording per `AGENTS.md.d/CLI-WRITING-STYLE-v1.md`.
- `ruff format` and `ruff check` clean; tests: `python3 -m unittest discover -s tests/backfillctl`.
- **No git commits** (project CLAUDE.md): leave changes in the working tree for the user.

## Review Focus

1. A blocker diversion lands outside the target hosts → must not happen; test in Task 1.
2. N' is legal by the raw-mapping exemption but another shard of the same PG was already moved onto N' (or onto N''s host) in this run → N' must be refused; test in Task 2.
3. N' is a source itself (e.g. a target host above the level), or of another device class → not a candidate, no unpin; test in Task 2.
4. A `--target-hosts` name that exists but whose OSDs are all out, down or another class → clear error, not a silent empty run; test in Task 3.
5. On a real capture, every non-pin move's target is on a target host and the pairs still apply cleanly → fixture invariant test in Task 3.

---

### Task 1: Restrict Planner targets to given hosts

**Files:**
- Modify: `src/backfillctl/shed.py` (`Planner.__init__` ~line 341, `shed()` ~line 910, module docstring, `ShedResult`)
- Test: `tests/backfillctl/test_shed.py`

**Interfaces:**
- Produces: `Planner.__init__(cluster, sources, level, level_class, *, relieve: bool, target_hosts: frozenset[str] | None = None)`; `self.target_hosts` stored.
- Produces: `shed(cluster, sources, level, level_class=None, target_hosts: frozenset[str] | None = None) -> ShedResult`.
- Produces: `ShedResult.target_hosts: list[str] | None` (sorted; None when not restricted).
- Test helper: `test_shed.Cluster.shed(self, *sources, level=None, level_class=None, max_target_util=None, target_hosts=None)` passes `frozenset(target_hosts)` or None through.

- [ ] **Step 1: Write the failing tests** (new class `TargetHostsTest` in `test_shed.py`)

```python
class TargetHostsTest(unittest.TestCase):
    """--target-hosts: only OSDs on these hosts receive data."""

    def test_targets_are_on_the_target_hosts_only(self):
        c = Cluster()
        c.util[31] = 5.0  # emptiest, but on h3
        c.util[41] = 30.0
        c.pg("1.0", [0, 10, 20])
        self.assertEqual(pairs(c.shed(0)), [("1.0", 0, 0, 31)])
        result = c.shed(0, target_hosts={"h4"})
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41)])
        self.assertEqual(result.target_hosts, ["h4"])

    def test_blocker_is_diverted_onto_a_target_host_only(self):
        # As BlockerTest.test_blocker_is_diverted, restricted to h4 and h5.
        c = Cluster()
        c.util[10] = 95.0
        c.util[31] = 10.0
        c.util[41] = 20.0
        result = c.pg("1.0", [0, 10, 20], [0, 11, 20]).shed(0, target_hosts={"h4", "h5"})
        self.assertEqual(pairs(result), [("1.0", 0, 0, 41), ("1.0", 1, 10, 50)])
        self.assertEqual(result.diverted_count, 1)

    def test_no_target_host_with_room_leaves_the_shard(self):
        c = Cluster()
        c.util[50] = c.util[51] = 89.5  # over the 89% cap with a 1% shard
        result = c.pg("1.0", [0, 10, 20]).shed(0, target_hosts={"h5"})
        self.assertEqual((result.moves, len(result.unplaceable)), ([], 1))

    def test_no_restriction_by_default(self):
        self.assertIsNone(Cluster().pg("1.0", [0, 10, 20]).shed(0).target_hosts)
```

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m unittest discover -s tests/backfillctl -k TargetHostsTest`
Expected: FAIL/ERROR (`unexpected keyword argument 'target_hosts'`).

- [ ] **Step 3: Implement**

In `Planner.__init__`, build each class's candidate list from OSDs not in `sources` **and**, when `target_hosts` is not None, with `cluster.osd_host.get(o) in target_hosts`. Thread `target_hosts` through `shed()` into `Planner` and into `ShedResult.target_hosts` (sorted list or None). Update the `shed.py` module docstring's "A legal target is ..." paragraph with one clause on target hosts.

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest discover -s tests/backfillctl`
Expected: all pass (existing ones unchanged).

- [ ] **Step 5: `ruff format` + `ruff check` on changed files** — clean. (No commit.)

---

### Task 2: Unpin preference under target hosts

**Files:**
- Modify: `src/backfillctl/shed.py` (`Planner.place` ~line 408, new `Planner.unpin_target`, `shed_sources` ~line 714, `ShedResult`, module docstring)
- Modify: `src/backfillctl/messages.py` (new `UNPIN_NOTE`)
- Test: `tests/backfillctl/test_shed.py`

**Interfaces:**
- Consumes: `Planner.target_hosts` (Task 1).
- Produces: `Planner.unpin_target(self, state: PgState, shard: MappedShard | ArrivingShard) -> int | None` — N' of the PG's existing pair N'→`shard.up_osd` with N' not in `state.pg["up"]` (live pair), N' a candidate (`N' in self.order_key`, so on a target host, of the shard's class, not a source), and `self.target_hosts is not None`; else None.
- Produces: `messages.UNPIN_NOTE: str` — suggested: `"back to the OSD CRUSH maps it to, dropping its upmap pair"`.
- Produces: `ShedResult.unpinned_count` property: requested moves whose `note == UNPIN_NOTE`.

- [ ] **Step 1: Write the failing tests** (new class `UnpinTest` in `test_shed.py`; CRUSH chose osd.40 for shard 0, an existing pin 40→0 holds it on osd.0)

```python
class UnpinTest(unittest.TestCase):
    """With target hosts, a pinned shard goes back to its CRUSH OSD when legal."""

    @staticmethod
    def pinned(pgid="1.0", up=(0, 10, 20)) -> Cluster:
        c = Cluster()
        c.util[40] = 30.0  # CRUSH's choice
        c.util[50] = 10.0  # emptier
        c.upmaps.append({"pgid": pgid, "mappings": [{"from": 40, "to": up[0]}]})
        return c.pg(pgid, list(up))

    def test_pinned_shard_goes_back_to_its_crush_osd(self):
        result = self.pinned().shed(0, target_hosts={"h4", "h5"})
        self.assertEqual(pairs(result), [("1.0", 0, 0, 40)])
        self.assertEqual(result.moves[0].note, messages.UNPIN_NOTE)
        self.assertEqual(result.unpinned_count, 1)

    def test_replicated_pinned_shard_goes_back_too(self):
        result = self.pinned("2.0").shed(0, target_hosts={"h4", "h5"})
        self.assertEqual(pairs(result), [("2.0", "-", 0, 40)])

    def test_no_unpin_without_target_hosts(self):
        result = self.pinned().shed(0)
        self.assertEqual(pairs(result), [("1.0", 0, 0, 50)])  # 40 is raw: banned
        self.assertEqual(result.unpinned_count, 0)

    def test_crush_osd_not_a_target_host_is_not_unpinned_to(self):
        result = self.pinned().shed(0, target_hosts={"h5"})
        self.assertEqual(pairs(result), [("1.0", 0, 0, 50)])

    def test_crush_osd_over_the_cap_falls_back(self):
        c = self.pinned()
        c.util[40] = 89.5
        self.assertEqual(pairs(c.shed(0, target_hosts={"h4", "h5"})), [("1.0", 0, 0, 50)])

    def test_crush_osd_at_the_level_falls_back(self):
        c = self.pinned()
        c.util[0] = 80.0
        c.util[40] = 39.5  # + 1% shard: 40.5%, at or above the level
        result = c.shed(0, level=40.0, level_class="hdd", target_hosts={"h4", "h5"})
        self.assertEqual(pairs(result)[0], ("1.0", 0, 0, 50))

    def test_crush_osd_on_a_host_the_pg_uses_falls_back(self):
        # Review Focus 2: osd.41 (h4) holds another shard of the PG.
        result = self.pinned(up=(0, 41, 20)).shed(0, target_hosts={"h4", "h5"})
        self.assertEqual(pairs(result), [("1.0", 0, 0, 50)])

    def test_crush_osd_that_is_a_source_is_not_unpinned_to(self):
        # Review Focus 3.
        result = self.pinned().shed(0, 40, target_hosts={"h4", "h5"})
        self.assertNotIn(("1.0", 0, 0, 40), pairs(result))

    def test_stale_pair_is_ignored(self):
        # 40 still in 'up': the pair 40->0 is stale, 40 is no unpin target.
        c = Cluster()
        c.util[50] = 10.0
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 40, "to": 0}]})
        result = c.pg("1.0", [0, 40, 20]).shed(0, target_hosts={"h5"})
        self.assertEqual(pairs(result), [("1.0", 0, 0, 50)])
```

Import `messages` from `_support` if not already imported in `test_shed.py`.

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m unittest discover -s tests/backfillctl -k UnpinTest`
Expected: FAIL (`UNPIN_NOTE` / `unpinned_count` missing; pair goes to 50).

- [ ] **Step 3: Implement**

- `Planner.unpin_target` per the Interfaces block, using `self.cluster.existing_pairs(state.pg)`.
- In `place()`: factor the per-candidate legality checks of the loop (`target_projection` with given `forbidden_osds`, `below`, `lowest_relieved`, `chains`) into a helper returning the final projection or None, used by both the loop and the unpin check. Before the loop, if `unpin_target` gives N' and N' is not in `state.new_up`, run the helper with `state.forbidden_osds - {N'}` (and the same `forbidden_hosts`, incl. `avoid_host`); if legal, return `(projected, N')`. Otherwise run the loop unchanged.
- In `shed_sources`, pass `note=UNPIN_NOTE` to `state.add_move` when `picked[1] == planner.unpin_target(state, shard)`.
- Module docstring: one sentence on the unpin preference under target hosts.

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest discover -s tests/backfillctl`
Expected: all pass.

- [ ] **Step 5: `ruff format` + `ruff check`** — clean. (No commit.)

---

### Task 3: `balance --target-hosts` CLI, validation, output, docs

**Files:**
- Modify: `src/backfillctl/balance.py` (`build_parser`, `plan`, `render`, module docstring)
- Modify: `README.md` (`balance` bullet; short "after adding hosts" workflow)
- Test: `tests/backfillctl/test_balance.py`

**Interfaces:**
- Consumes: `shed(..., target_hosts=...)`, `ShedResult.target_hosts`, `ShedResult.unpinned_count` (Tasks 1–2).
- Produces: `--target-hosts HOST [HOST ...]` (`dest="target_hosts"`, `nargs="+"`, default None).
- Produces: `check_target_hosts(hosts: list[str], osd_host: dict[int, str], class_osds: list[int], osd_class: str) -> frozenset[str]` in `balance.py`; exits `ERROR: --target-hosts: ...` naming every host unknown to the OSD tree, and separately every host with no up and in OSD of the class. Called in `plan()` before the PG dump is read.

- [ ] **Step 1: Write the failing tests** (new class `TargetHostsTest` in `test_balance.py`)

```python
class TargetHostsTest(unittest.TestCase):
    def test_moves_go_to_the_target_hosts(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[31] = 5.0
        result = c.pg("1.0", [0, 10, 20]).plan("--target-hosts", "h5")
        self.assertEqual(pairs(result), [("1.0", 0, 0, 50)])

    def test_unknown_host_is_named(self):
        with self.assertRaises(SystemExit) as cm:
            Cluster().plan("--target-hosts", "h5", "nope")
        self.assertTrue(str(cm.exception).startswith("ERROR: --target-hosts: "))
        self.assertIn("nope", str(cm.exception))
        self.assertNotIn("h5", str(cm.exception))

    def test_host_without_class_osds_is_named(self):
        # Review Focus 4.
        c = Cluster()
        c.classes[50] = c.classes[51] = "ssd"
        with self.assertRaises(SystemExit) as cm:
            c.plan("--target-hosts", "h5")
        self.assertIn("h5", str(cm.exception))

    def test_stderr_names_target_hosts_and_counts_unpins(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[40] = 30.0
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 40, "to": 0}]})
        _, err = c.pg("1.0", [0, 10, 20]).rendered("--target-hosts", "h4", "h5")
        self.assertIn("h4, h5", err)
        self.assertIn("1 ", err)  # tighten to the exact unpin sentence chosen

    def test_unpin_json_entry_is_up_to_crush_osd(self):
        c = Cluster()
        c.util[0] = 80.0
        c.util[40] = 30.0
        c.upmaps.append({"pgid": "1.0", "mappings": [{"from": 40, "to": 0}]})
        out, _ = c.pg("1.0", [0, 10, 20]).rendered(
            "--target-hosts", "h4", "--pgremapper-mappings"
        )
        [entry] = upmap_pairs(out)  # import upmap_pairs from _support
        self.assertEqual(entry["mapping"], {"from": 0, "to": 40})
```

Also add `(["--target-hosts", "nope"], "ERROR: --target-hosts: ")` to the cases of `SourcesTest.test_bad_class_and_osds_fail_before_the_pg_dump`.

Review Focus 5: in `FixtureInvariantTest`, add a test that runs `plan_from_state(bal, <fixture>, "--target-hosts", <two hosts from that fixture's osd tree>)` and asserts every non-pin move's target is on one of them, plus `check_pairs_apply` and `check_one_shard_per_host`. Pick the hosts by reading the fixture; check the `upmap_pairs` entry shape against `shared.upmap_entry` and adjust the JSON assertion's keys if needed.

- [ ] **Step 2: Run to verify they fail**

Run: `python3 -m unittest discover -s tests/backfillctl -k TargetHosts`
Expected: FAIL (`unrecognized arguments: --target-hosts`).

- [ ] **Step 3: Implement**

- `build_parser`: `--target-hosts` with help like "Move data onto OSDs of only these hosts (e.g. newly added ones); see below."
- `plan()`: validate right after `--osds` checks; pass the frozenset to `shed()`.
- `render()`: when `r.target_hosts`, the "Targets:" paragraph begins "Targets: the {cls} OSDs on {', '.join(hosts)}, …" instead of "the other {cls} OSDs"; after `print_outcome`, if `r.unpinned_count`, one sentence: "{n} of the moves send a shard back to the OSD CRUSH maps it to, dropping its upmap pair."
- Module docstring: a paragraph on `--target-hosts`: the restriction (applies to diversions too), the unpin preference, and the workflow (balancer off → add hosts → `cancel-backfill` → `balance --target-hosts` in rounds → balancer on). State that it assumes `cancel-backfill` ran first, since backfills in motion already count in the projections.
- `README.md`: extend the `balance` bullet with `--target-hosts`, and add a 2–3 line "after adding hosts" note with the workflow. Keep it short.

- [ ] **Step 4: Run all tests**

Run: `python3 -m unittest discover -s tests/backfillctl`
Expected: all pass.

- [ ] **Step 5: `ruff format` + `ruff check`; check `backfillctl balance --help` renders** — clean. (No commit; the user commits.)
