# `backfillctl balance --target-hosts` — design

## Intent

After adding empty storage hosts to a cluster whose OSDs are near
backfillfull, CRUSH wants to move a lot of data everywhere. The operator
wants the urgent part first: get data off the fullest OSDs (that is what
frees space, e.g. in CephFS) onto the new hosts only, without the rest of
the reshuffle. The upmap balancer finishes the job slowly afterwards.

Workflow:

1. `ceph balancer off`
2. Add the new hosts.
3. `backfillctl cancel-backfill` — pin everything where it is.
4. `backfillctl balance --target-hosts NEW1 NEW2 ...` (optionally
   `--until-util`) — apply, let backfills finish, repeat as needed.
5. `ceph balancer on` — let it finish balancing gradually.

## Goals

- `--target-hosts HOST [HOST ...]`: only OSDs on these hosts receive data
  in the run.
- Relieving the fullest OSDs stays the top priority: the source side
  (fullest source first, its largest shard first) is unchanged.
- Where it costs nothing in relief, avoid moving data twice: a shard
  pinned away from a target host goes back to the OSD CRUSH maps it to.

## Non-goals

- Minimizing total data movement at the expense of relief.
- `--target-hosts` for `drain` (the Planner support is generic; exposing it
  there is a later, separate decision).
- Checking that `cancel-backfill` was run first; the help states the
  assumption.

## Anti-goals

- Changing how `balance` behaves without `--target-hosts`.
- A selection rule that is hard to explain in the help text.

## Approaches considered

1. **Restriction only.** Simplest; some data moves twice once the balancer
   takes over.
2. **Restriction + unpin preference, only under `--target-hosts`
   (picked).** Same relief as 1, given the level clause below (a fuller
   target can hold back later moves off a source, so the unpin is limited
   to where it cannot); fewer double moves; generic `balance`
   untouched. Reads as part of "fill the new hosts": put data where CRUSH
   already wants it.
3. **Unpin as its own flag, or always on.** More general, but one more
   option to explain, or a change in generic balancing behavior.

## Design

**Level and sources: unchanged.** The class mean includes the new, empty
hosts, so the default level is low; targets must stay below it, so the new
hosts fill to "balanced" and stop. `--until-util` bounds relief instead.

**Restriction.** In `Planner.place()`, a candidate must be on a target
host. This applies to every placement of the run, including blocker
diversions (`resolve_blockers`, `try_pin_elsewhere`): only target hosts
receive data. Pins are unaffected (their data stays put).

**Unpin preference (only with `--target-hosts`).** For a shard mapped to
source A where the PG has a live existing pair N'→A (N' not in `up`;
stale pairs ignored, as in `Planner.chains`) and N' is on a target host:

- N' must be a candidate of the shard's device class (on a target host,
  up and in, not a source) and not in the PG's acting set.
- N' is exempt from the raw-CRUSH-mapping ban for this shard only
  (`PgState.forbidden_osds` otherwise contains it; `uncommit` keeps raw
  OSDs banned).
- If N' passes every other legality check (host, `--max-target-util`,
  below the level, below the source, relieve invariants, no chain) and
  ends more than the shard's size below the level, it is chosen;
  otherwise the least-utilized legal target, as today.
- Why the level clause (`Planner.costs_relief`): a later move off the
  source must leave it above every target it sent data to. Such a move
  comes while the source is at or above the level, with a shard no larger,
  so a target ending more than a shard below the level cannot stop it.
  Without the clause, a fuller N' near the level could leave a source at
  the level for an extra round (found in final review). Chosen over a
  three-clause rule (also allowing N' no fuller than the emptiest or the
  source's other targets) to keep the help text simple: on a capture with
  300 synthetic pins both gave the same 2 unpins (unconditional: 4).

Output is the ordinary pair A→N'. Verified in pgremapper's source
(`mappingstate.go:tryRemap`, called from `import-mappings`): a new mapping
that is the exact opposite of an existing one removes it. `fold_pairs`
already folds it to the no-op N'→N', which `chain_link` ignores.

**Validation.** Each `--target-hosts` name must be a host in the OSD map
with at least one up-and-in OSD of `--class`; otherwise exit with an
error listing the bad names.

**Reporting.** The stderr "Targets:" paragraph names the target hosts.
Unpins carry `Move.unpin` (counted on stderr) and `messages.UNPIN_NOTE` in
the NOTE column; a later "PG stays toofull" note may replace the latter.
Host names match by short name, as `drain --hosts` (`shared.host_osds`).

## Left to the builder

- Exact NOTE / stderr wording (following `CLI-WRITING-STYLE-v1.md`).
- How the target-host set is threaded into `Planner` (constructor
  parameter is the obvious choice).
- Tests (`tests/backfillctl/`): restriction honored (including blocker
  diversion); unpin chosen when legal; fallback when N' is illegal (above
  level, over `--max-target-util`, host clash); stale pair ignored; no
  unpin without `--target-hosts`; JSON entry is A→N'; validation errors.
- README and `balance --help` updated, including the workflow above.
