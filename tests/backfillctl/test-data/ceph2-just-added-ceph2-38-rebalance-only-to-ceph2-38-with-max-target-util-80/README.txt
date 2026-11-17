Fixture: real, cluster-sized save-state capture of one round of
'balance --target-hosts' applied to a new host, its backfills just started.

Captured: 2026-11-17, cluster ceph2, shortly after
../ceph2-just-added-ceph2-38-all-pgs-pinned-in-place-no-backfills/ (host38
is host ceph2-38), with

  backfillctl save-state <this directory>

so it is anonymized, trimmed, and includes backfill_positions.json (1976
PGs).

What's going on: on the state of that capture,

  backfillctl balance --target-hosts ceph2-38 --max-target-util 80

was run and its 1984 upmaps applied with pgremapper: 1499 unpins (dropping
a pin that kept a shard off host38) and 485 new pairs onto host38. The
capture was taken once peering had finished. Ratios are Ceph's defaults
(nearfull 0.85, backfillfull 0.90, full 0.95) and the class mean is still
86.98%, with no data moved yet:

  - 1976 PGs of pool 19 (EC 8+2, ~175 GiB shards) are remapped, each with
    one shard backfilling (650) or waiting to (1326) onto host38: exactly
    76 per host38 OSD. Most positions are MIN; none is past 3%.
  - The other 8 upmaps, of pool 16's ~1.2 GiB PGs, had already finished.
  - host38's OSDs are at 0.8-0.9%, projected at 79.6-79.7%, within 0.35
    points of the share of the shards mapped to them ('up'), under the 80%
    cap. The old hdd OSDs are at 88.6-89.6%.
  - 593 of the 2092 pins remain; 13172 upmap pairs on 6769 PGs.

Running the same round again proposes nothing. With the default
--max-target-util, 'balance --target-hosts host38' proposes the 156 moves
(68 of them unpins) that, with the 1984 applied, make up the 2140 a single
run proposed on the pinned capture. Either way, 304 sources stay at 87.7%:
one shard above the level (the class mean), so a move would have to take a
target above the source it relieves.

tests/backfillctl/test_balance.py replays it (AppliedRoundFixtureTest,
FixtureInvariantTest).
