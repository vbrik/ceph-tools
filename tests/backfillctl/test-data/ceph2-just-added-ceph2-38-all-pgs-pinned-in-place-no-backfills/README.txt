Fixture: real, cluster-sized save-state capture of a nearly full cluster
right after a new, empty host was added and every shard CRUSH wanted to move
onto it was pinned in place. Nothing is backfilling. This is the starting
point 'balance --target-hosts' is meant for.

Captured: 2026-11-17, cluster ceph2, with

  backfillctl save-state <this directory>

so it is anonymized and trimmed as save-state's captures are. Host ceph2-38
is host38 after anonymization. backfill_positions.json is empty: no PG is
remapped.

What's going on: 952 OSDs (874 hdd, 78 ssd) on 40 hosts, all up and in.
Ratios are Ceph's defaults: nearfull 0.85, backfillfull 0.90, full 0.95.
The upmap balancer was off, as the --target-hosts workflow in
'backfillctl balance --help' has it.

  - The old hdd OSDs are at 88.6-89.6%, evened out since
    ../ceph2-37-filling-with-partial-backfills/ (host37 is at 88.1%).
  - host38's 26 hdd OSDs (osd.926-951, ~16.4 TiB) are at 0.3%.
  - The hdd class's capacity-weighted mean is 86.98%.

All 8968 PGs are active+clean (some scrubbing), up == acting. There are
14246 upmap pairs on 7049 PGs. 2092 of them, one per PG and all in pool 19
(EC 8+2, ~175 GiB shards), pin a shard that CRUSH maps to a host38 OSD back
onto its old OSD (pair N'->A): 60-98 per host38 OSD. 22 PGs' pairs chain
(15 of them pin a host38 shard); balance leaves those alone.

Why this fixture matters: almost every shard balance sends to host38 could
be a pin dropped (an unpin: pair A->N', so the data moves once), rather
than a new pair onto some other host38 OSD. 'balance --target-hosts host38'
proposes 2140 moves (365 TiB), the same as without --target-hosts.
Initially, only 59 of them were unpins: each source sheds only 2-3 shards,
largest first, and its largest is rarely the pinned one. With pinned shards
shed first, 1573 are, 7 send a pinned shard to another host38 OSD, and 560
move a shard that is not pinned. Where an unpin is refused, it is mostly
because the host38 OSD would end up above the source it relieves; the
pinned shard then usually stays, and its source sheds another.

tests/backfillctl/test_balance.py replays it (FixtureInvariantTest).
