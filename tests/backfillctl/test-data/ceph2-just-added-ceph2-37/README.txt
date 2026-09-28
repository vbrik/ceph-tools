Fixture: real, cluster-sized save-state capture of a nearly full cluster
right after a new, empty host was added.

Captured: 2026-09-28, cluster ceph2, with

  backfillctl save-state <this directory>

so it is anonymized and trimmed as save-state's captures are. Host ceph2-37
is host37 after anonymization.

What's going on: 926 OSDs (848 hdd, 78 ssd) on 39 hosts, all up and in.
Ratios are Ceph's defaults: nearfull 0.85, backfillfull 0.90, full 0.95.

  - 822 existing hdd OSDs (~18.3 TiB) average 91.6% (85-92%, most at
    91-92%), above backfillfull_ratio.
  - host37's 26 hdd OSDs (~16.4 TiB) are at 0.3%.
  - The hdd class's capacity-weighted mean is 89.16%: the new host is ~3%
    of the class's capacity, so it lowers the mean by only ~2.5 points.

Pool 19, EC 8+2 with 8192 PGs, holds the bulk of the data, in ~175 GiB
shards: about 1 point of an old OSD, 1.05 of a new one. The CRUSH failure
domain is host, so a PG can put at most one shard on host37. 11 PGs are
remapped (9 backfilling, 2 backfill_wait); each has a backfill position.
5935 existing upmap pairs; 9 PGs' pairs chain.

Why this fixture matters: it is the capture that showed 'balance' filling a
new host only halfway. With a one-sided level (mean + 2 = 91.16%), each old
OSD gets below it after shedding one or two shards: 1118 moves, 191.6 TiB,
which leaves host37 at ~45%. Evening out the class means filling host37 to
the mean less --max-deviation (87.16%): ~2200 moves, ~370 TiB.

tests/backfillctl/test_balance.py replays it (FixtureInvariantTest).
