Fixture: real, live-cluster measure-rate capture: two samples, 32 s apart,
of backfills draining a host.

Captured: 2026-09-25, cluster ceph1, 17.2.5 (quincy), while host ceph1-28
(host28 after anonymization, 26 OSDs) was being drained: 390 of the 394
remapped PGs (EC pools 27, 18 and 24; k=8, 10 and 14) have a shard on it.
Made with

  backfillctl measure-rate --save-state <this directory>

(default --interval 30), so it is anonymized and trimmed as save-state's
captures are, and unpatched. Layout: the static snapshots here; each
sample's pg_dump_pgs.json and backfill_positions.json in 1/ and 2/;
times.json, when each sample's PG dump and each PG's 'pg query' were read,
in seconds from the first dump. Every PG was queried 0.8-1.2 s into its
sample, so each was measured over 31.8-32.0 s.

Every one of the 674 copy movements has a backfill position in both samples,
so every RATE is exact. Totals: 2485 objects/s, 544.6 MiB/s.

Cross-check against 'ceph -s' at the time, "recovery: 2.5 GiB/s, 1.45k
objects/s": Ceph counts an object once per recovery, at its logical size,
however many shards it pushes. Counted that way (per PG, not per shard;
num_bytes, not the shard's 1/k), the capture gives 1481 objects/s and
2.54 GiB/s.

tests/backfillctl/test_measure_rate.py replays it (DrainCaptureTest).
