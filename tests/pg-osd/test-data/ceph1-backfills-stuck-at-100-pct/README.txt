Fixture: real, live-cluster snapshot showing pg-movements.py's false-100%
PROGRESS case (see PROGRESS_100_NOTE / progress_reads_100 in shared.py).

Captured: 2026-09-22, cluster ceph1 (ad1bf53c-a6ca-11ec-b47e-b04f13b8e306),
17.2.5 (quincy), while host ceph1-28 was being rebalanced off of (its 26 OSDs
were the source of 2118 of the cluster's 3623 in-flight shard backfills,
osd_max_backfills=1). Made with

  pg-movements.py --save-state <this directory>

19 shard rows read PROGRESS 100% (stat_sum.num_objects_misplaced +
num_objects_degraded == 0) while still listed (up != acting), all in pool 27
(an EC k8m2 pool, pg_num=4096), and every one of the 19 PGs has at least one
shard sourced from a ceph1-28 OSD.

Why they read 100% but hadn't finished: confirmed live (not from this
snapshot alone) by querying one of them, PG 27.126, directly with
'ceph pg 27.126 query'. Its state was active+remapped+backfilling with a
non-empty backfill_targets, and its backfill scan position
(recovery_progress.backfill_info / peer last_backfill) measurably advanced
between repeated queries minutes apart -- genuinely still copying data, not
wedged. Ceph's own misplaced/degraded counters had simply already hit zero
before the scan itself reached the PG's actual end. The same 19 PG/shard rows
were still present, unchanged, across two full pg-movements.py runs several
minutes apart, i.e. this can persist far longer than the "reads 100%, about
to finish" case this heuristic was written for.

Using the pg's seed and pool 27's (power-of-two) pg_num to reverse the
bit-reversed hash range Ceph sorts backfill scan objects by, PG 27.126's scan
was independently estimated at ~79% through its own range at capture time,
consistent with real, if slow, forward progress rather than a stall -- not
reproduced by these tools, which stay off live per-PG queries by design (see
module docstrings); this fixture and analysis were done ad hoc to confirm the
false-100% behavior before fixing it.

tests/pg-osd/test_pg_movements.py replays this snapshot with --load-state and
checks that PROGRESS_100_NOTE is printed and that 27.126's two rows still read
literal 100%.
