Fixture: real, live-cluster snapshot showing the false-100% PROGRESS case
of Ceph's misplaced counters. It predates 'save-state' capturing backfill
positions, so a replay shows counter-based PROGRESS, marked '~' (see
PROGRESS_APPROX_NOTE in shared.py). See
../ceph1-resumed-backfills-exact-progress for the cause, and a capture
with positions.

Captured: 2026-09-22, cluster ceph1, 17.2.5 (quincy), while host ceph1-28
(host28 after anonymization, 26 OSDs) was being rebalanced off of: it was the
source of 1986 of the cluster's 3353 in-flight shard backfills (1986
remapped PGs), osd_max_backfills=1. Made with

  backfillctl save-state <this directory>

so it is complete (every subcommand can --load-state it), anonymized, and
unpatched: every value, up_primary included, is what the cluster reported.
It replaces an earlier capture of the same episode, taken hours before with
pg-movements's old per-script --save-state, which lacked osd_dump.json and
crush_rule_dump.json and had up_primary synthesized.

36 shard rows read PROGRESS 100% (stat_sum.num_objects_misplaced +
num_objects_degraded == 0) while still listed (up != acting), in 26 PGs, all
in pool 27 (an EC k8m2 pool, pg_num=4096); every one of the 26 PGs has at
least one shard sourced from a host28 OSD.

Why they read 100% but haven't finished: confirmed live (not from this
snapshot alone) on one of them, PG 27.126, with 'ceph pg 27.126 query'. At
the earlier capture its state was active+remapped+backfilling with a
non-empty backfill_targets, and its backfill scan position
(recovery_progress.backfill_info / peer last_backfill) measurably advanced
between queries minutes apart -- genuinely still copying data, not wedged.
Ceph's own misplaced/degraded counters had simply hit zero before the scan
reached the PG's actual end. Using the PG's seed and pool 27's power-of-two
pg_num to reverse the bit-reversed hash order Ceph scans objects in, its scan
was estimated at ~79% through its range then. At this capture, hours later,
it was still active+remapped+backfilling (backfill_targets 208(3), 515(4)),
with the same two rows still at 100%: this can persist far longer than the
"reads 100%, about to finish" case the heuristic was written for.

tests/pg-osd/test_show_backfill.py replays this snapshot with --load-state and
checks that PROGRESS_APPROX_NOTE is printed and that 27.126's two rows read
100% from the counters (not exact); tests/pg-osd/test_backfillctl.py uses it as a real --load-state
directory for the dispatcher.
