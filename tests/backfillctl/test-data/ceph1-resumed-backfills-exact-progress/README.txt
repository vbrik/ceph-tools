Fixture: real, live-cluster snapshot of backfills whose misplaced counters
read 0 (so a counter-based PROGRESS says 100%) while they are really
anywhere from 6.6% to 99.8% done. This is what shared.copy_progress's
backfill positions are for.

Captured: 2026-09-23, cluster ceph1, 17.2.5 (quincy), still rebalancing off
host ceph1-28 (see ../ceph1-backfills-stuck-at-100-pct/README.txt), with

  backfillctl save-state <this directory>

so it is complete, anonymized, unpatched, and includes backfill_positions.json:
the backfill position of every remapped PG's targets (1523 PGs, in EC pools
18, 24 and 27, pg_num 512, 1024 and 4096).

What happened: shortly before, at 18:11-18:22 UTC, nearly every remapped PG
re-peered. Each of them then resumed its backfill from the targets' saved
last_backfill instead of starting over. On a resumed backfill, each target
reports as many objects as the primary, or more (e.g. 18.1d's targets
154299 against its primary's 154072). Ceph estimates misplaced as roughly
primary objects minus target objects, so the estimate reads 0. 711 of the
remapped PGs have misplaced + degraded == 0 here, and their positions put
them at 6.6% (27.500) to 99.8% (27.c4f). Live queries before the capture
showed the positions of such PGs (e.g. 18.1d) advancing minutes apart: they
were copying, not stuck.

Cross-checks of reading the position as a share of the PG:
  - 18.0 started its backfill fresh (target at MIN, not resumed), so its
    counter is sound: 8.19% by the counter, 8.26% by the position.
  - Each position's key, bit-reversed, has the PG's seed in its low bits
    (backfill_fraction rejects a key that doesn't), for all 1523 PGs.

Shards of one PG are not necessarily at the same position: in 52 PGs they
differ, e.g. 27.ae2's shard 9 (to osd.646) at 97.7% and its shard 4 (to
osd.663) at 9.9%, which a per-PG average would show as 53.8% for both.

tests/backfillctl/test_show_backfill.py replays it (FixtureReplayTest): every row
is exact, 27.500 reads 6.6%, 27.ae2's shards read 97.7% and 9.9%, and no '~'
note is printed. tests/backfillctl/test_cancel_uphill.py replays it too
(ExactProgressTest).
