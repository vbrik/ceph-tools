Fixture: real, cluster-sized save-state capture of a new host that balance
has half filled, with many of its backfills part done. This is what counting
a backfill's copied part only once is for (placement.with_copied).

Captured: 2026-09-30, cluster ceph2, two days after
../ceph2-just-added-ceph2-37/ (host37 is host ceph2-37), with

  backfillctl save-state <this directory>

so it is anonymized, trimmed, and includes backfill_positions.json: the
backfill position of every remapped PG's targets (1004 PGs).

What's going on: 'backfillctl balance' was run on a state like that of
../ceph2-just-added-ceph2-37/ and its ~2200 upmaps applied (the upmap
balancer has been off since before host37 was added). Each host37 OSD was
sent 84-85 shards, nearly all of pool 19 (EC 8+2, ~175 GiB shards): about 88%
of its capacity. All 1004 remapped PGs are backfilling (or waiting to) onto
host37; none is backfill_toofull.

A backfill's copied part is already in 'ceph osd df' kb_used. osd.924, for
example:

  kb_used                                         59.55%
  46 shards held (in acting)                      48.00%
  38 shards arriving, in full                     39.66%
  the part of those 38 copied (34 are under way)  11.15%
  held + copied part                              59.16%  (~ kb_used)

Adding the 38 arriving shards in full to kb_used projects 99.2%; adding only
what they have yet to copy projects 88.1%. Counted twice, the copies put
host37's OSDs at 93.1-99.8%. Every one of them really ends at 88.0-88.1%,
within 1 point of the share of the shards mapped to it ('up'). A balance run
then took host37's OSDs for sources, proposing moves off the new host onto
old OSDs at 88.6%. It also noted all 1004 PGs as having a shard backfilling
onto a source too full to take it.

tests/backfillctl/test_balance.py replays it (PartlyCopiedFixtureTest,
FixtureInvariantTest), and tests/backfillctl/test_show_backfill.py too
(osd.924's PROJ).
