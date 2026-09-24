Fixture: real, live-cluster snapshot with 7 remapped PGs whose pins chain
(A->B, B->C), which pgremapper 1.0.0 cannot apply (see
shared.avoid_chains):

- 19.1399, 19.146e, 19.1a6f, 19.1d52: two shards of the PG chain, e.g.
  19.1399 shard 9 moves 414->890 while shard 5 moves 341->414.
- 19.1128: the pin 888->545 chains with the existing pair 545->94.
- 19.3a4, 19.5fd: the existing pairs already chain (890->110, 110->753 and
  889->19, 19->669); pgremapper drops one link as stale when it changes
  the PG.

Captured: 2026-09-24 with 'backfillctl save-state' (anonymized). osd_dump.json
keeps the ratios, erasure code profiles and pg_upmap_items.
