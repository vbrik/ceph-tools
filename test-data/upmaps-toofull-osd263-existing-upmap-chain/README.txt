Fixture: real, live-cluster instance of the "arriving OSD is itself the 'to'
of an existing upmap pair" case that upmaps-to-unstick-toofull-backfills.py's
via_existing_upmap / UP_OSD '*' handling exists for (see "Why the raw
CRUSH mapping matters" in the script's module docstring).

Captured: 2026-08-17 ~06:17 from a live cluster, via:

  ceph osd tree --format json          > osd_tree.json
  ceph osd df --format json            > osd_df.json
  ceph osd dump --format json          > osd_dump.json
  ceph osd pool ls detail --format json > pool_ls_detail.json
  ceph osd crush rule dump --format json > crush_rule_dump.json
  ceph pg ls backfill_toofull --format json > pg_ls_backfill_toofull.json

What's actually going on: osd.263 (on host12) is overfull and has 6 EC
shards (pool 19) newly arriving on it, all in backfill_toofull. For 4 of
those 6 PGs, osd.263 is not CRUSH's own pick for that shard -- it only ended
up there because an existing pg_upmap_items pair already points some other
OSD's 'to' at 263 (e.g. PG 19.bd5's existing pairs are "344->570,625->263",
so osd.625 is what CRUSH chose and 263 is where an earlier upmap sent it).
Diverting these four therefore means rewriting that pair's 'to' from 263 to
the new target, not adding a fresh "263->target" pair (which Ceph's upmap
validation would silently drop, since 263 was never CRUSH's own pick).

Verified against the live cluster: running the script reports 6
backfill_toofull PGs cluster-wide, 6 shards to divert, 0 unplaceable, and 4
of the 6 proposals flagged with UP_OSD '*'. Table output:

  PGID     SHARD  ACTING_OSD  ACTING_UTIL  ACTING_HOST  UP_OSD    UP_UTIL  UP_HOST  TARGET_OSD  TARGET_UTIL  TARGET_HOST  EXISTING_UPMAPS
  19.7be   1      osd.863     88.4%        host35       osd.263   87.6%    host12   osd.842     86.9%        host36       554->687
  19.bd5   8      osd.625     88.7%        host27       osd.263*  87.6%    host12   osd.829     87.0%        host34       344->570,625->263
  19.d85   9      osd.189     87.8%        host30       osd.263   87.6%    host12   osd.617     87.0%        host11       243->618,154->668
  19.118a  2      osd.618     88.4%        host13       osd.263*  87.6%    host12   osd.446     87.0%        host26       818->151,618->263
  19.122e  7      osd.487     88.7%        host12       osd.263*  87.6%    host12   osd.813     87.0%        host35       866->356,487->263
  19.1ce0  0      osd.723     88.7%        host27       osd.263*  87.6%    host12   osd.45      87.0%        host11       655->454,575->831,723->263

19.7be and 19.d85 are unflagged: their EXISTING_UPMAPS pairs don't have 263
as a 'to', so osd.263 there really is CRUSH's own raw pick.

Use this fixture to exercise the via_existing_upmap / '*' path end-to-end
(this is the scenario that prompted removing the old "unappliable" skip in
favor of still proposing these rows, flagged). For the plain "found
something to divert, nothing flagged" path see
upmaps-toofull-osd457-down/; for the "no problems" path see
upmaps-toofull-nominal-synthetic/.

Replay this fixture directly (no live cluster, no fake `ceph` needed) with:

  upmaps-to-unstick-toofull-backfills.py --load-state .

ANONYMIZED: cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule
names have been replaced with deterministic fake values (see
anonymize_snapshots() in the script) before committing this fixture. PG
ids, OSD ids and utilizations are real and untouched, since those are what
the script's analysis and this fixture's expected output depend on.
