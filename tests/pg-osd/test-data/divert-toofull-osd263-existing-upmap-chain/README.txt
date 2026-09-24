Fixture: real, live-cluster instance of the "arriving OSD is itself the 'to'
of an existing upmap pair" case that divert-toofull's
raw-CRUSH-mapping handling exists for (see placement.raw_crush_osds and
divert_toofull.print_pgremapper_mappings).

Captured: 2026-08-17 ~06:17 from a live cluster, via:

  ceph osd tree --format json          > osd_tree.json
  ceph osd df --format json            > osd_df.json
  ceph osd dump --format json          > osd_dump.json
  ceph osd pool ls detail --format json > pool_ls_detail.json
  ceph osd crush rule dump --format json > crush_rule_dump.json
  ceph pg ls backfill_toofull --format json > pg_ls_backfill_toofull.json

RENAMED: pg_ls_backfill_toofull.json has since been renamed to
pg_dump_pgs.json, content unchanged, to match 'backfillctl save-state''s
later unified snapshot format, which every subcommand's --load-state now
reads that PG data from (filtering it client-side for the flag it cares
about).

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
backfill_toofull PGs cluster-wide, 6 arriving shards, all 6 at or above the
default --min-up-util (osd.263 is at 87.6%, this cluster's
nearfull_ratio is 85%), and 0 unplaceable. Every proposed target clears the
default --max-target-util of 89%: each is at 86.9-87.0% now and projected
to reach 87.7% once its shard has landed.

Expected proposals, one per line as PGID SHARD ACTING_OSD UP_OSD TARGET_OSD
(checked by the tests against what the script plans):

  19.7be   1  863  263  837
  19.bd5   8  625  263  842
  19.d85   9  189  263  850
  19.118a  2  618  263  839
  19.122e  7  487  263  813
  19.1ce0  0  723  263  829

The output does not mark which proposals are the "existing upmap" kind; for
reference, the PGs' pg_upmap_items pairs were:

  19.7be   554->687                                 (263 is CRUSH's own pick)
  19.bd5   344->570,625->263                        (263 is a 'to')
  19.d85   243->618,154->668                        (263 is CRUSH's own pick)
  19.118a  818->151,618->263                        (263 is a 'to')
  19.122e  866->356,487->263                        (263 is a 'to')
  19.1ce0  655->454,575->831,723->263               (263 is a 'to')

--pgremapper-mappings still maps 'from' 263 for all six, e.g. {"pgid":
"19.bd5", "mapping": {"from": 263, "to": 842}}. 'pgremapper import-mappings'
turns that into a rewrite of the existing 625->263 pair to 625->842 for the
four 'to' PGs, and adds a fresh pair for the other two.

Use this fixture to exercise the raw-CRUSH-mapping path end-to-end. For the
plain "found something to divert" path see divert-toofull-osd457-down/; for
the "no problems" path see divert-toofull-nominal-synthetic/.

Replay this fixture directly (no live cluster, no fake `ceph` needed) with:

  backfillctl --load-state . divert-toofull

ANONYMIZED: cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule
names have been replaced with deterministic fake values (see
anonymize_snapshots() in the script) before committing this fixture. PG
ids, OSD ids and utilizations are real and untouched, since those are what
the script's analysis and this fixture's expected output depend on.
