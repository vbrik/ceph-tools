Fixture: genuine no-problems path for upmaps-to-unstick-toofull-backfills.py
-- i.e. the case where 'ceph pg ls backfill_toofull' matches nothing.

SYNTHETIC: pg_ls_backfill_toofull.json is hand-written, not captured. At
capture time (2026-08-11 ~09:05) the live cluster had an active instance of
the target scenario (osd.457 down on host27 -- see the sibling
../upmaps-toofull-osd457-down/ fixture), so there was no genuinely
problem-free moment to capture from. Its content is exactly what
'ceph pg ls backfill_toofull --format json' returns when nothing matches
(see _extract_pg_stats()'s "pg_ready" comment in the script):

  {"pg_ready":true}

The other five files (osd_tree.json, osd_df.json, osd_dump.json,
pool_ls_detail.json, crush_rule_dump.json) are the real snapshot from that
same capture, copied verbatim from ../upmaps-toofull-osd457-down/ --
cluster topology/weights are independent of which PGs are backfill_toofull,
so reusing them here is faithful, not fabricated.

Expected behavior with this fixture: the script should print "0
backfill_toofull PG(s) cluster-wide, 0 with newly-arriving shard(s)... " and
"proposed 0 remap(s), 0 unplaceable", with no warnings and no proposal
table.

If a truly problem-free capture is wanted later (all six files genuinely
captured at once with zero backfill_toofull PGs), recapture when
osd.457 is back up/replaced and 19.21f has cleared backfill_toofull.

Replay this fixture directly (no live cluster, no fake `ceph` needed) with:

  upmaps-to-unstick-toofull-backfills.py --load-state .

ANONYMIZED: cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule
names have been replaced with deterministic fake values (see
anonymize_snapshots() in the script) before committing this fixture. PG
ids, OSD ids and utilizations are real and untouched, since those are what
the script's analysis and this fixture's expected output depend on.
