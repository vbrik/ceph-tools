Fixture: real, small-scale instance of the exact scenario
divert-toofull targets (single OSD down, its host's
siblings absorb the vacated PG and blow past backfillfull_ratio).

Captured: 2026-08-11 ~09:05 from a live cluster, via:

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

What's actually going on: osd.457 is down/out, on host27. PG 19.21f
(EC pool 19) lost its acting OSD in shard slot 7, and CRUSH re-placed that
slot within the same host bucket, landing it on osd.625 -- also on
host27. That host is now too full, so the PG sits in backfill_toofull.
This is the single-OSD-out / same-host-retry mechanism from the script's
module docstring, just with only one PG affected so far (osd.457 had only
just gone down at capture time).

Verified against the live cluster: running the script (it needs no
argument to say where to look -- it scans every backfill_toofull PG
cluster-wide, not just osd.457's) correctly reports 1 backfill_toofull PG
cluster-wide, 1 arriving shard, and proposes remapping 19.21f shard 7 from
osd.625 to osd.849 (host35, 86.8% util, projected to reach 87.5% once
the shard has landed; checked by hand: the PG's 1384102474816 bytes over
k=8 is a 173 GB shard, 0.72% of osd.849's 24.1 TB, on top of its 86.80%).

Expected proposals, one per line as PGID SHARD ACTING_OSD UP_OSD TARGET_OSD
(checked by the tests against what the script plans; 'none' is an unknown
acting OSD):

  19.21f  7  none  625  849

This fixture also pins down where --min-up-util draws its line.
osd.625 is at 89.4% against this cluster's backfillfull_ratio of 0.90, so
it is below the ratio and yet is demonstrably the blocker -- Ceph refuses a
backfill on the target's projected usage once the shard lands, not on its
usage today. That is why the default threshold is nearfull_ratio (85%) and
not backfillfull_ratio: at backfillfull_ratio this genuinely stuck shard
would be filtered out and the fixture would propose nothing. The default
--max-target-util here is 89%, and osd.849 at 86.8% clears it.

This fixture is also the one that exercises the unknown-ACTING-OSD case:
osd.457 is already out, so the slot it left in 'acting' reads as
CRUSH_ITEM_NONE: the proposal's acting OSD is unknown (None), which the
table shows as 'none' with '-' for the utilization and host that would
have been derived from it.

Use this fixture to exercise the "found something to divert" path. It is
NOT a no-problems fixture -- see divert-toofull-nominal-synthetic/ for that
(same topology files, but pg_dump_pgs.json is a hand-edited
empty result, since the live cluster had no genuinely problem-free moment
available at capture time).

Replay this fixture directly (no live cluster, no fake `ceph` needed) with:

  backfillctl --load-state . divert-toofull

ANONYMIZED: cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule
names have been replaced with deterministic fake values (see
anonymize_snapshots() in the script) before committing this fixture. PG
ids, OSD ids and utilizations are real and untouched, since those are what
the script's analysis and this fixture's expected output depend on.
