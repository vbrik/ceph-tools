Fixture: real, small-scale instance of the exact scenario
upmaps-to-unstick-toofull-backfills.py targets (single OSD down, its host's
siblings absorb the vacated PG and blow past backfillfull_ratio).

Captured: 2026-08-11 ~09:05 from a live cluster, via:

  ceph osd tree --format json          > osd_tree.json
  ceph osd df --format json            > osd_df.json
  ceph osd dump --format json          > osd_dump.json
  ceph osd pool ls detail --format json > pool_ls_detail.json
  ceph osd crush rule dump --format json > crush_rule_dump.json
  ceph pg ls backfill_toofull --format json > pg_ls_backfill_toofull.json

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
osd.625 to osd.849 (host35, 86.8% util). Table output:

                   ---- ACTING ----    --------- UP ---------    ------- TARGET -------
  PGID    SHARD    OSD   UTIL  HOST    OSD      UTIL   HOST      OSD      UTIL   HOST
  19.21f  7        none  -     -       osd.625  89.4%  host27    osd.849  86.8%  host35

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
CRUSH_ITEM_NONE and the row shows 'none' with '-' for the utilization and
host that would have been derived from it.

Use this fixture to exercise the "found something to divert" path. It is
NOT a no-problems fixture -- see upmaps-toofull-nominal-synthetic/ for that
(same topology files, but pg_ls_backfill_toofull.json is a hand-edited
empty result, since the live cluster had no genuinely problem-free moment
available at capture time).

Replay this fixture directly (no live cluster, no fake `ceph` needed) with:

  upmaps-to-unstick-toofull-backfills.py --load-state .

ANONYMIZED: cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule
names have been replaced with deterministic fake values (see
anonymize_snapshots() in the script) before committing this fixture. PG
ids, OSD ids and utilizations are real and untouched, since those are what
the script's analysis and this fixture's expected output depend on.
