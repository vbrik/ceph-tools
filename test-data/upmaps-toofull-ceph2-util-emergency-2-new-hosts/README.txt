Fixture: real, cluster-sized capture of a general utilization emergency --
NOT the single-OSD-down scenario the other fixtures cover. Every OSD is up
and in; the cluster as a whole is simply too full, and two new hosts had
just been added to relieve it.

Added to the repo: 2026-09-20, captured from a live cluster via:

  ceph osd tree --format json          > osd_tree.json
  ceph osd df --format json            > osd_df.json
  ceph osd dump --format json          > osd_dump.json
  ceph osd pool ls detail --format json > pool_ls_detail.json
  ceph osd crush rule dump --format json > crush_rule_dump.json
  ceph pg ls backfill_toofull --format json > pg_ls_backfill_toofull.json

What's actually going on: 900 OSDs across 38 hosts, none down or out.
backfillfull_ratio is 0.91 here, where the other fixtures have Ceph's 0.90
default; nearfull_ratio is 0.85 and full_ratio is 0.95. That difference is
useful on its own -- it is what proves the thresholds are read from the
capture rather than hard-coded. Individual OSD utilization runs to 93.4%, i.e. past
backfillfull_ratio, even though no host averages above 84.2%. 808 PGs of
the EC pool 19 are in backfill_toofull, contributing 1513 newly-arriving
shards.

host50 and host51 are the two newly-added hosts: 12 OSDs each, averaging
70.6% and 71.5% against a cluster where most hosts sit around 78-84%.
They are absorbing shards, not blocking them.

Why this fixture matters: it is the one that exercises both safety
thresholds, and it is the capture that exposed the two bugs they fix.

  - backfill_toofull is a property of the PG, not of each shard arriving
    on it. Of the 1513 arriving shards, 537 are arriving on OSDs below
    nearfull_ratio, and every one of those 537 is on host50 (274) or
    host51 (263) -- the two new hosts, at ~70%, which plainly are not what
    is blocking anything. Diverting them wastes target OSDs that genuinely
    stuck shards then cannot get, so --min-up-util (default:
    nearfull_ratio) leaves them alone.

  - Target ranking is relative, so with no cap "least utilized" degrades
    to "least catastrophic" once the candidate pool is drawn down. With
    --max-target-util disabled this capture proposed every one of the 822
    usable hdd OSDs, 342 of them at or above backfillfull_ratio -- remaps
    that re-wedge the moment they are applied. --max-target-util
    (default: backfillfull_ratio minus 1) excludes them, and the OSDs
    within a point of the ratio, which may lack room for the shard.

Expected results with the default thresholds (--min-up-util 85,
--max-target-util 90, both derived from this cluster's own ratios):

  808 backfill_toofull PGs, 808 with newly-arriving shards
  1513 arriving shards, 976 at or above --min-up-util, 537 left alone
  candidate target OSDs: hdd=242, ssd=78
  242 remaps proposed, 734 unplaceable
  no proposed target above --max-target-util (so none within a point of
  backfillfull_ratio)
  no diverted shard arriving on an OSD below nearfull_ratio

The table is 242 rows, too long to quote here the way the small fixtures
do, so the test asserts those counts and invariants instead of an exact
table (see FixtureReplayTest in
test_upmaps_to_unstick_toofull_backfills.py). The 734 unplaceable shards
are counted on stderr after the table, without a reason: the heuristic found
no target for them, which does not prove none exists.

For reference, the pre-threshold behavior is still reachable and is what
the invariant test guards against regressing to:

  upmaps-to-unstick-toofull-backfills.py --load-state . \
      --min-up-util 0 --max-target-util 100
  -> 573 remaps proposed, 940 unplaceable, 93 targets past backfillfull

(Before the "target strictly emptier than the arriving OSD" rule this was 822
remaps proposed, 691 unplaceable, 342 past backfillfull.)

Replay this fixture directly (no live cluster, no fake `ceph` needed) with:

  upmaps-to-unstick-toofull-backfills.py --load-state .

ANONYMIZED: cluster fsid, OSD IPs/uuids, hostnames and pool/CRUSH-rule
names have been replaced with deterministic fake values (see
anonymize_snapshots() in the script) before committing this fixture. PG
ids, OSD ids and utilizations are real and untouched, since those are what
the script's analysis and this fixture's expected results depend on.
