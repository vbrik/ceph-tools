Fixture: real, live-cluster instance of the case cancel-backfills-into-osd.py's
"companion pins" exist for (see "Companion pins" in the script's module
docstring): pinning a shard back to its acting OSD would put two shards of the
PG on one host, so the other, also-moving shard of that PG must be pinned back
with it.

Captured: 2026-09-21 from a live cluster, with

  cancel-backfills-into-osd.py --save-state <this directory> 896

which stores the (anonymized) output of these commands:

  ceph osd tree --format json                > osd_tree.json
  ceph osd df --format json                  > osd_df.json
  ceph osd dump --format json                > osd_dump.json   (cut down to
                                               the erasure code profiles)
  ceph osd pool ls detail --format json      > pool_ls_detail.json
  ceph osd crush rule dump --format json     > crush_rule_dump.json
  ceph pg ls remapped --format json          > pg_ls_remapped.json

The cluster: EC pool 19 (k=8, m=2, CRUSH failure domain host), 688 remapped
PGs, 585 of them in backfill_toofull because the fullest OSDs are at 90-93%
(backfillfull_ratio was 0.91 at capture time; osd_dump.json is cut down and
does not store it). osd.896 (host51) is at 81.1%, well below that, and many
PGs are being moved onto it.

What is going on: 6 EC shards are arriving on osd.896. For 5 of those PGs a
second shard is moving too, and CRUSH placed its destination on the same host
as the acting OSD of the shard heading for 896. Pinning only the shard into
896 would therefore leave two shards on one host, which Ceph rejects. E.g.
19.7e9: shard 0 would go back to osd.627 (host32), but shard 9 is arriving on
osd.149, also host32; pinning shard 9 back to osd.497 as well resolves it.

Expected output for osd 896 (--pgremapper), 6 requested pins and 5 companions:

  19.7e9 896 627
  19.7e9 149 497
  19.92e 896 231
  19.94c 896 522
  19.94c 716 266
  19.14cd 232 337
  19.14cd 896 614
  19.1b16 896 591
  19.1b16 524 578
  19.1fed 896 5
  19.1fed 12 207

--import-mappings prints the same 11 pairs as a JSON array (one entry per pair,
in this order). Applying them with separate 'pgremapper remap' runs is what
fails on this cluster, so --pgremapper warns that 5 PGs (all but 19.92e) need
more than one line.

The companions are 19.7e9 149->497, 19.94c 716->266, 19.14cd 232->337,
19.1b16 524->578 and 19.1fed 12->207. 19.14cd shard 1 (314->347) is also
moving but between two OSDs of the same host, so it does not clash and is not
pinned. 19.1b16 is the one PG here that is already backfilling (~81%); the
others are at 0% in backfill_toofull.

Other OSDs worth replaying:
  osd 74   2 pins, no companions (19.16fc shard 6, 19.1eb3 shard 8)
  osd 682  1 pin (19.16fc shard 7 from osd.231) with 19.16fc shard 6
           (osd.74, same host as osd.231) as its companion
  osd 231  no backfills into it

tests/test_cancel_backfills_into_osd.py replays this snapshot with
--load-state, so its assertions do not depend on the live cluster.
