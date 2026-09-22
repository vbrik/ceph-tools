Fixture: real, live-cluster snapshot in which stopping the backfills into
osd.896 needs pins beyond the shards arriving on it (see "Companion pins" and
"Blockers" in stop-backfills-into-osd.py's module docstring): pinning a shard
back to its acting OSD can put two shards of the PG on one host, which Ceph
silently drops, and other shards of the same PG can be headed for OSDs over
backfillfull_ratio, which keeps the whole PG in backfill_toofull.

Captured: 2026-09-21 from a live cluster, with

  stop-backfills-into-osd.py --save-state <this directory> --osd 896

which stores the (anonymized) output of these commands:

  ceph osd tree --format json                > osd_tree.json
  ceph osd df --format json                  > osd_df.json
  ceph osd dump --format json                > osd_dump.json   (cut down, see
                                               below)
  ceph osd pool ls detail --format json      > pool_ls_detail.json
  ceph osd crush rule dump --format json     > crush_rule_dump.json
  ceph pg ls remapped --format json          > pg_ls_remapped.json

osd_dump.json keeps only the erasure code profiles and the full ratios. This
capture was made before the ratios were kept, so full_ratio 0.95,
backfillfull_ratio 0.91 and nearfull_ratio 0.85 -- the values the cluster
reported when it was taken -- were added to the file by hand afterwards.

The cluster: EC pool 19 (k=8, m=2, CRUSH failure domain host), 688 remapped
PGs, 585 of them in backfill_toofull because the fullest OSDs are at 90-93%
against a backfillfull_ratio of 91%. osd.896 (host51) is at 81.1%, well below
that, and many PGs are being moved onto it.

What is going on: 6 EC shards are arriving on osd.896. For 5 of those PGs a
second shard is moving too, and CRUSH placed its destination on the same host
as the acting OSD of the shard heading for 896. Pinning only the shard into
896 would therefore leave two shards on one host, which Ceph rejects. E.g.
19.7e9: shard 0 would go back to osd.627 (host32), but shard 9 is arriving on
osd.149, also host32; pinning shard 9 back to osd.497 as well resolves it.
These 5 companion pins are unconditional (see "Companion pins" in the module
docstring): the tool needs no flag to find them. Those same second shards also
happen to go to OSDs at 90-92% (osd.149 would reach 91.6% with its shard), so
with --pin-blockers they are additionally reported as blockers ("blocks shard
N") rather than plain companions. 19.92e has none of the host problem, so
without --pin-blockers it gets only its one pin into 896; with the flag, its
shard 6 (osd.99 -> osd.337, 92.5% used) is found as a pure blocker that keeps
its shard 4 (osd.231 -> osd.896) from starting.

Expected output for osd 896 --pgremapper --pin-blockers: 6 pins into osd.896
and 7 blockers (all of them NOTE "blocks shard N"):

  19.7e9 896 627
  19.7e9 149 497
  19.92e 896 231
  19.92e 337 99
  19.94c 896 522
  19.94c 716 266
  19.14cd 314 347
  19.14cd 232 337
  19.14cd 896 614
  19.1b16 896 591
  19.1b16 524 578
  19.1fed 896 5
  19.1fed 12 207

19.14cd shard 1 (314 -> 347, a move between two OSDs of the same host) does not
clash with anything, but osd.314 is at 91.5% so it is a blocker too (only found
with --pin-blockers; without it, 19.14cd's output is just "232 337" and
"896 614"). 19.1b16 is the one PG here that is already backfilling (~81%); the
others are at 0% in backfill_toofull.

Without --pin-blockers (the default), the same command drops the 2 pure-blocker
lines (19.92e's "337 99" and 19.14cd's "314 347"), leaving 11 lines for 5 PGs;
19.92e then has just its one pin ("896 231") and no second line.

--import-mappings prints the same 13 pairs (11 without --pin-blockers) as a
JSON array (one entry per pair, in this order). Applying them with separate
'pgremapper remap' runs is what fails on this cluster, so --pgremapper warns
that 6 PGs need more than one line with --pin-blockers (5 without it, since
19.92e then has only one line).

Chained pairs ("Chained pairs" in the module docstring): 13 of the 688 remapped
PGs have an OSD that CRUSH wants in one shard slot while it currently holds
another shard of the PG, e.g. 19.1299: shard 1 is going to osd.579 while
osd.579 still holds shard 8, which is going to osd.825. Pinning them gives the
chain 891->579, 579->825, which Ceph only applies in the order 579->825 first.
Dry runs of pgremapper 1.0.0 on exactly this PG: in the valid order
import-mappings panics ("conflicting mapping 579->825 found when trying to map
891->579"), and in the order 891->579, 579->825 it plans the single pair
891->825, a different mapping. So the tool leaves such PGs out of
--import-mappings/--pgremapper and prints 'ceph osd pg-upmap-items 19.1299 579 825
891 579' on stderr. Replay with osd 891 (or 274, 883, 884, ... for the others);
none of the 13 is a ring.

Other OSDs worth replaying:
  osd 74   2 pins, nothing else (19.16fc shard 6, 19.1eb3 shard 8)
  osd 682  1 pin (19.16fc shard 7 from osd.231) plus 19.16fc shard 6 (osd.74,
           same host as osd.231) as its companion -- unconditional, so present
           either way; with --pin-blockers it is also at 91.6% with its shard,
           so it is labeled a blocker ("blocks shard 7") instead of a plain
           companion, but it is pinned regardless of the flag
  osd 231  no backfills into it

tests/pg-osd/test_stop_backfills_into_osd.py replays this snapshot with
--load-state, so its assertions do not depend on the live cluster. See also
stop-backfills-into-osd-ceph2-osd896-blocker-in-same-pg/, a later capture.
