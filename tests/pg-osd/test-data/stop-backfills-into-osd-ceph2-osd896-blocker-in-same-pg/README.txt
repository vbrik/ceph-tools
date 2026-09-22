Fixture: real, live-cluster snapshot of the case blockers exist for (see
"Blockers" in stop-backfills-into-osd.py's module docstring): osd.896 has just
ONE backfill arriving, the one the operator wants (19.92e shard 4, from osd.231),
and it is still backfill_toofull although osd.896 is at 81.1%.

Captured: 2026-09-21, later the same day as the sibling fixture
stop-backfills-into-osd-ceph2-osd896-host-clash-companions, once the operator
had cancelled the other backfills into osd.896 and expected 19.92e to start.
Made with

  stop-backfills-into-osd.py --save-state <this directory> 896

(same six files as the sibling; osd_dump.json is cut down to the erasure code
profiles and the full ratios, backfillfull_ratio being 0.91 here.)

Why the PG is stuck: backfill_toofull is a property of the PG. 19.92e has a
second shard moving, shard 6 from osd.99 to osd.337, and osd.337 is at 92.5%,
over backfillfull_ratio, so it refuses the reservation and the whole PG waits,
including shard 4 going to the nearly empty osd.896.

Expected output for osd 896 (--pgremapper --pin-blockers):

  19.92e 896 231
  19.92e 337 99

The first line stops the backfill into osd.896, the second -- NOTE "blocks shard
4: target osd.337 would be at 93.4%, over backfillfull" -- stops the one into a
different OSD that is holding the PG. An operator who wants 231 -> 896 to
proceed drops the FIRST line and keeps the second; the pin is valid on its own
(osd.99 on host17, no other shard of the PG there), and with it 19.92e has only
the wanted backfill left. In --import-mappings form the same is a
two-entry array.

Without --pin-blockers (the default), the second line is never found: the
output is just "19.92e 896 231" and a NOTE that a shard of the PG blocking it
may not have been pinned. That is the whole point of this fixture -- see
"Blockers" in the module docstring.

tests/pg-osd/test_stop_backfills_into_osd.py replays this snapshot with --load-state.
