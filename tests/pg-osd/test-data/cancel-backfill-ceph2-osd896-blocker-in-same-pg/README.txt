Fixture: real, live-cluster snapshot of the case blockers exist for (see
'cancel-backfill --help' and cancel_backfill.find_blockers): osd.896 has just
ONE backfill arriving, the one the operator wants (19.92e shard 4, from osd.231),
and it is still backfill_toofull although osd.896 is at 81.1%.

Captured: 2026-09-21, later the same day as the sibling fixture
cancel-backfill-ceph2-osd896-host-clash-companions, once the operator
had cancelled the other backfills into osd.896 and expected 19.92e to start.
Made with

  backfillctl cancel-backfill --save-state <this directory> --osd 896

(same six files as the sibling; osd_dump.json is cut down to the erasure code
profiles and the full ratios, backfillfull_ratio being 0.91 here.)

RENAMED: this used --save-state's own capture, so the PG listing was saved
as pg_ls_remapped.json (what 'ceph pg ls remapped' returns). It has since
been renamed to pg_dump_pgs.json, content unchanged, to match
'backfillctl save-state''s later unified snapshot format, which every
subcommand's --load-state now reads that PG data from (filtering it
client-side for the flag it cares about).

Why the PG is stuck: backfill_toofull is a property of the PG. 19.92e has a
second shard moving, shard 6 from osd.99 to osd.337, and osd.337 is at 92.5%,
over backfillfull_ratio, so it refuses the reservation and the whole PG waits,
including shard 4 going to the nearly empty osd.896.

Expected pairs for osd 896 --pin-blockers (as '<pgid> <up osd> <acting osd>'):

  19.92e 896 231
  19.92e 337 99

The first pin stops the backfill into osd.896, the second -- NOTE "blocks shard
4: target osd.337 would be at 93.4%, over backfillfull" in the table -- stops
the one into a different OSD that is holding the PG. An operator who wants
231 -> 896 to proceed drops the FIRST entry and keeps the second; the pin is
valid on its own (osd.99 on host17, no other shard of the PG there), and with
it 19.92e has only the wanted backfill left. In --pgremapper-mappings form the
same is a two-entry JSON array.

Without --pin-blockers (the default), the second line is never found: the
output is just "19.92e 896 231" and a NOTE that a shard of the PG blocking it
may not have been pinned. That is the whole point of this fixture -- see
cancel_backfill.find_blockers.

tests/pg-osd/test_cancel_backfill.py replays this snapshot with --load-state.
