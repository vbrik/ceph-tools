#!/bin/bash

#ceph mgr module enable osd_perf_query
qid=$(ceph osd perf query add client_id)
sleep 10
ceph osd perf counters get $qid
ceph osd perf query remove $qid
