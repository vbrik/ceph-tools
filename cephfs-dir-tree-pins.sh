#!/bin/bash
# SPDX-License-Identifier: MIT

num_mds=$(ceph fs status | grep active | wc -l)
subtrees=$(ceph tell mds.0 get subtrees)
for ((rank = 0; rank < num_mds; rank++)); do
    echo Rank $rank:
    jq ".[] | select(.export_pin==$rank) | .dir.path" <<< "$subtrees"
    echo
done
