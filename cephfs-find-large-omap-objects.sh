#!/bin/bash
# SPDX-License-Identifier: MIT

ceph pg dump --format json 2>/dev/null \
    | jq '.pg_map.pg_stats[]
        | select(.stat_sum.num_large_omap_objects > 0)
        | {pgid, acting_primary, n_large_omap: .stat_sum.num_large_omap_objects}'
