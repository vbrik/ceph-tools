#!/bin/bash
# SPDX-License-Identifier: MIT

osd=$1

echo From:
ceph osd dump -f json | jq -c ".pg_upmap_items[] | select(.mappings[].from == $osd)"

echo 
echo To:
ceph osd dump -f json | jq -c ".pg_upmap_items[] | select(.mappings[].to == $osd)"

