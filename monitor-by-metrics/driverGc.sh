#!/bin/bash
WORKDIR=`dirname $0`
. ${WORKDIR}/utils.sh

list_all_apps

a=$(find_queue_app hdmi-default)
echo $a

baseUrl=$(get_base_url $a)
echo $baseUrl

appHost=$(find_driver_hostname hdmi-default)
gcCount=$(list_driver_full_gc_count $appHost)
echo $gcCount
#get_executors $a
#get_all_stages_active $a
