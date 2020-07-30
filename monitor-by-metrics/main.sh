#!/bin/bash
WORKDIR=`dirname $0`
. ${WORKDIR}/utils.sh

list_all_apps

queue=hdmi-default
a=$(find_queue_app $queue)
echo $a

baseUrl=$(get_base_url $a)
echo $baseUrl

#get_jobs $a
#get_executors $a
#get_all_stages_active $a
