#!/bin/bash
WORKDIR=`dirname $0`
. ${WORKDIR}/utils.sh
source ${WORKDIR}/env.sh

all_queues=$(list_all_apps|awk '{print $1 ":" $7}')
for qinfo in $all_queues
do
  a=$(echo $qinfo|awk -F : '{print $1}')
  q=$(echo $qinfo|awk -F : '{print $2}')
  echo $q " " $a
  python3.5 spark/main.py stageAnalysis --appId=$a --queue=$q --status active
done
