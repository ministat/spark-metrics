#!/bin/bash
WORKDIR=`dirname $0`
. ${WORKDIR}/utils.sh
. ${WORKDIR}/env.sh

declare -A g_queue

function generate_prometheus_metrics() {
   local allMetrics="$1"
   local queue=$2
   local m
   local k v
   for m in $(echo "$allMetrics"| tr "," "\n")
   do
      k=$(echo $m|awk -F = '{print $1}')
      v=$(echo $m|awk -F = '{print $2}')
      if [ "$v" != "" ]; then
         echo $k{queue=\"$queue\"} $v
      else
         logger -t SPARK-METRICS "Fail to get valid metrics for queue $queue"
      fi
   done
}

for i in $(list_all_apps | awk '{print($1":"$7)}')
do
   appId=$(echo "$i"|awk -F : '{print $1}')
   queue=$(echo "$i"|awk -F : '{print $2}')
   if [ "${g_queue[${queue}]}" == "" ];then
      g_queue+=([${queue}]="filled")
   else
      continue
   fi
   stageMetrics=$(python3.5 spark/spark.py jobStage --appId $appId --allStatus 2>/dev/null)
   taskMetrics=$(python3.5 spark/spark.py jobTask --appId $appId --allStatus 2>/dev/null)
   stageDetailsMetrics=$(python3.5 spark/spark.py stage --appId $appId --allStatus 2>/dev/null)
   executorMetrics=$(python3.5 spark/spark.py executor --appId $appId --allStatus 2>/dev/null)
   generate_prometheus_metrics $stageMetrics $queue
   generate_prometheus_metrics $taskMetrics $queue
   generate_prometheus_metrics $stageDetailsMetrics $queue
   generate_prometheus_metrics $executorMetrics $queue
done

