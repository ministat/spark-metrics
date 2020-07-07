#!/bin/bash
WORKDIR=`dirname $0`
. ${WORKDIR}/utils.sh
. ${WORKDIR}/env.sh

function generate_prometheus_metrics() {
   local allMetrics="$1"
   local queue=$2
   local m
   local k v
   for m in $(echo "$allMetrics"| tr "," "\n")
   do
      k=$(echo $m|awk -F = '{print $1}')
      v=$(echo $m|awk -F = '{print $2}')
      echo $k{queue=\"$queue\"} $v
   done
}

for i in $(list_all_apps|awk '{print($7"|"$11)}')
do
   queue=$(echo "$i"|awk -F \| '{print $1}')
   appHttpHost=$(echo "$i"|awk -F \| '{print $2}')
   driverHost=$(echo $appHttpHost|cut -d '/' -f3|cut -d ':' -f1)
   gcCount=$(list_driver_full_gc_count $driverHost)
   echo "spark_driver_fullgc{queue=\"$queue\"} $gcCount" 
done

