#!/bin/bash
#set -x
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
      echo $k{queue=\"$queue\"} $v
   done
}

function list_driver_connection_count() {
  local driverHost=$1
  local appId=$2
  local appMasterPid=$(ssh $DEV_MACHINE "su - b_carmel -c 'ssh $driverHost /apache/java/bin/jps -m|grep Application|grep $appId'"|awk '{print $1}')
  local tcp10000Conn=$(ssh $DEV_MACHINE "su - b_carmel -c 'ssh $driverHost netstat -anpt 2>/dev/null|grep $appMasterPid/java'"|grep 10000|grep EST|wc -l)
  echo $tcp10000Conn
}

for i in $(list_all_apps|awk '{print($1"|"$7"|"$11)}')
do
   appId=$(echo "$i"|awk -F \| '{print $1}')
   queue=$(echo "$i"|awk -F \| '{print $2}')
   # avoid collecting metrics for the same queue
   if [ "${g_queue[${queue}]}" == "" ];then
      g_queue+=([${queue}]="filled")
   else
      continue
   fi

   appHttpHost=$(echo "$i"|awk -F \| '{print $3}')
   driverHost=$(echo $appHttpHost|cut -d '/' -f3|cut -d ':' -f1)
   tcp10000Count=$(list_driver_connection_count $driverHost $appId)
   echo "spark_driver_tcp_connection{queue=\"$queue\"} $tcp10000Count" 
done
