#!/bin/bash
set -x
WORKDIR=`dirname $0`

function send_to_spark_metrics_prometheus() {
  local prometheus_http_url=$1
  local data=$(sh $WORKDIR/collect_metrics.sh)
cat << EOF  | curl -s -w "%{http_code}\n" --data-binary @- ${prometheus_http_url}/metrics/job/pushgateway/instance/pushgateway
$data
EOF
}

function send_to_spark_log_prometheus() {
  local prometheus_http_url=$1
  local data=$(sh $WORKDIR/collect_full_gc_metrics.sh)
cat << EOF  | curl -s -w "%{http_code}\n" --data-binary @- ${prometheus_http_url}/metrics/job/pushgateway/instance/pushgateway
$data
EOF
}

if [ $# -ne 1 ];then
   echo "$0 <prometheus_url>"
   exit 1
fi

url=$1
send_to_spark_metrics_prometheus $url
send_to_spark_log_prometheus $url
