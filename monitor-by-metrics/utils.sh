#!/bin/bash

DEV_MACHINE=hermesdevour002-700165.stratus.rno.ebay.com
RES_MGR=hermes-rno-rm-2.vip.hadoop.ebay.com:50030

function list_all_apps() {
  ssh $DEV_MACHINE "su - b_carmel -c '/apache/hadoop/bin/yarn application -list -appTypes SPARK 2>/dev/null| grep Thrift | grep RUNNING | grep -v regression|grep -v Server[0-9].*'"
}

function list_all_killed_apps() {
  ssh $DEV_MACHINE "su - b_carmel -c '/apache/hadoop/bin/yarn application -appStates KILLED -appTypes SPARK -list| grep Thrift |grep -v regression'"
}

function list_driver_full_gc_count() {
  local driverHost=$1
  local appMasterPid=$(ssh $DEV_MACHINE "su - b_carmel -c 'ssh $driverHost /apache/java/bin/jps -m|grep Application'"|awk '{print $1}')
  #echo $appMasterPid
  local gcTrackDir=$(ssh $DEV_MACHINE "su - b_carmel -c 'ssh $driverHost ps aux|grep $appMasterPid'"|awk -F "spark.yarn.app.container.log.dir=" '{print $2}'|awk '{print $1}')
cat << EOF > find_full_gc_count.sh
ssh $driverHost grep "Full\ GC" $gcTrackDir/stdout|wc -l
EOF
  scp find_full_gc_count.sh $DEV_MACHINE:/home/b_carmel
  ssh $DEV_MACHINE "chown b_carmel:b_carmel /home/b_carmel/find_full_gc_count.sh"
  local gcCount=$(ssh $DEV_MACHINE "su - b_carmel -c 'sh /home/b_carmel/find_full_gc_count.sh'")
  #local gcCount=$(ssh $DEV_MACHINE "su - b_carmel -c 'ssh $driverHost grep '\"'Full\ GC'\"' $gcTrackDir/stdout'"|wc -c)
  echo $gcCount
}

function find_driver_hostname() {
  local queue=$1
  local allApps=$(list_all_apps)
  local appHttpHost=$(echo "$allApps"|grep $queue|awk '{print $11}')
  local driverHost=$(echo $appHttpHost|cut -d '/' -f3|cut -d ':' -f1)
  echo $driverHost
}

function find_queue_app() {
  local queue=$1
  local allApps=$(list_all_apps)
  local app=$(echo "$allApps"|grep $queue|awk '{print $1}')
  echo $app
}

function get_base_url() {
  local appId=$1
  echo "https://${RES_MGR}/proxy/${appId}/api/v1/applications/${appId}"
}

function get_all_stages_active() {
  local appId=$1
  local baseUrl=$(get_base_url $appId)
  curl -iLk -X GET ${baseUrl}/stages?anonymous=true&status=active
}

function get_executors() {
  local appId=$1
  local baseUrl=$(get_base_url $appId)
  curl -iLk -X GET ${baseUrl}/executors?anonymous=true
}

function get_allexecutors() {
  local appId=$1
  local baseUrl=$(get_base_url $appId)
  curl -iLk -X GET ${baseUrl}/allexecutors?anonymous=true
}

function get_jobs() {
  local appId=$1
  local baseUrl=$(get_base_url $appId)
  curl -iLk -X GET ${baseUrl}/jobs?anonymous=true
}

function get_stage_task_summary() {
  local appId=$1
  local stageId=$2
  local baseUrl=$(get_base_url $appId)
  curl -iLk -X GET ${baseUrl}/stages/$stageId/0/taskSummary?anonymous=true
}

function get_stage_task_list() {
  local appId=$1
  local stageId=$2
  local baseUrl=$(get_base_url $appId)
  curl -iLk -X GET ${baseUrl}/stages/$stageId/0/taskList?anonymous=true
}
