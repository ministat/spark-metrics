#!/bin/bash

DEV_MACHINE=hermesdevour002-700165.stratus.rno.ebay.com
RES_MGR=hermes-rno-rm-2.vip.hadoop.ebay.com:50030

function list_all_apps() {
  ssh $DEV_MACHINE "su - b_carmel -c '/apache/hadoop/bin/yarn application -list -appTypes SPARK 2>/dev/null| grep Thrift | grep RUNNING | grep -v regression|grep -v Server[0-9].*'"
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
