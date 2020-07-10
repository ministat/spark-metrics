import argparse
import json
import logging
import requests
import sys
import time
from logging import handlers
from six import iteritems

RESOUCE_MGR_URL="https://hermes-rno-rm-2.vip.hadoop.ebay.com:50030/proxy"

TASK_STAT_MAP={
    'active':'numActiveTasks',
    'skipped':'numSkippedTasks',
    'failed':'numFailedTasks',
    'completed':'numCompletedTasks',
    'all':'numTasks'
}
JOB_TASK_STAT_MAP={
    'spark_job_numActiveTasks':'numActiveTasks',
    'spark_job_numSkippedTasks':'numSkippedTasks',
    'spark_job_numFailedTasks':'numFailedTasks',
    'spark_job_numCompletedTasks':'numCompletedTasks',
    'spark_job_numTasks':'numTasks'
}

STAGE_STAT_MAP={
    'active':'numActiveStages',
    'skipped':'numSkippedStages',
    'failed':'numFailedStages',
    'completed':'numCompletedStages'
}

JOB_STAGE_STAT_MAP={
    'spark_job_numActiveStages':'numActiveStages',
    'spark_job_numSkippedStages':'numSkippedStages',
    'spark_job_numFailedStages':'numFailedStages',
    'spark_job_numCompletedStages':'numCompletedStages'
}

STAGE_STAT_DETAIL_MAP={
    'spark_stage_executorRunTime':'executorRunTime',
    'spark_stage_executorCpuTime':'executorCpuTime',
    'spark_stage_inputBytes':'inputBytes',
    'spark_stage_outputBytes':'outputBytes',
    'spark_stage_inputRecords':'inputRecords',
    'spark_stage_outputRecords':'outputRecords',
    'spark_stage_shuffleReadBytes':'shuffleReadBytes',
    'spark_stage_shuffleReadRecords':'shuffleReadRecords',
    'spark_stage_shuffleWriteBytes':'shuffleWriteBytes',
    'spark_stage_shuffleWriteRecords':'shuffleWriteRecords',
    'spark_stage_memoryBytesSpilled':'memoryBytesSpilled',
    'spark_stage_diskBytesSpilled':'diskBytesSpilled'
}

def logSetup():
   handler = handlers.RotatingFileHandler('spark-metrics.log', maxBytes=10**7, backupCount=10)
   log_format = logging.Formatter('%(asctime)s %(levelname)s [%(process)d]: %(message)s', '%b %d %H:%M:%S')
   log_format.converter = time.gmtime
   handler.setFormatter(log_format)
   logger = logging.getLogger()
   logger.addHandler(handler)
   logger.setLevel(logging.INFO)

def getBaseUrl(appId):
   return "{res_url}/{app}/api/v1/applications/{app}".format(res_url=RESOUCE_MGR_URL, app=appId)

def getAllMetrics(jData, statMap):
   m={}
   for k,v in statMap.items():
       for t in jData:
          if k in m:
            m[k]+=t[v] if v in t else 0
          else:
            m[k]=t[v] if v in t else 0
   return m

def getEmptyAllMetrics(statMap):
   m={}
   for k,v in statMap.items():
       m[k]=0
   return m

def getMetrics(jobJson, statArgs, statMap):
   metricsStat = TASK_STAT_MAP[statArgs]
   task = 0
   for t in jobJson:
       task += t[metricsStat]
   return task

def metricsInternal(appId, jobsOrStages, **kwargs):
   data = ""
   url = getBaseUrl(appId)
   fullUrl = url + "/" + jobsOrStages
   queryParam = {'anonymous': 'true'}
   if kwargs:
      for k,v in iteritems(kwargs):
          queryParam[k] = v
   response = requests.get(fullUrl, verify=False, params=queryParam, timeout=5)
   if response != None and response.status_code == 200:
      data = response.json()
   else:
      print("Fail to get REST response from {u}".format(u=fullUrl), file=sys.stderr)
   return data

def printMetricsMap(m):
   res=""
   for k,v in m.items():
       if not res:
          res+="{k1}={v1}".format(k1=k,v1=v)
       else:
          res+=",{k1}={v1}".format(k1=k,v1=v)
   print(res)

def jobTaskMetrics(args):
   result = 0
   data = metricsInternal(args.appId, "jobs", status='running')
   if data:
      if args.debug:
         print(data)
      if args.allStatus:
         m = getAllMetrics(data, JOB_TASK_STAT_MAP)
         printMetricsMap(m)
      else:
         result = getMetrics(data, args.taskStatus, TASK_STAT_MAP)
         print(result)
   else:
      if args.allStatus:
         m = getEmptyAllMetrics(JOB_TASK_STAT_MAP)
         printMetricsMap(m)

def jobStageMetrics(args):
   result = 0
   data = metricsInternal(args.appId, "jobs", status='running')
   if data:
      if args.debug:
         print(data)
      if args.allStatus:
         m = getAllMetrics(data, JOB_STAGE_STAT_MAP)
         printMetricsMap(m)
      else:
         result = getMetrics(data, args.stageStatus, STAGE_STAT_MAP) 
         print(result)
   else:
      if args.allStatus:
         m = getEmptyAllMetrics(JOB_STAGE_STAT_MAP)
         printMetricsMap(m)

def getAllExecutorMetrics(data):
    m={}
    exeCnt = 0
    usedOnHeapGt50 = 0
    usedOffHeapGt50 = 0
    totalActTask = 0
    totalMaxTask = 0
    totalGCTime = 0
    dead = 0
    removeReasons = 0
    explicitTerm = 0
    for d in data:
        if not d['isActive']:
           dead+=1
           removeReasons+=1
           removeReason = d['removeReason']
           if removeReason.endswith("exited from explicit termination request."):
               explicitTerm += 1
        else:
           exeCnt += 1
        if d['id'] != "driver":
           totalActTask += d['activeTasks']
           totalMaxTask += d['maxTasks']
           if 'memoryMetrics' not in d:
              logging.error("No memory metrics: {d}".format(d=d))
           else:
              usedOnHeap = float(d['memoryMetrics']['usedOnHeapStorageMemory'])
              usedOffHeap = float(d['memoryMetrics']["usedOffHeapStorageMemory"])
              totalOnHeap = float(d['memoryMetrics']["totalOnHeapStorageMemory"])
              totalOffHeap = float(d['memoryMetrics']["totalOffHeapStorageMemory"])
              if usedOnHeap / totalOnHeap > 0.5:
                 usedOnHeapGt50 += 1
              if usedOffHeap / totalOffHeap > 0.5:
                 usedOffHeapGt50 += 1
           totalGCTime += d['totalGCTime']
        else:
           m['spark_driver_memusage_percent'] = int(float(d['memoryUsed'])/float(d['maxMemory'])*100)
           m['spark_driver_totalGCTime'] = d['totalGCTime']
    m['spark_executor_deadCnt'] = dead
    m['spark_executor_count'] = exeCnt
    m['spark_executor_explicitTerminationCnt'] = explicitTerm
    m['spark_executor_usedOnHeap_gt50percent'] = usedOnHeapGt50
    m['spark_executor_usedOffHeap_gt50percent'] = usedOffHeapGt50
    m['spark_executor_usage_percent'] = int(float(totalActTask)/float(totalMaxTask)*100)
    m['spark_executor_totalGCTime'] = totalGCTime
    if m['spark_executor_usage_percent'] > 100:
        logging.error(data)
    return m

def lostExecutors(args):
    data = metricsInternal(args.appId, "executors")
    if data:
       if args.debug:
          print(data)
    m = getDeadExecutorMetrics(data)
    printMetricsMap(m)

def executorMetrics(args):
    data = metricsInternal(args.appId, "allexecutors")
    if data:
       if args.allStatus:
          m = getAllExecutorMetrics(data)
          printMetricsMap(m)
       if args.debug:
          print(data)
    else:
       if args.allStatus:
          m = getAllExecutorMetrics([])
          printMetricsMap(m)

def jobMetrics(args):
    data = metricsInternal(args.appId, "jobs", status=args.status)
    if data:
      for d in data:
          print("submissionTime: {st}, stages: {stages}, activeStages: {acts}, failedStages: {fs}, completedStages: {cs}, skippedStages: {sks}, task: {t}, activeTask: {at}, completedTask: {ct}, failedTask: {ft}, skippedTask: {skt}"
                .format(st=d['submissionTime'],
                        stages=d['stageIds'],
                        acts=d['numActiveStages'],
                        fs=d['numFailedStages'],
                        cs=d['numCompletedStages'],
                        sks=d['numSkippedStages'],
                        t=d['numTasks'],
                        at=d['numActiveTasks'],
                        ct=d['numCompletedTasks'],
                        ft=d['numFailedTasks'],
                        skt=d['numSkippedTasks']))
      if args.debug:
         print(data)

def stageMetrics(args):
   data = metricsInternal(args.appId, "stages", status=args.status)
   if data:
      if args.simplePrint:
          for d in data:
              print("submissionTime: {st}, firstTaskLaunchedTime: {flt}, tasks: {task}, activeTask: {at}, inputBytes: {ib}, outputBytes: {ob}, shuffleReadBytes: {srb}, shuffleWriteBytes: {swb}, executorRuntime: {ert}, executorCpuTime: {ect}"
                   .format(st=d['submissionTime'] if 'submissiontTime' in d else '',
                        flt=d['firstTaskLaunchedTime'] if 'firstTaskLaunchedTime' in d else '',
                        task=d['numTasks'],
                        at=d['numActiveTasks'],
                        ib=d['inputBytes'],
                        ob=d['outputBytes'],
                        srb=d['shuffleReadBytes'],
                        swb=d['shuffleWriteBytes'],
                        ert=d['executorRuntime'] if 'executorRuntime' in d else '',
                        ect=d['executorCpuTime']))
      if args.allStatus:
         m = getAllMetrics(data, STAGE_STAT_DETAIL_MAP)
         printMetricsMap(m)
      if args.debug:
         print(data)
   else:
      if args.allStatus:
         m = getEmptyAllMetrics(STAGE_STAT_DETAIL_MAP)
         printMetricsMap(m)

def commonArgs(parser):
   parser.add_argument("--appId", help="Specify the application Id", required=True)
   parser.add_argument("--debug", action="store_true", help="Enable debug, default is false")

if __name__=="__main__":
   logSetup()
   parser = argparse.ArgumentParser('Spark status monitor')

   subparser = parser.add_subparsers(dest="command")

   jobParser = subparser.add_parser('job', help='List job related metrics')
   jobParser.add_argument('--status', choices=['running','succeeded','failed','unknown'], help='Specify the job status you want to filter, default is failed', default='failed')
   commonArgs(jobParser)
   jobParser.set_defaults(func=jobMetrics)

   stageParser = subparser.add_parser('stage', help='List stage releated metrics')
   stageParser.add_argument('--status', choices=['active','complete','pending','failed'])
   stageParser.add_argument('--allStatus', action="store_true", help="Collect all status information, default is false", default=False)
   stageParser.add_argument('--simplePrint', action="store_true", help="Collect all status information, default is false", default=False)
   commonArgs(stageParser)
   stageParser.set_defaults(func=stageMetrics)

   jobTaskParser = subparser.add_parser('jobTask', help='Get statistic information for job tasks')
   jobTaskParser.add_argument('--taskStatus', choices=['active','skipped','failed','completed','all'], help='Specify the running jobs tasks status you want to query, default is active', default='active')
   jobTaskParser.add_argument('--allStatus', action="store_true", help="Collect all status information", default=False)
   commonArgs(jobTaskParser)
   jobTaskParser.set_defaults(func=jobTaskMetrics)

   jobStageParser = subparser.add_parser('jobStage', help='Get statistic information for job stages')
   jobStageParser.add_argument('--stageStatus', choices=['active','skipped','failed','completed'], help='Specify the running jobs stage status you want to query, default is active', default='active')
   jobStageParser.add_argument('--allStatus', action="store_true", help="Collect all status information", default=False)
   jobStageParser.set_defaults(func=jobStageMetrics)
   commonArgs(jobStageParser)

   executorParser = subparser.add_parser('executor', help='Get statistic information for executors')
   executorParser.add_argument('--allStatus', action="store_true", help="Collect all status related to executors", default=False)
   executorParser.set_defaults(func=executorMetrics)
   commonArgs(executorParser)

   
   if len(sys.argv) <= 1:
      sys.argv.append('--help')

   options = parser.parse_args()
   options.func(options)
