import argparse
import json
import logging
import math
import numpy as np
import operator
import os
import pandas as pd
import requests
import sys
import time

import param
from datetime import datetime, timedelta
from logging import handlers
from pandas.io.json import json_normalize
from six import iteritems

class Base:
   """
     cluster:
        hermes: 0
        apollo: 1
   """
   HERMES = 0
   APOLLO = 1
   def __init__(self, baseUrl, cluster):
      self.resourceUrl = baseUrl
      self.logSetup()
      self.pandasSetting()
      self.cluster = cluster
      self.SKEW_THRESHOLD = 100
      self.TIME_ISSUE_PERCENT_THRESHOLD = 0.3
      self.STAGE_NUMTASKS_THRESHOLD = 200
      self.HERMES = 0
      self.APOLLO = 1
      self.CPU_BOUND = 0
      self.IO_BOUND = 1
      self.MEM_BOUND = 2
      self.LONG_DELAY = 3
      self.MIX_BOUND = 10
      self.STAGES_INFO_FILE = "stages.csv"
      self.STAGES_GENERAL_INFO_FILE = "stages_info.csv"
      self.STAGES_SKEW_FILE = "stages_skew.csv"
      self.STAGES_CPU_RATE_FILE = "task_cpurate.csv"
      self.STAGES_IO_RATE_FILE = "task_iorate.csv"
      self.STAGES_TOTAL_CPUTIME_FILE = "total_cpu_time.csv"
      self.STAGES_TOTAL_NONE_CPUTIME_FILE = "total_none_cpu_time.csv"
      self.STAGES_MEM_FILE = "total_mem_issue.csv"
      self.STAGES_DATA_SKEW_FILE_POSTFIX = "data_skew.csv"
      self.SQLS_FILE = "sqls.csv"
      self.TASK_SUMMARY_KEYS =  ['diskBytesSpilled', 'executorBlockTime', 'executorCpuTime',
       'executorDeserializeCpuTime', 'executorDeserializeTime',
       'executorRunTime', 'executorWaitTime', 'gettingResultTime',
       'inputMetrics.bytesRead', 'inputMetrics.recordsRead', 'jvmGcTime',
       'launchDelay', 'memoryBytesSpilled', 'outputMetrics.bytesWritten',
       'outputMetrics.recordsWritten', 'peakExecutionMemory',
       'resultSerializationTime', 'resultSize', 'schedulerDelay',
       'shuffleReadMetrics.fetchWaitTime',
       'shuffleReadMetrics.localBlocksFetched', 'shuffleReadMetrics.readBytes',
       'shuffleReadMetrics.readRecords',
       'shuffleReadMetrics.remoteBlocksFetched',
       'shuffleReadMetrics.remoteBytesRead',
       'shuffleReadMetrics.remoteBytesReadToDisk',
       'shuffleReadMetrics.totalBlocksFetched',
       'shuffleWriteMetrics.writeBytes', 'shuffleWriteMetrics.writeRecords',
       'shuffleWriteMetrics.writeTime']
      self.TASK_SUMMARY_TO_LIST_MAP = {
       "executorDeserializeTime":"taskMetrics.executorDeserializeTime",
       "executorDeserializeCpuTime":"taskMetrics.executorDeserializeCpuTime",
       "executorRunTime":"taskMetrics.executorRunTime",
       "executorCpuTime":"taskMetrics.executorCpuTime",
       "executorBlockTime":"taskMetrics.executorBlockTime",
       "executorWaitTime":"taskMetrics.executorWaitTime",
       "jvmGcTime":"taskMetrics.jvmGcTime",
       "resultSerializationTime":"taskMetrics.resultSerializationTime",
       "shuffleReadMetrics.fetchWaitTime":"taskMetrics.shuffleReadMetrics.fetchWaitTime",
       "shuffleWriteMetrics.writeTime":"taskMetrics.shuffleWriteMetrics.writeTime",
       "shuffleWriteMetrics.writeBytes":"taskMetrics.shuffleWriteMetrics.bytesWritten",
       #"shuffleReadMetrics.readBytes":"taskMetrics.shuffleReadMetrics.readBytes", // no matching item
       "shuffleReadMetrics.remoteBytesRead":"taskMetrics.shuffleReadMetrics.remoteBytesRead",
       "shuffleReadMetrics.remoteBytesReadToDisk":"taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk",
     }

   def _get_utcnow_datatime(self):
      return datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

   def logSetup(self):
      handler = handlers.RotatingFileHandler('spark-metrics.log', maxBytes=10**7, backupCount=10)
      log_format = logging.Formatter('%(asctime)s %(levelname)s [%(process)d]: %(message)s', '%b %d %H:%M:%S')
      log_format.converter = time.gmtime
      handler.setFormatter(log_format)
      logger = logging.getLogger()
      logger.addHandler(handler)
      logger.setLevel(logging.INFO)

   def pandasSetting(self):
      pd.set_option('display.max_rows', None)
      pd.set_option('display.max_colwidth', 64)

   def _restApiCall(self, urlPath, **kwargs):
      fullUrl = self.resourceUrl
      if len(urlPath) > 0:
         fullUrl = self.resourceUrl + "/" + urlPath
      queryParam = {}
      if kwargs:
          for k,v in iteritems(kwargs):
             queryParam[k] = v
      data = ""
      response = requests.get(fullUrl, verify=False, params=queryParam, timeout=10)
      if response != None and response.status_code == 200:
          data = response.json()
      else:
          print("Fail to get REST response from {u}".format(u=fullUrl), file=sys.stderr)
      return data

   def getTasks(self, args):
      pass

   def getStages(self, args):
      pass

   def getApps(self, args):
      pass

   def getSqls(self, args):
      pass

   def getJobs(self, args):
      pass

   def getJob(self, args):
      pass

   def getExecutors(self, args):
      pass

   def _getSqls(self, relativeUrl, args):
      data = self._restApiCall(relativeUrl, anonymous='true')
      if args.debug:
         print(data)
      if len(data) > 0:
         df = json_normalize(data=data)
         print(df.columns)
         self._save_file(args, df, filename=self.SQLS_FILE)

   def _getJobs(self, relativeUrl, args):
      if args.status == 'all':
         data = self._restApiCall(relativeUrl, anonymous='true')
      else:
         data = self._restApiCall(relativeUrl, anonymous='true', status=args.status)
      if args.debug:
         print(data)
      if len(data) > 0:
         normDf = json_normalize(data=data)
         df = normDf.rename(columns={"killedTasksSummary.Finish but did not commit due to another attempt succeeded":"killedButNoCommit",
                                     "killedTasksSummary.another attempt succeeded":"killedAnother"})
         print(df)

   def _getJob(self, relativeUrl, args):
      data = self._restApiCall(relativeUrl, anonymous='true')
      if args.debug:
         print(data)
      if len(data) > 0:
         normDf = json_normalize(data=data)
         df = normDf.rename(columns={'killedTasksSummary.another attempt succeeded':'killedAnother'})
         print(df.columns)
         print(df)

   ##
   ## check whether skew occurs for any of time relative metrics
   ##
   def _isHigherThanThreshold(self, skewValue):
      if int(skewValue) > self.SKEW_THRESHOLD:
         return True
      return False

   ##
   ## determine some genearl issues
   ##
   def _hasTimeRelatedIssue(self, taskSumDf, targetMetrics):
      # ignore executorRunTime itself since we use it as baseline
      if targetMetrics == 'executorRunTime':
         return False
      maxExecutorRuntime = taskSumDf['executorRunTime'].values[0][4]
      tgtMetrics = taskSumDf[targetMetrics].values[0][4]
      if maxExecutorRuntime > 0 and ((tgtMetrics / maxExecutorRuntime) > self.TIME_ISSUE_PERCENT_THRESHOLD):
         return True
      return False

   def _dataSkewMetrics(self, taskSumDic):
      dataSkewMetric = []
      for k in self.TASK_SUMMARY_KEYS:
         if (k.endswith('bytesRead') or
             k.endswith('bytesWritten') or
             k.endswith('readBytes') or
             k.endswith('writeBytes')):
             if self._isHigherThanThreshold(taskSumDic[k]):
                dataSkewMetric.append(k)
      return dataSkewMetric

   def _timeSkewMetrics(self, taskSumDic):
      timeSkewMetric = []
      for k in self.TASK_SUMMARY_KEYS:
          if (k.endswith('Time') or
              k.endswith('Delay')):
              if self._isHigherThanThreshold(taskSumDic[k]):
                 timeSkewMetric.append(k)
      return timeSkewMetric

   def _timeIssueMetrics(self, taskSumDic, taskSumDf):
      timeIssueMetric = []
      for k in self.TASK_SUMMARY_KEYS:
          if (k.endswith('Time') or
              k.endswith('Delay')):
              if self._hasTimeRelatedIssue(taskSumDf, k):
                 timeIssueMetric.append(k)
      return timeIssueMetric

   def _getIOMetrics(self, taskSumDf):
      ioMetric = []
      for k in self.TASK_SUMMARY_KEYS:
         if (k.endswith('bytesRead') or
             k.endswith('bytesWritten') or
             k.endswith('readBytes') or
             k.endswith('writeBytes') or
             k.endswith('remoteBytesRead') or
             k.endswith('remoteBytesReadToDisk')):
             if taskSumDf[k].values[0][4] > 0:
                ioMetric.append(k)
      #for k in ioMetric:
      #   print("== io metrics: {k}:{v}".format(k=k,v=taskSumDf[k].values[0][4]))
      return ioMetric

   def _displayImpactedMetric(self, dataSkew, timeSkew, timeIssue):
      if len(timeIssue) > 0:
         print("== Please check: {t}".format(t=timeIssue))
      if len(dataSkew) > 0:
         print("== Data skew occur: {d}".format(d=dataSkew))
      if len(timeSkew) > 0:
         print("== Time skew: {t}".format(t=timeSkew))

   def _pickupImpactedMetric(self, taskSumDic, taskSumDf):
      dataSkew = self._dataSkewMetrics(taskSumDic)
      timeSkew = self._timeSkewMetrics(taskSumDic)
      timeIssue = self._timeIssueMetrics(taskSumDic, taskSumDf)
      ioMetric = self._getIOMetrics(taskSumDf)
      return dataSkew,timeSkew,timeIssue,ioMetric

   def _mkdirIfNotExist(self, dirname):
      if not os.path.exists(dirname):
         os.mkdir(dirname)

   def _save_file(self, args, df, **fileInfo):
      tgtDir = "."
      if len(args.saveTo) > 0:
         self._mkdirIfNotExist(args.saveTo)
         tgtDir = args.saveTo
         if len(args.queue) > 0:
            tgtDir = os.path.join(args.saveTo, args.queue)
            self._mkdirIfNotExist(tgtDir)
      filename = fileInfo['filename']
      s = ','
      if 'sep' in fileInfo:
         s = fileInfo['sep']
      df.to_csv(os.path.join(tgtDir, filename), sep=s, index=False)

   def _getStageAnalysis(self, relativeUrl, args):
      ## get the stages metrics
      data = self._getStagesInternal(relativeUrl, args)
      if len(data) == 0:
         return
      # logging.info(data)
      df = json_normalize(data=data)
      if args.debug:
         print(df)
      #notFinishedDf = df.loc[(df['executorRunTime'] == 0)]
      #if notFinishedDf.empty == False:
      #   print("== not finished stages ==")
      #   print(notFinishedDf[['stageId','status','executorRunTime']])
      #filteredDf = df.loc[(df['executorRunTime'] > 0) & (df['executorCpuTime'] > 0)]
      filteredDf = df
      now = self._get_utcnow_datatime()
      stagesFile = "{p}_{s}".format(p=now, s=self.STAGES_INFO_FILE)
      self._save_file(args, filteredDf, filename=stagesFile, sep='|')
      stageInfoDf = pd.DataFrame()
      finalDf = pd.DataFrame()
      cpuNodes = {}
      ioNodes = {}
      memNodes = {}
      ## iterate every stage and check the task summary metrics
      for index, row in filteredDf.iterrows():
         stageId = row['stageId']
         attemptId = row['attemptId']
         numTasks = row['numTasks']
         stageInfoDic = {}
         stageInfoDic['stageId'] = stageId
         stageInfoDic['numTasks'] = numTasks
         stageInfoDic['status'] = row['status']
         stageInfoDic['submissionTime'] = row['submissionTime']

         if attemptId > 0:
            logging.warn("== stage {s} has {a} retry".format(s=stageId, a=attemptId))
         if numTasks <= self.STAGE_NUMTASKS_THRESHOLD:
            logging.warn("== stage {s} has too small task number: {t}".format(s=stageId, t=numTasks))
         baseRelativeUrl = "{relativeUrl}/{stageId}/{attempt}".format(
                           relativeUrl=relativeUrl, stageId=stageId, attempt=attemptId)
         ## construct a new parameter to query ##
         parameter = param.Param(args)
         parameter.stageId = stageId
         parameter.attemptId = attemptId
         parameter.display = 'summary'
         taskSumData = self._getTasksInternal(baseRelativeUrl, parameter)
         if len(taskSumData) == 0:
            continue
         logging.info(taskSumData)
         taskDf = json_normalize(data=taskSumData)
         if args.debug:
            print(taskDf)
         taskAnalysisRes = dict(self.findTaskSkew(taskDf))
         self._analyzeTask(baseRelativeUrl, parameter, stageInfoDic, taskAnalysisRes, taskDf, cpuNodes, ioNodes, memNodes)
         analysisDf = pd.DataFrame([taskAnalysisRes])
         analysisDf['stageId'] = stageId
         finalDf = finalDf.append(analysisDf, ignore_index=True)
         stageInfoDf = stageInfoDf.append(pd.DataFrame([stageInfoDic]), ignore_index=True)
      print(finalDf)
      if finalDf.empty == False:
         self._save_file(args, finalDf, filename=self.STAGES_SKEW_FILE)
      if stageInfoDf.empty == False:
         self._save_file(args, stageInfoDf, filename=self.STAGES_GENERAL_INFO_FILE)
      self._hotHostsAggr(args, cpuNodes, ioNodes, memNodes)

   def _analyzeTask(self, baseRelativeUrl, parameter, stageInfoDic, taskSumDic, taskSumDf, cpuNodes, ioNodes, memNodes):
      dataSkew, timeSkew, timeIssue, ioMetric = self._pickupImpactedMetric(taskSumDic, taskSumDf)
      stageInfoDic['dataSkew'] = 0 if len(dataSkew) == 0 else 1
      self._initIntensiveType(stageInfoDic)
      if parameter.debug:
         self._displayImpactedMetric(dataSkew, timeSkew, timeIssue)
      parameter.display = 'list'
      data = self._getTasksInternal(baseRelativeUrl, parameter)
      if len(data) == 0:
         return
      df = json_normalize(data=data)
      ## check slow node if there is no data skew
      if len(dataSkew) == 0:
         self._listTaskLongTimeNodes(df, parameter, timeIssue)
         intensiveType = self._findIntensiveType(parameter.stageId, df, timeIssue, stageInfoDic)
         self._sortCPURate(df, parameter, intensiveType, cpuNodes, ioNodes, memNodes)
         self._sortIORate(df, parameter, ioMetric)
      else:
         self._save_file(parameter, df, filename="{stage}_{p}".format(stage=parameter.stageId, p=self.STAGES_DATA_SKEW_FILE_POSTFIX))
         logging.warn("== data skew occurs: {d}".format(d=dataSkew))
         print(df)


   def _listTaskLongTimeNodes(self, df, param, timeIssueMetrics):
      taskMetrics = ['host', 'duration', 'status',
                     #'taskMetrics.executorRunTime'
                    ]
      for m in timeIssueMetrics:
          if m in self.TASK_SUMMARY_TO_LIST_MAP:
             taskMetrics.append(self.TASK_SUMMARY_TO_LIST_MAP[m])
      durMean = df['duration'].mean()
      filterDf = df[df['duration'] > durMean]
      printDf = filterDf.sort_values('duration',ascending=False)[taskMetrics]
      stageId = param.stageId
      self._save_file(param, printDf, filename="{s}_duration.csv".format(s=stageId))
      #printDf.to_csv(os.path.join(param.saveTo, "{s}_duration.csv".format(s=stageId)))
      print("   ****** sort tasks by duration (descending) ******   ")
      print(printDf)


   def _initIntensiveType(self, stageInfoDf):
      stageInfoDf['CPUBound'] = 0
      stageInfoDf['MemBound'] = 0
      stageInfoDf['IOBound'] = 0
      stageInfoDf['ResourceLimit'] = 0
      stageInfoDf['MixBound'] = 0


   def _findIntensiveType(self, stageId, df, timeIssueMetrics, stageInfoDf):
      if 'executorCpuTime' in timeIssueMetrics:
         print("== stage {s} is CPU intensive".format(s=stageId))
         stageInfoDf['CPUBound'] = 1
         return self.CPU_BOUND
      elif 'jvmGcTime' in timeIssueMetrics:
         print("== stage {s} is Mem intensive".format(s=stageId))
         stageInfoDf['MemBound'] = 1
         return self.MEM_BOUND
      elif ('launchDelay' in timeIssueMetrics) or ('schedulerDelay' in timeIssueMetrics):
         print("== stage {s} has long time delay to run".format(s=stageId))
         stageInfoDf['ResourceLimit'] = 1
         return self.LONG_DELAY
      elif len(timeIssueMetrics) > 0:
         print("== stage {s} is IO intensive: {m}".format(s=stageId,m=timeIssueMetrics))
         stageInfoDf['IOBound'] = 1
         return self.IO_BOUND
      stageInfoDf['MixBound'] = 1
      return self.MIX_BOUND

   def _isExecutorTimeNaN(self, df):
      nanRunTime = df['taskMetrics.executorRunTime'].isnull().sum()
      nanCpuTime = df['taskMetrics.executorCpuTime'].isnull().sum()
      if nanRunTime > 0 or nanCpuTime > 0:
         return True
      else:
         return False

   def _sortCPURate(self, df, parameter, intensiveType, cpuNodesDic, ioNodesDic, memNodesDic):
      ## record the hot nodes for specific resource bound
      if self._isExecutorTimeNaN(df):
         logging.warn("Task executor RunTime/CpuTime is NaN")
         df = df.dropna(axis=0,how='any')
      if intensiveType == self.MEM_BOUND:
         df['taskJvmGcRate'] = df['taskMetrics.jvmGcTime'] / df['taskMetrics.executorRunTime']
         gcSortedDf = df.sort_values('taskJvmGcRate',ascending=False).dropna()
         for i, r in gcSortedDf.iterrows():
            host = r['host']
            jvmGcTime = r['taskMetrics.jvmGcTime']
            if host in memNodesDic:
               memNodesDic[host] += jvmGcTime
            else:
               memNodesDic[host] = jvmGcTime
      elif intensiveType == self.IO_BOUND:
         df['taskNoneCPUTimeRate'] = (df['taskMetrics.executorRunTime'] - df['taskMetrics.executorCpuTime']) / df['taskMetrics.executorRunTime']
         noneCpuDf = df.sort_values('taskNoneCPUTimeRate',ascending=False).dropna()
         for i, r in noneCpuDf.iterrows():
           host = r['host']
           noneCpuTime = r['taskMetrics.executorRunTime'] - r['taskMetrics.executorCpuTime']
           if host in ioNodesDic:
              ioNodesDic[host] += noneCpuTime
           else:
              ioNodesDic[host] = noneCpuTime
      if self.cluster == self.APOLLO:
         df['taskCpuRate'] = df['taskMetrics.executorCpuTime'] / 1000000 / df['taskMetrics.executorRunTime']
      elif self.cluster == self.HERMES:
         df['taskCpuRate'] = df['taskMetrics.executorCpuTime'] / df['taskMetrics.executorRunTime']
      cpuSortedDf = df.sort_values('taskCpuRate',ascending=False)
      for i, r in cpuSortedDf.iterrows():
         host = r['host']
         cputime = int(r['taskMetrics.executorCpuTime'])
         if cputime == 0:
            continue
         if self.cluster == self.APOLLO:
            cputime = cputime / 1000000
         if host in cpuNodesDic:
            cpuNodesDic[host] += cputime
         else:
            cpuNodesDic[host] = cputime

      bound = "Mix bound"
      if intensiveType == self.CPU_BOUND:
         bound = "CPU bound"
      elif intensiveType == self.MEM_BOUND:
         bound = "Mem bound (JvmGC)"
      elif intensiveType == self.IO_BOUND:
         bound = "IO bound"
      printedDf = cpuSortedDf[['host','duration','status','taskCpuRate','taskMetrics.executorRunTime','taskMetrics.executorCpuTime']] 
      print("   ****** {s}: sort tasks by CPU rate ******   ".format(s=bound))
      print(printedDf)
      self._save_file(parameter, printedDf, filename="{s}_{p}".format(s=parameter.stageId, p=self.STAGES_CPU_RATE_FILE))

   def _sortAndPrintDict(self, dic):
      #print(dic)
      sortedDic = sorted(dic.items(), key=lambda x: int(x[1]), reverse=True)
      for k,v in sortedDic:
         print("{k}: {v}".format(k=k,v=v))
      return sortedDic

   def _hotHostsAggr(self, args, cpuNodes, ioNodes, memNodes):
      if len(cpuNodes) > 0:
         print("=======================")
         print("==== CPU hot nodes ====")
         sortedCpuNodes = self._sortAndPrintDict(cpuNodes)
         cpuTimePd = pd.DataFrame(sortedCpuNodes, columns=['Hosts','CpuTime(ms)'])
         self._save_file(args, cpuTimePd, filename=self.STAGES_TOTAL_CPUTIME_FILE)
      if len(ioNodes) > 0:
         print("=======================")
         print("==== IO hot nodes ====")
         sortedIoNodes = self._sortAndPrintDict(ioNodes)
         ioPd = pd.DataFrame(sortedIoNodes, columns=['Hosts','NoneCpuTime(ms)'])
         self._save_file(args, ioPd, filename=self.STAGES_TOTAL_NONE_CPUTIME_FILE)
      if len(memNodes) > 0:
         print("=======================")
         print("==== MEM hot nodes ====")
         sortedMemNodes = self._sortAndPrintDict(memNodes)
         memPd = pd.DataFrame(sortedMemNodes, columns=['Hosts', 'JvmGcTime(ms)'])
         self._save_file(args, memPd, filename=self.STAGES_MEM_FILE)


   def _sortIORate(self, df, parameter, ioMetric):
      taskMetrics = []
      for m in ioMetric:
          if m in self.TASK_SUMMARY_TO_LIST_MAP:
             taskMetrics.append(self.TASK_SUMMARY_TO_LIST_MAP[m])
      for m in taskMetrics:
          rateMetric = "{o}.rateInKB".format(o=m)
          df[rateMetric] = df[m] / df['taskMetrics.executorRunTime']
          print("   ****** sort tasks by {m} ******   ".format(m=rateMetric))
          printedDf = df.sort_values(rateMetric,ascending=False)[['host','duration','status',rateMetric]]
          print(printedDf)
          self._save_file(parameter, printedDf, filename="{s}_{p}".format(s=parameter.stageId, p=self.STAGES_IO_RATE_FILE))

   def _displayStages(self, normDf, args):
      df = normDf.rename(columns={"killedTasksSummary.Finish but did not commit due to another attempt succeeded":"killedButNoCommit",
                                  "killedTasksSummary.another attempt succeeded":"killedAnother"})
      if args.skipNoIO:
         filteredDf = df[(df.inputBytes != 0) | (df.shuffleReadBytes != 0) | (df.outputBytes != 0) | (df.shuffleWriteBytes != 0)]
         df = filteredDf
      sortedColumn = args.sortBy
      selectedDf = df[['stageId',
                       'status',
                       'attemptId',
                       'description',
                       'numActiveTasks',
                       'numCompleteTasks',
                       'numFailedTasks',
                       'numKilledTasks',
                       'numTasks',
                       'inputBytes',
                       'outputBytes',
                       'shuffleReadBytes',
                       'shuffleWriteBytes']]
      if sortedColumn != 'none':
         outputDf = selectedDf.sort_values(sortedColumn, ascending=False)
      else:
         outputDf = selectedDf
      if args.output == 'table':
         print(outputDf)
      elif args.output == 'json':
         print(outputDf.to_json())

   def _getStagesInternal(self, relativeUrl, args):
      if args.status == 'all':
         data = self._restApiCall(relativeUrl, anonymous='true')
      else:
         data = self._restApiCall(relativeUrl, anonymous='true', status=args.status)
      return data

   def _getStages(self, relativeUrl, args):
      data = self._getStagesInternal(relativeUrl, args)
      if args.debug:
         print(data)
      if len(data) > 0:
         normDf = json_normalize(data=data)
         self._displayStages(normDf, args)

   def _getTasksInternal(self, baseRelativeUrl, args):
      if args.display == 'summary':
         target = "taskSummary"
      else:
         target = "taskList"
      data = self._restApiCall("{b}/{t}".format(b=baseRelativeUrl,t=target), anonymous='true')
      return data

   def _getTasks(self, baseRelativeUrl, args):
      data = self._getTasksInternal(baseRelativeUrl, args)
      if args.debug:
         print(data)
      if len(data) > 0:
         df = json_normalize(data=data)
         if args.display == 'list':
            if args.analyze:
               self.analyzeTasks(df)
            else:
               print(df)
         else:
             self.displayTaskSkew(df)

   def _renameTaskColumns(self, df):
      simpleDf = df.rename(
                    columns={"taskMetrics.executorRunTime":"task.executorRunTime",
                             "taskMetrics.executorCpuTime":"task.executorCpuTime",
                             "taskMetrics.jvmGcTime":"task.jvmGcTime",
                             "taskMetrics.executorDeserializeTime":"task.executorDeserializeTime",
                             "taskMetrics.executorDeserializeCpuTime":"task.executorDeserializeCpuTime",
                             "taskMetrics.inputMetrics.bytesRead":"task.inputMetrics.bytesRead"})
      return simpleDf

   def findTaskSkew(self, df):
      res = {}
      for k in self.TASK_SUMMARY_KEYS:
          v = df[k].values[0]
          uniques = np.unique(v)
          if len(uniques) <= 2:
             res[k] = 1
             continue
          mean = np.mean(v)
          std = np.std(v, ddof=1)
          dev = 0
          if math.isclose(mean, 0) is False:
             dev = std / mean * 100
          fDev = format(dev, '.0f')
          res[k] = fDev
      sortedRes = sorted(res.items(), key=lambda x: int(x[1]), reverse=True)
      return sortedRes

   def displayTaskSkew(self, df):
      sortedRes = self.findTaskSkew(df)
      for k, v in sortedRes:
          print("{k}: {v}".format(k=k,v=v))

   def analyzeTasks(self, df):
      simpleDf = self._renameTaskColumns(df)
      durMean = simpleDf['duration'].mean()
      gcMean = simpleDf['task.jvmGcTime'].mean()
      filterDf = simpleDf[simpleDf.duration > durMean]
      gcFilterDf = simpleDf[simpleDf['task.jvmGcTime'] > gcMean]
      simpleDf['taskInputReadRateInKB'] = simpleDf['task.inputMetrics.bytesRead'] / simpleDf['task.executorRunTime']
      print("")
      print("   ****** sort tasks by InputRead rate (K/B) ******   ")
      print(simpleDf.sort_values('taskInputReadRateInKB')[['host','duration','status','taskInputReadRateInKB','task.executorRunTime','task.executorCpuTime']])
      if self.cluster == self.APOLLO:
         simpleDf['taskCpuRate'] = simpleDf['task.executorCpuTime'] / 100000 / simpleDf['task.executorRunTime']
      elif self.cluster == self.HERMES:
         simpleDf['taskCpuRate'] = simpleDf['task.executorCpuTime'] / simpleDf['task.executorRunTime']
      print("")
      print("   ****** sort tasks by CPU rate ******   ")
      print(simpleDf.sort_values('taskCpuRate',ascending=False)[['host','duration','status','taskCpuRate','task.executorRunTime','task.executorCpuTime']])
      print("")
      print("   ****** sort tasks by duration (descending) ******   ")
      print(filterDf.sort_values('duration',ascending=False)[['host','duration','status','task.executorRunTime','task.executorCpuTime','task.jvmGcTime']])
      print("")
      print("   ****** sort tasks by JVM GC Time (milliseconds) ******   ")
      print(gcFilterDf.sort_values('task.jvmGcTime',ascending=False)[['host','duration','status','task.executorRunTime','task.executorCpuTime','task.jvmGcTime']])
