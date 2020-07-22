import argparse
import json
import logging
import math
import numpy as np
import operator
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
       "shuffleWriteMetrics.writeTime":"taskMetrics.shuffleWriteMetrics.writeTime"
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

   def getJobs(self, args):
      pass

   def getJob(self, args):
      pass

   def getExecutors(self, args):
      pass

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
      if maxExecutorRuntime > 0 and (tgtMetrics / maxExecutorRuntime > self.TIME_ISSUE_PERCENT_THRESHOLD):
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
             k.endswith('remoteBytesRead')):
             if math.isclose(taskSumDf[k].values[0][4], 0) is False:
                ioMetric.append(k)
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

   def _getStageAnalysis(self, relativeUrl, args):
      ## get the stages metrics
      data = self._getStagesInternal(relativeUrl, args)
      if len(data) == 0:
         return
      logging.info(data)
      df = json_normalize(data=data)
      if args.debug:
         print(df)
      filteredDf = df.loc[(df['executorRunTime'] > 0) & (df['executorCpuTime'] > 0)]
      ## save the stages metrics
      if len(args.saveTo) > 0:
         now = self._get_utcnow_datatime()
         filteredDf.to_csv("{p}_{s}".format(p=now,s=args.saveTo), sep='|')
      finalDf = pd.DataFrame()
      ## iterate every stage and check the task summary metrics
      for index, row in filteredDf.iterrows():
         stageId = row['stageId']
         attemptId = row['attemptId']
         numTasks = row['numTasks']
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
         self._getTaskAnalysis(baseRelativeUrl, parameter, taskAnalysisRes, taskDf)
         analysisDf = pd.DataFrame([taskAnalysisRes])
         analysisDf['stageId'] = stageId
         finalDf = finalDf.append(analysisDf, ignore_index=True)
      print(finalDf)
      if len(args.saveTo) > 0:
         finalDf.to_csv(args.saveTo)

   def _getTaskAnalysis(self, baseRelativeUrl, parameter, taskSumDic, taskSumDf):
      dataSkew, timeSkew, timeIssue, ioMetric = self._pickupImpactedMetric(taskSumDic, taskSumDf)
      if parameter.debug:
         self._displayImpactedMetric(dataSkew, timeSkew, timeIssue)
      ## check slow node if there is no data skew
      if len(dataSkew) == 0:
         parameter.display = 'list'
         data = self._getTasksInternal(baseRelativeUrl, parameter)
         if len(data) == 0:
            return
         df = json_normalize(data=data)
         #simpleDf = self._renameTaskColumns(df)
         self._listTaskLongTimeNodes(df, timeIssue)
         self._findIntensiveType(parameter.stageId, df, timeIssue)
         self._sortCPURate(df)
         self._sortIORate(df, ioMetric)

   def _listTaskLongTimeNodes(self, df, timeIssueMetrics):
      taskMetrics = ['host', 'duration', 'status', 'taskMetrics.executorRunTime']
      for m in timeIssueMetrics:
          if m in self.TASK_SUMMARY_TO_LIST_MAP:
             taskMetrics.append(self.TASK_SUMMARY_TO_LIST_MAP[m])
      durMean = df['duration'].mean()
      filterDf = df[df['duration'] > durMean]
      print("   ****** sort tasks by duration (descending) ******   ")
      print(filterDf.sort_values('duration',ascending=False)[taskMetrics])

   def _findIntensiveType(self, stageId, df, timeIssueMetrics):
      taskMetrics = ['host', 'duration', 'status']
      if 'executorCpuTime' in timeIssueMetrics:
         print("== stage {s} is CPU intensive".format(s=stageId))
      else:
         print("== stage {s} is IO intensive".format(s=stageId))

   def _sortCPURate(self, df):
      if self.cluster == self.APOLLO:
         df['taskCpuRate'] = df['taskMetrics.executorCpuTime'] / 100000 / df['taskMetrics.executorRunTime']
      elif self.cluster == self.HERMES:
         df['taskCpuRate'] = df['taskMetrics.executorCpuTime'] / df['taskMetrics.executorRunTime']
      print("   ****** sort tasks by CPU rate ******   ")
      print(df.sort_values('taskCpuRate',ascending=False)[['host','duration','status','taskCpuRate','taskMetrics.executorRunTime','taskMetrics.executorCpuTime']])

   def _sortIORate(self, df, ioMetric):
      taskMetrics = []
      for m in ioMetric:
          if m in self.TASK_SUMMARY_TO_LIST_MAP:
             taskMetrics.append(self.TASK_SUMMARY_TO_LIST_MAP[m])
      for m in taskMetrics:
          rateMetric = "{o}.rateInKB".format(o=m)
          df[rateMetric] = df[m] / df['taskMetrics.executorRunTime']
          print("   ****** sort tasks by {m} ******   ".format(m=rateMetric))
          print(df.sort_values(rateMetric,ascending=False)[['host','duration','status',rateMetric]])

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
      gcFilterDf = simpleDf[simpleDf.taskJvmGcTime > gcMean]
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
