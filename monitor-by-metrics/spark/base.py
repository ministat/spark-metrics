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

from datetime import datetime, timedelta
from logging import handlers
from pandas.io.json import json_normalize
from six import iteritems

#RESOUCE_MGR_URL="https://hermes-rno-rm-2.vip.hadoop.ebay.com:50030/proxy"

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

   def _getStages(self, relativeUrl, args):
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
           #print(df.columns)
           if args.skipNoIO:
              filteredDf = df[(df.inputBytes != 0) | (df.shuffleReadBytes != 0) | (df.outputBytes != 0) | (df.shuffleWriteBytes != 0)]
              df = filteredDf
           sortedColumn = args.sortBy
           selectedDf = df[['stageId','status','attemptId','description','numActiveTasks','numCompleteTasks','numFailedTasks','numKilledTasks','numTasks','inputBytes','outputBytes','shuffleReadBytes','shuffleWriteBytes']]
           if sortedColumn != 'none':
              outputDf = selectedDf.sort_values(sortedColumn, ascending=False)
           else:
              outputDf = selectedDf
           if args.output == 'table':
              print(outputDf)
           elif args.output == 'json':
              print(outputDf.to_json())

   def _getTasks(self, baseRelativeUrl, args):
        if args.display == 'summary':
           target = "taskSummary"
        else:
           target = "taskList"
        data = self._restApiCall("{b}/{t}".format(b=baseRelativeUrl,t=target), anonymous='true')
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
               self.findTaskSkew(df)

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
          #print("{k}: {v}, {d}".format(k=k, v=v, d=dev))
          res[k] = fDev
      sortedRes = sorted(res.items(), key=lambda x: int(x[1]), reverse=True)
      for k, v in sortedRes:
          print("{k}: {v}".format(k=k,v=v))

   def analyzeTasks(self, df):
      simpleDf = df.rename(columns={"taskMetrics.executorRunTime":"taskExecutorRunTime",
                                 "taskMetrics.executorCpuTime":"taskExecutorCpuTime",
                                 "taskMetrics.jvmGcTime":"taskJvmGcTime",
                                 "taskMetrics.executorDeserializeTime":"taskExecutorDeserializeTime",
                                 "taskMetrics.executorDeserializeCpuTime":"taskExecutorDeserializeCpuTime",
                                 "taskMetrics.inputMetrics.bytesRead":"taskInputBytesRead"})
      durMean = simpleDf['duration'].mean()
      gcMean = simpleDf['taskJvmGcTime'].mean()
      filterDf = simpleDf[simpleDf.duration > durMean]
      gcFilterDf = simpleDf[simpleDf.taskJvmGcTime > gcMean]
      simpleDf['taskInputReadRateInKB'] = simpleDf['taskInputBytesRead'] / simpleDf['taskExecutorRunTime']
      print("")
      print("   ****** sort tasks by InputRead rate (K/B) ******   ")
      print(simpleDf.sort_values('taskInputReadRateInKB')[['host','duration','status','taskInputReadRateInKB','taskExecutorRunTime','taskExecutorCpuTime']])
      if self.cluster == self.APOLLO:
         simpleDf['taskCpuRate'] = simpleDf['taskExecutorCpuTime'] / 100000 / simpleDf['taskExecutorRunTime']
      elif self.cluster == self.HERMES:
         simpleDf['taskCpuRate'] = simpleDf['taskExecutorCpuTime'] / simpleDf['taskExecutorRunTime']
      print("")
      print("   ****** sort tasks by CPU rate ******   ")
      print(simpleDf.sort_values('taskCpuRate',ascending=False)[['host','duration','status','taskCpuRate','taskExecutorRunTime','taskExecutorCpuTime']])
      print("")
      print("   ****** sort tasks by duration (descending) ******   ")
      print(filterDf.head(10).sort_values('duration',ascending=False)[['host','duration','status','taskExecutorRunTime','taskExecutorCpuTime','taskJvmGcTime']])
      print("")
      print("   ****** sort tasks by JVM GC Time (milliseconds) ******   ")
      print(gcFilterDf.head(10).sort_values('taskJvmGcTime',ascending=False)[['host','duration','status','taskExecutorRunTime','taskExecutorCpuTime','taskJvmGcTime']])
