import argparse
import json
import requests
import sys

RESOUCE_MGR_URL="https://hermes-rno-rm-2.vip.hadoop.ebay.com:50030/proxy"

def get_base_url(appId):
   return "{res_url}/{app}/api/v1/applications/{app}".format(res_url=RESOUCE_MGR_URL, app=appId)

def activeTaskFromJob(jobJson):
   actTask = 0
   for j in jobJson:
       actTask += j['numActiveStages']
   return actTask

def failedTaskFromJob(jobJson):
   failedTask = 0
   for j in jobJson:
       failedTask += j['numFailedTasks']
   return failedTask

def jobMetrics(args):
   url = get_base_url(args.appId)
   jobUrl = url + "/jobs"
   queryParam = {'anonymous': 'true', 'status': args.status}
   response = requests.get(jobUrl, verify=False, params=queryParam)
   if response != None and response.status_code == 200:
      data = response.json()
      for d in data:
          print("submissionTime: {st}, stages: {stages}, activeStages: {acts}, failedStages: {fs}, completedStages: {cs}, task: {t}, activeTask: {at}, completedTask: {ct}, failedTask: {ft}, skippedTask: {skt}"
                .format(st=d['submissionTime'],
                        stages=d['stageIds'],
                        acts=d['numActiveStages'],
                        fs=d['numFailedStages'],
                        cs=d['numCompletedStages'],
                        t=d['numTasks'],
                        at=d['numActiveTasks'],
                        ct=d['numCompletedTasks'],
                        ft=d['numFailedTasks'],
                        skt=d['numSkippedTasks']))
      if args.debug:
         print(data)

def stageMetrics(args):
   url = get_base_url(args.appId)
   jobUrl = url + "/stages"
   queryParam = {'anonymous': 'true', 'status': args.status}
   response = requests.get(jobUrl, verify=False, params=queryParam)
   if response != None and response.status_code == 200:
      data = response.json()
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
      if args.debug:
         print(data)

def commonArgs(parser):
   parser.add_argument("--appId", help="Specify the application Id", required=True)
   parser.add_argument("--debug", action="store_true", help="Enable debug, default is false")

if __name__=="__main__":
   parser = argparse.ArgumentParser('Spark status monitor')

   subparser = parser.add_subparsers(dest="command")

   jobParser = subparser.add_parser('job', help='List job related metrics')
   jobParser.add_argument('--status', choices=['running','succeeded','failed','unknown'], help='Specify the job status you want to filter, default is failed', default='failed')
   commonArgs(jobParser)
   jobParser.set_defaults(func=jobMetrics)

   stageParser = subparser.add_parser('stage', help='List stage releated metrics')
   stageParser.add_argument('--status', choices=['active','complete','pending','failed'])
   commonArgs(stageParser)
   stageParser.set_defaults(func=stageMetrics)

   if len(sys.argv) <= 1:
      sys.argv.append('--help')

   options = parser.parse_args()
   options.func(options)
