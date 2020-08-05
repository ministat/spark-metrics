import argparse
import sys

import hermes
import apollo
import param

from datetime import datetime

#hermes = hermes.Hermes("http://viewpoint.hermes-prod.svc.25.tess.io", 0)
hermes = hermes.Hermes("https://hermes-rno-rm-2.vip.hadoop.ebay.com:50030/proxy", 0)
apollo = apollo.Apollo("https://apollo-rno-shs-1.vip.hadoop.ebay.com:8080/api/v1/applications", 1)

def command(args):
   command = args.command[:1].upper() + args.command[1:]
   cluster = args.cluster
   parameter = param.Param(args)
   callfunc = "{c}.get{cmd}(parameter)".format(c=cluster,cmd=command)
   eval(callfunc)

def commonArgs(parser):
   parser.add_argument('--cluster', choices=['hermes','apollo'], type=str, help="Specify the cluster name, default is hermes", default='hermes')
   parser.add_argument("--appId", help="Specify the application Id", required=True)
   parser.add_argument("--queue", help="Specify the queue name for displaying, default is ''")
   parser.add_argument("--debug", action="store_true", help="Enable debug, default is false")
   parser.add_argument("--output", choices=['json','table'], help="Display output in json or table format, default is table", default='table')

if __name__=="__main__":
   parser = argparse.ArgumentParser('Spark status monitor')
   subparser = parser.add_subparsers(dest="command")

   jobsParser = subparser.add_parser('jobs', help='List all jobs status')
   jobsParser.add_argument('--status', type=str, choices=['running','succeeded','unknown','failed', 'all'], help="Specify the status", default='all')
   commonArgs(jobsParser)
   jobsParser.set_defaults(func=command)

   jobParser = subparser.add_parser('job', help='List the details of the specified job')
   jobParser.add_argument('--jobId', type=str, help="Specify the job id", required=True)
   commonArgs(jobParser)
   jobParser.set_defaults(func=command)

   stageParser = subparser.add_parser('stages', help='List stage releated metrics')
   stageParser.add_argument('--status', type=str, choices=['active','complete','pending','failed', 'all'], help="Specify the status, default is all", default='all')
   stageParser.add_argument('--skipNoIO', action="store_true", help="Skip the stages which do not have any IO operations")
   stageParser.add_argument('--sortBy', type=str, choices=['numTasks','inputBytes','outputBytes','shuffleReadBytes','shuffleWriteBytes'],
                            help="Specify the sort by column in descending order, default is numTasks", default='numTasks')
   commonArgs(stageParser)
   stageParser.set_defaults(func=command)

   taskParser = subparser.add_parser('tasks', help='List all the tasks')
   taskParser.add_argument("--stageId", help="Specify the stageId")
   taskParser.add_argument("--attemptId", type=int, default=0, help="Specify the attemptId, default is 0")
   taskParser.add_argument("--display", choices=['summary','list'], default='summary',
      help="Which kinds of display do you want? summary or list, default is summary")
   taskParser.add_argument("--analyze", action="store_true", default=False,
      help="Analyze the task list if display option is list, default is false")
   taskParser.set_defaults(func=command)
   commonArgs(taskParser)

   sqlParser = subparser.add_parser('sqls', help='List all the running SQLs')
   sqlParser.set_defaults(func=command)
   sqlParser.add_argument('--saveTo', type=str, help='Specify where the csv data is saved to, default is sqls.csv', default="sqls.csv")
   commonArgs(sqlParser)

   stageAnalysisParser = subparser.add_parser('stageAnalysis', help='Analyze the stages')
   stageAnalysisParser.add_argument('--status', type=str, choices=['active','complete','pending','failed', 'all'], help="Specify the status, default is all", default='all')
   stageAnalysisParser.add_argument('--saveTo', type=str, help='Specify where the csv data is saved to, default is stageAnalysis', default="{s}".format(s=datetime.utcnow().strftime("%Y-%m-%dT%H:%M")))
   commonArgs(stageAnalysisParser)
   stageAnalysisParser.set_defaults(func=command)
   if len(sys.argv) <= 1:
      sys.argv.append('--help')

   options = parser.parse_args()
   options.func(options)
