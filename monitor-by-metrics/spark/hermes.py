import base as ParentClass
from pandas.io.json import json_normalize

class Hermes(ParentClass.Base):
    def __init__(self, baseUrl, cluster):
        super().__init__(baseUrl, cluster)

    def getStages(self, args):
        relativeUrl = "{appId}/api/v1/applications/{appId}/stages".format(appId=args.appId)
        self._getStages(relativeUrl, args)

    def getTasks(self, args):
        baseRelativeUrl = "{appId}/api/v1/applications/{appId}/stages/{stageId}/{attempt}".format(
                           appId=args.appId, stageId=args.stageId, attempt=args.attemptId)
        self._getTasks(baseRelativeUrl, args)

    def getJobs(self, args):
        relativeUrl = "{appId}/api/v1/applications/{appId}/jobs".format(appId=args.appId)
        self._getJobs(relativeUrl, args)

    def getJob(self, args):
        relativeUrl = "{appId}/api/v1/applications/{appId}/jobs/{jobId}".format(appId=args.appId,jobId=args.jobId)
        self._getJob(relativeUrl, args)
