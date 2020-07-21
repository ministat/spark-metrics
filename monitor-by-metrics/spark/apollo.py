import base as ParentClass
from pandas.io.json import json_normalize

class Apollo(ParentClass.Base):
    def __init__(self, baseUrl, cluster):
        super().__init__(baseUrl, cluster)

    def getStages(self, args):
        relativeUrl = "{appId}/1/stages".format(appId=args.appId)
        self._getStages(relativeUrl, args)

    def getTasks(self, args):
        baseRelativeUrl = "{appId}/1/stages/{stageId}/{attempt}".format(
                           appId=args.appId, stageId=args.stageId, attempt=args.attemptId)
        self._getTasks(baseRelativeUrl, args)
