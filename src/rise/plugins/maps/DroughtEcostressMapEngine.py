import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class DroughtEcostressMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        pass

    def triggerNewAreaArchives(self):
        pass

    def updateNewMaps(self):
        pass

    def handleTask(self, oTask):
        try:
            logging.info("DroughtEcostressMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("DroughtEcostressMapEngine.handleTask: exception " + str(oEx))
