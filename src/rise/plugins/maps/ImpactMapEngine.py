import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class ImpactFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        pass

    def triggerNewAreaArchives(self):
        logging.info("VIIRS long archive is handled by the integrated chain")

    def handleTask(self, oTask):
        try:
            logging.info("ImpactFloodMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("ImpactFloodMapEngine.handleTask: exception " + str(oEx))

    def updateNewMaps(self):
        pass