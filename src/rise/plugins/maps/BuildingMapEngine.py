import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class BuildingMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        pass

    def handleTask(self, oTask):
        try:
            logging.info("BuildingMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("BuildingMapEngine.handleTask: exception " + str(oEx))
