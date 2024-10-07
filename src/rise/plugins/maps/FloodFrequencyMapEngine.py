import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class FloodFrequencyMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        pass

    def handleTask(self, oTask):
        try:
            logging.info("FloodFrequencyMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("FloodFrequencyMapEngine.handleTask: exception " + str(oEx))
