import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class NO2MapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.debug("NO2MapEngine.triggerNewAreaArchives: Map generated in the general Pollutant Map")

    def updateNewMaps(self):
        logging.debug("NO2MapEngine.updateNewMaps: Map generated in the general Pollutant Map")

    def handleTask(self, oTask):
        logging.debug("NO2MapEngine.handleTask: Map generated in the general Pollutant Map")