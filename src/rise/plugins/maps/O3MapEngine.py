import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class O3MapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.debug("O3MapEngine.triggerNewAreaArchives [" + self.m_oArea.name +"]: Map generated in the general Pollutant Map")

    def updateNewMaps(self):
        logging.debug("O3MapEngine.updateNewMaps [" + self.m_oArea.name +"]: Map generated in the general Pollutant Map")

    def handleTask(self, oTask):
        logging.debug("O3MapEngine.handleTask [" + self.m_oArea.name +"]: Map generated in the general Pollutant Map")