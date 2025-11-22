from datetime import datetime, timedelta
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class S3LSTMaxMapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.debug("S3LSTMaxMapEngine.triggerNewAreaArchives [" + self.m_oArea.name +"]: Map generated in the general LST Map")

    def updateNewMaps(self):
        logging.debug("S3LSTMaxMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Map generated in the general LST Map")

    def handleTask(self, oTask):
        logging.debug("S3LSTMaxMapEngine.handleTask [" + self.m_oArea.name +"]: Map generated in the general LST Map")