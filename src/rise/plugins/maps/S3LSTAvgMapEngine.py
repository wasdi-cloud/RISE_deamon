from datetime import datetime, timedelta
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class S3LSTAvgMapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.debug("S3LSTAvgMapEngine.triggerNewAreaArchives: Map generated in the general LST Map")

    def updateNewMaps(self):
        logging.debug("S3LSTAvgMapEngine.updateNewMaps: Map generated in the general LST Map")

    def handleTask(self, oTask):
        logging.debug("S3LSTAvgMapEngine.handleTask: Map generated in the general LST Map")