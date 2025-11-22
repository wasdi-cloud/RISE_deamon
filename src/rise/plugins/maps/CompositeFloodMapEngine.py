import logging

from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class CompositeFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        logging.info("CompositeFloodMapEngine.triggerNewAreaMaps [" + self.m_oArea.name +"]: handled by SarFloodMapEngine")

    def triggerNewAreaArchives(self):
        logging.info("CompositeFloodMapEngine.triggerNewAreaArchives [" + self.m_oArea.name +"]: handled by SarFloodMapEngine")

    def updateNewMaps(self):

        # Check if the initial short archive is finished or not
        if not self.isShortArchiveFinished("integrated_archive"):
            logging.info("CompositeFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: the initial short archive is not yet finished we will wait it to finish")
            return

        logging.info("CompositeFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: handled by SarFloodMapEngine")

    def handleTask(self, oTask):
        try:
            logging.info("CompositeFloodMapEngine.handleTask [" + self.m_oArea.name +"]: handled by SarFloodMapEngine " + oTask.id)
        except Exception as oEx:
            logging.error("CompositeFloodMapEngine.handleTask [" + self.m_oArea.name + "]: exception " + str(oEx))
