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
        logging.info("CompositeFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: handled by SarFloodMapEngine")

    def handleTask(self, oTask):
        try:
            logging.info("CompositeFloodMapEngine.handleTask [" + self.m_oArea.name +"]: handled by SarFloodMapEngine " + oTask.id)
        except Exception as oEx:
            logging.error("CompositeFloodMapEngine.handleTask [" + self.m_oArea.name + "]: exception " + str(oEx))
