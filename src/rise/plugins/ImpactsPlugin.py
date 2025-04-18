import logging
from src.rise.plugins.RisePlugin import RisePlugin


class ImpactsPlugin(RisePlugin):
    """

    """
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)

    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("ImpactsPlugin.updateNewMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "rasor_impacts":
                    logging.info("ImpactsPlugin.updateNewMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.updateNewMaps()
                    break

        except Exception as oEx:
            logging.error("ImpactsPlugin.updateNewMaps: exception " + str(oEx))


    def triggerNewAreaMaps(self):
        logging.info("ImpactMapEngine.triggerNewAreaMaps: short term archive is handled by the integrated chain")
        return

    def triggerNewAreaArchives(self):
        logging.info("ImpactMapEngine.triggerNewAreaArchives: long archive is handled by the integrated chain")
        return

    def handleTask(self, oTask):
        oTask.mapId = "rasor_impacts"
        super().handleTask(oTask)