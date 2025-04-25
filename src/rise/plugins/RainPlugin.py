import logging

from src.rise.plugins.RisePlugin import RisePlugin


class RainPlugin(RisePlugin):
    """

    """
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)

    def triggerNewAreaArchives(self):
        logging.info("RainPlugin.triggerNewAreaArchives: long archive is handled by the integrated chain")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "imerg_cumulate_12":
                    logging.info("RainPlugin.triggerNewAreaArchives: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaArchives()
                    break

        except Exception as oEx:
            logging.error("RainPlugin.triggerNewAreaArchives: exception " + str(oEx))

    def triggerNewAreaMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RainPlugin.triggerNewAreaMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "imerg_cumulate_12":
                    logging.info("RainPlugin.triggerNewAreaMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaMaps()
                    break

        except Exception as oEx:
            logging.error("RainPlugin.triggerNewAreaMaps: exception " + str(oEx))

    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RainPlugin.updateNewMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "imerg_cumulate_12":
                    logging.info("RainPlugin.updateNewMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.updateNewMaps()
                    break

        except Exception as oEx:
            logging.error("RainPlugin.updateNewMaps: exception " + str(oEx))        


    def handleTask(self, oTask):
        oTask.mapId = "imerg_cumulate_12"
        super().handleTask(oTask)