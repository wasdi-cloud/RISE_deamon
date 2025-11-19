import logging
from src.rise.plugins.RisePlugin import RisePlugin


class PollutantPlugin(RisePlugin):
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)



    def triggerNewAreaMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("PollutantPlugin.triggerNewAreaMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "pollutant_map":
                    logging.info("PollutantPlugin.triggerNewAreaMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaMaps()
                    break

        except Exception as oEx:
            logging.error("PollutantPlugin.triggerNewAreaMaps: exception " + str(oEx))
            
            
    
    def triggerNewAreaArchives(self):
        logging.info("PollutantPlugin.triggerNewAreaArchives: long archive is handled by the integrated chain")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "pollutant_map":
                    logging.info("PollutantPlugin.triggerNewAreaArchives: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaArchives()
                    break

        except Exception as oEx:
            logging.error("PollutantPlugin.triggerNewAreaArchives: exception " + str(oEx))


    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        # logging.debug("PollutantPlugin.updateNewMaps OVERRIDE")
        # try:
        #     for oMapEngine in self.m_aoMapEngines:
        #         if oMapEngine.m_oMapEntity.id == "pollutant_map":
        #             logging.info("PollutantPlugin.updateNewMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
        #             oMapEngine.updateNewMaps()
        #             break
        #
        # except Exception as oEx:
        #     logging.error("PollutantPlugin.updateNewMaps: exception " + str(oEx))


    def handleTask(self, oTask):
        oTask.mapId = "pollutant_map"
        super().handleTask(oTask)