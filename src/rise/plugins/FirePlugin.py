import logging
from src.rise.plugins.RisePlugin import RisePlugin


class FirePlugin(RisePlugin):
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)



    def triggerNewAreaMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("FirePlugin.triggerNewAreaMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "active_fire_map":
                    logging.info("FirePlugin.triggerNewAreaMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaMaps()
                    break

        except Exception as oEx:
            logging.error("FirePlugin.triggerNewAreaMaps: exception " + str(oEx))
            
            
    
    def triggerNewAreaArchives(self):
        logging.info("FirePlugin.triggerNewAreaArchives: long archive is handled by the integrated chain")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "active_fire_map":
                    logging.info("FirePlugin.triggerNewAreaArchives: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaArchives()
                    break

        except Exception as oEx:
            logging.error("FirePlugin.triggerNewAreaArchives: exception " + str(oEx))


    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("FirePlugin.updateNewMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "active_fire_map":
                    logging.info("FirePlugin.updateNewMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.updateNewMaps()
                    break

        except Exception as oEx:
            logging.error("FirePlugin.updateNewMaps: exception " + str(oEx))        


    def handleTask(self, oTask):
        oTask.mapId = "active_fire_map"
        super().handleTask(oTask)