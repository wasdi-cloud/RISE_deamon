import logging

from src.rise.plugins.RisePlugin import RisePlugin


class RainPlugin(RisePlugin):
    """

    """
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)

    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RainPlugin.updateNewMaps OVERRIDE")
        try:
            oMapEngine = self.m_aoMapEngines[0]
            logging.info("RainPlugin.updateNewMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
            oMapEngine.updateNewMaps()

        except Exception as oEx:
            logging.error("RisePlugin.updateNewMaps: exception " + str(oEx))


    def handleTask(self, oTask):
        oTask.mapId = "imerg_cumulate_12"
        super().handleTask(oTask)