import logging
import os
from pathlib import Path

from src.rise.RiseDeamon import RiseDeamon


class RiseMapEngine:

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        self.m_oConfig = oConfig
        self.m_oArea = oArea
        self.m_oPluginEntity = oPlugin
        self.m_oPluginEngine = oPluginEngine
        self.m_oPluginConfig = None
        self.m_oMapEntity = oMap

        try:
            oParentPath = Path(oConfig.myFilePath).parent
            oPluginConfigPath = oParentPath.joinpath(oPlugin.id + ".json")
            if os.path.isfile(oPluginConfigPath):
                self.m_oPluginConfig = RiseDeamon.readConfigFile(oPluginConfigPath)

        except Exception as oEx:
            logging.error("RiseMapEngine.init: exception " + str(oEx))

    def triggerNewAreaMaps(self):
        logging.info("RiseMapEngine.triggerNewAreaMaps")

    def getName(self):
        if self.m_oMapEntity is not None:
            return self.m_oMapEntity.name
        return ""

    def handleTask(self, oTask):
        logging.info("RiseMapEngine.handleTask")