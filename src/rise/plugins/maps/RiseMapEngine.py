import logging
import os
from pathlib import Path

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository


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
        try:
            logging.info("RiseMapEngine.handleTask: handle task " + oTask.id)
            oTaskRepo = WasdiTaskRepository()
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
            sNewStatus = wasdi.getProcessStatus(oTask.id)

            if sNewStatus == "ERROR" or sNewStatus == "STOPPED":
                logging.warning("RiseMapEngine.handleTask: the new status is not done but " + sNewStatus + " update status and exit")
                oTask.status = sNewStatus
                oTaskRepo.updateEntity(oTask)
                return False

            if sNewStatus == "DONE":
                logging.info("RiseMapEngine.handleTask: task done, lets proceed!")
                return True
            else:
                logging.info("RiseMapEngine.handleTask: task is still ongoing, for now we do nothing (state = " + sNewStatus + ")")
                return False

        except Exception as oEx:
            logging.error("RiseMapEngine.handleTask: exception " + str(oEx))
            return False