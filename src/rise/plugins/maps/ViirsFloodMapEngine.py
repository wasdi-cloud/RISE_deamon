import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ViirsFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.runViirsLastWeek(self.m_oMapEntity)

    def runViirsLastWeek(self, oMap):
        try:
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(oMap)

            aoViirsArchiveParameters = None

            oMapConfig = None

            for oMapConfig in self.m_oPluginConfig.maps:
                if oMapConfig.id == oMap.id:
                    aoViirsArchiveParameters = oMapConfig.params
                    break

            if aoViirsArchiveParameters is None:
                logging.warning("ViirsFloodMapEngine.runViirsLastWeek: impossible to find parameters for map " + oMap.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMap.id, self.m_oPluginEntity.id,
                                                                sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"]:
                                logging.info("ViirsFloodMapEngine.runViirsLastWeek: task already on-going")
                                return True

            aoViirsArchiveParameters = vars(aoViirsArchiveParameters)
            aoViirsArchiveParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()
            iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoViirsArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            aoViirsArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoViirsArchiveParameters["MOSAICBASENAME"] = self.m_oArea.id.replace("-", "") + oMap.id.replace("_", "")

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoViirsArchiveParameters)
            oWasdiTask = WasdiTask()
            oWasdiTask.areaId = self.m_oArea.id
            oWasdiTask.mapId = oMap.id
            oWasdiTask.id = sProcessorId
            oWasdiTask.pluginId = self.m_oPluginEntity.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoViirsArchiveParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = True

            oWasdiTaskRepository.addEntity(oWasdiTask)
            logging.info(
                "ViirsFloodMapEngine.runViirsLastWeek: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(oMap) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.runViirsLastWeek: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            logging.info("ViirsFloodMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.handleTask: exception " + str(oEx))
