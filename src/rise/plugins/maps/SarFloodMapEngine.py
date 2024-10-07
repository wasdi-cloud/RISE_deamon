import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class SarFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.runHasardLastWeek(self.m_oMapEntity)

    def runHasardLastWeek(self, oMap):
        try:
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(oMap)

            aoSarArchiveParameters = None

            oMapConfig = None

            for oMapConfig in self.m_oPluginConfig.maps:
                if oMapConfig.id == oMap.id:
                    aoSarArchiveParameters = oMapConfig.params
                    break

            if aoSarArchiveParameters is None:
                logging.warning("SarFloodMapEngine.runHasardLastWeek: impossible to find parameters for map " + oMap.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMap.id, self.m_oPluginEntity.id,
                                                                sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"]:
                                logging.info("SarFloodMapEngine.runHasardLastWeek: task already on-going")
                                return True

            aoSarArchiveParameters = vars(aoSarArchiveParameters)
            aoSarArchiveParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()
            iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoSarArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            aoSarArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoSarArchiveParameters["MOSAICBASENAME"] = self.m_oArea.id.replace("-", "") + oMap.id.replace("_", "")

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoSarArchiveParameters)
            oWasdiTask = WasdiTask()
            oWasdiTask.areaId = self.m_oArea.id
            oWasdiTask.mapId = oMap.id
            oWasdiTask.id = sProcessorId
            oWasdiTask.pluginId = self.m_oPluginEntity.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoSarArchiveParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = True

            oWasdiTaskRepository.addEntity(oWasdiTask)
            logging.info(
                "SarFloodMapEngine.runHasardLastWeek: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                    oMap) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.runHasardLastWeek: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            logging.info("SarFloodMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleTask: exception " + str(oEx))

