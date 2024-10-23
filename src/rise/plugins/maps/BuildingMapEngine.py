import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class BuildingMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.runBuildingsArchive(True)

        if self.m_oArea.supportArchive:
            self.runBuildingsArchive(False)

    def runBuildingsArchive(self, bOnlyLastWeek):
        try:
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

            oMapConfig = self.getMapConfig()

            if oMapConfig is None:
                logging.warning("BuildingMapEngine.runBuildingsArchive: impossible to find configuration for map " + self.m_oMapEntity.id)
                return

            aoAppParameters = oMapConfig.params

            if aoAppParameters is None:
                logging.warning("BuildingMapEngine.runBuildingsArchive: impossible to find parameters for map " + self.m_oMapEntity.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id, self.m_oPluginEntity.id, sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"] == bOnlyLastWeek:
                                logging.info("BuildingMapEngine.runBuildingsArchive: task already on-going")
                                return True

            aoAppParameters = vars(aoAppParameters)
            aoAppParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()

            if bOnlyLastWeek:
                iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                aoAppParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            else:
                aoAppParameters["ARCHIVE_START_DATE"] = oMapConfig.startArchiveDate
                iEnd = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoAppParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoAppParameters["MOSAICBASENAME"] = self.m_oArea.id.replace("-", "") + self.m_oMapEntity.id.replace("_", "")

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoAppParameters)
            oWasdiTask = WasdiTask()
            oWasdiTask.areaId = self.m_oArea.id
            oWasdiTask.mapId = self.m_oMapEntity.id
            oWasdiTask.id = sProcessorId
            oWasdiTask.pluginId = self.m_oPluginEntity.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoAppParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = bOnlyLastWeek

            oWasdiTaskRepository.addEntity(oWasdiTask)
            logging.info(
                "BuildingMapEngine.runBuildingsArchive: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                    self.m_oMapEntity) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.runHasardArchive: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            logging.info("BuildingMapEngine.handleTask: handle task " + oTask.id)
        except Exception as oEx:
            logging.error("BuildingMapEngine.handleTask: exception " + str(oEx))
