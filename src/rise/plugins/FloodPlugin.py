import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.MapRepository import MapRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.RisePlugin import RisePlugin


class FloodPlugin(RisePlugin):
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)

    def triggerNewAreaMaps(self):
        logging.debug("FloodPlugin.triggerNewAreaMaps")

        try:
            oMapRepository = MapRepository()
            aoMaps = oMapRepository.findAllMapsById(self.m_oPlugin.maps)

            for oMap in aoMaps:
                logging.info("Starting Archive for map " + oMap.name)
                if oMap.id == "sar_flood":
                    self.runHasardLastWeek(oMap)
                elif oMap.id == "viirs_flood":
                    self.runViirsLastWeek(oMap)

        except Exception as oEx:
            logging.error("FloodPlugin.triggerNewAreaMaps: exception " + str(oEx))

    def runHasardLastWeek(self, oMap):
        try:
            sWorkspaceId = self.createOrOpenWorkspace(oMap)

            aoSarArchiveParameters = None

            oMapConfig = None

            for oMapConfig in self.m_oPluginConfig.maps:
                if oMapConfig.id == oMap.id:
                    aoSarArchiveParameters = oMapConfig.params
                    break

            if aoSarArchiveParameters is None:
                logging.warning("FloodPlugin.runHasardLastWeek: impossible to find parameters for map " + oMap.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMap.id, self.m_oPlugin.id, sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"]:
                                logging.info("FloodPlugin.runHasardLastWeek: task already on-going")
                                return True

            aoSarArchiveParameters = vars(aoSarArchiveParameters)
            aoSarArchiveParameters["BBOX"] = self.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

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
            oWasdiTask.pluginId = self.m_oPlugin.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoSarArchiveParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = True

            oWasdiTaskRepository.add(oWasdiTask)
            logging.info("FloodPlugin.runHasardLastWeek: Started " + oMapConfig.processor + " in Workspace " + self.getWorkspaceName(oMap) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("FloodPlugin.runHasardLastWeek: exception " + str(oEx))


    def runViirsLastWeek(self, oMap):
        try:
            sWorkspaceId = self.createOrOpenWorkspace(oMap)

            aoViirsArchiveParameters = None

            oMapConfig = None

            for oMapConfig in self.m_oPluginConfig.maps:
                if oMapConfig.id == oMap.id:
                    aoViirsArchiveParameters = oMapConfig.params
                    break

            if aoViirsArchiveParameters is None:
                logging.warning("FloodPlugin.runViirsLastWeek: impossible to find parameters for map " + oMap.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMap.id, self.m_oPlugin.id,
                                                                sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"]:
                                logging.info("FloodPlugin.runViirsLastWeek: task already on-going")
                                return True

            aoViirsArchiveParameters = vars(aoViirsArchiveParameters)
            aoViirsArchiveParameters["BBOX"] = self.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

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
            oWasdiTask.pluginId = self.m_oPlugin.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoViirsArchiveParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = True

            oWasdiTaskRepository.add(oWasdiTask)
            logging.info(
                "FloodPlugin.runHasardLastWeek: Started " + oMapConfig.processor + " in Workspace " + self.getWorkspaceName(oMap) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("FloodPlugin.runHasardLastWeek: exception " + str(oEx))


