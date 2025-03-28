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

    def triggerNewAreaArchives(self):
        self.runBuildingsArchive(False)

    def runBuildingsArchive(self, bShortArchive):
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
                            if oTask.pluginPayload["shortArchive"] == bShortArchive:
                                logging.info("BuildingMapEngine.runBuildingsArchive: task already on-going")
                                return True

            aoAppParameters = vars(aoAppParameters)
            aoAppParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()

            if bShortArchive:
                iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                aoAppParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            else:
                aoAppParameters["ARCHIVE_START_DATE"] = oMapConfig.startArchiveDate
                iEnd = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoAppParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoAppParameters["OUTPUT_BASENAME"] = self.getBaseName()

            if not self.m_oConfig.daemon.simulate:
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
                oWasdiTask.pluginPayload["shortArchive"] = bShortArchive
                oWasdiTask.application = oMapConfig.processor
                oWasdiTask.referenceDate = ""

                oWasdiTaskRepository.addEntity(oWasdiTask)
                logging.info(
                    "BuildingMapEngine.runBuildingsArchive: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                        self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("SarFloodMapEngine.runBuildingsArchive: simulation mode on - we do not run nothing")

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.runHasardArchive: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            logging.info("BuildingMapEngine.handleTask: handle task " + oTask.id)

            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            for sFile in asWorkspaceFiles:
                asNameParts = sFile.split("_")
                bAddFile = True
                if len(asNameParts) != 3:
                    bAddFile = False
                if len(asNameParts) < 3:
                    bAddFile = False
                if asNameParts[0] != self.getBaseName():
                    bAddFile = False
                if asNameParts[2] != "Urban.tif":
                    bAddFile = False

                if bAddFile:
                    logging.info("BuildingMapEngine.handleTask: found building map " + sFile)
                    oActualDate = datetime.strptime(asNameParts[1], "%Y-%m-%d")
                    oMapConfig = self.getMapConfig("building_cw")
                    self.addAndPublishLayer(sFile, oActualDate, True, self.getStyleForMap(), True, sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)
                else:
                    logging.info("BuildingMapEngine.handleTask: delete not building map file " + sFile)
                    wasdi.deleteProduct(sFile)
        except Exception as oEx:
            logging.error("BuildingMapEngine.handleTask: exception " + str(oEx))
        finally:
            # In any case, this task is done
            oTask.status = "DONE"
            oTaskRepository = WasdiTaskRepository()
            oTaskRepository.updateEntity(oTask)

    def updateNewMaps(self):
        # Open our workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        # Get the config to run a single day auto flood chain
        oMapConfig = self.getMapConfig("citywatch")

        # without this config we have a problem
        if oMapConfig is None:
            logging.warning("ViirsFloodMapEngine.updateNewMaps: impossible to find configuration for map  citywatch")
            return

        asFilesInWorkspace = wasdi.getProductsByActiveWorkspace()

        sLastUrbanMap = ""

        for sFile in asFilesInWorkspace:
            asNameParts = sFile.split("_")
            bIsUrbanMap = True
            if len(asNameParts) != 3:
                bIsUrbanMap = False
            if len(asNameParts) < 3:
                bIsUrbanMap = False
            if asNameParts[0] != self.getBaseName():
                bIsUrbanMap = False
            if asNameParts[2] != "Urban.tif":
                bIsUrbanMap = False

            if bIsUrbanMap:
                sReferenceDate = asNameParts[1]
                if sReferenceDate > sLastUrbanMap:
                    sLastUrbanMap = sReferenceDate

        bStartNewMap = False

        oToday = datetime.now()

        if sLastUrbanMap=="":
            bStartNewMap = True
        else:
            oLastDate = datetime.strptime(sLastUrbanMap,"%Y-%m-%d")
            oDaysSpent = oToday - oLastDate
            iDays = oDaysSpent.days
            if iDays>oMapConfig.shortArchiveDaysBack:
                bStartNewMap = True

        if bStartNewMap:
            sToday = oToday.strftime("%Y-%m-%d")
            # Did we already start any map today?
            oWasdiTaskRepository = WasdiTaskRepository()

            # Take all our task for today
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                                self.m_oPluginEntity.id, sWorkspaceId,
                                                                oMapConfig.processor,
                                                                sToday)

            # if we have existing tasks
            for oTask in aoExistingTasks:
                if self.isRunningStatus(oTask.status):
                    logging.info("ViirsFloodMapEngine.updateNewMaps: a task is still ongoing " + oTask.id)
                    return

            aoCityWatchParameters = oMapConfig.params

            # Well, we need the params in the config
            if aoCityWatchParameters is None:
                logging.warning("BuildingMapEngine.updateNewMaps: impossible to find parameters for map " + self.m_oMapEntity.id)
                return

            if not self.m_oConfig.daemon.simulate:
                aoCityWatchParameters["OUTPUT_BASENAME"] = self.getBaseName()
                aoCityWatchParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
                aoCityWatchParameters["END_DATE"] = sToday
                sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoCityWatchParameters)

                oWasdiTask = WasdiTask()
                oWasdiTask.areaId = self.m_oArea.id
                oWasdiTask.mapId = self.m_oMapEntity.id
                oWasdiTask.id = sProcessorId
                oWasdiTask.pluginId = self.m_oPluginEntity.id
                oWasdiTask.workspaceId = sWorkspaceId
                oWasdiTask.startDate = datetime.now().timestamp()
                oWasdiTask.inputParams = aoCityWatchParameters
                oWasdiTask.status = "CREATED"
                oWasdiTask.application = oMapConfig.processor
                oWasdiTask.referenceDate = sToday

                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info(
                    "BuildingMapEngine.updateNewMaps: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                        self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("BuildingMapEngine.updateNewMaps: simulation mode on - we do not run nothing")
        else:
            logging.info("BuildingMapEngine.updateNewMaps: the last map is of " + sLastUrbanMap + " no need to restart yet")

        return
