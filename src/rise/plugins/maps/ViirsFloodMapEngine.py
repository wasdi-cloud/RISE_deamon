import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ViirsFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.runViirsArchive(True)

    def triggerNewAreaArchives(self):
        self.runViirsArchive(False)

    def runViirsArchive(self, bShortArchive):
        try:
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

            oMapConfig = self.getMapConfig()

            aoViirsArchiveParameters = oMapConfig.params

            if aoViirsArchiveParameters is None:
                logging.warning("ViirsFloodMapEngine.runViirsArchive: impossible to find parameters for map " + self.m_oMapEntity.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id, self.m_oPluginEntity.id, sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"] == bShortArchive:
                                logging.info("ViirsFloodMapEngine.runViirsArchive: task already on-going")
                                return True

            aoViirsArchiveParameters = vars(aoViirsArchiveParameters)
            aoViirsArchiveParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()

            if bShortArchive:
                iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                aoViirsArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            else:
                aoViirsArchiveParameters["ARCHIVE_START_DATE"] = oMapConfig.startArchiveDate
                iEnd = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoViirsArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoViirsArchiveParameters["VIIRS_BASENAME"] = self.getBaseName()

            if not self.m_oConfig.daemon.simulate:
                sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoViirsArchiveParameters)

                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoViirsArchiveParameters,oMapConfig.processor,"")
                oWasdiTask.pluginPayload["shortArchive"] = bShortArchive

                oWasdiTaskRepository.addEntity(oWasdiTask)
                logging.info(
                    "ViirsFloodMapEngine.runViirsArchive: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("ViirsFloodMapEngine.runViirsArchive: simulation mode on - we do not run nothing")

            return True
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.runViirsArchive: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            if not super().handleTask(oTask):
                return False

            logging.info("ViirsFloodMapEngine.handleTask: handle task " + oTask.id)

            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            if len(asWorkspaceFiles) == 0:
                # In any case, this task is done
                oTask.status = "DONE"
                oTaskRepository = WasdiTaskRepository()
                oTaskRepository.updateEntity(oTask)
                logging.warning("ViirsFloodMapEngine.handleTask: we do not have files in the workspace... ")
                return False

            if oTask.application == "viirs_flood":
                return self.handleDailyMap(oTask, asWorkspaceFiles)
            else:
                bShortArchive = False
                if "shortArchive" in oTask.pluginPayload:
                    bShortArchive = oTask.pluginPayload["shortArchive"]

                return self.handleArchiveTask(oTask, asWorkspaceFiles, bShortArchive)

        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.handleTask: exception " + str(oEx))
            return False

    def handleDailyMap(self, oTask, asWorkspaceFiles):
        try:
            sDate = oTask.referenceDate
            sBaseName = oTask.inputParams["BASENAME"]
            sFileName = sBaseName + "_" + sDate + "_flooded.tif"
            oDate = datetime.strptime(sDate, "%Y-%m-%d")

            oMapConfig = self.getMapConfig("viirs_flood")

            if sFileName in asWorkspaceFiles:
                self.addAndPublishLayer(sFileName, oDate, True, "viirs_daily_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

            oTimeDelta = timedelta(days=1)
            oYesterday = oDate-oTimeDelta
            sDate = oYesterday.strftime("%Y-%m-%d")
            sFileName = sBaseName + "_" + sDate + "_flooded.tif"
            if sFileName in asWorkspaceFiles:
                self.addAndPublishLayer(sFileName, oYesterday, True, "viirs_daily_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleDailyTask: exception " + str(oEx))

    def handleArchiveTask(self, oTask, asWorkspaceFiles, bOnlyLastWeek):

        fFirstMapTimestamp = -1.0
        fLastMapTimestamp = -1.0

        try:
            logging.info("ViirsFloodMapEngine.handleTask: task done, lets proceed!")

            sBaseName = oTask.inputParams["VIIRS_BASENAME"]
            sStartDate = oTask.inputParams["ARCHIVE_START_DATE"]
            sEndDate = oTask.inputParams["ARCHIVE_END_DATE"]

            try:
                oStartDay = datetime.strptime(sStartDate, '%Y-%m-%d')
            except:
                logging.error('ViirsFloodMapEngine.handleShortArchiveTask: Start Date not valid')
                return False

            try:
                oEndDay = datetime.strptime(sEndDate, '%Y-%m-%d')
            except:
                logging.error('ViirsFloodMapEngine.handleShortArchiveTask: End Date not valid')
                return False

            oTimeDelta = timedelta(days=1)

            oActualDate = oStartDay

            while oActualDate <= oEndDay:
                sDate = oActualDate.strftime("%Y-%m-%d")
                sFileName = sBaseName + "_" +sDate + "_flooded.tif"

                if sFileName not in asWorkspaceFiles:
                    oActualDate = oActualDate + oTimeDelta
                    continue

                logging.info("ViirsFloodMapEngine.handleShortArchiveTask: Found " + sFileName + ", publish it")

                oMapConfig = self.getMapConfig("viirs_flood")
                oLayer = self.addAndPublishLayer(sFileName, oActualDate, bOnlyLastWeek, sMapIdForStyle="viirs_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

                if oLayer is not None:
                    if fFirstMapTimestamp == -1.0:
                        fFirstMapTimestamp = oLayer.referenceDate
                    elif oLayer.referenceDate < fFirstMapTimestamp:
                        fFirstMapTimestamp = oLayer.referenceDate

                    if fLastMapTimestamp == -1.0:
                        fLastMapTimestamp = oLayer.referenceDate
                    elif oLayer.referenceDate > fLastMapTimestamp:
                        fLastMapTimestamp = oLayer.referenceDate

                oActualDate = oActualDate + oTimeDelta

            # notify users
            self.notifyEndOfTask(oTask.areaId, True, "Low Res Flooded Area Detection")

            return True
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.handleShortArchiveTask: exception " + str(oEx))
            return False
        finally:
            bChanged = False

            # And if we do not have yet archive start and end date, set it
            if self.m_oArea.archiveStartDate <=0 and fFirstMapTimestamp>0:
                self.m_oArea.archiveStartDate = fFirstMapTimestamp
                bChanged = True

            if self.m_oArea.archiveEndDate <=0 and fLastMapTimestamp>0:
                self.m_oArea.archiveEndDate = fLastMapTimestamp
                bChanged = True

            if bChanged:
                # Update the area if needed
                oAreaRepository = AreaRepository()
                oAreaRepository.updateEntity(self.m_oArea)

    def viirsMapFromDate(self, sToday):

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig("viirs_daily_flood")
        aoViirsParameters = oMapConfig.params
        aoViirsParameters = vars(aoViirsParameters)

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId, oMapConfig.processor,
                                                            sToday)

        # if we have existing tasks
        for oTask in aoExistingTasks:
            if self.isRunningStatus(oTask.status):
                logging.info("ViirsFloodMapEngine.updateNewMaps: a task is still ongoing " + oTask.id)
                return

        sBaseName = self.getBaseName()

        sOutputFileName = sBaseName + "_" + sToday + "_flooded.tif"

        asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

        if sOutputFileName not in asWorkspaceFiles:
            if not self.m_oConfig.daemon.simulate:

                aoViirsParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
                aoViirsParameters["BASENAME"] = sBaseName
                aoViirsParameters["EVENTDATE"] = sToday

                if "HighResWaterMap.tif" in asWorkspaceFiles:
                    aoViirsParameters["HIGH_RES_WATER_MAP"] = "HighResWaterMap.tif"

                sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoViirsParameters)

                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoViirsParameters,oMapConfig.processor,sToday)
                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info("ViirsFloodMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for date " + sToday)
            else:
                logging.warning("ViirsFloodMapEngine.updateNewMaps: simulation mode on - we do not run nothing")
        else:
            logging.info("ViirsFloodMapEngine.updateNewMaps: the VIIRS map for " + sToday + " is already available")

    def updateNewMaps(self):
        # Open our workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        # Get the config to run a single day auto flood chain
        oMapConfig = self.getMapConfig("viirs_daily_flood")

        # without this config we have a problem
        if oMapConfig is None:
            logging.warning("ViirsFloodMapEngine.updateNewMaps: impossible to find configuration for map " + self.m_oMapEntity.id)
            return

        aoViirsParameters = oMapConfig.params

        # Well, we need the params in the config
        if aoViirsParameters is None:
            logging.warning("ViirsFloodMapEngine.updateNewMaps: impossible to find parameters for map " + self.m_oMapEntity.id)
            return

        oToday = datetime.today()
        sToday = oToday.strftime("%Y-%m-%d")
        self.viirsMapFromDate(sToday)

        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")
        self.viirsMapFromDate(sYesterday)

        return
