import datetime
from  datetime import timedelta
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ActiveFireMapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        # Take today
        iEnd = datetime.datetime.today()
        # Get the map config
        oMapConfig = self.getMapConfig()
        # And go back of the number of days of the short archive
        iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
        # Ok set the values!
        sStartDate = iStart.strftime("%Y-%m-%d")
        sEndDate = iEnd.strftime("%Y-%m-%d")

        # Open the workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        # Get the parameters from the config
        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:
            # Set the parameters
            aoParameters["bbox"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            aoParameters["STARTDATE"] = sStartDate
            aoParameters["ENDDATE"] = sEndDate
            aoParameters["COMPOSITE"] = False

            # Start the processor
            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)
            if not self.checkProcessorId(sProcessorId):
                return
            
            # Create the task with isShortArchive = True
            oWasdiTask = self.createNewTask(sProcessorId, sWorkspaceId, aoParameters, oMapConfig.processor, sStartDate, True)

            oWasdiTaskRepository = WasdiTaskRepository()
            oWasdiTaskRepository.addEntity(oWasdiTask)
            logging.info("ActiveFireMapEngine.triggerNewAreaMaps [" + self.m_oArea.name +"]: Started " + oMapConfig.processor + " from " + sStartDate + " to " + sEndDate)
        else:
            logging.warning("ActiveFireMapEngine.triggerNewAreaMaps [" + self.m_oArea.name +"]: simulation mode on - we do not run nothing")


    def triggerNewAreaArchives(self):
        logging.info("ActiveFireMapEngine.triggerNewAreaArchives [" + self.m_oArea.name +"]: Currently not developed.")

    def getMapNameForDate(self, sDate):
        sBaseName = self.getBaseName()
        sMapName = "ActiveFire_" + sBaseName + "_" + sDate + ".tif"
        return sMapName

    def updateNewMaps(self):
        logging.info("ActiveFireMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Update New Maps")

        if not self.isShortArchiveFinished():
            logging.info("ActiveFireMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Short Archive is still running, we wait it to finish before starting new daily maps.")
            return

        # Open the workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        asFiles = wasdi.getProductsByActiveWorkspace()

        # Check if we have to run for today
        oToday = datetime.datetime.now(datetime.UTC)
        sDay = oToday.strftime("%Y-%m-%d")

        sTodayMapName = self.getMapNameForDate(sDay)
        if sTodayMapName not in asFiles:
            self.runForDate(sDay, sWorkspaceId)
        else:
            logging.info("ActiveFireMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Today's map already exists, no need to run again.")

        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")

        sYesterdayMapName = self.getMapNameForDate(sYesterday)
        if sYesterdayMapName not in asFiles:
            self.runForDate(sYesterday, sWorkspaceId)
        else:
            logging.info("ActiveFireMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Yesterday's map already exists, no need to run again.")

    def runForDate(self, sDate, sWorkspaceId):
        logging.info("ActiveFireMapEngine.runForDate [" + self.m_oArea.name +"]: Update New Maps for date " + sDate)

        oMapConfig = self.getMapConfig()

        # Did we already start any map for this date?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, "active_fire_map",
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sDate)
        # if we have existing tasks
        for oTask in aoExistingTasks:
            if self.isRunningStatus(oTask.status):
                logging.info("ActiveFireMapEngine.runForDate [" + self.m_oArea.name +"]: a task is still ongoing  for day " + sDate + " we will wait it to finish " + oTask.id)
                return
        
        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:
            aoParameters["bbox"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            aoParameters["STARTDATE"] = sDate
            aoParameters["ENDDATE"] = sDate

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)
            if not self.checkProcessorId(sProcessorId):
                return
            
            oWasdiTask = self.createNewTask(sProcessorId, sWorkspaceId, aoParameters, oMapConfig.processor, sDate)
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("ActiveFireMapEngine.runForDate [" + self.m_oArea.name +"]: Started " + oMapConfig.processor + " for " + sDate)
        else:
            logging.warning("ActiveFireMapEngine.runForDate [" + self.m_oArea.name +"]: simulation mode on - we do not run nothing")


    def handleTask(self, oTask):
        try:
            # First, we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: handle task " + oTask.id)

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is None:
                logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: cannot read the payload, we stop here ")
                return

            if "Daily Fire Maps" not in aoPayload:
                logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: Daily Fire Maps not in the payload, we stop here ")
                return

            asDailyFireMaps = aoPayload["Daily Fire Maps"]

            if len(asDailyFireMaps) <= 0:
                logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: Daily Fire Maps array is empty, we stop here ")
                return

            
            # checking if the file really exist in the wasdi product list
            asFiles = wasdi.getProductsByActiveWorkspace()

            if oTask.isShortArchive:
                logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: This is a short archive task, publishing all daily fire maps ")

                for sFile in asDailyFireMaps:
                    if sFile not in asFiles:
                        # should exist
                        logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: the map " + sFile + " does not exist in the product list ,something is wrong, we stop here ")
                        continue

                    sReferenceDate = ""
                    asNameParts = sFile.split("_")
                    if len(asNameParts) >= 3:
                        sReferenceDate = asNameParts[-1].replace(".tif","")
                    
                    if sReferenceDate == "":
                        logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: cannot extract the reference date from the file name , we skip it " + sFile)
                        continue

                    oReferenceDate = datetime.datetime.strptime(sReferenceDate, "%Y-%m-%d")
                    oMapConfig = self.getMapConfig()

                    self.addAndPublishLayer(sFile, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                            bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                            sResolution=oMapConfig.resolution, sInputData=oMapConfig.inputData)

            else:
                # Take the first one item from asDailyFireMap
                logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: This is a single day task, publishing only the first daily fire map ")
                sFile = asDailyFireMaps[0]

                if sFile not in asFiles:
                    # should exist
                    logging.info("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: the map does not exist in the product list ,something is wrong, we stop here ")
                    return

                oReferenceDate = datetime.datetime.strptime(oTask.referenceDate, "%Y-%m-%d")
                sMapConfig = "active_fire_map"
                oMapConfig = self.getMapConfig(sMapConfig)

                self.addAndPublishLayer(sFile, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                        bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                        sResolution=oMapConfig.resolution, sInputData=oMapConfig.inputData,
                                        sOverrideMapId=sMapConfig)

        except Exception as oEx:
            logging.error("ActiveFireMapEngine.handleTask [" + self.m_oArea.name +"]: exception " + str(oEx))
