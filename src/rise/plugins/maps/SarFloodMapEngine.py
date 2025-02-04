import json
import logging
import uuid
from datetime import datetime, timedelta

import wasdi

from src.rise.business.Event import Event
from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.EventRepository import EventRepository
from src.rise.data.LayerRepository import LayerRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class SarFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)
        self.m_sChainParamsFile = "integratedSarChainParams.json"

    def triggerNewAreaMaps(self):
        '''
        Trigger the execution of the last period for a new Area
        :return:
        '''
        self.runIntegratedArchive(False)

    def triggerNewAreaArchives(self):
        '''
        Trigger the execution of the archive for a new Area
        :return:
        '''
        if self.m_oArea.supportArchive:
            self.runIntegratedArchive(True)

    def runIntegratedArchive(self, bFullArchive):
        '''
        Executes the integrated sar flood archive for a short or long period
        :param bFullArchive:
        :return:
        '''
        try:
            # Create or open the right workspace (this user, this area, this plugin)
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

            # And take the configuration of the integrated_archive Map
            oMapConfig = self.getMapConfig("integrated_archive")

            if oMapConfig is None:
                logging.warning("SarFloodMapEngine.runIntegratedArchive: impossible to find configuration for map " + self.m_oMapEntity.id)
                return

            aoIntegratedArchiveParameters = oMapConfig.params

            if aoIntegratedArchiveParameters is None:
                logging.warning("SarFloodMapEngine.runIntegratedArchive: impossible to find parameters for map " + self.m_oMapEntity.id)
                return

            # We need to check if the task is alredy ongoing
            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id, self.m_oPluginEntity.id, sWorkspaceId)

            if len(aoExistingTasks) > 0:
                for oTask in aoExistingTasks:
                    if "integratedArchive" in oTask.pluginPayload and "fullArchive" in oTask.pluginPayload:
                        if oTask.pluginPayload["fullArchive"] == bFullArchive:
                            logging.info("SarFloodMapEngine.runIntegratedArchive: task already on-going")
                            return True

            # We need to prepare the parameters for the integrated archive
            aoIntegratedArchiveParameters = vars(aoIntegratedArchiveParameters)
            aoIntegratedArchiveParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            if bFullArchive:
                # Take today
                iEnd = datetime.today()
                # The short archive will arrive until this date
                iEnd = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                # Set from the declared beginning
                aoIntegratedArchiveParameters["ARCHIVE_START_DATE"] = oMapConfig.startArchiveDate
                # Until the end date of the short archive
                aoIntegratedArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            else:
                # Take today
                iEnd = datetime.today()
                # And go back of the number of days of the short archive
                iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                # Ok set the values!
                aoIntegratedArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
                aoIntegratedArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")

            aoIntegratedArchiveParameters["MOSAICBASENAME"] = self.getBaseName()

            # We need also the builings plugin config
            oPluginConfig = self.m_oPluginEngine.getPluginConfig()
            sUrbanMapsPluginId = "rise_building_plugin"

            if oPluginConfig is not None:
                try:
                    sUrbanMapsPluginId = oPluginConfig.building_plugin_id
                except:
                    pass

            sUrbanMapsMapId = "building_cw"
            if oPluginConfig is not None:
                try:
                    sUrbanMapsMapId = oPluginConfig.building_map_id
                except:
                    pass

            # We need to interact with the buildings. Here we pass to the app the workspace and the area name
            sUrbanMapsWorkspaceName = self.m_oArea.id + "|" + sUrbanMapsPluginId + "|" + sUrbanMapsMapId
            aoIntegratedArchiveParameters["URBAN_MAPS_WS_NAME"] = sUrbanMapsWorkspaceName
            aoIntegratedArchiveParameters["areaName"] = self.m_oArea.id.replace("-", "") + sUrbanMapsMapId.replace("_", "")

            if not self.m_oConfig.daemon.simulate:
                sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoIntegratedArchiveParameters)
                oWasdiTask = WasdiTask()
                oWasdiTask.areaId = self.m_oArea.id
                oWasdiTask.mapId = self.m_oMapEntity.id
                oWasdiTask.id = sProcessorId
                oWasdiTask.pluginId = self.m_oPluginEntity.id
                oWasdiTask.workspaceId = sWorkspaceId
                oWasdiTask.startDate = datetime.now().timestamp()
                oWasdiTask.inputParams = aoIntegratedArchiveParameters
                oWasdiTask.status = "CREATED"
                oWasdiTask.application = oMapConfig.processor
                oWasdiTask.referenceDate = ""
                oWasdiTask.pluginPayload["integratedArchive"] = True
                oWasdiTask.pluginPayload["fullArchive"] = bFullArchive

                oWasdiTaskRepository.addEntity(oWasdiTask)
                logging.info(
                    "SarFloodMapEngine.runIntegratedArchive: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                        self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("SarFloodMapEngine.runIntegratedArchive: simulation mode on - we do not run nothing")

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.runIntegratedArchive: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("SarFloodMapEngine.handleTask: handle task " + oTask.id)

            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            if len(asWorkspaceFiles) == 0:
                logging.warning("SarFloodMapEngine.handleTask: we do not have files in the workspace... ")
                # In any case, this task is done
                oTask.status = "DONE"
                oTaskRepository = WasdiTaskRepository()
                oTaskRepository.updateEntity(oTask)
                return False

            # This was the short or long archive?
            if oTask.application == "integrated_sar_flood_archive" or oTask.application == "sar_archive_generator":
                bFullArchive = False
                if "fullArchive" in oTask.pluginPayload:
                    bFullArchive = oTask.pluginPayload["fullArchive"]

                return self.handleArchiveTask(oTask, asWorkspaceFiles, bFullArchive)
            else:
                # This was a daily map
                return self.handleDailyTask(oTask, asWorkspaceFiles)
        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleTask: exception " + str(oEx))
            return False
        finally:
            # In any case, this task is done
            oTask.status = "DONE"
            oTaskRepository = WasdiTaskRepository()
            oTaskRepository.updateEntity(oTask)

    def handleDailyTask(self, oTask, asWorkspaceFiles):
        try:
            sDate = oTask.referenceDate
            sBaseName = oTask.inputParams["MOSAICBASENAME"]
            sFileName = sBaseName + "_" + sDate + "_flood.tif"
            oDate = datetime.strptime(sDate,"%Y-%m-%d")
            oMapConfig = self.getMapConfig("autofloodchain2")

            if sFileName in asWorkspaceFiles:
                self.addAndPublishLayer(sFileName, oDate, True, "autofloodchain2", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleDailyTask: exception " + str(oEx))

    def handleEvents(self, sEventFinderId):
        """
        Handle the events found by Integrated SAR Archive
        :param sEventFinderId:
        :return:
        """

        try:
            # Get the event finder payload
            aoEventFinderPayload = wasdi.getProcessorPayloadAsJson(sEventFinderId)

            # List of events
            aoEvents = []
            if "OUTPUT" in aoEventFinderPayload["EventFinderOutputs"]:
                aoEvents = aoEventFinderPayload["EventFinderOutputs"]["OUTPUT"]

            # List of Urban Maps
            asUrbanMaps =  []
            if "UrbanMaps" in aoEventFinderPayload:
                asUrbanMaps = aoEventFinderPayload["UrbanMaps"]

            # List of composites
            asCompositeMaps = []
            if "CompositeMaps" in aoEventFinderPayload:
                asCompositeMaps = aoEventFinderPayload["CompositeMaps"]

            oEventRepository = EventRepository()

            # For each event
            for iEvent in range(len(aoEvents)):
                try:
                    oEvent = aoEvents[iEvent]
                    sUrbanMap = asUrbanMaps[iEvent]
                    sCompositeMap = asCompositeMaps[iEvent]

                    oActualDate = datetime.strptime(oEvent["peakDate"], '%d-%m-%Y')

                    oMapConfig = self.getMapConfig("urban_flood")
                    self.addAndPublishLayer(sUrbanMap, oActualDate, True, "urban_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)
                    oMapConfig = self.getMapConfig("flood_composite")
                    self.addAndPublishLayer(sCompositeMap, oActualDate, True, "flood_composite", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

                    oEventEntity = Event()
                    oEventEntity.name= "Flood_" + oEvent["peakDate"]
                    oEventEntity.type = "flood"
                    oEventEntity.bbox = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox)
                    oEventEntity.startDate = oEvent["startDate"]
                    oEventEntity.peakDate = oEvent["peakDate"]
                    oEventEntity.endDate = oEvent["endDate"]
                    oEventEntity.areaId = self.m_oArea.id
                    oEventEntity.id = str(uuid.uuid4())
                    oEventRepository.addEntity(oEventEntity)

                except Exception as oEx:
                    logging.error("SarFloodMapEngine.handleEvents: error creating events " + str(oEx))

        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleEvents: error " + str(oEx))
        return

    def handleArchiveTask(self, oTask, asWorkspaceFiles, bFullArchive):

        fFirstMapTimestamp = -1.0
        fLastMapTimestamp = -1.0

        try:
            logging.info("SarFloodMapEngine.handleArchiveTask: task done, lets proceed!")

            # We take the base name, start and end
            sBaseName = oTask.inputParams["MOSAICBASENAME"]
            sStartDate = oTask.inputParams["ARCHIVE_START_DATE"]
            sEndDate = oTask.inputParams["ARCHIVE_END_DATE"]

            try:
                oStartDay = datetime.strptime(sStartDate, '%Y-%m-%d')
            except:
                logging.error('SarFloodMapEngine.handleArchiveTask: Start Date not valid')
                return False

            try:
                oEndDay = datetime.strptime(sEndDate, '%Y-%m-%d')
            except:
                logging.error('SarFloodMapEngine.handleArchiveTask: End Date not valid')
                return False

            oTimeDelta = timedelta(days=1)
            oActualDate = oStartDay

            # For each date of the archive
            while oActualDate <= oEndDay:
                sDate = oActualDate.strftime("%Y-%m-%d")
                sFileName = sBaseName + "_" +sDate + "_flood.tif"

                # If the file is in the workspace
                if sFileName not in asWorkspaceFiles:
                    oActualDate = oActualDate + oTimeDelta
                    continue

                logging.info("SarFloodMapEngine.handleArchiveTask: Found " + sFileName + ", add the layer to db")

                oMapConfig = self.getMapConfig("sar_flood")
                oLayer = self.addAndPublishLayer(sFileName, oActualDate, not bFullArchive, self.getStyleForMap(), sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

                if oLayer is None:
                    logging.warning("SarFloodMapEngine.handleArchiveTask: layer not good!")
                    continue

                if fFirstMapTimestamp == -1.0:
                    fFirstMapTimestamp = oLayer.referenceDate
                elif oLayer.referenceDate < fFirstMapTimestamp:
                    fFirstMapTimestamp = oLayer.referenceDate

                if fLastMapTimestamp == -1.0:
                    fLastMapTimestamp = oLayer.referenceDate
                elif oLayer.referenceDate > fLastMapTimestamp:
                    fLastMapTimestamp = oLayer.referenceDate

                oActualDate = oActualDate + oTimeDelta

            # Read the payload of the integrated sar archive
            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            # Get the id of sar archive...
            sSarArchiveId = aoPayload["sar_archive"]["procId"]
            # ... apply permanet water ...
            sPermWaterId = aoPayload["perm_water"]["procId"]
            # ... and Event Finder
            sEventFinderId = aoPayload["event_finder"]["procId"]

            aoChainParams = {}

            # We read the SAR Archive payload to get the orbits found
            aoSarPayload = wasdi.getProcessorPayloadAsJson(sSarArchiveId)
            aoChainParams["orbits"] = aoSarPayload["orbits"]
            aoChainParams["CopDemMap"] = ""
            if "CopDemMap" in aoSarPayload:
                aoChainParams["CopDemMap"] = aoSarPayload["CopDemMap"]

            # We read the apply permanent water map to get the name of the high res water map
            aoChainParams["water_map"] = "HighResWaterMap.tif"
            aoPermWaterPayload = wasdi.getProcessorPayloadAsJson(sPermWaterId)

            if aoPermWaterPayload is not None:
                if "water_map" in aoPermWaterPayload:
                    aoChainParams["water_map"] = aoPermWaterPayload["water_map"]

            self.handleEvents(sEventFinderId)

            # Previous version, if available
            aoOldChainParams = self.getWorkspaceUpdatedJsonFile(self.m_sChainParamsFile, True)

            if aoOldChainParams is not None:
                if "lastMapDate" in aoOldChainParams:
                    sOldLastMapDate = aoOldChainParams["lastMapDate"]
                    if sEndDate>sOldLastMapDate:
                        sEndDate = sOldLastMapDate

            aoChainParams["lastMapDate"] = sEndDate

            # Take a local copy
            sJsonFilePath = wasdi.getPath(self.m_sChainParamsFile)

            # Now we write the new json
            with open(sJsonFilePath, "w") as oFile:
                json.dump(aoChainParams, oFile)

            # And we add it, updated, to WASDI
            wasdi.addFileToWASDI(self.m_sChainParamsFile)

            # notify users
            self.notifyEndOfTask(oTask.areaId, True)

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleArchiveTask: exception " + str(oEx))
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


    def startDailySARFloodDetection(self, sDay, oMapConfig, aoFloodChainParameters):
        # Open our workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId, oMapConfig.processor, sDay)

        sTodayTaskId = None
        iTimestamp=0

        # if we have existing tasks
        for oTask in aoExistingTasks:

            if self.isRunningStatus(oTask.status):
                logging.info("SarFloodMapEngine.updateNewMaps: a task is still ongoing " + oTask.id)
                return
            elif self.isDoneStatus(oTask.status):
                if oTask.startDate>iTimestamp:
                    sTodayTaskId = oTask.id
                    iTimestamp = oTask.startDate

        bForceReRun = False
        bStillToRun = False

        # Now we try to read the params saved in the workspace
        aoIntegratedChainParams = self.getWorkspaceUpdatedJsonFile(self.m_sChainParamsFile, False)

        sChainOrbits = ""
        sWaterMap = ""
        sCopDemMap = ""
        sLastMapDate = ""

        if aoIntegratedChainParams is not None:
            sChainOrbits = aoIntegratedChainParams["orbits"]
            sWaterMap = aoIntegratedChainParams["water_map"]
            sCopDemMap = aoIntegratedChainParams["CopDemMap"]
            sLastMapDate = aoIntegratedChainParams["lastMapDate"]

        # If we have a task
        if sTodayTaskId is not None:
            logging.info("SarFloodMapEngine.updateNewMaps: We already ran once today: read the payload of the application " + sTodayTaskId)
            # And if it is done

            # We need to check if there are new images: take the payload
            aoFloodChainPayload = wasdi.getProcessorPayloadAsJson(sTodayTaskId)

            # Take the orbits
            asOrbits = sChainOrbits.split(",")

            if "ResultsPerOrbit" in aoFloodChainPayload:
                # For each orbit
                for sOrbit in asOrbits:

                    if sOrbit == "":
                        continue

                    # For each result
                    oSelectedResult = None
                    for oResultPerOrbit in aoFloodChainPayload["ResultsPerOrbit"]:
                        try:
                            # If this is our orbit
                            if oResultPerOrbit["orbit"]==sOrbit:
                                #Select it
                                oSelectedResult = oResultPerOrbit
                                break
                        except:
                            pass

                    iResults = 0
                    if oSelectedResult is not None:
                        try:
                            iResults = oSelectedResult["images"]
                        except:
                            pass

                    # Search the images
                    aoImages = wasdi.searchEOImages("S1", sDay, sDay, iOrbitNumber=int(sOrbit), sProductType="GRD", oBoundingBox=self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True))

                    # Do we have results?
                    if aoImages is not None:
                        # Are more than before?
                        if len(aoImages)>iResults:
                            # We definitly need to re-run
                            logging.info("SarFloodMapEngine.updateNewMaps: Found new images available for Orbit " + sOrbit + " set Force Re-Run = True")
                            bForceReRun = True
        else:
            logging.info("This is the first run of the day")
            bStillToRun = True

        if bForceReRun or bStillToRun:

            if not self.m_oConfig.daemon.simulate:
                aoFloodChainParameters = vars(aoFloodChainParameters)
                aoFloodChainParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
                aoFloodChainParameters["MOSAICBASENAME"] = self.getBaseName()
                aoFloodChainParameters["ENDDATE"] = sDay
                aoFloodChainParameters["FORCE_RE_RUN"] = True
                aoFloodChainParameters["ORBITS"] = sChainOrbits

                if sWaterMap != "":
                    aoFloodChainParameters["PERMANENT_WATER_MAP_NAME"] = sWaterMap

                if sCopDemMap != "":
                    aoFloodChainParameters["copdem_wm"] = sCopDemMap

                sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoFloodChainParameters)

                oWasdiTask = WasdiTask()
                oWasdiTask.areaId = self.m_oArea.id
                oWasdiTask.mapId = self.m_oMapEntity.id
                oWasdiTask.id = sProcessorId
                oWasdiTask.pluginId = self.m_oPluginEntity.id
                oWasdiTask.workspaceId = sWorkspaceId
                oWasdiTask.startDate = datetime.now().timestamp()
                oWasdiTask.inputParams = aoFloodChainParameters
                oWasdiTask.status = "CREATED"
                oWasdiTask.application = oMapConfig.processor
                oWasdiTask.referenceDate = sDay

                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info(
                    "SarFloodMapEngine.updateNewMaps: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                        self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("SarFloodMapEngine.updateNewMaps: simulation mode on - we do not run nothing")


    def updateNewMaps(self):
        # Open our workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        # Get the config to run a single day auto flood chain
        oMapConfig = self.getMapConfig("autofloodchain2")

        # without this config we have a problem
        if oMapConfig is None:
            logging.warning("SarFloodMapEngine.updateNewMaps: impossible to find configuration for map " + self.m_oMapEntity.id)
            return

        aoFloodChainParameters = oMapConfig.params

        # Well, we need the params in the config
        if aoFloodChainParameters is None:
            logging.warning(
                "SarFloodMapEngine.updateNewMaps: impossible to find parameters for map " + self.m_oMapEntity.id)
            return

        oToday = datetime.today()
        sToday = oToday.strftime("%Y-%m-%d")
        self.startDailySARFloodDetection(sToday,oMapConfig,aoFloodChainParameters)

        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")
        self.startDailySARFloodDetection(sYesterday,oMapConfig,aoFloodChainParameters)