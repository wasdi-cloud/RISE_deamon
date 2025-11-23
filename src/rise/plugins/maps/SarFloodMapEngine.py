import json
import logging
import uuid
from datetime import datetime, timedelta

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.business.Event import Event
from src.rise.business.WidgetInfo import WidgetInfo
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.EventRepository import EventRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.data.WidgetInfoRepository import WidgetInfoRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine
from src.rise.utils import RiseUtils


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



    def updateNewMaps(self):

        # Check if the initial short archive is finished or not
        if not self.isShortArchiveFinished(sProcessor="integrated_sar_flood_archive"):
            logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: the initial short archive is not yet finished we will wait it to finish")
            return

        # Open our workspace
        self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        # Get the config to run a single day auto flood chain
        oMapConfig = self.getMapConfig("autofloodchain2")

        # without this config we have a problem
        if oMapConfig is None:
            logging.warning("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: impossible to find configuration for map " + self.m_oMapEntity.id)
            return

        aoFloodChainParameters = oMapConfig.params

        # Well, we need the params in the config
        if aoFloodChainParameters is None:
            logging.warning("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: impossible to find parameters for map " + self.m_oMapEntity.id)
            return

        # Open our workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

        oToday = datetime.today()
        sToday = oToday.strftime("%Y-%m-%d")
        self.startDailySARFloodDetection(sToday,oMapConfig,aoFloodChainParameters, sWorkspaceId)

        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")
        self.startDailySARFloodDetection(sYesterday,oMapConfig,aoFloodChainParameters, sWorkspaceId)

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
                logging.warning("SarFloodMapEngine.runIntegratedArchive [" + self.m_oArea.name +"]: impossible to find configuration for map " + self.m_oMapEntity.id)
                return

            aoIntegratedArchiveParameters = oMapConfig.params

            if aoIntegratedArchiveParameters is None:
                logging.warning("SarFloodMapEngine.runIntegratedArchive [" + self.m_oArea.name +"]: impossible to find parameters for map " + self.m_oMapEntity.id)
                return

            # We need to check if the task is alredy ongoing
            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id, self.m_oPluginEntity.id, sWorkspaceId)

            if len(aoExistingTasks) > 0:
                for oTask in aoExistingTasks:
                    if "integratedArchive" in oTask.pluginPayload and "fullArchive" in oTask.pluginPayload:
                        if oTask.pluginPayload["fullArchive"] == bFullArchive:
                            logging.info("SarFloodMapEngine.runIntegratedArchive [" + self.m_oArea.name +"]: task already on-going")
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
                # Until the start date of the short archive
                aoIntegratedArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
                # Suffix of the Flood Frequency Map
                aoIntegratedArchiveParameters["FFM_IDENTIFIER"] = "archiveffm"
            else:
                # Take today
                iEnd = datetime.today()
                # And go back of the number of days of the short archive
                iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                # Ok set the values!
                aoIntegratedArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
                aoIntegratedArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
                # It will take the default suffix for the flood frequency map

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
                if not self.checkProcessorId(sProcessorId):
                    return
                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoIntegratedArchiveParameters,oMapConfig.processor,"")
                oWasdiTask.pluginPayload["integratedArchive"] = True
                oWasdiTask.pluginPayload["fullArchive"] = bFullArchive
                oWasdiTask.isShortArchive = not bFullArchive

                oWasdiTaskRepository.addEntity(oWasdiTask)
                logging.info("SarFloodMapEngine.runIntegratedArchive [" + self.m_oArea.name +"]: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName( self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("SarFloodMapEngine.runIntegratedArchive [" + self.m_oArea.name +"]: simulation mode on - we do not run nothing")

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.runIntegratedArchive [" + self.m_oArea.name +"]: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
               return False
            
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

            logging.info("SarFloodMapEngine.handleTask [" + self.m_oArea.name +"]: handle task " + oTask.id)

            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            if len(asWorkspaceFiles) == 0:
                logging.warning("SarFloodMapEngine.handleTask [" + self.m_oArea.name +"]: we do not have files in the workspace... ")

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
            logging.error("SarFloodMapEngine.handleTask [" + self.m_oArea.name +"]: exception " + str(oEx))
            return False

    def handleDailyTask(self, oTask, asWorkspaceFiles):
        try:
            sDate = oTask.referenceDate
            sBaseName = oTask.inputParams["MOSAICBASENAME"]
            sFileName = sBaseName + "_" + sDate + "_" + oTask.inputParams["SUFFIX"]
            oDate = datetime.strptime(sDate,"%Y-%m-%d")
            oMapConfig = self.getMapConfig("autofloodchain2")

            if sFileName in asWorkspaceFiles:
                self.updateChainParamsDate(self.m_sChainParamsFile, sDate, "lastMapDate")
                self.addAndPublishLayer(sFileName, oDate, True, "autofloodchain2", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleDailyTask [" + self.m_oArea.name +"]: exception " + str(oEx))

    def handleEvents(self, sEventFinderId, asWorkspaceFiles, sSuffix):
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

            # List of Urban Detections
            aoUrbanDetections =  []
            if "UrbanDetections" in aoEventFinderPayload:
                aoUrbanDetections = aoEventFinderPayload["UrbanDetections"]

            # List of composites
            asCompositeMaps = []
            if "CompositeMaps" in aoEventFinderPayload:
                asCompositeMaps = aoEventFinderPayload["CompositeMaps"]

            # List of Urban Detections
            aoCompositeDetections =  []
            if "Composites" in aoEventFinderPayload:
                aoCompositeDetections = aoEventFinderPayload["Composites"]

            # List of impacts detections
            aoImpactDetections = []
            if "ImpactDetections" in aoEventFinderPayload:
                aoImpactDetections = aoEventFinderPayload["ImpactDetections"]


            oEventRepository = EventRepository()
            sBaseName = self.getBaseName("sar_flood")

            oImpactsPluginConfig = RiseDeamon.getPluginConfig("rise_impact_plugin", self.m_oConfig)

            # For each event
            for iEvent in range(len(aoEvents)):
                try:
                    # Get the event and the maps
                    oEvent = aoEvents[iEvent]
                    sUrbanMap = asUrbanMaps[iEvent]
                    sCompositeMap = asCompositeMaps[iEvent]

                    oEventPeakDate = datetime.strptime(oEvent["peakDate"], '%Y-%m-%d')

                    sFloodMap = sBaseName + "_" + oEvent["peakDate"] + "_" + sSuffix 

                    # Check if we have the daily flood map in the workspace
                    if sFloodMap in asWorkspaceFiles:
                        # Ok we can publish it forcing Keep Layer to True
                        oMapConfig = self.getMapConfig("sar_flood")
                        sInputData = oMapConfig.inputData
                        self.addAndPublishLayer(sFloodMap, oEventPeakDate, True, "sar_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=sInputData, bKeepLayer=True, bForceRepublish=True)

                    # Check if we have the urban map in the workspace
                    if sUrbanMap in asWorkspaceFiles:
                        oMapConfig = self.getMapConfig("urban_flood")
                        sInputData = self.getInputDataForUrbanFlood(iEvent=iEvent, aoUrbanDetections=aoUrbanDetections, sInputData=oMapConfig.inputData)
                        self.addAndPublishLayer(sUrbanMap, oEventPeakDate, True, "urban_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=sInputData, bKeepLayer=True, sOverrideMapId=oMapConfig.id)

                    # Check and publish the composite map
                    if sCompositeMap in asWorkspaceFiles:
                        oMapConfig = self.getMapConfig("flood_composite")
                        sInputData = self.getInputDateForCompositeMap(iEvent=iEvent, aoCompositeDetections=aoCompositeDetections, oEvent=oEvent, sInputData=oMapConfig.inputData)
                        self.addAndPublishLayer(sCompositeMap, oEventPeakDate, True, "flood_composite", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=sInputData, bKeepLayer=True, sOverrideMapId=oMapConfig.id)

                    # Roads
                    sExtensionRoadImpacts = sBaseName +"_event_" + oEvent["endDate"] + "_max_extension_impacts_roads.shp"
                    sUrbanRoadImpacts = sBaseName.replace("sarflood", "buildingcw") +"_urban_" + oEvent["peakDate"] + "_urbanflood_impacts_roads.shp"
                    self.mergeOrPublishImpactsShape(sExtensionRoadImpacts, sUrbanRoadImpacts, sCompositeMap, sUrbanMap, "roads", sBaseName, oEvent["peakDate"], oImpactsPluginConfig, asWorkspaceFiles, True)

                    # Exposures
                    sExtensionExposureImpacts = sBaseName +"_event_" + oEvent["endDate"] + "_max_extension_impacts_exposure.shp"
                    sUrbanExposureImpacts = sBaseName.replace("sarflood", "buildingcw") +"_urban_" + oEvent["peakDate"] + "_urbanflood_impacts_exposure.shp"
                    self.mergeOrPublishImpactsShape(sExtensionExposureImpacts, sUrbanExposureImpacts, sCompositeMap, sUrbanMap, "exposures", sBaseName, oEvent["peakDate"], oImpactsPluginConfig, asWorkspaceFiles, True)

                    # Markers
                    sExtensionMarkersImpacts = sBaseName +"_event_" + oEvent["endDate"] + "_max_extension_impacts_markers.shp"
                    sUrbanMarkersImpacts = sBaseName.replace("sarflood", "buildingcw") +"_urban_" + oEvent["peakDate"] + "_urbanflood_impacts_markers.shp"
                    self.mergeOrPublishImpactsShape(sExtensionMarkersImpacts, sUrbanMarkersImpacts, sCompositeMap, sUrbanMap, "markers", sBaseName, oEvent["peakDate"], oImpactsPluginConfig, asWorkspaceFiles, True)

                    # Population
                    sExtensionPopImpacts = sBaseName +"_event_" + oEvent["endDate"] + "_max_extension_pop_affected.tif"
                    sUrbanPopImpacts = sBaseName.replace("sarflood", "buildingcw") +"_urban_" + oEvent["peakDate"] + "_urbanflood_pop_affected.tif"
                    self.mergeOrPublishImpactsRaster(sExtensionPopImpacts, sUrbanPopImpacts, sCompositeMap, sUrbanMap, "population", sBaseName, oEvent["peakDate"], oImpactsPluginConfig, asWorkspaceFiles, True)

                    #Crops are disabled for Urban Floods
                    sExtensionCropsImpacts = sBaseName +"_event_" + oEvent["endDate"] + "_max_extension_impacts_crops.tif"
                    self.checkAndPublishImpactLayer(sExtensionCropsImpacts, sCompositeMap,"crops", oEventPeakDate, oImpactsPluginConfig, asWorkspaceFiles)
                    
                    iPeakDate = datetime.now().timestamp()
                    try:
                        iPeakDate = datetime.strptime(oEvent["peakDate"], "%Y-%m-%d").timestamp()
                    except:
                        logging.warning("Error converting event peak date " + str(oEvent["peakDate"]))

                    # Check if we already inserted the event
                    aoTestEvents = oEventRepository.findByParams(sAreaId=self.m_oArea.id, sPeakStringDate=oEvent["peakDate"], sType="FLOOD")

                    if len(aoTestEvents)<=0:
                        #No: we start also the rain app!
                        self.startRainMaps(oEvent["peakDate"])

                        oEventEntity = Event()
                        oEventEntity.name= "Flood_" + oEvent["peakDate"]
                        oEventEntity.type = "FLOOD"
                        oEventEntity.bbox = self.m_oArea.bbox
                        oEventEntity.peakStringDate = oEvent["peakDate"]
 
                        iStartDate = datetime.now().timestamp()
                        iEndDate = datetime.now().timestamp()

                        try:
                            iStartDate = datetime.strptime(oEvent["startDate"], "%Y-%m-%d").timestamp()
                        except:
                            logging.warning("Error converting event start date " + str(oEvent["startDate"]))
                         
                        try:
                            iEndDate = datetime.strptime(oEvent["endDate"], "%Y-%m-%d").timestamp()
                        except:
                            logging.warning("Error converting event end date " + str(oEvent["endDate"]))
 
                        oEventEntity.startDate = iStartDate
                        oEventEntity.peakDate = iPeakDate
                        oEventEntity.endDate = iEndDate
                        oEventEntity.areaId = self.m_oArea.id
                        oEventEntity.id = str(uuid.uuid4())
                        oEventEntity.publicEvent = False
                        oEventEntity.inGoing = False
                        oEventEntity.description = "Automatic Flood Event peak = " + oEvent["peakDate"]
                        oEventEntity.markerCoordinates = self.m_oArea.markerCoordinates
                        oEventRepository.addEntity(oEventEntity)

                    else:
                        logging.debug("Event " + "Flood_" + oEvent["peakDate"] + " already in the db")

                except Exception as oEx:
                    logging.error("SarFloodMapEngine.handleEvents [" + self.m_oArea.name +"]: error creating events " + str(oEx))
            
            for oImpactDetection in aoImpactDetections:
                try:
                    sMapType = oImpactDetection["TYPE"]
                    sProcessObjId = oImpactDetection["PROCID"]
                    sDate = oImpactDetection["DATE"]
                    self.createImpactsOfTheDayWidget(sProcessObjId, sDate,sMapType)
                except Exception as oEx:
                    logging.error("SarFloodMapEngine.handleEvents [" + self.m_oArea.name +"]: error processing impact detection " + str(oEx))

        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleEvents [" + self.m_oArea.name +"]: error " + str(oEx))
        return

    def getInputDataForUrbanFlood(self, iEvent, aoUrbanDetections, sInputData=""):
        """
        Get the input data for the Urban Flood Map
            :param iEvent Index of the event in the list of urban detections
            :param aoUrbanDetections List of the urban detections done
            :param sInputData Initial input data string
            :return: updated input data string 
        """

        try:
            if iEvent<len(aoUrbanDetections):
                oUrbanDetection = aoUrbanDetections[iEvent]
                sProcId = oUrbanDetection["PROCID"]
                aoPayload = wasdi.getProcessorPayloadAsJson(sProcId)
                if "UrbanBuildingMaps" in aoPayload:
                    sInputData = sInputData + " Buildings Map: " + aoPayload["UrbanBuildingMaps"]
                if "PRE_VV" in aoPayload:
                    sInputData = sInputData + " PRE VV Coherence: " + aoPayload["PRE_VV"]
                if "PRE_VH" in aoPayload:
                    sInputData = sInputData + " PRE VH Coherence: " + aoPayload["PRE_VH"]
                if "POST_VV" in aoPayload:
                    sInputData = sInputData + " POST VV Coherence: " + aoPayload["POST_VV"]
                if "POST_VH" in aoPayload:
                    sInputData = sInputData + " POST VH Coherence: " + aoPayload["POST_VH"]
        except:
            logging.warning("SarFloodMapEngine.getInputDataForUrbanFlood [" + self.m_oArea.name +"]: Error  trying to read the inputs of Urban Detection for map")
        
        return sInputData

    def getInputDateForCompositeMap(self, iEvent, aoCompositeDetections, oEvent, sInputData=""):
        """
        Get the input data for the Composite Map
            :param iEvent Index of the event in the list of urban detections
            :param aoCompositeDetections List of the composite detections done
            :param oEvent Event object
            :param sInputData Initial input data string
            :return: updated input data string
        """

        try:
            if iEvent<len(aoCompositeDetections):
                oCompositeDetection = aoCompositeDetections[iEvent]
                sProcId = oCompositeDetection["PROCID"]
                aoPayload = wasdi.getProcessorPayloadAsJson(sProcId)
                if "sar_added_files" in aoPayload:
                    sInputData = sInputData + " Buildings Map: " + str(aoPayload["sar_added_files"])
                else:
                    try:
                        sInputData = "Daily SAR Flood Maps from " + oEvent["startDate"] + " to " + oEvent["endDate"]
                    except Exception as oEx:
                        logging.warning("SarFloodMapEngine.getInputDateForCompositeMap [" + self.m_oArea.name +"]: error building input data string " + str(oEx))
        except:
            logging.warning("SarFloodMapEngine.getInputDateForCompositeMap: Error  [" + self.m_oArea.name +"] trying to read the inputs of an Event Composite Map ")

    def checkAndPublishImpactLayer(self, sImpactMap, sInputData, sMapId, oEventPeakDate, oImpactsPluginConfig, asWorkspaceFiles):
        if sImpactMap in asWorkspaceFiles:
            oImpactsMapConfig = RiseDeamon.getMapConfigFromPluginConfig(oImpactsPluginConfig,sMapId)
            if oImpactsMapConfig is not None:
                self.addAndPublishLayer(sImpactMap, oEventPeakDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData, bKeepLayer=True, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)
    

    def handleArchiveTask(self, oTask, asWorkspaceFiles, bFullArchive):

        fFirstMapTimestamp = -1.0
        fLastMapTimestamp = -1.0

        try:
            logging.info("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: task done, lets proceed!")

            # We take the base name  and start/end archive date
            sBaseName = oTask.inputParams["MOSAICBASENAME"]
            sStartDate = oTask.inputParams["ARCHIVE_START_DATE"]
            sEndDate = oTask.inputParams["ARCHIVE_END_DATE"]

            try:
                oStartDay = datetime.strptime(sStartDate, '%Y-%m-%d')
            except:
                logging.error('SarFloodMapEngine.handleArchiveTask [' + self.m_oArea.name + "]: Start Date not valid")
                return False

            try:
                oEndDay = datetime.strptime(sEndDate, '%Y-%m-%d')
            except:
                logging.error('SarFloodMapEngine.handleArchiveTask [' + self.m_oArea.name + "]: End Date not valid")
                return False

            oTimeDelta = timedelta(days=1)
            oActualDate = oStartDay

            oMapConfig = self.getMapConfig("sar_flood")

            # For each date of the archive
            while oActualDate <= oEndDay:
                sDate = oActualDate.strftime("%Y-%m-%d")

                # Name of the daily bare soil flood map
                sFileName = sBaseName + "_" +sDate + "_" + oTask.inputParams["SUFFIX"]

                # If the file is in the workspace
                if sFileName not in asWorkspaceFiles:
                    # We can proceed to the next date
                    oActualDate = oActualDate + oTimeDelta
                    continue

                logging.info("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: Found " + sFileName + ", add the layer to db")
                oLayer = self.addAndPublishLayer(sFileName, oActualDate, not bFullArchive, "sar_flood", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData)

                if oLayer is None:
                    logging.warning("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: layer not good!")
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

            # Save the chain params
            self.saveChainParams(self.m_sChainParamsFile, aoChainParams)

            # Update the task with the chain params
            try:
                oTask.pluginPayload["chainParams"] = aoChainParams
                oWasdiTaskRepository = WasdiTaskRepository()
                oWasdiTaskRepository.updateEntity(oTask)
            except Exception as oEx:
                logging.error("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: error updating task to save Chain Params " + str(oEx))

            # Handle the events array
            self.handleEvents(sEventFinderId, asWorkspaceFiles, oTask.inputParams["SUFFIX"])

            # Flood Frequency Map
            sFFmMap = sBaseName + "_ffm_flood.tif"
            sFFmDataMap = sBaseName + "_ffm_data.tif"
            sFFmPercMap = sBaseName + "_ffm_frequency.tif"

            if not bFullArchive:
                # Publish the FFM
                if  sFFmMap in asWorkspaceFiles:
                    oMapConfig = self.getMapConfig("flood_frequency_map")
                    oLayer = self.addAndPublishLayer(sFFmMap, oActualDate, True, "flood_frequency_map", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData, sOverrideMapId=oMapConfig.id, bKeepLayer=True)

                    if oLayer is None:
                        logging.warning("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: problems publishing ffm!")
                # Publish the Data Count
                if  sFFmDataMap in asWorkspaceFiles:
                    oMapConfig = self.getMapConfig("flood_frequency_map_data")
                    oLayer = self.addAndPublishLayer(sFFmDataMap, oActualDate, True, "flood_frequency_map_data", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData, sOverrideMapId=oMapConfig.id, bKeepLayer=True)

                    if oLayer is None:
                        logging.warning("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: problems publishing ffm data!")
                # Publish the Percentage FFM
                if  sFFmPercMap in asWorkspaceFiles:
                    oMapConfig = self.getMapConfig("flood_frequency_map_perc")
                    oLayer = self.addAndPublishLayer(sFFmPercMap, oActualDate, True, "flood_frequency_map_perc", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData, sOverrideMapId=oMapConfig.id, bKeepLayer=True)

                    if oLayer is None:
                        logging.warning("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: problems publishing ffm perc!")
            else:
                # This is the long term archive: we need to sum the short archive ffm with the long one
                sFullArchiveFFmMap = sBaseName + "_archiveffm_flood.tif"
                
                # TODO: the update of the percentage map is not yet implemented
                sFullArchiveFFmPercMap = sBaseName + "_archiveffm_frequency.tif"

                if sFullArchiveFFmMap in asWorkspaceFiles:

                    logging.info("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: trying to sum full FFM with the short archive one")

                    # We have the ffm done for the full archive: we need to sum it to the short-near real time one
                    aoTiffAddParams = {}

                    # Sum the long and short archive maps
                    aoTiffAddParams["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox)
                    aoTiffAddParams["DATA_TYPE"] = "uint16"
                    aoTiffAddParams["OVERRIDE_OUTPUT"] = True

                    aoTiffAddParams["INPUT_FILES"] = [sFFmMap, sFullArchiveFFmMap]
                    aoTiffAddParams["OUTPUT_FILE"] = sFFmMap

                    if not self.m_oConfig.daemon.simulate:
                        sSumFloodMapsId = wasdi.executeProcessor("tiff_images_add", aoTiffAddParams)

                        # We should have also the data maps
                        sDataMap = sBaseName + "_ffm_data.tif"
                        sFullDataMap = sBaseName + "_archiveffm_data.tif"

                        if sDataMap in asWorkspaceFiles and sFullDataMap in asWorkspaceFiles:
                            aoTiffAddParams["INPUT_FILES"] = [sDataMap, sFullDataMap]
                            aoTiffAddParams["OUTPUT_FILE"] = sDataMap

                            sSumDataMapsId2 = wasdi.executeProcessor("tiff_images_add", aoTiffAddParams)
                            sStatus = wasdi.waitProcess(sSumDataMapsId2)
                            if sStatus == "DONE":
                                oMapConfig = self.getMapConfig("flood_frequency_map_data")
                                oLayer = self.addAndPublishLayer(sDataMap, oActualDate, True,
                                                                 "flood_frequency_map?data", sResolution=oMapConfig.resolution,
                                                                 sDataSource=oMapConfig.dataSource,
                                                                 sInputData=oMapConfig.inputData,
                                                                 sOverrideMapId=oMapConfig.id, bForceRepublish=True, bKeepLayer=True)

                                if oLayer is None:
                                    logging.warning("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: problems publishing ffm data map!")

                        sStatus = wasdi.waitProcess(sSumFloodMapsId)
                        if sStatus == "DONE":
                            oMapConfig = self.getMapConfig("flood_frequency_map")
                            oLayer = self.addAndPublishLayer(sFFmMap, oActualDate, not bFullArchive, "flood_frequency_map",
                                                             sResolution=oMapConfig.resolution,
                                                             sDataSource=oMapConfig.dataSource,
                                                             sInputData=oMapConfig.inputData, sOverrideMapId=oMapConfig.id, bForceRepublish=True)

                            if oLayer is None:
                                logging.warning("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: problems publishing ffm!")
                    else:
                        logging.info("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: simulation mode on")

            self.updateChainParamsDate(self.m_sChainParamsFile, sEndDate, "lastMapDate")

            # notify users
            self.notifyEndOfTask(oTask.areaId, True, "High Res Flooded Area Detection")

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleArchiveTask [" + self.m_oArea.name +"]: exception " + str(oEx))
            return False
        finally:
            bChanged = False

            # And if we do not have yet archive start and end date, set it
            if (self.m_oArea.archiveStartDate <=0 and fFirstMapTimestamp>0) or (fFirstMapTimestamp>0 and fFirstMapTimestamp<self.m_oArea.archiveStartDate):
                self.m_oArea.archiveStartDate = fFirstMapTimestamp
                bChanged = True

            if (self.m_oArea.archiveEndDate <=0 and fLastMapTimestamp>0) or (fLastMapTimestamp>0 and fLastMapTimestamp>self.m_oArea.archiveEndDate):
                self.m_oArea.archiveEndDate = fLastMapTimestamp
                bChanged = True

            if not self.m_oArea.firstShortArchivesReady and not bFullArchive:
                self.m_oArea.firstShortArchivesReady = True
                bChanged = True
            
            if not self.m_oArea.firstFullArchivesReady and bFullArchive:
                self.m_oArea.firstFullArchivesReady = True
                bChanged = True 

            if bChanged:
                # Update the area if needed
                oAreaRepository = AreaRepository()
                oAreaRepository.updateEntity(self.m_oArea)

    def startDailySARFloodDetection(self, sDay, oMapConfig, aoFloodChainParameters, sWorkspaceId):
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
                logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: a task is still ongoing " + oTask.id)
                return
            elif self.isDoneStatus(oTask.status):
                if oTask.startDate>iTimestamp:
                    sTodayTaskId = oTask.id
                    iTimestamp = oTask.startDate

        aoFloodChainParameters = vars(aoFloodChainParameters)

        bForceReRun = False
        bStillToRun = False

        # Now we try to read the params saved in the workspace
        aoIntegratedChainParams = self.getWorkspaceUpdatedJsonFile(self.m_sChainParamsFile)

        sChainOrbits = ""
        sWaterMap = ""
        sCopDemMap = ""
        sLastMapDate = ""

        if aoIntegratedChainParams is None:
            aoIntegratedChainParams = self.recoverChainParams()

        if aoIntegratedChainParams is not None:
            sChainOrbits = aoIntegratedChainParams["orbits"]
            sWaterMap = aoIntegratedChainParams["water_map"]
            sCopDemMap = aoIntegratedChainParams["CopDemMap"]
            if "lastMapDate" in aoIntegratedChainParams:
                sLastMapDate = aoIntegratedChainParams["lastMapDate"]
        else:
            logging.warning("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: no chain params found, we try to recover at least orbits from past run")
            sChainOrbits = self.recoverOrbitsFromAutofloodChain()
            sWaterMap = "HighResWaterMap.tif"

        # If we have a task
        if sTodayTaskId is not None:
            logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: We already ran once for date " + sDay + ": read the payload of the application " + sTodayTaskId)
            # And if it is done

            # We need to check if there are new images: take the payload
            aoFloodChainPayload = wasdi.getProcessorPayloadAsJson(sTodayTaskId)

            # Take the orbits
            asOrbits = sChainOrbits.split(",")

            if "ResultsPerOrbit" in aoFloodChainPayload:

                logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: check if we find more images for day " + sDay)
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
                    
                    # Filter by Platform
                    aoImages = self.filterImagesByPlatform(aoFloodChainPayload, aoImages)

                    # Do we have results?
                    if aoImages is not None:
                        # Are more than before?
                        if len(aoImages)>iResults:
                            # We definitely need to re-run
                            logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Found new images available for Orbit " + sOrbit + " set Force Re-Run = True")
                            bForceReRun = True
        else:
            logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name +"]: This is the first run for day " + sDay)
            bStillToRun = True

        if bForceReRun or bStillToRun:

            if not self.m_oConfig.daemon.simulate:
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

                if not self.checkProcessorId(sProcessorId):
                    return

                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoFloodChainParameters,oMapConfig.processor,sDay)
                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name + "]: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.warning("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name + "]: simulation mode on - we do not run nothing")
        else:
            logging.info("SarFloodMapEngine.updateNewMaps [" + self.m_oArea.name + "]: nothing to re-start, done.")
    
    def startRainMaps(self, sEventDate):
        try:
            logging.debug("SarFloodMapEngine.startRainMaps [" + self.m_oArea.name + "]: Starting Rain Maps for event " + sEventDate)
            
            oWasdiTaskRepository = WasdiTaskRepository()

            oRainPluginConfig = RiseDeamon.getPluginConfig("rise_rain_plugin", self.m_oConfig)
            oMapConfig = RiseDeamon.getMapConfigFromPluginConfig(oRainPluginConfig, "imerg_cumulate_12")

            aoParameters = oMapConfig.params
            aoParameters = vars(aoParameters)

            if not self.m_oConfig.daemon.simulate:
                sWorkspaceId = self.openRainWorkspace()

                aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
                aoParameters["BASE_NAME"] = self.getBaseName()
                aoParameters["REFERENCE_DATETIME"] = sEventDate + " " + "23:59"

                sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)
                if not self.checkProcessorId(sProcessorId):
                    return
                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParameters,oMapConfig.processor,sEventDate)
                # Override: one for all in the tasks!
                oWasdiTask.mapId = "imerg_cumulate"
                oWasdiTask.pluginId = "rise_rain_plugin"
                
                oWasdiTask.pluginPayload["time"] = "23"
                oWasdiTask.pluginPayload["event"] = True
                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info("SarFloodMapEngine.startRainMaps [" + self.m_oArea.name +"]: Started " + oMapConfig.processor + " for " + sEventDate)
            else:
                logging.warning("SarFloodMapEngine.startRainMaps [" + self.m_oArea.name + "]: simulation mode on - we do not run nothing")

        except Exception as oEx:
            logging.error("SarFloodMapEngine.startRainMaps [" + self.m_oArea.name +"]:" + str(oEx))
        finally:
            # Re-Open the right workspace
            self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)            


    def openRainWorkspace(self):
        # We need the plugin 
        sRainPluginId = "rise_rain_plugin"
        # And the map id
        sImergMapId = "imerg_cumulate_12"

        # We need to interact with the buildings. Here we pass to the app the workspace and the area name
        sFloodsWorkspaceName = self.m_oArea.id + "|" + sRainPluginId + "|" + sImergMapId

        # Open our workspace
        sWorkspaceId = wasdi.openWorkspace(sFloodsWorkspaceName)

        return sWorkspaceId
    

    def recoverChainParams(self):    
        try:
            oWasdiTaskRepository = WasdiTaskRepository()
            aoOldRun = wasdi.getProcessesByWorkspace(iStartIndex=0, iEndIndex=20, sStatus="DONE", sOperationType="RUNPROCESSOR", sName="integrated_sar_flood_archive")

            for oOldRun in aoOldRun:
                sTaskId = oOldRun["processObjId"]
                oIntegratedChainTask = oWasdiTaskRepository.getEntityById(sTaskId)
                if oIntegratedChainTask is not None:
                    if oIntegratedChainTask.pluginPayload is not None:
                        if "chainParams" in oIntegratedChainTask.pluginPayload:
                            aoIntegratedChainParams = oIntegratedChainTask.pluginPayload["chainParams"]
                            return aoIntegratedChainParams
        except Exception as oEx:
            logging.error("SarFloodMapEngine.recoverChainParams [" + self.m_oArea.name +"]: error " + str(oEx))
            return None
        
    def recoverOrbitsFromAutofloodChain(self):    
        try:
            aoOldRun = wasdi.getProcessesByWorkspace(iStartIndex=0, iEndIndex=20, sStatus="DONE", sOperationType="RUNPROCESSOR", sName="autofloodchain2")

            for oOldRun in aoOldRun:
                sTaskId = oOldRun["processObjId"]
                oPayload = wasdi.getProcessorPayloadAsJson(sTaskId)
                if oPayload is not None:
                    if "Orbits" in oPayload:
                        sOrbits = oPayload["Orbits"]
                        if sOrbits != "":
                            return sOrbits

        except Exception as oEx:
            logging.error("SarFloodMapEngine.recoverOrbitsFromAutofloodChain [" + self.m_oArea.name +"]: error " + str(oEx))
            return ""        
        
    def filterImagesByPlatform(self, aoFloodChainPayload, aoImages):

        if "PLATFORM_FILTER" in aoFloodChainPayload:
            # We need to filter the images
            sPlatformFilter = aoFloodChainPayload["PLATFORM_FILTER"]
            if sPlatformFilter is not None and sPlatformFilter != "":
                # Filter the images
                asPlatformFilter = [x.strip() for x in sPlatformFilter.split(',')]
                aoFilteredImages = []
                for oImage in aoImages:
                    if oImage["fileName"].startswith(tuple(asPlatformFilter)):
                        aoFilteredImages.append(oImage)

                aoImages = aoFilteredImages
        
        return aoImages 
    
    def createImpactsOfTheDayWidget(self, sTaskId, sReferenceDate, sMapType):
        try:

            oWidgetInfoRepository = WidgetInfoRepository()

            aoWidgets = oWidgetInfoRepository.findByParams(sWidget="impacts_" + sMapType, sAreaId=self.m_oArea.id, sReferenceDate=sReferenceDate)

            if len(aoWidgets) > 0:
                return

            #self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
            oPayload = wasdi.getProcessorPayloadAsJson(sTaskId)

            if oPayload is None:
                logging.info("SarFloodMapEngine.createImpactsOfTheDayWidget [" + self.m_oArea.name +"]: no payload found for task " + sTaskId)
                return
            
            oWidgetPayload = {}

            if "Roads" in oPayload:
                oWidgetPayload["roadsCount"] = len(oPayload["Roads"])
            
            if "Exposures" in oPayload:
                oWidgetPayload["exposuresCount"] = len(oPayload["Exposures"])

            if "AffectedPopulation" in oPayload:
                oWidgetPayload["populationCount"] = int(oPayload["AffectedPopulation"])

            if "AffectedLandUse" in oPayload:
                oWidgetPayload["affectedLandUse"] = oPayload["AffectedLandUse"]

            # Get the widget info
            oWidgetInfo = WidgetInfo.createWidgetInfo("impacts_" + sMapType, self.m_oArea, "text", "warning", "", "", sReferenceDate)
            oWidgetInfo.payload = oWidgetPayload

            # Add or update the widget
            oWidgetInfoRepository.addEntity(oWidgetInfo)
            logging.info("SarFloodMapEngine.createImpactsOfTheDayWidget [" + self.m_oArea.name + "]: added daily impacts widget for " + sMapType)

        except Exception as oEx:
            logging.error("SarFloodMapEngine.createImpactsOfTheDayWidget [" + self.m_oArea.name + "]: exception " + str(oEx))    