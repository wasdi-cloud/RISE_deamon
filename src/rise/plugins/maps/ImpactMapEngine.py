import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import uuid

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.business.WidgetInfo import WidgetInfo
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine
from src.rise.utils import RiseUtils
from src.rise.data.WidgetInfoRepository import WidgetInfoRepository

class ImpactMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        logging.info("ImpactMapEngine.triggerNewAreaMaps: short term archive is handled by the integrated chain")

    def triggerNewAreaArchives(self):
        logging.info("ImpactMapEngine.triggerNewAreaArchives: long archive is handled by the integrated chain")

    def updateNewMaps(self):

        # Open our workspace
        sWorkspaceId = self.openSarFloodWorkspace()
        # Get the baresoil flood Suffix
        sSuffix = self.getBaresoilSuffix()
        
        # Get the list of files
        asFiles = wasdi.getProductsByActiveWorkspace()

        # Check today
        oToday = datetime.today()
        sToday = oToday.strftime("%Y-%m-%d")
        self.updateImpactMapsForDay(sToday, sWorkspaceId, asFiles, sSuffix)

        # Check yesterday
        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")
        self.updateImpactMapsForDay(sYesterday, sWorkspaceId, asFiles, sSuffix)

    def updateImpactMapsForDay(self, sDay, sWorkspaceId, asFiles, sSuffix):
        oMapConfig = self.getMapConfig("rasor_impacts")

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sDay)

        # Flag to run or not on bare soil
        bRunForBareSoil = True
        # Flag to run or not on Urban
        bRunForUrban = True

        # if we have existing tasks
        if len(aoExistingTasks) > 0:
            # We need to verify for both Bare Soil and Urban
            for oTask in aoExistingTasks:
                if "targetMapType" in oTask.pluginPayload:
                    sTargetMapType = oTask.pluginPayload["targetMapType"]
                    if sTargetMapType == "baresoil":
                        bRunForBareSoil = False
                        logging.info("ImpactMapEngine.updateNewMaps: run on bare soil already done today")
                    elif sTargetMapType == "urban":
                        bRunForUrban = False
                        logging.info("ImpactMapEngine.updateNewMaps: run on urban already done today")

        sOriginalBaseName = self.getBaseName("sar_flood")

        if bRunForBareSoil:    
            sBaseName = sOriginalBaseName + "_" + sDay + "_" + sSuffix

            if sBaseName in asFiles:
                logging.info("ImpactMapEngine.updateImpactMapsForDay: found a new daily sar map")

                aoParams = oMapConfig.params
                aoParams = vars(aoParams)

                aoParams["date"] = sDay
                aoParams["hazard_input"] = sBaseName
                aoParams["hazard_pixel_value"] = 3

                aoParams["exposure_file_name"] =  sOriginalBaseName + "_exposure_baresoil_" + sDay + ".shp"
                aoParams["exposure_markers_file"] = sOriginalBaseName + "_markers_baresoil_" + sDay + ".shp"
                aoParams["roads_file_name"] = sOriginalBaseName + "_roads_baresoil_" + sDay + ".shp"
                aoParams["lulc_map_name"] = sOriginalBaseName + "_lulc_baresoil_" + sDay + ".tif"
                aoParams["crops_file_name"] = sOriginalBaseName + "_crops_baresoil_" + sDay + ".tif"

                if not self.m_oConfig.daemon.simulate:
                    sTaskId = wasdi.executeProcessor(oMapConfig.processor, aoParams)

                    logging.info("ImpactMapEngine.updateImpactMapsForDay: started impact detection for " + sBaseName)

                    oWasdiTask = self.createNewTask(sTaskId,sWorkspaceId,aoParams, oMapConfig.processor, sDay)
                    oWasdiTask.pluginPayload["targetMapType"] = "baresoil"
                    oWasdiTask.pluginPayload["targetMap"] = sBaseName

                    oWasdiTaskRepository.addEntity(oWasdiTask)
                else:
                    logging.info(
                        "ImpactMapEngine.updateImpactMapsForDay: simulation mode is on, think I started an impact detection on bare soil for day " + sDay)
            else:
                logging.info("ImpactMapEngine.updateImpactMapsForDay: No Bare Soil Flood Map found for date " + sDay)

        if bRunForUrban:
            sBaseName = self.getBaseName("urban_flood")

            asDailyUrbanFloodMaps = []

            for sFile in asFiles:
                if sFile.startswith(sBaseName) and sDay in sFile and sFile.endswith("flood.tif"):
                    asDailyUrbanFloodMaps.append(sFile)

            if len(asDailyUrbanFloodMaps)<=0:
                logging.info("ImpactMapEngine.updateImpactMapsForDay: No Urban Flood Maps found for " + sDay)
            else:
                for sUrbanFloodFile in asDailyUrbanFloodMaps:
                    if sUrbanFloodFile in asFiles:
                        logging.info("ImpactMapEngine.updateImpactMapsForDay: found a new daily Urban Flood map for " + sDay)

                        aoParams = oMapConfig.params
                        aoParams = vars(aoParams)

                        aoParams["date"] = sDay
                        aoParams["hazard_input"] = sUrbanFloodFile
                        aoParams["hazard_pixel_value"] = 1

                        aoParams["exposure_file_name"] = sOriginalBaseName + "_exposure_urban_" + sDay + ".shp"
                        aoParams["exposure_markers_file"] = sOriginalBaseName + "_markers_urban_" + sDay + ".shp"
                        aoParams["roads_file_name"] = sOriginalBaseName + "_roads_urban_" + sDay + ".shp"
                        aoParams["lulc_map_name"] = sOriginalBaseName + "_lulc_urban_" + sDay + ".tif"
                        aoParams["crops_file_name"] = sOriginalBaseName + "_crops_urban_" + sDay + ".tif"
                        aoParams["pop_file_name"] = sOriginalBaseName + "_pop_urban_" + sDay + ".tif"
                        aoParams["compute_crops"] = False

                        if not self.m_oConfig.daemon.simulate:
                            sTaskId = wasdi.executeProcessor(oMapConfig.processor, aoParams)

                            logging.info("ImpactMapEngine.updateImpactMapsForDay: started impact detection for " + sBaseName)

                            oWasdiTask = self.createNewTask(sTaskId,sWorkspaceId,aoParams,oMapConfig.processor,sDay)
                            oWasdiTask.pluginPayload["targetMapType"] = "urban"
                            oWasdiTask.pluginPayload["targetMap"] = sBaseName

                            oWasdiTaskRepository.addEntity(oWasdiTask)
                        else:
                            logging.info("ImpactMapEngine.updateImpactMapsForDay: simulation mode is on, think I started an impact detection for Urban for day " + sDay)

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("ImpactMapEngine.handleTask: handle task " + oTask.id)

            if not "targetMapType" in oTask.pluginPayload:
                logging.info("ImpactMapEngine.handleTask: the task does not have the targetMapType tag, I can only exit" )
                return False
            
            sTargetMapType = oTask.pluginPayload["targetMapType"]

            self.createImpactsOfTheDayWidget(oTask, sTargetMapType)

            # Open the SAR Workspace
            self.openSarFloodWorkspace()
            oPluginConfig = self.m_oPluginEngine.getPluginConfig()

            sDay = oTask.referenceDate

            asFiles = wasdi.getProductsByActiveWorkspace()
            sBaseName = self.getBaseName("sar_flood")
            
            sSuffix = self.getBaresoilSuffix()
            sSuffix = sSuffix.replace(".tif","")

            sInput1 = "Bare Soil Flood " + sDay
            sInput2 = "Urban Flood " + sDay

            # Exposure
            sBareSoilImpactFile = sBaseName + "_exposure_baresoil_" + sDay + ".shp"
            sUrbanImpactFile = sBaseName + "_exposure_urban_" + sDay + ".shp"
            self.mergeOrPublishImpactsShape(sBareSoilImpactFile, sUrbanImpactFile, sInput1, sInput2, "exposures",sBaseName, sDay, oPluginConfig, asFiles, False)

            # Markers
            sBareSoilImpactFile = sBaseName + "_markers_baresoil_" + sDay + ".shp"
            sUrbanImpactFile = sBaseName + "_markers_urban_" + sDay + ".shp"
            self.mergeOrPublishImpactsShape(sBareSoilImpactFile, sUrbanImpactFile, sInput1, sInput2, "markers",sBaseName, sDay, oPluginConfig, asFiles, False)

            # Roads
            sBareSoilImpactFile = sBaseName + "_roads_baresoil_" + sDay + ".shp"
            sUrbanImpactFile = sBaseName + "_roads_urban_" + sDay + ".shp"
            self.mergeOrPublishImpactsShape(sBareSoilImpactFile, sUrbanImpactFile, sInput1, sInput2, "roads",sBaseName, sDay, oPluginConfig, asFiles, False)

            # Population
            sBareSoilImpactFile = sBaseName + "_" + sDay + "_" + sSuffix + "_pop_affected.tif"
            sUrbanImpactFile = sBaseName + "_pop_urban_" + sDay + ".tif"
            self.mergeOrPublishImpactsRaster(sBareSoilImpactFile, sUrbanImpactFile, sInput1, sInput2, "population",sBaseName, sDay, oPluginConfig, asFiles, False)

            # Crops 
            sBareSoilImpactFile = sBaseName + "_crops_baresoil_" + sDay + ".tif"
            self.checkAndPublishImpactLayer(sBareSoilImpactFile, asFiles, oTask, "crops")

            # Create the widgets
            self.createWidgetInfo(oTask)
        
        except Exception as oEx:
            logging.error("ImpactMapEngine.handleTask: exception " + str(oEx))

    def checkAndPublishImpactLayer(self, sFile, asFiles, oTask, sMapId):
        if sFile in asFiles:
            oMapConfig = self.getMapConfig()
            logging.info("ImpactMapEngine.checkAndPublishImpactLayer: found impacts Map to publish " + sFile)
            self.addAndPublishLayer(sFile, datetime.strptime(oTask.referenceDate, "%Y-%m-%d"), bPublish=True,
                                    sMapIdForStyle=sMapId,
                                    sOverrideMapId=sMapId,
                                    sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource,
                                    sInputData=oMapConfig.inputData)
    

    def openSarFloodWorkspace(self):
        # We need the flood plugin config
        oPluginConfig = self.m_oPluginEngine.getPluginConfig()
        sFloodsPluginId = "rise_flood_plugin"

        if oPluginConfig is not None:
            try:
                sFloodsPluginId = oPluginConfig.floods_plugin_id
            except:
                pass

        # And the sar_flood config
        sSarFloodMapId = "sar_flood"

        if oPluginConfig is not None:
            try:
                sSarFloodMapId = oPluginConfig.flood_sarmap_id
            except:
                pass
        
        # We need to interact with the buildings. Here we pass to the app the workspace and the area name
        sFloodsWorkspaceName = self.m_oArea.id + "|" + sFloodsPluginId + "|" + sSarFloodMapId

        # Open our workspace
        sWorkspaceId = wasdi.openWorkspace(sFloodsWorkspaceName)

        return sWorkspaceId        
    
    def getBaresoilSuffix(self):
        # We need the flood plugin config
        oPluginConfig = self.m_oPluginEngine.getPluginConfig()
        sFloodsPluginId = "rise_flood_plugin"

        if oPluginConfig is not None:
            try:
                sFloodsPluginId = oPluginConfig.floods_plugin_id
            except:
                pass

        # And the sar_flood config
        sSarFloodMapId = "sar_flood"

        if oPluginConfig is not None:
            try:
                sSarFloodMapId = oPluginConfig.flood_sarmap_id
            except:
                pass

        # Default value
        sSuffix = "baresoil-flood.tif"
        # Open the flood plugin config
        oParentPath = Path(self.m_oConfig.myFilePath).parent
        oPluginConfigPath = oParentPath.joinpath(sFloodsPluginId + ".json")
        if os.path.isfile(oPluginConfigPath):

            oSarMapConfig = None

            oFloodPluginConfig = RiseDeamon.readConfigFile(oPluginConfigPath)

            for oMapConfig in oFloodPluginConfig.maps:
                if oMapConfig.id == sSarFloodMapId:
                    oSarMapConfig = oMapConfig
                    break

            if oSarMapConfig:
                aoParams = oSarMapConfig.params
                aoParams = vars(aoParams)
                sSuffix = aoParams["SUFFIX"]
        
        return sSuffix
    
    def createWidgetInfo(self, oTask):
        try:

            self.openSarFloodWorkspace()

            oPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if oPayload is not None:

                # Add Affected Population Widget if we have a population value
                if "AffectedPopulation" in oPayload:
                    iPopulation = int(oPayload["AffectedPopulation"])

                    # If there is population affected we create the widget
                    if iPopulation > 0:

                        # Get the new Widget Info
                        oWidgetInfo = WidgetInfo.createWidgetInfo("population", self.m_oArea, "number", "family_restroom", "WIDGET.AFFECTED_PPL", str(iPopulation), oTask.referenceDate)
                        self.addOrUpdateIntegerWidget(oWidgetInfo, oTask)

                # Add Affected Roads Widget if we have a roads value
                if "Roads" in oPayload:
                    iRoads = self.countRoadsFromPayload(oPayload)

                    if iRoads > 0:
                        oWidgetInfo = WidgetInfo.createWidgetInfo("alerts", self.m_oArea, "text", "bus_alert", "WIDGET.AFFECTED_ROAD", str(iRoads), oTask.referenceDate)
                        self.addOrUpdateIntegerWidget(oWidgetInfo, oTask, "WIDGET.AFFECTED_ROAD")
                
                # Add Affected Buildings Widget if we have a buildings value
                if "Exposures" in oPayload:
                    iExposures = int(len(oPayload["Exposures"]))

                    if iExposures > 0:
                        oWidgetInfo = WidgetInfo.createWidgetInfo("alerts", self.m_oArea, "text", "home", "WIDGET.AFFECTED_BUILDINGS", str(iExposures), oTask.referenceDate)
                        self.addOrUpdateIntegerWidget(oWidgetInfo, oTask, "WIDGET.AFFECTED_BUILDINGS")

        except Exception as oEx:
            logging.error("ImpactMapEngine.createWidgetInfo: exception " + str(oEx))

    def addOrUpdateIntegerWidget(self, oWidgetInfo, oTask, sTitle=None):

        # Search the input map used for these impacts
        sInputMap = ""

        if "targetMap" in oTask.pluginPayload:
            sInputMap = oTask.pluginPayload["targetMap"]
            oWidgetInfo.payload["input_maps"] = [sInputMap]

        # Now we search if we have already a widget for this area and date
        oWidgetInfoRepository = WidgetInfoRepository()
        aoExistingWidgets = oWidgetInfoRepository.findByParams(oWidgetInfo.widget, oWidgetInfo.areaId, oWidgetInfo.referenceDate, sTitle)

        if len(aoExistingWidgets) == 0 or sInputMap == "":
            # It is the first, we create it
            oWidgetInfoRepository.addEntity(oWidgetInfo)

        elif len(aoExistingWidgets) == 1:
            # We have one, check if we need to update it
            oExistingWidget = aoExistingWidgets[0]

            # Take the input maps 
            asInputMaps = oExistingWidget.payload["input_maps"]

            if asInputMaps is None:
                asInputMaps = []

            # Is this target map already in the list?
            if sInputMap not in asInputMaps:

                # No: we add it
                asInputMaps.append(sInputMap)
                # We update the widget
                oExistingWidget.payload["input_maps"] = asInputMaps

                # We have to sum the population
                iOldNumber = int(oExistingWidget.content)
                iNewNumber = int(oWidgetInfo.content) + iOldNumber
                oExistingWidget.content = str(iNewNumber)

                # We update the widget
                oWidgetInfoRepository.updateEntity(oExistingWidget)
            else:
                # We have the same map, we do not need to update
                logging.info("ImpactMapEngine.addOrUpdateIntegerWidget: widget already exists for this area and date")        
    
    def createImpactsOfTheDayWidget(self, oTask, sMapType):
        try:

            oWidgetInfoRepository = WidgetInfoRepository()

            aoWidgets = oWidgetInfoRepository.findByParams(sWidget="impacts_" + sMapType, sAreaId=self.m_oArea.id, sReferenceDate=oTask.referenceDate)

            if len(aoWidgets) > 0:
                return

            self.openSarFloodWorkspace()
            oPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if oPayload is None:
                logging.info("ImpactMapEngine.createImpactsOfTheDayWidget: no payload found for task " + oTask.id)
                return
            
            oWidgetPayload = {}

            if "Roads" in oPayload:
                oWidgetPayload["roadsCount"] = self.countRoadsFromPayload(oPayload)
            
            if "Exposures" in oPayload:
                oWidgetPayload["exposuresCount"] = len(oPayload["Exposures"])

            if "AffectedPopulation" in oPayload:
                oWidgetPayload["populationCount"] = int(oPayload["AffectedPopulation"])

            if "AffectedLandUse" in oPayload:
                oWidgetPayload["affectedLandUse"] = oPayload["AffectedLandUse"]

            # Get the widget info
            oWidgetInfo = WidgetInfo.createWidgetInfo("impacts_" + sMapType, self.m_oArea, "text", "warning", "", "", oTask.referenceDate)
            oWidgetInfo.payload = oWidgetPayload

            # Add or update the widget
            oWidgetInfoRepository.addEntity(oWidgetInfo)
            logging.info("ImpactMapEngine.createImpactsOfTheDayWidget: added daily impacts widget for " + sMapType)

        except Exception as oEx:
            logging.error("ImpactMapEngine.createImpactsOfTheDayWidget: exception " + str(oEx))

    
    def countRoadsFromPayload(self, oPayload):

        # If we have no payload, we return 0
        if oPayload is None:
            return 0

        try:
            # We count the roads based on the original Element id.
            iRoadsCount = 0
            aiRoadIds = []

            # If we have no roads, we return 0
            if "Roads" in oPayload:
                # Loop through the roads in the payload
                for oRoad in oPayload["Roads"]:
                    # Initialize the id as 0
                    iRoadId = 0

                    # But we should have it in the entity
                    if "id" in oRoad:
                        iRoadId = oRoad["id"]
                    
                    # Did we already count this road?
                    if iRoadId not in aiRoadIds:
                        # No, add it to the list
                        aiRoadIds.append(iRoadId)
                        iRoadsCount += 1
                    # If it is already counted, should be another segment of the same road

            # Ok, here we should have the count of roads
            return iRoadsCount

        except Exception as oEx:
            logging.error("ImpactMapEngine.countRoadsFromPayload: exception " + str(oEx))
            if "Roads" in oPayload:
                # Try to return the length of the roads array
                len(oPayload["Roads"])
            else:
                # At this point we return 0
                return 0
