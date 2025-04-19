import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ImpactMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        logging.info("ImpactMapEngine.triggerNewAreaMaps: short term archive is handled by the integrated chain")

    def triggerNewAreaArchives(self):
        logging.info("ImpactMapEngine.triggerNewAreaArchives: long archive is handled by the integrated chain")

    def updateNewMaps(self):

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

        # We need to interact with the buildings. Here we pass to the app the workspace and the area name
        sFloodsWorkspaceName = self.m_oArea.id + "|" + sFloodsPluginId + "|" + sSarFloodMapId

        # Open our workspace
        sWorkspaceId = wasdi.openWorkspace(sFloodsWorkspaceName)
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

        if bRunForBareSoil:
            sBaseName = self.getBaseName("sar_flood")
            sBaseName += "_" + sDay + "_" + sSuffix

            if sBaseName in asFiles:
                logging.info("ImpactMapEngine.updateImpactMapsForDay: found a new daily sar map")

                aoParams = oMapConfig.params
                aoParams = vars(aoParams)

                aoParams["date"] = sDay
                aoParams["hazard_input"] = sBaseName
                aoParams["hazard_pixel_value"] = 3

                aoParams["exposure_file_name"] = "exposure_baresoil_" + sDay + ".shp"
                aoParams["exposure_markers_file"] = "markers_baresoil_" + sDay + ".shp"
                aoParams["roads_file_name"] = "roads_baresoil_" + sDay + ".shp"
                aoParams["lulc_map_name"] = "lulc_baresoil_" + sDay + ".tif"
                aoParams["crops_file_name"] = "crops_baresoil_" + sDay + ".tif"

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
                        aoParams["hazard_input"] = sBaseName
                        aoParams["hazard_pixel_value"] = 1

                        aoParams["exposure_file_name"] = "exposure_urban_" + sDay + ".shp"
                        aoParams["exposure_markers_file"] = "markers_urban_" + sDay + ".shp"
                        aoParams["roads_file_name"] = "roads_urban_" + sDay + ".shp"
                        aoParams["lulc_map_name"] = "lulc_urban_" + sDay + ".tif"
                        aoParams["crops_file_name"] = "crops_urban_" + sDay + ".tif"

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

            sDay = oTask.referenceDate

            asFiles = wasdi.getProductsByActiveWorkspace()

            if sTargetMapType == "baresoil":
                logging.info("ImpactMapEngine.handleTask: handling impacts on bare soil map")
                sImpactFile = "exposure_baresoil_" + sDay + ".shp"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "markers_baresoil_" + sDay + ".shp"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "roads_baresoil_" + sDay + ".shp"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "lulc_baresoil_" + sDay + ".tif"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "crops_baresoil_" + sDay + ".tif"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
            elif sTargetMapType == "urban":
                logging.info("ImpactMapEngine.handleTask: handling impacts on urban map")
                sImpactFile = "exposure_urban_" + sDay + ".shp"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "markers_urban_" + sDay + ".shp"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "roads_urban_" + sDay + ".shp"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "lulc_urban_" + sDay + ".tif"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
                sImpactFile = "crops_urban_" + sDay + ".tif"
                self.checkAndPublishImpactLayer(sImpactFile, asFiles, oTask)
            else:
                logging.warning("ImpactMapEngine.handleTask: target map type not recognized:" + sTargetMapType + " . we stop here")

        except Exception as oEx:
            logging.error("ImpactMapEngine.handleTask: exception " + str(oEx))

    def checkAndPublishImpactLayer(self, sFile, asFiles, oTask):
        if sFile in asFiles:
            oMapConfig = self.getMapConfig()
            logging.info("ImpactMapEngine.checkAndPublishImpactLayer: found impacts Map to publish " + sFile)
            self.addAndPublishLayer(sFile, datetime.strptime(oTask.referenceDate, "%Y-%m-%d"), True,
                                    "urban_flood",
                                    sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource,
                                    sInputData=oMapConfig.inputData)
