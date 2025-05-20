import logging
from datetime import datetime, timedelta
import os
from pathlib import Path

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.data.MapRepository import MapRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class FloodDepthMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.info("FloodDepthMapEngine.triggerNewAreaArchives: Flood Depth long Archive Not supported")

    def updateNewMaps(self):
        # Take today as reference date
        oToday = datetime.now()

        sToday = oToday.strftime("%Y-%m-%d")
        # Check today
        self.runForDate(sToday)

        # Go to yesterday
        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")

        self.runForDate(sYesterday)

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            sDate = oTask.referenceDate
            oDate = datetime.strptime(sDate,"%Y-%m-%d")

            oMapConfig = self.getMapConfig()
            sBaseName = self.getBaseName("sar_flood")

            sDepthFileName = sBaseName + "_" + sDate + "_water-depth.tif"
            sSurfaceFileName = sBaseName + "_" + sDate + "_water-surface.tif"
            sBareSoilFileName = sBaseName + "_" + sDate + self.getBaresoilSuffix()

            self.openSarFloodWorkspace()
            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            if sDepthFileName in asWorkspaceFiles:
                self.addAndPublishLayer(sDepthFileName, oDate, True, oMapConfig.id, sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=sBareSoilFileName, bForceRepublish=True)

            # NOTE: the surface?
            
        except Exception as oEx:
            logging.error("FloodDepthMapEngine.handleTask: exception " + str(oEx))

    def runForDate(self, sDate):
        # Get the flood depth map config
        oFloodDepthConfig = self.getMapConfig()

        sWorkspaceId = self.openSarFloodWorkspace()

        # Did we already start any map for this day?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for this day
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId, oFloodDepthConfig.processor, sDate)

        # if we have existing tasks
        if len(aoExistingTasks)>0:
            logging.info("FloodDepthMapEngine.runForDate: a task is still ongoing or executed for day " + sDate + ". Nothing to do")
            return

        # We read the params of the floodchain to have the suffix        
        sBaseName = self.getBaseName("sar_flood")
        sSuffix = self.getBaresoilSuffix()

        # This should be the daily SAR map
        sFileName = sBaseName + "_" + sDate + "_" + sSuffix

        # Take the list of files in the workspace
        asFiles = wasdi.getProductsByActiveWorkspace()

        # Is the file in the workspace?
        if sFileName in asFiles:
            
            sDemFile = sBaseName + "_dem.tif"

            aoParams = vars(self.getMapConfig().params)

            if sDemFile in asFiles:
                aoParams["GENERATE_DEM"] = False
                aoParams["DEM_DELETE"] = False
                aoParams["DEM"] = sDemFile
            else:
                aoParams["GENERATE_DEM"] = True
                aoParams["DEM_DELETE"] = False
                aoParams["DEM_OUTPUT"] = sDemFile
            
            aoParams["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParams["FLOODMAP"] = sFileName
            aoParams["OUTPUT_WATER_DEPTH"] = sFileName.replace(sSuffix,"water-depth.tif")
            aoParams["OUTPUT_WATER_SURFACE"] = sFileName.replace(sSuffix,"water-surface.tif")

            if not self.m_oConfig.daemon.simulate:
                # Run the Flood Depths app
                sProcessorId = wasdi.executeProcessor(oFloodDepthConfig.processor, aoParams)

                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParams, oFloodDepthConfig.processor, sDate)
                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info(
                    "FloodDepthMapEngine.updateNewMaps: Started flood depth in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                        self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.info("FloodDepthMapEngine.updateNewMaps: simulation mode on, like I started flood depth for date " + sDate)
        else:
            logging.info("FloodDepthMapEngine.updateNewMaps: there is no new flood Map for date " + sDate)


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
