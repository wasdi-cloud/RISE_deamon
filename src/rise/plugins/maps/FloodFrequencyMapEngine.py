import json
import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.data.MapRepository import MapRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class FloodFrequencyMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)
        self.m_sChainParamsFile = "integratedSarChainParams.json"

    def triggerNewAreaMaps(self):
        logging.info("FloodFrequencyMapEngine.triggerNewAreaMaps [" + self.m_oArea.name +"]: Flood Frequency Map short archive is handled by the integrated chain")


    def triggerNewAreaArchives(self):
        logging.info("FloodFrequencyMapEngine.triggerNewAreaArchives[" + self.m_oArea.name +"]: Flood Frequency Map long archive is handled by the integrated chain")

    
    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            # We need the sar flood map entity
            oMapRepository = MapRepository()
            oSarMap = oMapRepository.getEntityById("sar_flood")

            # Get the reference date
            sDate = oTask.referenceDate
            oDate = datetime.strptime(sDate,"%Y-%m-%d")

            # We open the workspace
            self.m_oPluginEngine.createOrOpenWorkspace(oSarMap)
            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            # We check and update the 3 maps if present
            sFileName = self.getFFMfloodMapName()
            oMapConfig = self.getMapConfig("flood_frequency_map")

            if sFileName in asWorkspaceFiles:
                self.updateChainParamsDate(self.m_sChainParamsFile, sDate, "lastFFMUpdate")
                self.addAndPublishLayer(sFileName, oDate, True, "flood_frequency_map", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData, bForceRepublish=True)

            sFileName = self.getFFMdataMapName()
            oMapConfig = self.getMapConfig("flood_frequency_map_data")

            if sFileName in asWorkspaceFiles:
                self.addAndPublishLayer(sFileName, oDate, True, "flood_frequency_map_data", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData, bForceRepublish=True)

            sFileName = self.getFFMpercMapName()
            oMapConfig = self.getMapConfig("flood_frequency_map_perc")

            if sFileName in asWorkspaceFiles:
                self.addAndPublishLayer(sFileName, oDate, True, "flood_frequency_map_perc", sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource, sInputData=oMapConfig.inputData, bForceRepublish=True)

        except Exception as oEx:
            logging.error("FloodFrequencyMapEngine.handleTask [" + self.m_oArea.name + "]: exception " + str(oEx))

    def updateNewMaps(self):

        if self.m_oMapEntity.id != "flood_frequency_map":
            logging.debug("FloodFrequencyMapEngine.updateNewMaps [" + self.m_oArea.name + "]: map id is not flood_frequency_map, we stop here")
            return
        
        # Take today as reference date
        oToday = datetime.now()
        # Go to yesterday
        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")

        self.runFFMMapForDate(sYesterday)

    def runFFMMapForDate(self, sDate):
        # Take today as reference date
        oMapRepository = MapRepository()
        oSarMap = oMapRepository.getEntityById("sar_flood")

        # We open the workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(oSarMap)

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId, "floodfrequencymap", sDate)

        # if we have existing tasks
        if len(aoExistingTasks)>0:
            logging.info("FloodFrequencyMapEngine.updateNewMaps[" + self.m_oArea.name +"]: a task is still ongoing or executed for day " + sDate + ". Nothing to do")
            return

        # We read the params of the floodchain to have the suffix
        oAutoFloodChainMapConfig = self.getMapConfig("autofloodchain2")
        sBaseName = self.getBaseName("sar_flood")
        oAutoFloodChainParams =oAutoFloodChainMapConfig.params
        aoFloodChainParameters = vars(oAutoFloodChainParams)

        # This should be the new daily SAR map
        sFileName = sBaseName + "_" + sDate + "_" + aoFloodChainParameters["SUFFIX"]

        # Take the list of files in the workspace
        asFiles = wasdi.getProductsByActiveWorkspace()

        # Is the file in the workspace?
        if sFileName in asFiles:
            # Take the chain params
            aoChainParams = self.getWorkspaceUpdatedJsonFile(self.m_sChainParamsFile)

            if aoChainParams is None:
                logging.warning("FloodFrequencyMapEngine.updateNewMaps [" + self.m_oArea.name +"]: the chain params file is not available: likely, the archive still have to finish. We stop here")
                return

            # Check the last date of the FFM
            sLastFFMDate = "0000-00-00"
            if "lastFFMUpdate" in aoChainParams:
                sLastFFMDate = aoChainParams["lastFFMUpdate"]

            # If it has not been added yet
            if sDate>sLastFFMDate:
                aoFFMParams = vars(self.getMapConfig().params)
                aoFFMParams["prefix"] = sBaseName
                aoFFMParams["updateExistingMap"] = True
                aoFFMParams["floodMapToUpdate"] = self.getFFMfloodMapName()
                aoFFMParams["dataMapToUpdate"] = self.getFFMdataMapName()
                aoFFMParams["frequencyMapToUpdate"] = self.getFFMpercMapName()
                aoFFMParams["startDate"] = sDate
                aoFFMParams["endDate"] = sDate

                if not self.m_oConfig.daemon.simulate:
                    # Run the FFM to update
                    sProcessorId = wasdi.executeProcessor("floodfrequencymap", aoFFMParams)
                    if not self.checkProcessorId(sProcessorId):
                        return

                    oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoFFMParams,"floodfrequencymap", sDate)
                    oWasdiTaskRepository.addEntity(oWasdiTask)

                    logging.info("FloodFrequencyMapEngine.updateNewMaps [" + self.m_oArea.name +"]: Started floodfrequencymap in Workspace " + self.m_oPluginEngine.getWorkspaceName( self.m_oMapEntity) + " for Area " + self.m_oArea.name)
                else:
                    logging.info("FloodFrequencyMapEngine.updateNewMaps [" + self.m_oArea.name +"]: simulation mode on, like I started FFM for date " + sDate)
        else:
            logging.info("FloodFrequencyMapEngine.updateNewMaps [" + self.m_oArea.name +"]: there is no new flood Map for date " + sDate)

    def getFFMfloodMapName(self):
        return self.getBaseName("sar_flood")+"_ffm_flood.tif"

    def getFFMdataMapName(self):
        return self.getBaseName("sar_flood")+"_ffm_data.tif"
    
    def getFFMpercMapName(self):
        return self.getBaseName("sar_flood")+"_ffm_frequency.tif"    