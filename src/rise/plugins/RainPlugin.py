import datetime
import logging
import uuid
import wasdi
import os
import json 

from pathlib import Path
from src.rise.data.LayerRepository import LayerRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.RisePlugin import RisePlugin
from src.rise.geoserver.GeoserverService import GeoserverService

class RainPlugin(RisePlugin):
    """

    """
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)

    def triggerNewAreaArchives(self):
        logging.info("RainPlugin.triggerNewAreaArchives: long archive is handled by the integrated chain")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "imerg_cumulate_12":
                    logging.info("RainPlugin.triggerNewAreaArchives: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaArchives()
                    break

        except Exception as oEx:
            logging.error("RainPlugin.triggerNewAreaArchives: exception " + str(oEx))

    def triggerNewAreaMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RainPlugin.triggerNewAreaMaps OVERRIDE")
        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "imerg_cumulate_12":
                    logging.info("RainPlugin.triggerNewAreaMaps: Starting ONCE new Maps for all the Sub Maps: " + oMapEngine.getName())
                    oMapEngine.triggerNewAreaMaps()
                    break

        except Exception as oEx:
            logging.error("RainPlugin.triggerNewAreaMaps: exception " + str(oEx))

    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RainPlugin.updateNewMaps OVERRIDE")

        sWorkspaceName = self.m_oPluginConfig.workspace
        sWorkspaceId = wasdi.openWorkspace(sWorkspaceName)
        sProcessorId = str(uuid.uuid4())
        aoParameters = {}
        sProcessor="global_rain"
        oNow = datetime.datetime.now(datetime.UTC)
        sDay = oNow.strftime("%Y-%m-%d")
        sHour = oNow.strftime("%H")

        try:
            for oMapEngine in self.m_aoMapEngines:
                if oMapEngine.m_oMapEntity.id == "imerg_cumulate_12":
                    oWasdiTaskRepository = WasdiTaskRepository()
                    oTask = oMapEngine.createNewTask(sProcessorId,sWorkspaceId,aoParameters,sProcessor,sDay)
                    oTask.mapId = "imerg_cumulate"
                    oTask.pluginPayload["time"] = sHour
                    oWasdiTaskRepository.addEntity(oTask)

                    break

        except Exception as oEx:
            logging.error("RainPlugin.updateNewMaps: exception " + str(oEx))        


    def handleTask(self, oTask):
        
        # This is not a real WASDI task, consider it DONE in any case
        oWasdiTaskRepository = WasdiTaskRepository()
        oTask.status = "DONE"
        #oWasdiTaskRepository.updateEntity(oTask)

        # Get Reference Date and Time
        sReferenceDate = oTask.referenceDate
        sTime = "00"

        try:
            sTime = oTask.pluginPayload["time"]
        except Exception as oInEx:
            logging.warning("RainPlugin.handleTask:  error reading the time from task payload " + str(oInEx))

        # Open the workspace
        wasdi.openWorkspace(self.m_oPluginConfig.workspace)
        # Get files in the workspace
        asFilesInWorkspace = wasdi.getProductsByActiveWorkspace()
        # Get processes in the workspace
        aoProcesses = wasdi.getProcessesByWorkspace(iEndIndex=10, sOperationType="RUNPROCESSOR", sName="imerg_fixed_time_cumulate")
        # Layer repo, will be used later
        oLayerRepository = LayerRepository()
        # Get the Geoserver Service
        oGeoserverService = GeoserverService()

        # For each Process
        for oProcess in aoProcesses:
            # We need it done
            if oProcess["status"] != "DONE":
                continue

            # Get the payload
            aoPayload = json.loads(oProcess["payload"])

            # We create the input data string
            sInputData = ""
            if "IMERG_FILES" in aoPayload:
                for sInputFile in aoPayload["IMERG_FILES"]:
                    sInputData += sInputFile + " "
            
            # Default reference datetime
            sReferenceDateTime = sReferenceDate + " " + sTime + ":00"

            aoInputs = aoPayload["INPUTS"]

            if "REFERENCE_DATETIME" in aoInputs:
                # But better if declared in the payload
                sReferenceDateTime = aoInputs["REFERENCE_DATETIME"]

            # Create datetime object from string
            oReferenceDateTime = datetime.datetime.strptime(sReferenceDateTime, "%Y-%m-%d %H:%M")                        

            # Now for each output generated
            if "OUTPUTS" in aoPayload:
                # Extract the array of output files
                asOutputs = aoPayload["OUTPUTS"]

                # For each generated rain map
                for sOutputFile in asOutputs:

                    # Check if the output file is in the workspace
                    if sOutputFile in asFilesInWorkspace:
                        logging.info("RainPlugin.handleTask: output file " + sOutputFile + " is present")
                        sLayerId = sOutputFile.replace(".tif","")

                        # If the layer does not exists
                        if not oGeoserverService.existsLayer(sLayerId):
                            # We publish it
                            self.publishRasterLayer(oGeoserverService, sOutputFile, sStyleName="imerg_cumulate")

                        # Get the right map engine id  from the file name
                        sMapId = "imerg_cumulate_12"
                        if "3hr" in sOutputFile:
                            sMapId = "imerg_cumulate_3"
                        elif "6hr" in sOutputFile:
                            sMapId = "imerg_cumulate_6"
                        elif "24hr" in sOutputFile:
                            sMapId = "imerg_cumulate_24"
                        
                        # Search if the layer entity is already in DB for this area
                        aoFilters = {}
                        aoFilters["id"] = sLayerId
                        aoFilters["areaId"] = self.m_oArea.id
                        aoFilters["mapId"] = sMapId

                        aoLayers = oLayerRepository.getEntitiesByField(aoFilters)

                        if aoLayers is not None and len(aoLayers)>0:
                            # Layer already existing
                            logging.info("RainPlugin.handleTask: Layer Entity " + sLayerId + " already exists in DB")
                            continue

                        # Not exists, we create it: get the map engine
                        oMapEngine = self.getMapEngineFromMapId(sMapId)
                        # Create the layer entity
                        oCreatedLayer = oMapEngine.getLayerEntity(sLayerId, oReferenceDateTime.timestamp(), sDataSource="", oCreationDate=None, sResolution="", sInputData=sInputData, aoProperties=None)
                        oCreatedLayer.published = True
                        # Add it to the repository
                        oLayerRepository.addEntity(oCreatedLayer)

                    else:
                        logging.warning("RainPlugin.handleTask: output file " + sOutputFile + " is NOT present, we skip it")
                        continue
        
        return


    def publishRasterLayer(self, oGeoserverService, sFileName, sStyleName=None):
        try:
            sLocalFilePath = wasdi.getPath(sFileName)
            sLayerName = Path(str(sLocalFilePath)).stem

            oWorkspace = oGeoserverService.getWorkspace(self.m_oConfig.geoserver.workspace)

            if oWorkspace is None:
                oGeoserverService.createWorkspace(self.m_oConfig.geoserver.workspace)

            oStore = oGeoserverService.publishRasterLayer(sLocalFilePath, self.m_oConfig.geoserver.workspace, sLayerName, sStyleName)
            os.remove(sLocalFilePath)

            if oStore is not None:
                return True
            else:
                return False
        except Exception as oEx:
            logging.error("RiseMapRainPluginEngine.publishRasterLayer exception " + str(oEx))

        return False

