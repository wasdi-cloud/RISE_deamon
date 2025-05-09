import logging
from datetime import datetime, timedelta
import os
from pathlib import Path
import uuid

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.business.Event import Event
from src.rise.business.WidgetInfo import WidgetInfo
from src.rise.data.EventRepository import EventRepository
from src.rise.data.MapRepository import MapRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class FloodEventFinderMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.info("FloodEventFinderMapEngine.triggerNewAreaArchives: Flood Depth long Archive Not supported")

    def updateNewMaps(self):
        # Take today as reference date
        oToday = datetime.now()

        # sToday = oToday.strftime("%Y-%m-%d")
        # # Check today
        # self.runForDate(sToday)

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

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is not None:
                # Now we need to understand if there is a new event

                if "OUTPUT" in aoPayload:
                    aoOutputEvents = aoPayload["OUTPUT"]

                    oEventRepository = EventRepository()
                    aoEvents = oEventRepository.getOngoing(self.m_oArea.id)                    

                    bAreThereOngoingEvents = False
                    for oOutputEvent in aoOutputEvents:
                        if  str(oOutputEvent["endDate"]) == "Ongoing":
                            bAreThereOngoingEvents = True
                            logging.info("FloodEventFinderMapEngine.handleTask: there is an ongoing flood event for date " + sDate)

                            if len(aoEvents) == 0:
                                oEvent = Event()
                                oEvent.name= "Flood_" + oOutputEvent["startDate"]
                                oEvent.type = "FLOOD"
                                oEvent.bbox = self.m_oArea.bbox
                                oEvent.peakStringDate = oOutputEvent["startDate"]

                                iStartDate = datetime.now().timestamp()
                                iPeakDate = datetime.now().timestamp()

                                try:
                                    iStartDate = datetime.strptime(oOutputEvent["startDate"], "%Y-%m-%d").timestamp()
                                except:
                                    logging.warning("Error converting event start date " + str(oOutputEvent["startDate"]))
                                
                                try:
                                    iPeakDate = datetime.strptime(oOutputEvent["peakDate"], "%Y-%m-%d").timestamp()
                                except:
                                    logging.warning("Error converting event end date " + str(oOutputEvent["peakDate"]))

                                oEvent.peakDate = iPeakDate
                                oEvent.startDate = iStartDate

                                oEvent.areaId = self.m_oArea.id
                                oEvent.id = str(uuid.uuid4())

                                oEvent.publicEvent = False
                                oEvent.inGoing = True
                                oEvent.description = "Automatic Flood Event peak = " + oOutputEvent["peakDate"]
                                oEvent.markerCoordinates = self.m_oArea.markerCoordinates
                                oEventRepository.addEntity(oEvent)

                            else:
                                logging.info("FloodEventFinderMapEngine.handleTask: there is already an ongoing flood event for date " + sDate)
                                oEvent = aoEvents[0]
                                oEvent.peakStringDate = oOutputEvent["peakDate"]

                    if not bAreThereOngoingEvents:
                        # We have to close the event
                        if len(aoEvents) > 0:
                            oEvent = aoEvents[0]
                            oEvent.inGoing = False
                            oEventRepository.updateEntity(oEvent)
                            logging.info("FloodEventFinderMapEngine.handleTask: flood event ended for date " + sDate)
                            
        except Exception as oEx:
            logging.error("FloodEventFinderMapEngine.handleTask: exception " + str(oEx))

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
            logging.info("FloodEventFinderMapEngine.runForDate: a task is still ongoing or executed for day " + sDate + ". Nothing to do")
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

            aoParams = vars(self.getMapConfig().params)
            
            aoParams["BASENAME"] = sBaseName
            aoParams["SUFFIX"] = sSuffix
            aoParams["UPDATE_MODE"] = True
            aoParams["SPECIFIC_DATE"] = sDate

            if not self.m_oConfig.daemon.simulate:
                # Run the Flood Depths app
                sProcessorId = wasdi.executeProcessor(oFloodDepthConfig.processor, aoParams)

                oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParams, oFloodDepthConfig.processor, sDate)
                oWasdiTaskRepository.addEntity(oWasdiTask)

                logging.info(
                    "FloodEventFinderMapEngine.updateNewMaps: Started event finder in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                        self.m_oMapEntity) + " for Area " + self.m_oArea.name)
            else:
                logging.info("FloodEventFinderMapEngine.updateNewMaps: simulation mode on, like I started event finder for date " + sDate)
        else:
            logging.info("FloodEventFinderMapEngine.updateNewMaps: there is no new flood Map for date " + sDate)


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

    def addOrUpdateWidgetInfo(self, oEvent):

        sMessage = "Flood in " + self.m_oArea.name 
        iEventReferenceDate = iPeakDate = datetime.now().timestamp()
        oWidgetInfo = WidgetInfo.createWidgetInfo("events", self.m_oArea, "text", "flag", "WIDGET.EVENTS", sMessage, iEventReferenceDate)