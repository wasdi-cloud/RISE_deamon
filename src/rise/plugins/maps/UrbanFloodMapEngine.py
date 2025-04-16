import logging
import math
from datetime import datetime, timedelta

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class UrbanFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        logging.info("Urban Flood Map short archive is handled by the integrated chain")

    def triggerNewAreaArchives(self):
        logging.info("Urban Flood long archive is handled by the integrated chain")

    def updateNewMaps(self):
        oMapConfig = self.getMapConfig("sar_flood")

        aoParams = oMapConfig.params
        aoParams=vars(aoParams)
        sSuffix = aoParams["SUFFIX"]

        # Open our workspace
        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(oMapConfig)
        # Get the list of files
        asFiles = wasdi.getProductsByActiveWorkspace()

        # Check today
        oToday = datetime.today()
        sToday = oToday.strftime("%Y-%m-%d")
        self.updateUrbanFloodForDay(sToday, sWorkspaceId, asFiles, sSuffix)

        # Check yesterday
        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")
        self.updateUrbanFloodForDay(sYesterday, sWorkspaceId, asFiles, sSuffix)

    def updateUrbanFloodForDay(self, sDay, sWorkspaceId, asFiles, sSuffix):
        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            "flood_finder_in_archive", sDay)

        # if we have existing tasks
        if len(aoExistingTasks) > 0:
            logging.info("UrbanFloodMapEngine.updateNewMaps: today task already done or ongoing")
            return

        sBaseName = self.getBaseName("sar_flood")
        sBaseName += "_" + sDay + "_" + sSuffix

        if sBaseName in asFiles:
            logging.info("UrbanFloodMapEngine.updateNewMaps: found a new daily sar map")
            aoFloodFinderInArchiveParams = {}
            aoFloodFinderInArchiveParams["FLOOD_VALUE"] = 3
            aoFloodFinderInArchiveParams["NOT_FLOODED_VALUE"] = 1
            aoFloodFinderInArchiveParams["PERMANENT_WATER_VALUE"] = 2
            aoFloodFinderInArchiveParams["THREE_STATE"] = 1
            aoFloodFinderInArchiveParams["NO_DATA_VALUE"] = 0
            aoFloodFinderInArchiveParams["TARGET_FILE"] = sBaseName

            if not self.m_oConfig.daemon.simulate:
                sTaskId = wasdi.executeProcessor("flood_finder_in_archive", aoFloodFinderInArchiveParams)

                logging.info("UrbanFloodMapEngine.updateNewMaps: started flood finder in archive")
                oWasdiTask = self.createNewTask(sTaskId,sWorkspaceId,aoFloodFinderInArchiveParams,"flood_finder_in_archive",sDay)
                oWasdiTaskRepository.addEntity(oWasdiTask)
            else:
                logging.info("UrbanFloodMapEngine.updateNewMaps: simulation mode is on, think I started a flood finder in archive for day " + sDay)
        else:
            logging.info("UrbanFloodMapEngine.updateNewMaps: no flood map for date " + sDay + ", nothing to do")

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("UrbanFloodMapEngine.handleTask: handle task " + oTask.id)

            if oTask.application == "flood_finder_in_archive":
                # We executed a flood finder: we check if we need to start an urban flood or not
                logging.info("UrbanFloodMapEngine.handleTask: it is a flood finder, check if there are floods")

                # Take the payload of flood finder
                aoFinderPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

                # Safe checks
                if aoFinderPayload is None:
                    logging.warning("UrbanFloodMapEngine.handleTask: impossible to read the payload of task " + oTask.id)
                    return

                if "flooded" not in aoFinderPayload:
                    logging.info("UrbanFloodMapEngine.handleTask: impossible to get flooded from the payload of task " + oTask.id)
                    return

                # We ran with one target image
                aoFlooded = aoFinderPayload["flooded"]

                if len(aoFlooded)<=0:
                    logging.info("UrbanFloodMapEngine.handleTask: no flood found")
                    return

                oFlooded = aoFlooded[0]

                fPercFlooded = oFlooded["perc_flooded"]

                # Do we have any flood detected
                if fPercFlooded<=0:
                    logging.info("UrbanFloodMapEngine.handleTask: no flood found for date " + oTask.referenceDate)
                    return

                # Yes !!
                logging.info("UrbanFloodMapEngine.handleTask: Found some flood " + str(fPercFlooded) + "% for date " + oTask.referenceDate)

                self.startUrbanFlood(oTask.referenceDate)
            elif oTask.application == "edrift_auto_urban_flood":
                logging.info("UrbanFloodMapEngine.handleTask: check the urban flood output for date " + oTask.referenceDate)

                oUrbanFloodPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

                if oUrbanFloodPayload is None:
                    logging.info("UrbanFloodMapEngine.handleTask: Impossible to get the payload of the urban flood task, nothing to do")
                    return

                if "outputs" not in oUrbanFloodPayload:
                    logging.info("UrbanFloodMapEngine.handleTask: Outputs not available in the urban flood task, nothing to do")
                    return

                asUrbanMapFiles = oUrbanFloodPayload["outputs"]
                asFilesInWorkspace = wasdi.getProductsByActiveWorkspace()
                oMapConfig = self.getMapConfig()

                for sUrbanMapFile in asUrbanMapFiles:
                    if sUrbanMapFile in asFilesInWorkspace:
                        logging.info("UrbanFloodMapEngine.handleTask: found urban map in workspace " + sUrbanMapFile)
                        self.addAndPublishLayer(sUrbanMapFile, datetime.strptime(oTask.referenceDate,"%Y-%m-%d"), True, "urban_flood",
                                                sResolution=oMapConfig.resolution, sDataSource=oMapConfig.dataSource,
                                                sInputData=oMapConfig.inputData)

            else:
                logging.warning("UrbanFloodMapEngine.handleTask: NOT recognized application " + oTask.application)

        except Exception as oEx:
            logging.error("UrbanFloodMapEngine.handleTask: exception " + str(oEx))


    def startUrbanFlood(self, sDay):
        try:
            logging.info("UrbanFloodMapEngine.startUrbanFlood: Starting Urban Flood Detection for day " + sDay)
            oMapConfig = self.getMapConfig()

            aoParameters = oMapConfig.params
            aoParameters = vars(aoParameters)

            aoParameters["bbox"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["areaName"] = self.getBaseName()
            aoParameters["date"] =  sDay

            sBuildingsWorkspaceId = self.getBuildingsWorkspaceId()
            oDay = datetime.strptime(sDay, "%Y-%m-%d")

            sUrbanMap = self.findUrbanFootprintsInWorkspace(sBuildingsWorkspaceId, self.getBaseName("building_cw"), oDay)

            oSarMap = self.getMapConfig("sar_flood")

            sTargetWorkspace = self.m_oPluginEngine.createOrOpenWorkspace(oSarMap)

            if sUrbanMap != "":
                aoParameters["UrbanFootprintsMap"] = sUrbanMap
                asFilesInWorkspace = wasdi.getProductsByActiveWorkspace()

                if sUrbanMap not in asFilesInWorkspace:
                    wasdi.getFileFromWorkspaceId(sBuildingsWorkspaceId, sUrbanMap)
            else:
                aoParameters["AutomaticUrbanFootprintProcessor"]="world_cover_extractor"

            oWasdiTaskRepository = WasdiTaskRepository()

            # Take all the autofloodchain2 tasks for this day
            aoBareSoilTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, "sar_flood",
                                                                self.m_oPluginEntity.id, sTargetWorkspace,
                                                                "autofloodchain2", sDay)

            # We need to find the reference POST S1 Images:

            asTargetSLCImages = []
            if len(aoBareSoilTasks)<=0:
                # Strange, if we are here one at least should have been found, no?
                logging.warning("UrbanFloodMapEngine.startUrbanFlood: we cannot find any SAR autofloodchain2 run for day " + sDay)
            else:
                # Can be more than one
                logging.info("UrbanFloodMapEngine.startUrbanFlood: Found " + str(len(aoBareSoilTasks)) + " run for day " + sDay)

                for oTask in aoBareSoilTasks:
                    # If are not DONE are not of our interest

                    if self.isDoneStatus(oTask.status):
                        # Read the payload
                        aoBareSoilPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

                        # Check we have it
                        if aoBareSoilPayload is None:
                            continue

                        # And Input Images is in it
                        if "InputImages" in aoBareSoilPayload:
                            asInputImages = aoBareSoilPayload["InputImages"]

                            # Must be a valid list
                            if asInputImages is None:
                                continue

                            # Check all the input images
                            for sImage in asInputImages:
                                # If we did not add it yet, we must add it
                                if sImage not in asTargetSLCImages:
                                    asTargetSLCImages.append(sImage)
                                    logging.info("Adding target S1 Image " + sImage)

            # If none is found, we will try the default app even if it will be hard..
            if len(asTargetSLCImages) == 0:
                asTargetSLCImages.append("")

            if not self.m_oConfig.daemon.simulate:
                # For each target SLC Image found
                for sTargetSLCImage in asTargetSLCImages:

                    # Trigger the event
                    aoParameters["targetPostImage"] = sTargetSLCImage
                    sTaskId = wasdi.executeProcessor("edrift_auto_urban_flood", aoParameters)

                    oWasdiTask = self.createNewTask(sTaskId,sTargetWorkspace,aoParameters,"edrift_auto_urban_flood",sDay)
                    oWasdiTaskRepository.addEntity(oWasdiTask)
            else:
                logging.info("UrbanFloodMapEngine.startUrbanFlood: simulation mode on, I'm not really starting urban detection for " + sDay)

        except Exception as oEx:
            logging.error("UrbanFloodMapEngine.startUrbanFlood: exception " + str(oEx))

    def findUrbanFootprintsInWorkspace(self, sFootprintWorkspaceName, sAreaName, oReferenceDate):

        sFoundFootprintsMap = ""

        if sFootprintWorkspaceName != "":
            asPotentialFootprints = wasdi.getProductsByWorkspace(sFootprintWorkspaceName)
            if asPotentialFootprints is not None:
                aoCandidates = []
                for sPotentialFootprint in asPotentialFootprints:
                    if sPotentialFootprint.startswith(sAreaName) and sPotentialFootprint.endswith("Urban.tif"):
                        try:
                            asNameParts = sPotentialFootprint.split("_")
                            if len(asNameParts) == 3:

                                oMapDate = datetime.strptime(asNameParts[1], "%Y-%m-%d")
                                oDistance = oReferenceDate-oMapDate
                                iDistance = math.fabs(oDistance.days)
                                oCandidate = {"file": sPotentialFootprint,
                                              "date": oMapDate,
                                              "distance": iDistance}

                                aoCandidates.append(oCandidate)
                        except:
                            pass

                if len(aoCandidates) > 0:
                    wasdi.wasdiLog("Found " + str(len(aoCandidates)) + " Potential Footprint maps in the dedicated workspace")
                    sSelectedFile = aoCandidates[0]["file"]
                    iDistance = aoCandidates[0]["distance"]

                    for oCandidate in aoCandidates:
                        if oCandidate["distance"]<iDistance:
                            sSelectedFile = oCandidate["file"]
                            iDistance = oCandidate["distance"]

                    sFoundFootprintsMap = sSelectedFile

        return sFoundFootprintsMap

    def getBuildingsWorkspaceId(self):
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

        return sUrbanMapsWorkspaceName