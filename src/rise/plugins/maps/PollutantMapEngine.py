import datetime
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine
from src.rise.utils.RiseUtils import listTostring


class PollutantMapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        # logging.info("PollutantMapEngine.triggerNewAreaArchives: IMERG long Archive Not supported")
        logging.info("PollutantMapEngine.triggerNewAreaArchives: long Archive Not supported.")

    def updateNewMaps(self):
        logging.info("PollutantMapEngine.updateNewMaps: Update New Maps")

        oNow = datetime.datetime.now(datetime.UTC)

        sDay = oNow.strftime("%Y-%m-%d")

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig("pollutant_map")

        # # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, "pollutant_map", self.m_oPluginEntity.id,
                                                            sWorkspaceId, oMapConfig.processor, sDay)

        # if we have existing tasks
        for oTask in aoExistingTasks:
            if self.isRunningStatus(oTask.status):
                logging.info(
                    "PollutantMapEngine.updateNewMaps: a task is still ongoing  for day " + sDay + " we will wait it to finish " + oTask.id)
                return
        # oToday = datetime.datetime.today()
        # sToday = oToday.strftime("%Y-%m-%d")
        oYesterday = oNow - datetime.timedelta(days=1)
        sYesterday = oYesterday.strftime("%Y-%m-%d")
        sBaseName = self.getBaseName()
        # TODO this is static for now but will change to be dynamic then will prob check a list of the other elements

        asPollutants = ["NO2", "HCHO", "CO", "O3", "CH4", "SO2"]
        asPollutantsToCreateNewAppFpr = []
        asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()
        for sPollutantName in asPollutants:
            # todo this is a temp fix for the output file name,
            sOutputFileName1 = sBaseName + "_S5_" + sPollutantName + "_" + sYesterday + "_" + sYesterday
            sOutputFileName2 = sBaseName + "_S5_" + sPollutantName + "_Day" + sYesterday
            # here we found the pollutant element in the workspace so no need to do it again
            if sOutputFileName1 in asWorkspaceFiles or sOutputFileName2 in asWorkspaceFiles or sOutputFileName1 + ".tif" in asWorkspaceFiles or sOutputFileName2 + ".tif" in asWorkspaceFiles:
                logging.info(
                    "PollutantMapEngine.updateNewMaps: We already have this product ready for today , no need to run again , product name is " + sOutputFileName1)
                continue
            else:
                asPollutantsToCreateNewAppFpr.append(sPollutantName)
        # here we have product for each pollutant, so no need to launch a new app
        if len(asPollutantsToCreateNewAppFpr) == 0:
            logging.info(
                "PollutantMapEngine.updateNewMaps: We already have for all pollutants element a  product ready for today , no need to run again")
            return

        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            aoParameters["Pollutants"] = listTostring(asPollutantsToCreateNewAppFpr)
            aoParameters["STARTDATE"] = sYesterday
            aoParameters["ENDDATE"] = sYesterday
            aoParameters["DELETEDAILYMAPS"] = True

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)
            if not self.checkProcessorId(sProcessorId):
                return
            oWasdiTask = self.createNewTask(sProcessorId, sWorkspaceId, aoParameters, oMapConfig.processor, sDay)
            # Override: one for all in the tasks!
            oWasdiTask.mapId = "pollutant_map"
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("PollutantMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sDay)
        else:
            logging.warning("PollutantMapEngine.updateNewMaps: simulation mode on - we do not run nothing")

    def handleTask(self, oTask):
        try:
            # First, we check if it is safe and done
            if not super().handleTask(oTask):
                return False
            logging.info("PollutantMapEngine.handleTask: handle task " + oTask.id)
            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)
            if aoPayload is None:
                logging.info("PollutantMapEngine.handleTask: cannot read the payload, we stop here ")
                return
            # oToday = datetime.datetime.today()
            # sToday = oToday.strftime("%Y-%m-%d")
            oNow = datetime.datetime.now(datetime.UTC)
            oYesterday = oNow - datetime.timedelta(days=1)
            sYesterday = oYesterday.strftime("%Y-%m-%d")
            sBaseName = self.getBaseName()
            # TODO this is static for now but will change to be dynamic then will prob check a list of the other elements
            asPollutants = ["NO2", "HCHO", "CO", "O3", "CH4", "SO2"]
            asFiles = wasdi.getProductsByActiveWorkspace()
            oReferenceDate = datetime.datetime.strptime(oTask.referenceDate, "%Y-%m-%d")
            sMapConfig = "pollutant_map"
            for sPollutantName in asPollutants:
                sOutputFileName1 = sBaseName + "_S5_" + sPollutantName + "_" + sYesterday + "_" + sYesterday
                sOutputFileName2 = sBaseName + "_S5_" + sPollutantName + "_Day" + sYesterday
                # checking if the file really exists in the wasdi product list
                if sOutputFileName1 in asFiles or sOutputFileName1 + ".tif" in asFiles:
                    # here we will add the .tif in case it does not exist
                    if not sOutputFileName1.endswith(".tif"):
                        sOutputFileName1 += ".tif"

                    oMapConfig = self.getMapConfig(sPollutantName)

                    if oMapConfig is None:
                        logging.info(
                            "PollutantMapEngine.handleTask: cannot find map config for map id " + sPollutantName + " we stop here ")
                        continue
                    # publish each map alone
                    self.addAndPublishLayer(sOutputFileName1, oReferenceDate, bPublish=True,
                                            sMapIdForStyle=sPollutantName, bKeepLayer=False,
                                            sDataSource=oMapConfig.dataSource, sResolution=oMapConfig.resolution,
                                            sInputData=oMapConfig.inputData, sOverrideMapId=sPollutantName)
                elif sOutputFileName2 in asFiles or sOutputFileName2 + ".tif" in asFiles:

                    # here we will add the .tif in case it does not exist
                    if not sOutputFileName1.endswith(".tif"):
                        sOutputFileName1 += ".tif"

                    oMapConfig = self.getMapConfig(sPollutantName)

                    if oMapConfig is None:
                        logging.info(
                            "PollutantMapEngine.handleTask: cannot find map config for map id " + sPollutantName + " we stop here ")
                        continue
                    # publish each map alone
                    self.addAndPublishLayer(sOutputFileName1, oReferenceDate, bPublish=True,
                                            sMapIdForStyle=sPollutantName, bKeepLayer=False,
                                            sDataSource=oMapConfig.dataSource, sResolution=oMapConfig.resolution,
                                            sInputData=oMapConfig.inputData, sOverrideMapId=sPollutantName)
                else:
                    # should exist
                    logging.info(
                        "PollutantMapEngine.handleTask: the map does not exist in the product list ,something is wrong, we stop here ")
                    continue







        except Exception as oEx:
            logging.error("PollutantMapEngine.handleTask: exception " + str(oEx))
