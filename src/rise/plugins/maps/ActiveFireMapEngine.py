import datetime
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ActiveFireMapEngine(RiseMapEngine):
    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        # logging.info("ActiveFireMapEngine.triggerNewAreaArchives: IMERG long Archive Not supported")
        logging.info("ActiveFireMapEngine.triggerNewAreaArchives: Currently not developed.")

    def updateNewMaps(self):
        logging.info("ActiveFireMapEngine.updateNewMaps: Update New Maps")

        oNow = datetime.datetime.now(datetime.UTC)
        sDay = oNow.strftime("%Y-%m-%d")

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig("active_fire_map")

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, "active_fire_map",
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sDay)


        # if we have existing tasks
        for oTask in aoExistingTasks:
            if self.isRunningStatus(oTask.status):
                logging.info("ActiveFireMapEngine.updateNewMaps: a task is still ongoing  for day " + sDay + " we will wait it to finish " + oTask.id)
                return
        oToday = datetime.datetime.today()
        sToday = oToday.strftime("%Y-%m-%d")
        sBaseName = self.getBaseName()
        sOutputFileName ="ActiveFire_"+ sBaseName + "_" + sToday + ".tif"

        asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

        if sOutputFileName in asWorkspaceFiles:
            logging.info("ActiveFireMapEngine.updateNewMaps: We already have this product ready for today , no need to run again , product name is " + sOutputFileName)
            return


        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["bbox"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            # aoParameters["REFERENCE_DATETIME"] = sDay + " " + sHour + ":00"

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)
            if not self.checkProcessorId(sProcessorId):
                return
            oWasdiTask = self.createNewTask(sProcessorId, sWorkspaceId, aoParameters, oMapConfig.processor, sDay)
            # Override: one for all in the tasks!
            oWasdiTask.mapId = "active_fire_map"
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("ActiveFireMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sDay)
        else:
            logging.warning("ActiveFireMapEngine.updateNewMaps: simulation mode on - we do not run nothing")

    def handleTask(self, oTask):
        try:
            # First, we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("ActiveFireMapEngine.handleTask: handle task " + oTask.id)

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is None:
                logging.info("ActiveFireMapEngine.handleTask: cannot read the payload, we stop here ")
                return

            if "Daily Fire Maps" not in aoPayload:
                logging.info("ActiveFireMapEngine.handleTask: Daily Fire Maps not in the payload, we stop here ")
                return

            asDailyFireMaps = aoPayload["Daily Fire Maps"]

            if len(asDailyFireMaps) <= 0:
                logging.info("ActiveFireMapEngine.handleTask: Daily Fire Maps array is empty, we stop here ")
                return

            # Take the first one item from asDailyFireMap

            sFile = asDailyFireMaps[0]

            # checking if the file really exist in the wasdi product list
            asFiles = wasdi.getProductsByActiveWorkspace()

            if sFile not in asFiles:
                # should exist
                logging.info("ActiveFireMapEngine.handleTask: the map does not exist in the product list ,something is wrong, we stop here ")
                return

            oReferenceDate = datetime.datetime.strptime(oTask.referenceDate, "%Y-%m-%d")
            sMapConfig = "active_fire_map"
            oMapConfig = self.getMapConfig(sMapConfig)

            self.addAndPublishLayer(sFile, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                    bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                    sResolution=oMapConfig.resolution, sInputData=oMapConfig.inputData,
                                    sOverrideMapId=sMapConfig)

        except Exception as oEx:
            logging.error("ActiveFireMapEngine.handleTask: exception " + str(oEx))
