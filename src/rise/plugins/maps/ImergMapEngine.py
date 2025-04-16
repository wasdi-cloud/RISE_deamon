import logging
from datetime import datetime

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ImergMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.info("ImergMapEngine.triggerNewAreaArchives: IMERG long Archive Not supported")

    def updateNewMaps(self):
        logging.info("ImergMapEngine.triggerNewAreaArchives: Update New Maps")

        oNow = datetime.now()
        sDay = oNow.strftime("%Y-%m-%d")
        sHour = oNow.strftime("%H")

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig("imerg_cumulate")

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, self.m_oMapEntity.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sDay)

        bWaitNextHour = False
        for oTask in aoExistingTasks:

            if "time" in oTask.pluginPayload:
                sTime = oTask.pluginPayload["time"]
                if sTime==sHour:
                    bWaitNextHour = True
                    break

        if bWaitNextHour:
            logging.info("ImergMapEngine.triggerNewAreaArchives: found task for " + sDay + " " + sHour + ", we wait next hour")
            return

        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASE_NAME"] = self.getBaseName()
            aoParameters["REFERENCE_DATETIME"] = sDay + " " + sHour + ":00"

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)

            oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParameters,oMapConfig.processor,sDay)
            oWasdiTask.pluginPayload["time"] = sHour
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("ViirsFloodMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sDay + " " + sHour)
        else:
            logging.warning("ViirsFloodMapEngine.updateNewMaps: simulation mode on - we do not run nothing")

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("ImergMapEngine.handleTask: handle task " + oTask.id)

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is None:
                logging.info("ImergMapEngine.handleTask: cannot read the payload, we stop here ")
                return

            if "OUTPUTS" not in aoPayload:
                logging.info("ImergMapEngine.handleTask: OUTPUTS not in the payload, we stop here ")
                return

            asOutputs = aoPayload["OUTPUTS"]

            if len(asOutputs)<=0:
                logging.info("ImergMapEngine.handleTask: OUTPUTS array is empty, we stop here ")
                return

            sTime = "00"

            try:
                sTime = oTask.pluginPayload["time"]
            except Exception as oInEx:
                logging.warning("ImergMapEngine.handleTask:  error reading the time from task payload " + str(oInEx))

            sInputData = ""

            if "IMERG_FILES" in aoPayload:
                for sInputFile in aoPayload["IMERG_FILES"]:
                    sInputData += sInputFile + " "

            asFiles = wasdi.getProductsByActiveWorkspace()

            for sFile in asOutputs:
                if sFile in asFiles:
                    logging.info("ImergMapEngine.handleTask: publishing " + sFile)

                    oReferenceDate = datetime.strptime(oTask.referenceDate, "%Y-%m-%d")
                    oReferenceDate = oReferenceDate.replace(hour=int(sTime))

                    oMapConfig = self.getMapConfig()

                    self.addAndPublishLayer(sFile, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                            bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                            sResolution=oMapConfig.resolution, sInputData=sInputData, bForceRepublish=True)

        except Exception as oEx:
            logging.error("ImergMapEngine.handleTask: exception " + str(oEx))