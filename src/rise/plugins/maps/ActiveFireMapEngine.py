import logging
import datetime

import wasdi

from src.rise.business.Plugin import Plugin
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
        sHour = oNow.strftime("%H")

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig("active_fire_map")

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, "active_fire_map",
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sDay)

        bWaitNextHour = False
        for oTask in aoExistingTasks:

            if "time" in oTask.pluginPayload:
                sTime = oTask.pluginPayload["time"]
                if sTime == sHour:
                    bWaitNextHour = True
                    break

        if bWaitNextHour:
            logging.info(
                "ActiveFireMapEngine.triggerNewAreaArchives: found task for " + sDay + " " + sHour + ", we wait next hour")
            return

        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["bbox"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            # aoParameters["REFERENCE_DATETIME"] = sDay + " " + sHour + ":00"

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)

            oWasdiTask = self.createNewTask(sProcessorId, sWorkspaceId, aoParameters, oMapConfig.processor, sDay)
            # Override: one for all in the tasks!
            oWasdiTask.mapId = "active_fire_map"
            oWasdiTask.pluginPayload["time"] = sHour
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("ActiveFireMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sDay + " " + sHour)
        else:
            logging.warning("ActiveFireMapEngine.updateNewMaps: simulation mode on - we do not run nothing")




    def handleTask(self, oTask):
        try:
            # First we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("ActiveFireMapEngine.handleTask: handle task " + oTask.id)

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is None:
                logging.info("ActiveFireMapEngine.handleTask: cannot read the payload, we stop here ")
                return

            if "OUTPUTS" not in aoPayload:
                logging.info("ActiveFireMapEngine.handleTask: OUTPUTS not in the payload, we stop here ")
                return

            asOutputs = aoPayload["OUTPUTS"]

            if len(asOutputs)<=0:
                logging.info("ActiveFireMapEngine.handleTask: OUTPUTS array is empty, we stop here ")
                return

            sTime = "00"

            try:
                sTime = oTask.pluginPayload["time"]
            except Exception as oInEx:
                logging.warning("ActiveFireMapEngine.handleTask:  error reading the time from task payload " + str(oInEx))

            sInputData = ""

            if "IMERG_FILES" in aoPayload:
                for sInputFile in aoPayload["IMERG_FILES"]:
                    sInputData += sInputFile + " "

            asFiles = wasdi.getProductsByActiveWorkspace()

            for sFile in asOutputs:
                if sFile in asFiles:
                    logging.info("ActiveFireMapEngine.handleTask: publishing " + sFile)

                    oReferenceDate = datetime.datetime.strptime(oTask.referenceDate, "%Y-%m-%d")
                    oReferenceDate = oReferenceDate.replace(hour=int(sTime))

                    sMapConfig = "imerg_cumulate_12"

                    if "Cumulative_24hr" in sFile:
                        sMapConfig = "imerg_cumulate_24"

                    if "Cumulative_6hr" in sFile:
                        sMapConfig = "imerg_cumulate_6"

                    if "Cumulative_3hr" in sFile:
                        sMapConfig = "imerg_cumulate_3"

                    oMapConfig = self.getMapConfig(sMapConfig)

                    bKeepLayer=False
                    if "event" in oTask.pluginPayload:
                        if oTask.pluginPayload["event"]:
                            logging.info("ImergMapEngine.handleTask: rain map related to an event, set Keep Layer = true")
                            bKeepLayer = True

                    self.addAndPublishLayer(sFile, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                            bKeepLayer=bKeepLayer, sDataSource=oMapConfig.dataSource,
                                            sResolution=oMapConfig.resolution, sInputData=sInputData, sOverrideMapId=sMapConfig)

        except Exception as oEx:
            logging.error("ImergMapEngine.handleTask: exception " + str(oEx))