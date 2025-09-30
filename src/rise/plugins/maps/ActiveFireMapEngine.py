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
        oMapConfig = self.getMapConfig("imerg_cumulate_12")

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, "imerg_cumulate",
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

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASE_NAME"] = self.getBaseName()
            aoParameters["REFERENCE_DATETIME"] = sDay + " " + sHour + ":00"

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)

            oWasdiTask = self.createNewTask(sProcessorId, sWorkspaceId, aoParameters, oMapConfig.processor, sDay)
            # Override: one for all in the tasks!
            oWasdiTask.mapId = "active_fire_map"
            oWasdiTask.pluginPayload["time"] = sHour
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("ActiveFireMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sDay + " " + sHour)
        else:
            logging.warning("ActiveFireMapEngine.updateNewMaps: simulation mode on - we do not run nothing")

