import datetime
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class DroughtEcostressMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        logging.info("DroughtEcostressMapEngine.triggerNewAreaMaps: we just call Update New Maps")
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        pass

    def updateNewMaps(self):
        logging.info("DroughtEcostressMapEngine.updateNewMaps: Update New Maps")

        oNow = datetime.datetime.now(datetime.UTC)

        sDay = oNow.strftime("%Y-%m-%d")
        # Replace the day with 1 to get the first day of the month
        sFirstDayOfMonth = oNow.replace(day=1).strftime("%Y-%m-%d")        

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig()

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMapConfig.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sDay)

        if len(aoExistingTasks) > 0:
            logging.info("DroughtEcostressMapEngine.updateNewMaps: a task is still ongoing or executed for day " + sDay + ". Nothing to do")
            return        

        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            aoParameters["DATEFROM"] = sFirstDayOfMonth
            aoParameters["DATETO"] = sDay

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)

            oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParameters,oMapConfig.processor,sDay)
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("DroughtEcostressMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sDay)
        else:
            logging.warning("DroughtEcostressMapEngine.updateNewMaps: simulation mode on - we do not run nothing")


    def handleTask(self, oTask):
        try:
            logging.info("DroughtEcostressMapEngine.handleTask: handle task " + oTask.id)

            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            oMapConfig = self.getMapConfig()

            asFiles = wasdi.getProductsByActiveWorkspace()

            sOutput1 = self.getBaseName() + datetime.strptime(oTask.referenceDate, "%Y_%m") + "_1.tif"
            sOutput2 = self.getBaseName() + datetime.strptime(oTask.referenceDate, "%Y_%m") + "_2.tif"
            sOutput3 = self.getBaseName() + datetime.strptime(oTask.referenceDate, "%Y_%m") + "_3.tif"

            asOutputFiles = [sOutput1, sOutput2, sOutput3]

            for sOutput in asOutputFiles:
                logging.info("DroughtEcostressMapEngine.handleTask: publishing " + sOutput)

                oReferenceDate = datetime.strptime(oTask.referenceDate, "%Y-%m-%d")

                self.addAndPublishLayer(sOutput, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                        bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                        sResolution=oMapConfig.resolution, sInputData=oMapConfig.inputData)            
        
        except Exception as oEx:
            logging.error("DroughtEcostressMapEngine.handleTask: exception " + str(oEx))
