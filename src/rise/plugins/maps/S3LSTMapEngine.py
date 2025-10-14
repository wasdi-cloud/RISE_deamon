from datetime import datetime, timedelta
import logging

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine

class S3LSTMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        logging.info("S3LSTMapEngine.triggerNewAreaArchives: LST long Archive Not supported")

    def updateNewMaps(self):
        logging.info("S3LSTMapEngine.triggerNewAreaArchives: Update New Maps")

        oToday = datetime.today()
        oTimeDelta = timedelta(days=1)
        oYesterday = oToday - oTimeDelta
        sYesterday = oYesterday.strftime("%Y-%m-%d")


        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig()

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMapConfig.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sYesterday)

        if len(aoExistingTasks)>0:
            logging.info("S3LSTMapEngine.updateNewMaps: a task is still ongoing or executed for day " + sYesterday + ". Nothing to do")
            return
        
        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["STARTDATE"] = sYesterday
            aoParameters["ENDDATE"] = sYesterday
            aoParameters["BASE_NAME"] = self.getBaseName()
            aoParameters["BASENAME"] = self.getBaseName()

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)

            if not self.checkProcessorId(sProcessorId):
                return

            oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParameters,oMapConfig.processor,sYesterday)
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("S3LSTMapEngine.updateNewMaps: Started " + oMapConfig.processor + " for " + sYesterday)
        else:
            logging.warning("S3LSTMapEngine.updateNewMaps: simulation mode on - we do not run nothing")

    def handleTask(self, oTask):
        try:
            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            logging.info("S3LSTMapEngine.handleTask: handle task " + oTask.id)

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is None:
                logging.info("S3LSTMapEngine.handleTask: cannot read the payload, we stop here ")
                return

            if "output" not in aoPayload:
                logging.info("S3LSTMapEngine.handleTask: output not in the payload, we stop here ")
                return

            sOutput = aoPayload["output"]

            oMapConfig = self.getMapConfig()

            sInputData = oMapConfig.inputData

            if "s3_input_files" in aoPayload:
                for sInputFile in aoPayload["s3_input_files"]:
                    sInputData += sInputFile + " "

            asFiles = wasdi.getProductsByActiveWorkspace()
        
            if sOutput in asFiles:
                logging.info("S3LSTMapEngine.handleTask: publishing " + sOutput)

                oReferenceDate = datetime.strptime(oTask.referenceDate, "%Y-%m-%d")

                self.addAndPublishLayer(sOutput, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                        bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                        sResolution=oMapConfig.resolution, sInputData=sInputData)

        except Exception as oEx:
            logging.error("S3LSTMapEngine.handleTask: exception " + str(oEx))