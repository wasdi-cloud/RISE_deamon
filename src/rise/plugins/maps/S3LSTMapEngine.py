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
                
        for oTask in aoExistingTasks:
            if self.isRunningStatus(oTask.status):
                logging.info("S3LSTMapEngine.updateNewMaps: a task is still ongoing or executed for day " + sYesterday + ". Nothing to do")
                return
        
        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        sType = aoParameters["TYPE_OF_DAILY_MAPS"]

        asTypes= []

        try:
            asTypes = sType.split(",")
        except Exception as oE:
            logging.error("Error splitting TYPE_OF_DAILY_MAPS parameter: " + str(oE))
            asTypes = ["MAX"]        
        
        asReadyFiles = wasdi.getProductsByActiveWorkspace()

        bMapsReady = True

        for sCurrentType in asTypes:
            sName = self.getBaseName() + "_S3_LST_Daily" + sCurrentType + "_" + sYesterday + ".tif"

            if sName not in asReadyFiles:
                logging.debug("S3LSTMapEngine.updateNewMaps: Not found  " + sName)
                bMapsReady = False
                break

        if  bMapsReady:
            logging.info("S3LSTMapEngine.updateNewMaps: All maps are already present for " + sYesterday + ". Nothing to do")
            return
        
        if not self.m_oConfig.daemon.simulate:

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["STARTDATE"] = sYesterday
            aoParameters["ENDDATE"] = sYesterday
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

            logging.info("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: task id " + oTask.id)

            aoPayload = wasdi.getProcessorPayloadAsJson(oTask.id)

            if aoPayload is None:
                logging.info("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: cannot read the payload, we stop here ")
                return

            if "output daily" not in aoPayload:
                logging.info("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: output not in the payload, we stop here ")
                return

            asOutputs = aoPayload["output daily"]

            if asOutputs is None or len(asOutputs) == 0:
                logging.info("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: output is empty, we stop here ")
                return

            oMapConfig = self.getMapConfig()

            sInputData = oMapConfig.inputData

            try:
                if "s3_input_files" in aoPayload:
                    for sInputFile in aoPayload["s3_input_files"]:
                        if isinstance(sInputFile, str):
                            sInputData += sInputFile + " "
                        else:
                            # Convert to string if it's not a string
                            sInputData += str(sInputFile) + " "
            except Exception as oEx:
                logging.error("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: error parsing input S3 files " + str(oEx))
                

            asFiles = wasdi.getProductsByActiveWorkspace()

            for sOutput in asOutputs:        
                if sOutput in asFiles:
                    logging.info("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: publishing " + sOutput)

                    oReferenceDate = datetime.strptime(oTask.referenceDate, "%Y-%m-%d")

                    sMapId = "s3_lst_max"

                    if "MIN" in sOutput:
                        sMapId = "s3_lst_min"
                    elif "AVG" in sOutput:
                        sMapId = "s3_lst_avg"

                    self.addAndPublishLayer(sOutput, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                            bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                            sResolution=oMapConfig.resolution, sInputData=sInputData, sOverrideMapId=sMapId)

        except Exception as oEx:
            logging.error("S3LSTMapEngine.handleTask [" + self.m_oArea.name +"]: exception " + str(oEx))