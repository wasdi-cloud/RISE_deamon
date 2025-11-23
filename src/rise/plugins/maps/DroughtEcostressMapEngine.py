import datetime
import logging
import calendar

import wasdi

from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class DroughtEcostressMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        logging.info("DroughtEcostressMapEngine.triggerNewAreaMaps  [" + self.m_oArea.name +"]: we just call Update New Maps")
        self.updateNewMaps()

    def triggerNewAreaArchives(self):
        pass

    def getStartEndDateFromDate(self, oDate):
        '''
        Given a date, return the start and end date of the decade before.
        For example, if the date is 2024-06-15, return (2024-06-01, 2024-06-10)
        if the date is 2024-06-25, return (2024-06-11, 2024-06-20)
        if the date is 2024-06-05, return (2024-05-21, 2024-05-31)
        '''

        iStartDay = 1
        iEndDay = 10
        iStartMonth = oDate.month
        iEndMonth = oDate.month
        iStartYear = oDate.year
        iEndYear = oDate.year

        iDay = oDate.day

        if iDay < 13:
            iStartDay = 21

            if oDate.month == 1:
                iStartMonth = 12
                iStartYear = oDate.year - 1
                iEndMonth = 12
                iEndYear = oDate.year - 1
            else:
                iStartMonth = oDate.month - 1
                iEndMonth = oDate.month - 1
            
            iEndDay = calendar.monthrange(iEndYear, iEndMonth)[1]
            
        elif iDay < 23:
            iStartDay = 1
            iEndDay = 9
        else:
            iStartDay = 11
            iEndDay = 20
        
        oStartDate = datetime.datetime(iStartYear, iStartMonth, iStartDay, 0, 0, 0)
        oEndDate = datetime.datetime(iEndYear, iEndMonth, iEndDay, 0, 0, 0)

        return oStartDate, oEndDate
    
    def getDecadeFromDate(self, oDate):
        iDay = oDate.day

        if iDay < 11:
            return 1
        elif iDay < 21:
            return 2
        else:
            return 3

    def updateNewMaps(self):
        logging.info("DroughtEcostressMapEngine.updateNewMaps [" + self.m_oArea.name + "]: Update New Maps")

        oNow = datetime.datetime.now(datetime.UTC)
        self.runForDate(oNow)

    def runForDate(self, oRunDate):
        logging.info("DroughtEcostressMapEngine.runForDate [" + self.m_oArea.name + "]: run for date " + oRunDate.strftime("%Y-%m-%d") )

        # We search the decade before
        (oStartDate, oEndDate) = self.getStartEndDateFromDate(oRunDate)
        iDecade = self.getDecadeFromDate(oStartDate)

        sDay = oRunDate.strftime("%Y-%m-%d")

        sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
        oMapConfig = self.getMapConfig()

        sReferenceDate = oEndDate.strftime("%Y-%m-%d")

        # Did we already start any map today?
        oWasdiTaskRepository = WasdiTaskRepository()

        # Take all our task for today
        aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMapConfig.id,
                                                            self.m_oPluginEntity.id, sWorkspaceId,
                                                            oMapConfig.processor, sReferenceDate)

        bIsRunning = False

        if len(aoExistingTasks) > 0:
            for oTask in aoExistingTasks:
                if self.isRunningStatus(oTask.status):
                    bIsRunning = True
                    break

        if bIsRunning:            
            logging.info("DroughtEcostressMapEngine.runForDate [" + self.m_oArea.name + "]: a task is still ongoing or executed for day " + sDay + ". Nothing to do")
            return        
        
        sOutput = self.getBaseName() + "_" + oEndDate.strftime("%Y-%m") + "_" + str(iDecade) + ".tif"
        aoFiles = wasdi.getProductsByActiveWorkspace()

        if sOutput in aoFiles:
            logging.info("DroughtEcostressMapEngine.runForDate [" + self.m_oArea.name + "]: file " + sOutput + " already exists. Nothing to do")
            return

        aoParameters = oMapConfig.params
        aoParameters = vars(aoParameters)

        if not self.m_oConfig.daemon.simulate:

            aoParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)
            aoParameters["BASENAME"] = self.getBaseName()
            aoParameters["DATEFROM"] = oStartDate.strftime("%Y-%m-%d")
            aoParameters["DATETO"] = oEndDate.strftime("%Y-%m-%d")

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoParameters)

            if not self.checkProcessorId(sProcessorId):
                return

            oWasdiTask = self.createNewTask(sProcessorId,sWorkspaceId,aoParameters,oMapConfig.processor,sReferenceDate)
            oWasdiTaskRepository.addEntity(oWasdiTask)

            logging.info("DroughtEcostressMapEngine.runForDate [" + self.m_oArea.name + "]: Started " + oMapConfig.processor + " for " + sDay)
        else:
            logging.warning("DroughtEcostressMapEngine.runForDate [" + self.m_oArea.name + "]: simulation mode on - we do not run nothing")


    def handleTask(self, oTask):
        try:
            logging.info("DroughtEcostressMapEngine.handleTask: handle task " + oTask.id)

            # First of all we check if it is safe and done
            if not super().handleTask(oTask):
                return False

            oMapConfig = self.getMapConfig()

            asFiles = wasdi.getProductsByActiveWorkspace()
            oReferenceDate = datetime.datetime.strptime(oTask.referenceDate, "%Y-%m-%d")
            iDecade = self.getDecadeFromDate(oReferenceDate)

            sOutput = self.getBaseName() + "_" + oReferenceDate.strftime("%Y_%m") + "_"  + str(iDecade) + ".tif"

            if sOutput in asFiles:
                logging.info("DroughtEcostressMapEngine.handleTask [" + self.m_oArea.name + "]: publishing " + sOutput)

                self.addAndPublishLayer(sOutput, oReferenceDate, bPublish=True, sMapIdForStyle=oMapConfig.id,
                                        bKeepLayer=False, sDataSource=oMapConfig.dataSource,
                                        sResolution=oMapConfig.resolution, sInputData=oMapConfig.inputData)            
            else:
                logging.info("DroughtEcostressMapEngine.handleTask [" + self.m_oArea.name + "]: file " + sOutput + " not found in workspace")
        
        except Exception as oEx:
            logging.error("DroughtEcostressMapEngine.handleTask [" + self.m_oArea.name + "]: exception " + str(oEx))