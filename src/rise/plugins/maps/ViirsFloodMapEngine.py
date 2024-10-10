import logging
from datetime import datetime, timedelta
from pathlib import Path

import wasdi

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.LayerRepository import LayerRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class ViirsFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.runViirsArchive(self.m_oMapEntity, True)

        if self.m_oArea.supportArchive:
            self.runViirsArchive(self.m_oMapEntity, False)

    def runViirsArchive(self, oMap, bOnlyLastWeek):
        try:
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(oMap)

            aoViirsArchiveParameters = None

            oMapConfig = None

            for oMapConfig in self.m_oPluginConfig.maps:
                if oMapConfig.id == oMap.id:
                    aoViirsArchiveParameters = oMapConfig.params
                    break

            if aoViirsArchiveParameters is None:
                logging.warning("ViirsFloodMapEngine.runViirsArchive: impossible to find parameters for map " + oMap.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMap.id, self.m_oPluginEntity.id,                                                                sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"] == bOnlyLastWeek:
                                logging.info("ViirsFloodMapEngine.runViirsArchive: task already on-going")
                                return True

            aoViirsArchiveParameters = vars(aoViirsArchiveParameters)
            aoViirsArchiveParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()

            if bOnlyLastWeek:
                iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)
                aoViirsArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            else:
                aoViirsArchiveParameters["ARCHIVE_START_DATE"] = oMapConfig.startArchiveDate
                iEnd = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoViirsArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoViirsArchiveParameters["MOSAICBASENAME"] = self.m_oArea.id.replace("-", "") + oMap.id.replace("_", "")

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoViirsArchiveParameters)
            oWasdiTask = WasdiTask()
            oWasdiTask.areaId = self.m_oArea.id
            oWasdiTask.mapId = oMap.id
            oWasdiTask.id = sProcessorId
            oWasdiTask.pluginId = self.m_oPluginEntity.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoViirsArchiveParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = bOnlyLastWeek

            oWasdiTaskRepository.addEntity(oWasdiTask)
            logging.info(
                "ViirsFloodMapEngine.runViirsArchive: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(oMap) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.runViirsArchive: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            if not super().handleTask(oTask):
                return False

            logging.info("ViirsFloodMapEngine.handleTask: handle task " + oTask.id)

            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            if len(asWorkspaceFiles) == 0:
                logging.warning("ViirsFloodMapEngine.handleTask: we do not have files in the workspace... ")
                return False

            if "shortArchive" in oTask.pluginPayload:
                if oTask.pluginPayload["shortArchive"]:
                    return self.handleArchiveTask(oTask, asWorkspaceFiles, oTask.pluginPayload["shortArchive"])

            return True
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.handleTask: exception " + str(oEx))
            return False

    def handleArchiveTask(self, oTask, asWorkspaceFiles, bOnlyLastWeek):

        fFirstMapTimestamp = -1.0
        fLastMapTimestamp = -1.0

        try:
            logging.info("ViirsFloodMapEngine.handleTask: task done, lets proceed!")

            sBaseName = oTask.inputParams["VIIRS_BASENAME"]
            sStartDate = oTask.inputParams["ARCHIVE_START_DATE"]
            sEndDate = oTask.inputParams["ARCHIVE_END_DATE"]

            try:
                oStartDay = datetime.strptime(sStartDate, '%Y-%m-%d')
            except:
                logging.error('ViirsFloodMapEngine.handleShortArchiveTask: Start Date not valid')
                return False

            try:
                oEndDay = datetime.strptime(sEndDate, '%Y-%m-%d')
            except:
                logging.error('ViirsFloodMapEngine.handleShortArchiveTask: End Date not valid')
                return False

            oTimeDelta = timedelta(days=1)

            oActualDate = oStartDay

            while oActualDate <= oEndDay:
                sDate = oActualDate.strftime("%Y-%m-%d")
                sFileName = sBaseName + "_" +sDate + "_bbox.tif"

                if sFileName not in asWorkspaceFiles:
                    logging.info("ViirsFloodMapEngine.handleShortArchiveTask: " + sFileName + " not present, continue")
                    oActualDate = oActualDate + oTimeDelta
                    continue

                if bOnlyLastWeek:
                    logging.info("ViirsFloodMapEngine.handleShortArchiveTask: Found " + sFileName + ", publish it")

                    sLayerName = Path(sFileName).stem

                    if not self.publishRasterLayer(sFileName):
                        logging.error(
                            "ViirsFloodMapEngine.handleShortArchiveTask: impossible to get the coverage store for " + sFileName)
                    else:
                        oLayerRepository = LayerRepository()
                        oLayer = self.getLayerEntity(sLayerName, oActualDate.timestamp())
                        oLayerRepository.addEntity(oLayer)

                        if fFirstMapTimestamp == -1.0:
                            fFirstMapTimestamp = oLayer.referenceDate
                        elif oLayer.referenceDate < fFirstMapTimestamp:
                            fFirstMapTimestamp = oLayer.referenceDate

                        if fLastMapTimestamp == -1.0:
                            fLastMapTimestamp = oLayer.referenceDate
                        elif oLayer.referenceDate > fLastMapTimestamp:
                            fLastMapTimestamp = oLayer.referenceDate

                oActualDate = oActualDate + oTimeDelta
            return True
        except Exception as oEx:
            logging.error("ViirsFloodMapEngine.handleShortArchiveTask: exception " + str(oEx))
            return False
        finally:
            # In any case, this task is done
            oTask.status = "DONE"
            oTaskRepository = WasdiTaskRepository()
            oTaskRepository.updateEntity(oTask)

            bChanged = False

            # And if we do not have yet archive start and end date, set it
            if self.m_oArea.archiveStartDate <=0 and fFirstMapTimestamp>0:
                self.m_oArea.archiveStartDate = fFirstMapTimestamp
                bChanged = True

            if self.m_oArea.archiveEndDate <=0 and fLastMapTimestamp>0:
                self.m_oArea.archiveEndDate = fLastMapTimestamp
                bChanged = True

            if bChanged:
                # Update the area if needed
                oAreaRepository = AreaRepository()
                oAreaRepository.updateEntity(self.m_oArea)
