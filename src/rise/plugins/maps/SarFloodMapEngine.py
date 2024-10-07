import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import wasdi

from src.rise.business.Layer import Layer
from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.LayerRepository import LayerRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.geoserver.GeoserverService import GeoserverService
from src.rise.plugins.maps.RiseMapEngine import RiseMapEngine


class SarFloodMapEngine(RiseMapEngine):

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        super().__init__(oConfig, oArea, oPlugin, oPluginEngine, oMap)

    def triggerNewAreaMaps(self):
        self.runHasardLastWeek(self.m_oMapEntity)

    def runHasardLastWeek(self, oMap):
        try:
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(oMap)

            aoSarArchiveParameters = None

            oMapConfig = None

            for oMapConfig in self.m_oPluginConfig.maps:
                if oMapConfig.id == oMap.id:
                    aoSarArchiveParameters = oMapConfig.params
                    break

            if aoSarArchiveParameters is None:
                logging.warning("SarFloodMapEngine.runHasardLastWeek: impossible to find parameters for map " + oMap.id)
                return

            oWasdiTaskRepository = WasdiTaskRepository()
            aoExistingTasks = oWasdiTaskRepository.findByParams(self.m_oArea.id, oMap.id, self.m_oPluginEntity.id,
                                                                sWorkspaceId)

            if aoExistingTasks is not None:
                if len(aoExistingTasks) > 0:
                    for oTask in aoExistingTasks:
                        if "shortArchive" in oTask.pluginPayload:
                            if oTask.pluginPayload["shortArchive"]:
                                logging.info("SarFloodMapEngine.runHasardLastWeek: task already on-going")
                                return True

            aoSarArchiveParameters = vars(aoSarArchiveParameters)
            aoSarArchiveParameters["BBOX"] = self.m_oPluginEngine.getWasdiBbxFromWKT(self.m_oArea.bbox, True)

            iEnd = datetime.today()
            iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

            aoSarArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
            aoSarArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
            aoSarArchiveParameters["MOSAICBASENAME"] = self.m_oArea.id.replace("-", "") + oMap.id.replace("_", "")

            sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoSarArchiveParameters)
            oWasdiTask = WasdiTask()
            oWasdiTask.areaId = self.m_oArea.id
            oWasdiTask.mapId = oMap.id
            oWasdiTask.id = sProcessorId
            oWasdiTask.pluginId = self.m_oPluginEntity.id
            oWasdiTask.workspaceId = sWorkspaceId
            oWasdiTask.startDate = datetime.now().timestamp()
            oWasdiTask.inputParams = aoSarArchiveParameters
            oWasdiTask.status = "CREATED"
            oWasdiTask.pluginPayload["shortArchive"] = True

            oWasdiTaskRepository.addEntity(oWasdiTask)
            logging.info(
                "SarFloodMapEngine.runHasardLastWeek: Started " + oMapConfig.processor + " in Workspace " + self.m_oPluginEngine.getWorkspaceName(
                    oMap) + " for Area " + self.m_oArea.name)

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.runHasardLastWeek: exception " + str(oEx))

    def handleTask(self, oTask):
        try:
            if not super().handleTask(oTask):
                return False

            logging.info("SarFloodMapEngine.handleTask: task done, lets proceed!")

            asWorkspaceFiles = wasdi.getProductsByActiveWorkspace()

            if len(asWorkspaceFiles) == 0:
                logging.warning("SarFloodMapEngine.handleTask: we do not have files in the workspace... ")
                return False

            if "shortArchive" in oTask.pluginPayload:
                if oTask.pluginPayload["shortArchive"]:
                    return self.handleShortArchiveTask(oTask, asWorkspaceFiles)

            return True
        except Exception as oEx:
            logging.error("SarFloodMapEngine.handleTask: exception " + str(oEx))
            return False

    def handleShortArchiveTask(self, oTask, asWorkspaceFiles):

        fFirstMapTimestamp = -1.0
        fLastMapTimestamp = -1.0

        try:
            logging.info("SarFloodMapEngine.handleTask: task done, lets proceed!")

            sBaseName = oTask.inputParams["MOSAICBASENAME"]
            sStartDate = oTask.inputParams["ARCHIVE_START_DATE"]
            sEndDate = oTask.inputParams["ARCHIVE_END_DATE"]

            try:
                oStartDay = datetime.strptime(sStartDate, '%Y-%m-%d')
            except:
                logging.error('SarFloodMapEngine.handleShortArchiveTask: Start Date not valid')
                return False

            try:
                oEndDay = datetime.strptime(sEndDate, '%Y-%m-%d')
            except:
                logging.error('SarFloodMapEngine.handleShortArchiveTask: End Date not valid')
                return False

            oTimeDelta = timedelta(days=1)

            oActualDate = oStartDay

            while oActualDate <= oEndDay:
                sDate = oActualDate.strftime("%Y-%m-%d")
                sFileName = sBaseName + "_" +sDate + "_flood.tif"

                if sFileName not in asWorkspaceFiles:
                    logging.info("SarFloodMapEngine.handleShortArchiveTask: " + sFileName + " not present, continue")
                    oActualDate = oActualDate + oTimeDelta
                    continue

                logging.info("SarFloodMapEngine.handleShortArchiveTask: Found " + sFileName + ", publish it")
                sLocalFilePath = wasdi.getPath(sFileName)
                #sNameOnly = os.path.basename(sLocalFilePath)
                #sGeoserverFolder = self.m_oConfig.geoserver.geoserverDataFolder
                #sDestinationFile = os.path.join(sGeoserverFolder, sNameOnly)
                #os.rename(sLocalFilePath, sDestinationFile)
                oGeoserverService = GeoserverService()
                sLayerName = Path(str(sLocalFilePath)).stem
                oStore = oGeoserverService.publishRasterLayer(sLocalFilePath, "rise", sLayerName)
                os.remove(sLocalFilePath)

                if oStore is None:
                    logging.error("SarFloodMapEngine.handleShortArchiveTask: impossible to get the coverage store for " + sFileName)
                else:
                    oLayerRepository = LayerRepository()
                    oLayer = Layer()
                    oLayer.mapId = self.m_oMapEntity.id
                    oLayer.areaId = self.m_oArea.id
                    oLayer.pluginId = self.m_oPluginEntity.id
                    oLayer.link = "rise:" + sLayerName
                    oLayer.referenceDate = oActualDate.timestamp()
                    #oLayer.properties
                    oLayer.source = self.m_oPluginEntity.name
                    oLayer.id = sLayerName
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
            logging.error("SarFloodMapEngine.handleShortArchiveTask: exception " + str(oEx))
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
                oAreaRepository = AreaRepository
                oAreaRepository.updateEntity(self.m_oArea)
