import glob
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import zipfile

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.business.Layer import Layer
from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.LayerRepository import LayerRepository
from src.rise.data.MapsParametersRepository import MapsParametersRepository
from src.rise.data.UserRepository import UserRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.geoserver.GeoserverService import GeoserverService
from src.rise.utils import RiseUtils


class RiseMapEngine:

    def __init__(self, oConfig, oArea, oPlugin, oPluginEngine, oMap):
        self.m_oConfig = oConfig
        self.m_oArea = oArea
        self.m_oPluginEntity = oPlugin
        self.m_oPluginEngine = oPluginEngine
        self.m_oPluginConfig = None
        self.m_oMapEntity = oMap

        try:
            oParentPath = Path(oConfig.myFilePath).parent
            oPluginConfigPath = oParentPath.joinpath(oPlugin.id + ".json")
            if os.path.isfile(oPluginConfigPath):
                self.m_oPluginConfig = RiseDeamon.readConfigFile(oPluginConfigPath)

        except Exception as oEx:
            logging.error("RiseMapEngine.init: exception " + str(oEx))

    def getMapConfig(self, sMapId=None):

        # get the map id
        if sMapId is None:
            sMapId = self.m_oMapEntity.id

        oMapConfig = None
        for oConfig in self.m_oPluginConfig.maps:
            if oConfig.id == sMapId:
                oMapConfig = oConfig
                break

        # get the area id
        sAreaId = self.m_oArea.id

        # look if, in the db, we have some custom parameters for the pair <area_id, map_id>
        oMapsParametersRepo = MapsParametersRepository()
        oFilter = {
            'areaId': sAreaId,
            'mapId': sMapId
        }
        aoParameters = oMapsParametersRepo.getEntitiesByField(oFilter)

        oParameter = None
        if aoParameters is not None:
            if len(aoParameters) == 1:
                oParameter = aoParameters[0]
            elif len(aoParameters) > 1:
                # we sort by decreasing modification timestamp, and we take the most recent modified parameters
                aoParameters.sort(key=lambda oParams: oParams.lastModifyTimestamp, reverse=True)
                oParameter = aoParameters[0]

        if oParameter is not None:
            try:
                oMapConfig.params = json.loads(oParameter.payload)
                return oMapConfig
            except Exception as oEx:
                logging.warning(f"RiseMapEngine.getMapConfig: exception {oEx}")

        return oMapConfig

    def getStyleForMap(self, sMapId=None):

        if sMapId is None:
            oMapConfig = self.getMapConfig()
        else:
            oMapConfig = self.getMapConfig(sMapId)

        if oMapConfig is None:
            return None

        sStyle = None
        try:
            sStyle = oMapConfig.style
        except:
            sStyle = None

        return sStyle

    def triggerNewAreaMaps(self):
        logging.info("RiseMapEngine.triggerNewAreaMaps")

    def triggerNewAreaArchives(self):
        logging.info("RiseMapEngine.triggerNewAreaArchives")

    def getName(self):
        if self.m_oMapEntity is not None:
            return self.m_oMapEntity.name
        return ""
    
    def getId(self):
        if self.m_oMapEntity is not None:
            return self.m_oMapEntity.id
        return ""
    
    def getEngineClassName(self):
        if self.m_oMapEntity is not None:
            return self.m_oMapEntity.className
        return ""

    def handleTask(self, oTask):
        '''
        Handle a Task created by this map engine
        :param oTask:
        :return:
        '''
        try:
            # We open the wasdi workspace
            logging.debug("RiseMapEngine.handleTask: handle task " + oTask.id)
            oTaskRepo = WasdiTaskRepository()
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

            # Get the status from WASDI
            sNewStatus = wasdi.getProcessStatus(oTask.id)

            if sNewStatus == "ERROR" or sNewStatus == "STOPPED":
                logging.warning("RiseMapEngine.handleTask: the new status is not done but " + sNewStatus + " update status and exit. Task id: " + oTask.id)
                oTask.status = sNewStatus
                oTaskRepo.updateEntity(oTask)
                return False

            if sNewStatus == "DONE":
                logging.debug("RiseMapEngine.handleTask: task done, lets proceed!")
                # In any case, this task is done
                oTask.status = sNewStatus
                oTaskRepo.updateEntity(oTask)
                return True
            else:
                logging.debug("RiseMapEngine.handleTask: task is still ongoing, for now we do nothing (state = " + sNewStatus + ")")
                return False

        except Exception as oEx:
            logging.error("RiseMapEngine.handleTask: exception " + str(oEx))
            return False

    def getLayerEntity(self, sLayerName, fTimestamp, sDataSource="", oCreationDate=None, sResolution="", sInputData="", aoProperties=None):

        if aoProperties is None:
            aoProperties = {}
        oLayer = Layer()
        oLayer.mapId = self.m_oMapEntity.id
        oLayer.areaId = self.m_oArea.id
        oLayer.pluginId = self.m_oPluginEntity.id
        oLayer.layerId = "rise:" + sLayerName
        oLayer.geoserverUrl = self.m_oConfig.geoserver.address
        if not oLayer.geoserverUrl.endswith('/'):
            oLayer.geoserverUrl = oLayer.geoserverUrl + '/'
        oLayer.geoserverUrl = oLayer.geoserverUrl + self.m_oConfig.geoserver.workspace + "/wms?"
        oLayer.referenceDate = fTimestamp
        oLayer.properties = aoProperties
        oLayer.source = self.m_oPluginEntity.name
        oLayer.id = sLayerName
        oLayer.dataSource = sDataSource
        if oCreationDate is not None:
            if isinstance(oCreationDate, int):
                oLayer.createdDate = oCreationDate
            elif isinstance(oCreationDate, datetime):
                oLayer.createdDate = oCreationDate.timestamp()
        else:
            oLayer.createdDate = datetime.now().timestamp()

        oLayer.resolution = sResolution
        oLayer.inputData = sInputData
        oLayer.workspaceId = wasdi.getActiveWorkspaceId();

        return oLayer
    
    def deleteLayer(self, sFileName):
        try:
            oLayerRepository = LayerRepository()

            sLayerName = Path(sFileName).stem
        
            oTestLayer = oLayerRepository.getEntityById(sLayerName)

            if oTestLayer is not None:
                # We need to clean it: delete our layer db entry
                oLayerRepository.deleteEntity(sLayerName)
                # Get the Geoserver Service
                oGeoserverService = GeoserverService()
                # If the layer exists
                if oGeoserverService.existsLayer(sLayerName):
                    # Delete it
                    oGeoserverService.deleteLayer(oTestLayer.layerId)

                # Set the layer as none to re-publish 
        except Exception as oEx:
            logging.error("RiseMapEngine.deleteLayer exception " + str(oEx))
            return False

    def addAndPublishLayer(self, sFileName, oReferenceDate, bPublish=True, sMapIdForStyle=None, bKeepLayer=False, sDataSource="", oCreationDate=None, sResolution="", sInputData="", asProperties=None, sOverrideMapId=None, sOverridePluginId=None, bForceRepublish=False, sForceStyle=None, bForceDeleteLocalFile=True):
        try:
            oLayerRepository = LayerRepository()
            sLayerName = Path(sFileName).stem
            oLayer = self.getLayerEntity(sLayerName, oReferenceDate.timestamp(), sDataSource, oCreationDate, sResolution, sInputData, asProperties)

            if sOverrideMapId:
                oLayer.mapId = sOverrideMapId

            if sOverridePluginId:
                oLayer.pluginId = sOverridePluginId

            oLayer.keepLayer = bKeepLayer
            oTestLayer = oLayerRepository.getEntityById(oLayer.id)

            if bForceRepublish and oTestLayer is not None:
                # We need to clean it: delete our layer db entry
                oLayerRepository.deleteEntity(oLayer.id)
                # Get the Geoserver Service
                oGeoserverService = GeoserverService()
                # If the layer exists
                if oGeoserverService.existsLayer(sLayerName):
                    # Delete it
                    oGeoserverService.deleteLayer(oLayer.layerId)

                # Set the layer as none to re-publish it
                oTestLayer = None

            if bForceRepublish and bForceDeleteLocalFile:
                # If we have already a local WASDI copy, delete it to be sure to take the last one from the workspace
                sLocalFilePath = wasdi.getSavePath() + sFileName
                
                try:
                    if os.path.exists(sLocalFilePath):
                        os.remove(sLocalFilePath)
                except Exception as oEx:
                    logging.warning("Error removing local file " + sLocalFilePath)


            if oTestLayer is None:
                logging.info("RiseMapEngine.addAndPublishLayer: publish Map: " + sLayerName)

                if sForceStyle is not None:
                    sStyle = sForceStyle
                else:
                    if sMapIdForStyle is not None:
                        sStyle = self.getStyleForMap(sMapIdForStyle)
                    else:
                        sStyle = self.getStyleForMap()

                if bPublish or bForceRepublish:

                    if self.isRasterFile(sFileName):
                        if not self.publishRasterLayer(sFileName, sStyle):
                            logging.error("RiseMapEngine.addAndPublishLayer: impossible to publish raster " + sLayerName)
                            return None
                        else:
                            oLayer.published = True
                    elif self.isShapeFile(sFileName):
                        if not self.publishShapeLayer(sFileName, sStyle):
                            logging.error("RiseMapEngine.addAndPublishLayer: impossible to publish shape " + sLayerName)
                            return None
                        else:
                            oLayer.published = True
                    else:
                        logging.error("The file type of " + sLayerName + " is not recognized, we cannot publish!")
                        return None

                oLayerRepository.addEntity(oLayer)
            return oLayer
        except Exception as oEx:
            logging.error("RiseMapEngine.addAndPublishLayer exception " + str(oEx))
            return None

    def publishRasterLayer(self, sFileName, sStyleName=None):
        try:
            sLocalFilePath = wasdi.getPath(sFileName)
            oGeoserverService = GeoserverService()
            sLayerName = Path(str(sLocalFilePath)).stem

            oWorkspace = oGeoserverService.getWorkspace(self.m_oConfig.geoserver.workspace)

            if oWorkspace is None:
                oGeoserverService.createWorkspace(self.m_oConfig.geoserver.workspace)

            oStore = oGeoserverService.publishRasterLayer(sLocalFilePath, self.m_oConfig.geoserver.workspace, sLayerName, sStyleName)
            os.remove(sLocalFilePath)

            if oStore is not None:
                return True
            else:
                return False
        except Exception as oEx:
            logging.error("RiseMapEngine.publishRasterLayer exception " + str(oEx))

        return False

    def publishShapeLayer(self, sFileName, sStyleName=None):
        try:
            sLocalFilePath = wasdi.getPath(sFileName)
            oGeoserverService = GeoserverService()
            sLocalFilePath = sLocalFilePath.replace(".shp", ".zip")
            sLayerName = Path(str(sLocalFilePath)).stem

            oWorkspace = oGeoserverService.getWorkspace(self.m_oConfig.geoserver.workspace)

            if oWorkspace is None:
                oGeoserverService.createWorkspace(self.m_oConfig.geoserver.workspace)

            asFiles = glob.glob(wasdi.getSavePath() + sFileName.replace(".shp","*"))                

            if not os.path.exists(sLocalFilePath):
                with zipfile.ZipFile(sLocalFilePath, 'w') as oZipFile:
                    for sFile in asFiles:
                        oZipFile.write(sFile, arcname=os.path.basename(sFile))
                asFiles.append(sLocalFilePath)

            oStore = oGeoserverService.publishShapeLayer (sLocalFilePath, self.m_oConfig.geoserver.workspace, sLayerName, sStyleName)
            
            for sFile in asFiles:
                os.remove(sFile)

            if oStore is not None:
                return True
            else:
                return False
        except Exception as oEx:
            logging.error("RiseMapEngine.publishRasterLayer exception " + str(oEx))

        return False

    def isRasterFile(self, sFileName):
        if sFileName is None:
            return False

        try:
            bIsRaster = False
            if sFileName.lower().endswith(".tif") or sFileName.lower().endswith(".tiff"):
                bIsRaster = True
            return bIsRaster
        except Exception as oEx:
            logging.error("RiseMapEngine.isRasterFile:  " + str(oEx))

        return False

    def isShapeFile(self, sFileName):
        if sFileName is None:
            return False

        try:
            bIsShapeFile = False
            if sFileName.lower().endswith(".shp"):
                bIsShapeFile = True
            return bIsShapeFile
        except Exception as oEx:
            logging.error("RiseMapEngine.isShapeFile:  " + str(oEx))

        return False

    def updateNewMaps(self):
        pass
    
    def saveChainParams(self, sFile, aoChainParams):
        try:
            # Take a local copy
            sJsonFilePath = wasdi.getPath(sFile)

            # Now we write the new json
            with open(sJsonFilePath, "w") as oFile:
                json.dump(aoChainParams, oFile)

            # And we add it, updated, to WASDI
            wasdi.addFileToWASDI(sFile, bForceUpdate=True)
        except Exception as oEx:
            logging.error("RiseMapEngine.saveChainParams: exception " + str(oEx))

    def updateChainParamsDate(self, sFile, sEndDate, sDateKey = "lastMapDate"):
        try:
            # Previous version, if available
            aoChainParams = self.getWorkspaceUpdatedJsonFile(sFile)

            if aoChainParams is not None:
                if sDateKey in aoChainParams:
                    sOldLastMapDate = aoChainParams[sDateKey]
                    if sEndDate < sOldLastMapDate:
                        sEndDate = sOldLastMapDate
            else:
                aoChainParams = {}    

            aoChainParams[sDateKey] = sEndDate

            # Take a local copy
            sJsonFilePath = wasdi.getPath(sFile)

            # Now we write the new json
            with open(sJsonFilePath, "w") as oFile:
                json.dump(aoChainParams, oFile)

            # And we add it, updated, to WASDI
            wasdi.addFileToWASDI(sFile, bForceUpdate=True)
        except Exception as oEx:
            logging.error("RiseMapEngine.updateChainParamsDate: exception " + str(oEx))

    def getWorkspaceUpdatedJsonFile(self, sJsonFile):

        sJsonFilePath = wasdi.getSavePath() + sJsonFile

        if os.path.isfile(sJsonFilePath):
            # Clean it and re-take it updated from wasdi
            os.remove(sJsonFilePath)

        # Take a local copy
        sJsonFilePath = wasdi.getPath(sJsonFile)

        # Previous version, if available
        aoChainParams = None

        # If we have a local file
        if os.path.isfile(sJsonFilePath):
            with open(sJsonFilePath, "r") as oFile:
                try:
                    aoChainParams = json.load(oFile)
                except:
                    pass

        return aoChainParams        

    def isRunningStatus(self, sStatus):
        if sStatus is None:
            return False
        if sStatus in ["RUNNING", "CREATED", "WAITING", "READY"]:
            return True
        return False

    def isFinishedStatus(self, sStatus):
        if sStatus is None:
            return False
        if sStatus in ["ERROR", "STOPPED", "DONE"]:
            return True
        return False

    def isDoneStatus(self, sStatus):
        if sStatus is None:
            return False
        return sStatus == "DONE"

    def getBaseName(self, sMapId=None):
        if sMapId is None:
            sMapId = self.m_oMapEntity.id

        return self.m_oArea.id.replace("-", "") + sMapId.replace("_", "")

    def notifyEndOfTask(self, sAreaId, bIncludeFieldsOperators, sMapType=""):
        """
        Notify by email the ADMIN and HQ users associated with an AoI about the end of a task that ran on that area.
        Optionally, the same email can be sent to FIELD operators.
        :param sAreaId: the id of the area
        :param bIncludeFieldsOperators: sends the email to FIELDS operators if true
        :param sMapType: If set, it will be included in the text of the notification
        :return:
        """

        try:
            if RiseUtils.isNoneOrEmpty(sAreaId):
                logging.error("RiseMapEngine.notifyEndOfTask. No id of the area")

            # get the area associated with the task
            oAreaRepository = AreaRepository()
            oArea = oAreaRepository.getEntityById(sAreaId)

            if oArea is None:
                logging.error("RiseMapEngine.notifyEndOfTask. Area not found in the db")

            aoUsersToNotify = []

            # get the ADMIN and HR users working in the organization associated with the area
            oUserRepository = UserRepository()
            oQuery = {
                'organizationId': oArea.organizationId,
                'role': {'$in': ['ADMIN', 'HQ']}
            }
            aoAdminHQUsers = oUserRepository.getEntitiesByField(oQuery)

            if aoAdminHQUsers is not None and len(aoAdminHQUsers) > 0:
                aoUsersToNotify.extend(aoAdminHQUsers)

            # get the fields operators in the area
            if bIncludeFieldsOperators:
                if oArea.fieldOperators:
                    oQuery = {
                        'userId': {'$in': oArea.fieldOperators}
                    }

                    aoFieldOperators = oUserRepository.getEntitiesByField(oQuery)
                    if aoFieldOperators is not None and len(aoFieldOperators) > 0:
                        aoUsersToNotify.extend(aoFieldOperators)

            # send the email
            sMailTitle = "RISE: map ready for " + str(oArea.name)

            sMailMessage = f"A new map is "

            if sMapType != "":
                sMailMessage = "The " + sMapType + " Maps are "

            sMailMessage += f"now available in RISE for the following area: {oArea.name}.\n" \
                           f"Kind regards,\nThe RISE team"

            for oUser in aoUsersToNotify:
                RiseUtils.sendEmailMailJet(self.m_oConfig, self.m_oConfig.notifications.riseAdminMail,
                                           oUser.email, sMailTitle, sMailMessage, True)

        except Exception as oEx:
            logging.error(f"RiseMapEngine.notifyEndOfTask. Error {oEx}")

    def createNewTask(self, sTaskId=None, sWorkspaceId=None, aoParameters=None, sApplication=None, sReferenceDate=None):
        oWasdiTask = WasdiTask()
        oWasdiTask.areaId = self.m_oArea.id
        oWasdiTask.mapId = self.m_oMapEntity.id
        oWasdiTask.id = sTaskId
        oWasdiTask.pluginId = self.m_oPluginEntity.id
        oWasdiTask.workspaceId = sWorkspaceId
        oWasdiTask.startDate = datetime.now().timestamp()
        oWasdiTask.inputParams = aoParameters
        oWasdiTask.status = "CREATED"
        oWasdiTask.application = sApplication
        oWasdiTask.referenceDate = sReferenceDate
        oWasdiTask.pluginPayload = {}
        return oWasdiTask

    def mergeOrPublishImpactsShape(self, sImpactMap1, sImpactMap2, sInputData1, sInputData2, sMapId, sBaseName, oEventPeakDate, oImpactsPluginConfig, asWorkspaceFiles, bKeepLayer=False):
        """
        Merge the two impact maps shape files and publish it
        :param sImpactMap1: First impact map
        :param sImpactMap2: Second impact map
        :param sInputData1: Input data for the first impact map
        :param sInputData2: Input data for the second impact map
        :param sMapId: Map ID for the impact map
        :param sBaseName: Base name for the impact map
        :param oEventPeakDate: Event peak date
        :param oImpactsPluginConfig: Plugin configuration for the impacts plugin
        :param asWorkspaceFiles: List of files in the workspace
        :return:
        """

        oImpactsMapConfig = RiseDeamon.getMapConfigFromPluginConfig(oImpactsPluginConfig,sMapId)
        if oImpactsMapConfig is None:
            logging.warning("RiseMapEngine.mergeOrPublishImpactsShape: impossible to find configuration for map " + sMapId)
            return
        
        oReferenceDate = datetime.strptime(oEventPeakDate, "%Y-%m-%d")

        # Check if we have only the first map in the workspace
        if sImpactMap1 in asWorkspaceFiles and sImpactMap2 not in asWorkspaceFiles:
            self.addAndPublishLayer(sImpactMap1, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData1, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)
        # Check if we have only the second map in the workspace                
        elif sImpactMap1 not in asWorkspaceFiles and sImpactMap2 in asWorkspaceFiles:
            self.addAndPublishLayer(sImpactMap2, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData2, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)
        # Check if we have both maps in the workspace
        elif sImpactMap1 in asWorkspaceFiles and sImpactMap2 in asWorkspaceFiles:
            # Create the merged impact map name
            sMergedImpactMap = sBaseName + "_event_" + oReferenceDate.strftime("%Y-%m-%d") + "_merged_impacts_" + sMapId + ".shp"

            if sMergedImpactMap not in asWorkspaceFiles:
                # Merge the shape files
                if RiseUtils.mergeShapeFiles([sImpactMap1,sImpactMap2], sMergedImpactMap, oImpactsMapConfig.style):
                    # Publish the merged impact map
                    oPublishedLayer = self.addAndPublishLayer(sMergedImpactMap, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData1 + " " + sInputData2, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id, bForceRepublish=True, bForceDeleteLocalFile=False)
                    if oPublishedLayer is not None:
                        self.deleteLayer(sImpactMap1)
                        self.deleteLayer(sImpactMap2)
                else:
                    # If the merge fails, we publish the two separated layers
                    logging.info("RiseMapEngine.mergeOrPublishImpactsShape: error merging shape files " + sImpactMap1 + " and " + sImpactMap2 + " we publish separated layers")
                    self.addAndPublishLayer(sImpactMap1, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData1, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)                
                    self.addAndPublishLayer(sImpactMap2, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData2, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)


    def mergeOrPublishImpactsRaster(self, sImpactMap1, sImpactMap2, sInputData1, sInputData2, sMapId, sBaseName, oEventPeakDate, oImpactsPluginConfig, asWorkspaceFiles, bKeepLayer=False):
        """
        Merge the two impact maps and publish it
        :param sImpactMap1: First impact map
        :param sImpactMap2: Second impact map
        :param sInputData1: Input data for the first impact map
        :param sInputData2: Input data for the second impact map
        :param sMapId: Map ID for the impact map
        :param sBaseName: Base name for the impact map
        :param oEventPeakDate: Event peak date
        :param oImpactsPluginConfig: Plugin configuration for the impacts plugin
        :param asWorkspaceFiles: List of files in the workspace
        :return:
        """

        oImpactsMapConfig = RiseDeamon.getMapConfigFromPluginConfig(oImpactsPluginConfig,sMapId)
        if oImpactsMapConfig is None:
            logging.warning("RiseMapEngine.mergeOrPublishImpactsRaster: impossible to find configuration for map " + sMapId)
            return
        
        oReferenceDate = datetime.strptime(oEventPeakDate, "%Y-%m-%d")

        # Check if we have only the first map in the workspace
        if sImpactMap1 in asWorkspaceFiles and sImpactMap2 not in asWorkspaceFiles:
            self.addAndPublishLayer(sImpactMap1, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData1, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)
        # Check if we have only the second map in the workspace                
        elif sImpactMap1 not in asWorkspaceFiles and sImpactMap2 in asWorkspaceFiles:
            self.addAndPublishLayer(sImpactMap2, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData2, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)
        # Check if we have both maps in the workspace
        elif sImpactMap1 in asWorkspaceFiles and sImpactMap2 in asWorkspaceFiles:
            # Create the merged impact map name
            sMergedImpactMap = sBaseName + "_event_" + oReferenceDate.strftime("%Y-%m-%d") + "_merged_impacts_" + sMapId + ".tif"

            if sMergedImpactMap not in asWorkspaceFiles:
                # Make the mosaic
                sMosaicStatus = wasdi.mosaic([sImpactMap1,sImpactMap2], sMergedImpactMap, iNoDataValue=0, iIgnoreInputValue=0)
                
                # Merge the shape files
                if sMosaicStatus == "DONE":
                    # Publish the merged impact map
                    oPublishedLayer = self.addAndPublishLayer(sMergedImpactMap, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData1 + " " + sInputData2, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id, bForceRepublish=True)
                    if oPublishedLayer is not None:
                        self.deleteLayer(sImpactMap1)
                        self.deleteLayer(sImpactMap2)
            else:
                # If the merge fails, we publish the two separated layers
                logging.info("RiseMapEngine.mergeOrPublishImpactsRaster: error merging shape files " + sImpactMap1 + " and " + sImpactMap2 + " we publish separated layers")
                self.addAndPublishLayer(sImpactMap1, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData1, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)                
                self.addAndPublishLayer(sImpactMap2, oReferenceDate, True, oImpactsMapConfig.id , sResolution=oImpactsMapConfig.resolution, sDataSource=oImpactsMapConfig.dataSource, sInputData=sInputData2, bKeepLayer=bKeepLayer, sForceStyle=oImpactsMapConfig.style, sOverridePluginId="rise_impact_plugin", sOverrideMapId=oImpactsMapConfig.id)

    def checkProcessorId(self, sProcessorId):
        if sProcessorId is None:
            sProcessorId = ""
        if sProcessorId == "":
            # Please make a safe print. If the self.m_oMapEntity != null print also the name
            if self.m_oMapEntity is not None:
                logging.error("RiseMapEngine.checkProcessorId: cannot start the processor "+self.m_oMapEntity.id)
                return False
            else:
                logging.error("RiseMapEngine.checkProcessorId: cannot start the processor and also mapEntity is null")
                return False


        
        return True