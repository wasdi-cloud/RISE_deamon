import json
import logging
import os
from datetime import datetime
from pathlib import Path

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.business.Layer import Layer
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.LayerRepository import LayerRepository
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
        if sMapId is None:
            sMapId = self.m_oMapEntity.id

        for oMapConfig in self.m_oPluginConfig.maps:
            if oMapConfig.id == sMapId:
                return oMapConfig

        return None

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

    def handleTask(self, oTask):
        '''
        Handle a Task created by this map engine
        :param oTask:
        :return:
        '''
        try:
            # We open the wasdi workspace
            logging.info("RiseMapEngine.handleTask: handle task " + oTask.id)
            oTaskRepo = WasdiTaskRepository()
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)

            # Get the status from WASDI
            sNewStatus = wasdi.getProcessStatus(oTask.id)

            if sNewStatus == "ERROR" or sNewStatus == "STOPPED":
                logging.warning("RiseMapEngine.handleTask: the new status is not done but " + sNewStatus + " update status and exit")
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
                logging.info("RiseMapEngine.handleTask: task is still ongoing, for now we do nothing (state = " + sNewStatus + ")")
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

        return oLayer

    def addAndPublishLayer(self, sFileName, oReferenceDate, bPublish=True, sMapIdForStyle=None, bKeepLayer=False, sDataSource="", oCreationDate=None, sResolution="", sInputData="", asProperties=None, sOverrideMapId=None, sOverridePluginId=None, bForceRepublish=False):
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


            if oTestLayer is None:
                logging.info("RiseMapEngine.addAndPublishLayer: publish Map: " + sLayerName)
                if sMapIdForStyle is not None:
                    sStyle = self.getStyleForMap(sMapIdForStyle)
                else:
                    sStyle = self.getStyleForMap()

                if bPublish:
                    if not self.publishRasterLayer(sFileName, sStyle):
                        logging.error("RiseMapEngine.addAndPublishLayer: impossible to publish " + sLayerName)
                    else:
                        oLayer.published = True

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

    def updateNewMaps(self):
        pass

    def getWorkspaceUpdatedJsonFile(self, sJsonFile, bDeleteFromWasdi):
        # Take a local copy
        sJsonFilePath = wasdi.getPath(sJsonFile)

        # Previous version, if available
        aoOldChainParams = None

        # If we have a local file
        if os.path.isfile(sJsonFilePath):
            # Clean it and re-take it updated from wasdi
            os.remove(sJsonFilePath)
            sJsonFilePath = wasdi.getPath(sJsonFile)

            # also delete from WASDI now, it will be re-written
            if bDeleteFromWasdi:
                wasdi.deleteProduct(sJsonFile)

            if os.path.isfile(sJsonFilePath):
                with open(sJsonFilePath, "r") as oFile:
                    try:
                        aoOldChainParams = json.load(oFile)
                    except:
                        pass

        return aoOldChainParams

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
            logging.error(f"RiseMApEngine.notifyEndOfTask. Error {oEx}")
