import logging
import os
from pathlib import Path

import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.business.Layer import Layer
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.geoserver.GeoserverService import GeoserverService


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

    def getStyleForMap(self):
        oMapConfig = self.getMapConfig()

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
        try:
            logging.info("RiseMapEngine.handleTask: handle task " + oTask.id)
            oTaskRepo = WasdiTaskRepository()
            sWorkspaceId = self.m_oPluginEngine.createOrOpenWorkspace(self.m_oMapEntity)
            sNewStatus = wasdi.getProcessStatus(oTask.id)

            if sNewStatus == "ERROR" or sNewStatus == "STOPPED":
                logging.warning("RiseMapEngine.handleTask: the new status is not done but " + sNewStatus + " update status and exit")
                oTask.status = sNewStatus
                oTaskRepo.updateEntity(oTask)
                return False

            if sNewStatus == "DONE":
                logging.info("RiseMapEngine.handleTask: task done, lets proceed!")
                return True
            else:
                logging.info("RiseMapEngine.handleTask: task is still ongoing, for now we do nothing (state = " + sNewStatus + ")")
                return False

        except Exception as oEx:
            logging.error("RiseMapEngine.handleTask: exception " + str(oEx))
            return False

    def getLayerEntity(self, sLayerName, fTimestamp, aoProperties=None):

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

        return oLayer

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
            logging.error("RiseMap Engine exception " + str(oEx))

        return False

    def updateNewMaps(self):
        pass