import logging
import os.path
from pathlib import Path

import shapely.wkt
import wasdi

from src.rise.RiseDeamon import RiseDeamon
from src.rise.data.MapRepository import MapRepository
from src.rise.utils import RiseUtils


class RisePlugin:

    def __init__(self, oConfig, oArea, oPlugin):
        self.m_oConfig = oConfig
        self.m_oArea = oArea
        self.m_oPluginEntity = oPlugin
        self.m_oPluginConfig = None
        self.m_aoMapEngines = []

        try:
            oParentPath = Path(oConfig.myFilePath).parent
            oPluginConfigPath = oParentPath.joinpath(oPlugin.id + ".json")
            if os.path.isfile(oPluginConfigPath):
                self.m_oPluginConfig = RiseDeamon.readConfigFile(oPluginConfigPath)

            oMapRepository = MapRepository()
            aoMaps = oMapRepository.getAllEntitiesById(self.m_oPluginEntity.maps)

            for oMap in aoMaps:
                logging.debug("RisePlugin.__init__: adding MapEngine " + oMap.name + " id= " + oMap.id)

                try:
                    oMapEngineClass = RiseUtils.getClass(oMap.className)
                    oMapEngine = oMapEngineClass(oConfig, oArea, oPlugin, self, oMap)
                    self.m_aoMapEngines.append(oMapEngine)
                except Exception as oInnerEx:
                    sName = ""
                    if oMap is not None:
                        if hasattr(oMap, "name"):
                            sName = oMap.name
                    logging.error("RisePlugin.__init__: exception creating MapEngine " + sName + ": " + str(oInnerEx))

        except Exception as oEx:
            logging.error("RisePlugin.init: exception " + str(oEx))

    def getMapEngineFromMapId(self, sMapId):

        if self.m_aoMapEngines is None:
            return None

        if len(self.m_aoMapEngines) == 0:
            return None

        for oMapEngine in self.m_aoMapEngines:
            if oMapEngine.m_oMapEntity is not None:
                if oMapEngine.m_oMapEntity.id == sMapId:
                    return oMapEngine

        return None

    def getWasdiBbxFromWKT(self, sWkt, bJson=False):
        """
        Transform a WKT geometry in a WASDI BBOX
        :param sWkt:
        :param bJson:
        :return:
        """
        try:
            oGeom = shapely.from_wkt(sWkt)
            sBbox = str(oGeom.bounds[3]) + "," + str(oGeom.bounds[0]) + "," + str(oGeom.bounds[1]) + "," + str(oGeom.bounds[2])

            if bJson:
                return wasdi.bboxStringToObject(sBbox)
            else:
                return sBbox

        except Exception as oEx:
            logging.error("RisePlugin.init: exception " + str(oEx))
        if bJson:
            return None
        else:
            return ""

    def triggerNewAreaMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RisePlugin.triggerNewAreaMaps")
        try:
            for oMapEngine in self.m_aoMapEngines:
                logging.info("RisePlugin.triggerNewAreaMaps: Starting Last Period for map " + oMapEngine.getName())
                oMapEngine.triggerNewAreaMaps()

            for oMapEngine in self.m_aoMapEngines:
                logging.info("RisePlugin.triggerNewAreaMaps: Starting Archive for map " + oMapEngine.getName())
                oMapEngine.triggerNewAreaArchives()

        except Exception as oEx:
            logging.error("RisePlugin.triggerNewAreaMaps: exception " + str(oEx))

    def getWorkspaceName(self, oMap):
        """
        Get Unique Workspace name from Area, Plugin and Map
        :param oMap:
        :return:
        """
        sWorkspaceName = self.m_oArea.id + "|" + self.m_oPluginEntity.id + "|" + oMap.id
        return sWorkspaceName

    def createOrOpenWorkspace(self, oMap):
        """
        Creates or open the unique workspace associated to this Area, Plugin and Map
        :param oMap:
        :return:
        """
        sWorkspaceName = self.getWorkspaceName(oMap)
        sWorkspaceId = wasdi.getWorkspaceIdByName(sWorkspaceName)

        if sWorkspaceId == "":
            wasdi.createWorkspace(sWorkspaceName)
            sWorkspaceId = wasdi.getWorkspaceIdByName(sWorkspaceName)

        wasdi.openWorkspaceById(sWorkspaceId)
        return sWorkspaceId

    def handleTask(self, oTask):
        '''
        Handle a task of the puglin
        :param oTask: Task entity
        :return:
        '''
        # Task must exists
        if oTask is None:
            logging.error("RiseMapEngine.handleTask: task is null")
            return

        # Get the Map engine associated
        oMapEngine = self.getMapEngineFromMapId(oTask.mapId)

        if oMapEngine is None:
            logging.error("RiseMapEngine.handleTask: Map Engine not found " + oTask.mapId + " for task " + oTask.id)
            return

        logging.info("RiseMapEngine.handleTask: calling handle Task on map " + oTask.mapId + " for plugin " + oTask.pluginId + " AreaId: " + oTask.areaId)

        # Ask the map to handle the task
        return oMapEngine.handleTask(oTask)

    def getPluginConfig(self):
        return self.m_oPluginConfig

    def updateNewMaps(self):
        """
        Trigger new area processors for this plugin
        :return:
        """
        logging.debug("RisePlugin.updateNewMaps")
        try:
            for oMapEngine in self.m_aoMapEngines:
                logging.info("RisePlugin.updateNewMaps: Starting today map for " + oMapEngine.getName())
                oMapEngine.updateNewMaps()

        except Exception as oEx:
            logging.error("RisePlugin.updateNewMaps: exception " + str(oEx))
