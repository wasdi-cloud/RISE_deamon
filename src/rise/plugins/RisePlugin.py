import logging
import os.path
from pathlib import Path

import shapely.wkt
import wasdi

from src.rise.RiseDeamon import RiseDeamon


class RisePlugin:

    def __init__(self, oConfig, oArea, oPlugin):
        self.m_oConfig = oConfig
        self.m_oArea = oArea
        self.m_oPlugin = oPlugin
        self.m_oPluginConfig = None

        try:
            oParentPath = Path(oConfig.myFilePath).parent
            oPluginConfigPath = oParentPath.joinpath(oPlugin.id + ".json")
            if os.path.isfile(oPluginConfigPath):
                self.m_oPluginConfig = RiseDeamon.readConfigFile(oPluginConfigPath)

        except Exception as oEx:
            logging.error("RisePlugin.init: exception " + str(oEx))

    def getWasdiBbxFromWKT(self, sWkt, bJson=False):
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
        logging.debug("RisePlugin.triggerNewAreaMaps")

    def getWorkspaceName(self, oMap):
        sWorkspaceName = self.m_oArea.id + "|" + self.m_oPlugin.id + "|" + oMap.id
        return sWorkspaceName

    def createOrOpenWorkspace(self, oMap):
        sWorkspaceName = self.getWorkspaceName(oMap)
        sWorkspaceId = wasdi.getWorkspaceIdByName(sWorkspaceName)

        if sWorkspaceId == "":
            wasdi.createWorkspace(sWorkspaceName)
            sWorkspaceId = wasdi.getWorkspaceIdByName(sWorkspaceName)

        wasdi.openWorkspaceById(sWorkspaceId)
        return sWorkspaceId