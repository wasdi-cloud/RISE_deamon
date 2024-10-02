import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.data.MapRepository import MapRepository
from src.rise.plugins.RisePlugin import RisePlugin


class FloodPlugin(RisePlugin):
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)

    def triggerNewAreaMaps(self):
        logging.debug("FloodPlugin.triggerNewAreaMaps")

        try:
            oMapRepository = MapRepository()
            aoMaps = oMapRepository.findAllMapsById(self.m_oPlugin.maps)

            for oMap in aoMaps:
                logging.info("Starting Archive for map " + oMap.name)
                if oMap.id == "sar_flood":
                    self.runHasardLastWeek(oMap)
                elif oMap.id == "viirs_flood":
                    self.runViirsLastWeek(oMap)

        except Exception as oEx:
            logging.error("FloodPlugin.triggerNewAreaMaps: exception " + str(oEx))

    def runHasardLastWeek(self, oMap):
        sWorkspaceName = self.getWorkspaceName(oMap)
        sWorkspaceId = wasdi.getWorkspaceIdByName(sWorkspaceName)

        if sWorkspaceId == "":
            wasdi.createWorkspace(sWorkspaceName)

        wasdi.openWorkspace(sWorkspaceName)

        aoSarArchiveParameters = None
        oMapConfig = None

        for oMapConfig in self.m_oPluginConfig.maps:
            if oMapConfig.id == oMap.id:
                aoSarArchiveParameters = oMapConfig.params
                break

        if aoSarArchiveParameters is None:
            logging.warning("FloodPlugin.runHasardLastWeek: impossible to find parameters for map " + oMap.id)
            return

        aoSarArchiveParameters["BBOX"] = self.getWasdiBbxFromWKT(self.m_oArea.bbox)

        iEnd = datetime.today()
        iStart = iEnd - timedelta(days=oMapConfig.shortArchiveDaysBack)

        aoSarArchiveParameters["ARCHIVE_START_DATE"] = iStart.strftime("%Y-%m-%d")
        aoSarArchiveParameters["ARCHIVE_END_DATE"] = iEnd.strftime("%Y-%m-%d")
        aoSarArchiveParameters["MOSAICBASENAME"] = ""

        sProcessorId = wasdi.executeProcessor(oMapConfig.processor, aoSarArchiveParameters)



    def runViirsLastWeek(self, oMap):
        pass


