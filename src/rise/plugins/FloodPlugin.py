import logging

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

        except Exception as oEx:
            logging.error("FloodPlugin.triggerNewAreaMaps: exception " + str(oEx))


