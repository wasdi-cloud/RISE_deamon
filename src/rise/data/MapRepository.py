import logging

from src.rise.business.Map import Map
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class MapRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "maps"
        self.m_sEntityClassName = f"{Map.__module__}.{Map.__qualname__}"

    def findAllMapsById(self, asMapIdsList):
        try:
            if asMapIdsList is None or len(asMapIdsList) == 0:
                logging.warning("MapRepository.findAllMapsById. No map ids specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"MapRepository.findAllMapsById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": {"$in": asMapIdsList}})

            if oRetrievedResult is None:
                logging.info(f"MapRepository.findAllMapsById. no results retrieved from db")
                return None

            aoRetrievedMaps = []
            for oResMap in oRetrievedResult:
                aoRetrievedMaps.append(Map(**oResMap))

            logging.info(f"MapRepository.findAllMapsById. retrieved {len(aoRetrievedMaps)} maps")
            return aoRetrievedMaps

        except Exception as oEx:
            logging.error(f"MapRepository.findAllMapsById. Exception {oEx}")

        return None
