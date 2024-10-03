import logging

from src.rise.data.RiseMongoRepository import RiseMongoRepository
from src.rise.business.Area import Area


class AreaRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "areas"
        self.m_sEntityClassName = f"{Area.__module__}.{Area.__qualname__}"

    def findAreaById(self, sAreaId):
        try:
            if sAreaId is None:
                logging.warning("AreaRepository.findAreaById. Area id not specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"AreaRepository.findAreaById. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sAreaId})

            if oRetrievedResult is None:
                logging.info(f"AreaRepository.findAreaById. No results retrieved from db")
                return None

            aoRetrievedAreas = []
            for oResArea in oRetrievedResult:
                aoRetrievedAreas.append(Area(**oResArea))

            if len(aoRetrievedAreas) > 0:
                return aoRetrievedAreas[0]
            else:
                return None
        except Exception as oEx:
            logging.error(f"AreaRepository.findAreaById. Exception {oEx}")

        return None

    def listAllAreas(self):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"AreaRepository.listAllAreas. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({})

            if oRetrievedResult is None:
                logging.info(f"AreaRepository.listAllAreas. No results retrieved from db")
                return None

            aoRetrievedArea = []
            for oResArea in oRetrievedResult:
                aoRetrievedArea.append(Area(**oResArea))

            logging.info(f"AreaRepository.listAllAreas. found {len(aoRetrievedArea)} areas")
            return aoRetrievedArea
        except Exception as oEx:
            print(f"AreaRepository.listAllAreas. Exception {oEx}")

        return None
