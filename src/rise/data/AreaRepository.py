import logging

from src.rise.business.Area import Area
from src.rise.data.RiseMongoRepository import RiseMongoRepository
from src.rise.utils import RiseUtils


class AreaRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "areas"
        self.m_sEntityClassName = f"{Area.__module__}.{Area.__qualname__}"

    def listActive(self, bActive=None):
        """
        List all the entities in a collection
        :return: the full list of entities in a collection
        """
        oCollection = self.getCollection()

        if oCollection is None:
            logging.warning(f"RiseMongoRepository.listAllEntities. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
            return None

        try:
            aoFilters = {}
            if bActive is not None:
                aoFilters["active"] = bActive

            oRetrievedResult = oCollection.find(aoFilters)

            if oRetrievedResult is None:
                logging.info(f"RiseMongoRepository.listAllEntities. No results retrieved from db")
                return None

            aoRetrievedEntities = []
            for oResEntity in oRetrievedResult:
                oEntityClass = RiseUtils.getClass(self.m_sEntityClassName)
                aoRetrievedEntities.append(oEntityClass(**oResEntity))

            return aoRetrievedEntities

        except Exception as oEx:
            logging.error(f"RiseMongoRepository.listAllEntities. Exception {oEx}")

        return None
