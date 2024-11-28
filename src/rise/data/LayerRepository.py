import logging

from src.rise.business.Layer import Layer
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class LayerRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "layers"
        self.m_sEntityClassName = f"{Layer.__module__}.{Layer.__qualname__}"


    def getLayersIdsOlderThanDate(self, fTimeStamp):
        """
        Given a timestamp representing a date, retrieves all the documents where the reference date is
        strictly less than the time stamp. If the timestamp is None or a negative value,
        then it returns the ids of all the layers stored in the database
        :param fTimeStamp: the timestamp used to fetch documents dated before it
        :return: the list of ids of layers dated before the time stamp
        """

        if fTimeStamp is None or fTimeStamp < 0.0:
            logging.info("LayerRepository.getLayersIdsOlderThanDate. Timestamp none or negative. "
                         "Returning all layers ids.")
            return self.listAllEntities()

        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"LayerRepository.getLayersIdsOlderThanDate. "
                                f"Collection {self.m_sCollectionName} not in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResults = oCollection.find({"referenceDate": {"$lt": fTimeStamp}})

            if oRetrievedResults is None:
                logging.info(f"LayerRepository.getLayersIdsOlderThanDate. No results retrieved from db")
                return None

            aoRetrievedLayers = list(map(lambda oLayerRes: Layer(**oLayerRes), oRetrievedResults))

            logging.info(f"LayerRepository.getLayersIdsOlderThanDate. Found {len(aoRetrievedLayers)} layers")
            return aoRetrievedLayers

        except Exception as oEx:
            logging.error(f"LayerRepository.getLayersIdsOlderThanDate. Exception {oEx}")

        return None

