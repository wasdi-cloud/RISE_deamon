import logging

from src.rise.data.MongoDBClient import MongoDBClient


# TODO: refine logging
class RiseMongoRepository:
    # name of the database connected to this repository
    s_sDB_NAME = "rise"

    def __init__(self):
        self.m_sCollectionName = None
        self.m_sEntityClassName = None

    def getCollection(self):
        oCollection = None
        try:
            oMongoClient = MongoDBClient()
            oDatabase = oMongoClient.client[RiseMongoRepository.s_sDB_NAME]

            if oDatabase is None:
                logging.warning(f"RiseMongoRepository.getCollection. database named '{RiseMongoRepository.s_sDB_NAME}' not found in Mongo")
                return None

            oCollection = oDatabase[self.m_sCollectionName]
        except Exception as oEx:
            logging.error(f"RiseMongoRepository.getCollection. Exception retrieving the collection")

        return oCollection


    def findEntityById(self, sEntityId):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"AreaRepository.findAreaById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sEntityId})

            if oRetrievedResult is None:
                print(f"AreaRepository.findAreaById. no results retrieved from db")
                return None

            aoRetrievedAreas = []
            for oResArea in oRetrievedResult:
                print(self.m_sEntityClassName)
                oEntityClass = self.getClass(self.m_sEntityClassName)
                aoRetrievedAreas.append(oEntityClass(**oResArea))

            if len(aoRetrievedAreas) > 0:
                return aoRetrievedAreas[0]
            else:
                return None
        except Exception as oEx:
            print(f"AreaRepository.findAreaById. Exception {oEx}")

        return None

    def getClass(self, sClassName):
        asParts = sClassName.split('.')
        oModule = ".".join(asParts[:-1])
        oType = __import__(oModule)
        for sComponent in asParts[1:]:
            oType = getattr(oType, sComponent)
        return oType
