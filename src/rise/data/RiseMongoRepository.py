import logging

from src.rise.data.MongoDBClient import MongoDBClient


# TODO: refine logging
class RiseMongoRepository:
    # name of the database connected to this repository
    s_sDB_NAME = "rise"  # TODO: define db name

    def __init__(self):
        self.m_sCollectionName = None


    def getCollection(self):
        oCollection = None
        try:
            oMongoClient = MongoDBClient()
            oDatabase = oMongoClient.client[RiseMongoRepository.s_sDB_NAME]

            if oDatabase is None:
                print(f"RiseMongoRepository.getCollection. database named '{RiseMongoRepository.s_sDB_NAME}' not found in Mongo")
                return None

            oCollection = oDatabase[self.m_sCollectionName]
        except:
            print(f"RiseMongoRepository.getCollection. Exception retrieving the collection")

        return oCollection

