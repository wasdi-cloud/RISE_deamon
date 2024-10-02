from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.RiseMongoRepository import RiseMongoRepository
from src.rise.business.Area import Area


class AreaRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "area"

    def findById(self, sEntityId):
        try:
            oMongoClient = MongoDBClient()
            oDatabase = oMongoClient.client[RiseMongoRepository.s_sDB_NAME]

            if oDatabase is None:
                print(f"database named {RiseMongoRepository.s_sDB_NAME} not found in Mongo")
                return None

            oCollection = oDatabase[self.m_sCollectionName]

            if oCollection is None:
                print(f"collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sEntityId})

            if oRetrievedResult is None:
                print(f"no results retrieved from db")
                return None

            oRetrievedArea = []
            for oRes in oRetrievedResult:
                oRetrievedBank = Area(**oRes)


        except:
            print("Exception")

        return None


