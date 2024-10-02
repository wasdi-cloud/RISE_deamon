from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.RiseMongoRepository import RiseMongoRepository
from src.rise.business.Area import Area


class AreaRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "areas"

    def findAreaById(self, sAreaId):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"AreaRepository.findAreaById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sAreaId})

            if oRetrievedResult is None:
                print(f"AreaRepository.findAreaById. no results retrieved from db")
                return None

            oRetrievedArea = []
            for oResArea in oRetrievedResult:
                oRetrievedArea.append(Area(**oResArea))

            return oRetrievedArea

        except:
            print("AreaRepository.findAreaById. Exception")

        return None

    def listAllAreas(self):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"AreaRepository.listAllAreas. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({})

            if oRetrievedResult is None:
                print(f"AreaRepository.listAllAreas. no results retrieved from db")
                return None

            aoRetrievedArea = []
            for oResArea in oRetrievedResult:
                aoRetrievedArea.append(Area(**oResArea))

            print(f"AreaRepository.listAllAreas. found {len(aoRetrievedArea)} areas")
            return aoRetrievedArea

        except:
            print("AreaRepository.listAllAreas. Exception")

        return None
