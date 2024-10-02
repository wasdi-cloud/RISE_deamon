from src.rise.business.Map import Map
from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class MapRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "map"


    def findAllMapsById(self, asMapIdsList):
        try:
            if asMapIdsList is None or len(asMapIdsList) == 0:
                print("MapRepository.findAllMapsById. No map ids specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                print(f"MapRepository.findAllMapsById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": {"$in": asMapIdsList}})

            if oRetrievedResult is None:
                print(f"MapRepository.findAllMapsById. no results retrieved from db")
                return None

            aoRetrievedMaps = []
            for oResMap in oRetrievedResult:
                aoRetrievedMaps.append(Map(**oResMap))

            print(f"MapRepository.findAllMapsById. retrieved {len(aoRetrievedMaps)} maps")
            return aoRetrievedMaps

        except:
            print("MapRepository.findAllMapsById. Exception")

        return None