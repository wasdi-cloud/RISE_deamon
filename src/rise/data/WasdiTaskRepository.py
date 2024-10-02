import logging

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.RiseMongoRepository import RiseMongoRepository
from src.rise.business.Area import Area


class WasdiTaskRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "wasdi_tasks"

    def add(self, oEntity):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"WasdiTaskRepository.add. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oCollection.insert_one(vars(oEntity))

            return True
        except:
            logging.error("WasdiTaskRepository.add. Exception")

        return False

    def findById(self, sId):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"WasdiTaskRepository.findAreaById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sId})

            if oRetrievedResult is None:
                print(f"WasdiTaskRepository.findAreaById. no results retrieved from db")
                return None

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(WasdiTask(**oRes))

            if len(aoEntities) > 0:
                return aoEntities[0]
            else:
                return None
        except:
            print("WasdiTaskRepository.findAreaById. Exception")

        return None

    def findByParams(self, sAreaId, sMapId, sPluginId, sWorkspaceId):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"WasdiTaskRepository.findAreaById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"areaId": sAreaId, "mapId": sMapId, "pluginId": sPluginId, "workspaceId": sWorkspaceId})

            if oRetrievedResult is None:
                print(f"WasdiTaskRepository.findAreaById. no results retrieved from db")
                return None

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(WasdiTask(**oRes))

            return aoEntities
        except:
            print("WasdiTaskRepository.findAreaById. Exception")

        return None

    def list(self):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"WasdiTaskRepository.list. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({})

            if oRetrievedResult is None:
                print(f"WasdiTaskRepository.list. no results retrieved from db")
                return None

            aoRetrieved = []
            for oRes in oRetrievedResult:
                aoRetrieved.append(WasdiTask(**oRes))

            print(f"WasdiTaskRepository.list. found {len(aoRetrieved)} areas")
            return aoRetrieved

        except:
            print("WasdiTaskRepository.list. Exception")

        return None
