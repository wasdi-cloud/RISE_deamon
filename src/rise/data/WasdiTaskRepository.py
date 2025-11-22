import logging

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class WasdiTaskRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "wasdi_tasks"
        self.m_sEntityClassName = f"{WasdiTask.__module__}.{WasdiTask.__qualname__}"

    def getCreatedList(self):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(
                    f"WasdiTaskRepository.getCreatedList. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"status": "CREATED"})

            if oRetrievedResult is None:
                print(f"WasdiTaskRepository.getCreatedList: no results retrieved from db")
                oRetrievedResult = []

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(WasdiTask(**oRes))

            return aoEntities
        except Exception as oEx:
            print("WasdiTaskRepository.getCreatedList: Exception " + str(oEx))

        return []

    def findByParams(self, sAreaId="", sMapId="", sPluginId="", sWorkspaceId="", sApplication="", sReferenceDate="", bIsShortArchive=None):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"WasdiTaskRepository.findByParams. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            aoFilters = {}

            if sAreaId is None:
                sAreaId = ""
            if sMapId is None:
                sMapId = ""
            if sPluginId is None:
                sPluginId = ""
            if sWorkspaceId is None:
                sWorkspaceId = ""
            if sApplication is None:
                sApplication = ""
            if sReferenceDate is None:
                sReferenceDate = ""

            if sAreaId != "":
                aoFilters["areaId"] = sAreaId
            if sMapId != "":
                aoFilters["mapId"] = sMapId
            if sPluginId != "":
                aoFilters["pluginId"] = sPluginId
            if sWorkspaceId != "":
                aoFilters["workspaceId"] = sWorkspaceId
            if sApplication != "":
                aoFilters["application"] = sApplication
            if sReferenceDate != "":
                aoFilters["referenceDate"] = sReferenceDate
            if bIsShortArchive is not None:
                aoFilters["isShortArchive"] = bIsShortArchive
            
            oRetrievedResult = oCollection.find(aoFilters)

            if oRetrievedResult is None:
                print(f"WasdiTaskRepository.findByParams. no results retrieved from db")
                return None

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(WasdiTask(**oRes))

            return aoEntities
        except:
            print("WasdiTaskRepository.findByParams. Exception")

        return []
