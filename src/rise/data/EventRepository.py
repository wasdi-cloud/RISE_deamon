import logging

from src.rise.business.Event import Event
from src.rise.data.RiseMongoRepository import RiseMongoRepository

class EventRepository(RiseMongoRepository):
    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "events"
        self.m_sEntityClassName = f"{Event.__module__}.{Event.__qualname__}"

    def findByParams(self, sAreaId="", sPeakStringDate="", sType=""):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"EventRepository.findByParams. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            aoFilters = {}

            if sAreaId is None:
                sAreaId = ""
            if sPeakStringDate is None:
                sPeakStringDate = ""
            if sType is None:
                sType = ""

            if sAreaId != "":
                aoFilters["areaId"] = sAreaId
            if sPeakStringDate != "":
                aoFilters["peakStringDate"] = sPeakStringDate
            if sType != "":
                aoFilters["type"] = sType

            oRetrievedResult = oCollection.find(aoFilters)

            if oRetrievedResult is None:
                logging.info(f"EventRepository.findByParams. no results retrieved from db")
                return None

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(Event(**oRes))

            return aoEntities
        except Exception as oEx:
            logging.error("EventRepository.findByParams. Exception " + str(oEx))

        return []        
    
    def getOngoing(self, sAreaId=""):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"EventRepository.getOngoing. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return []

            aoFilters = {}

            aoFilters["areaId"] = sAreaId            
            aoFilters["inGoing"] = True

            oRetrievedResult = oCollection.find(aoFilters)

            if oRetrievedResult is None:
                logging.debug(f"EventRepository.getOngoing. no results retrieved from db")
                return None

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(Event(**oRes))

            return aoEntities
        except Exception as oEx:
            logging.error("EventRepository.findByParams. Exception " + str(oEx))

        return []            