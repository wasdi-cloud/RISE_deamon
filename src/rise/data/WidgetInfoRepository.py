import logging

from src.rise.business.WidgetInfo import WidgetInfo
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class WidgetInfoRepository(RiseMongoRepository):
    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "widget_infos"
        self.m_sEntityClassName = f"{WidgetInfo.__module__}.{WidgetInfo.__qualname__}"
    
    def findByParams(self, sWidget="", sAreaId="", sReferenceDate=0, sTitle=""):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"WidgetInfoRepository.findByParams. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            aoFilters = {}

            if sAreaId is None:
                sAreaId = ""
            if sWidget is None:
                sWidget = ""
            if sTitle is None:
                sTitle = ""

            if sAreaId != "":
                aoFilters["areaId"] = sAreaId
            if sWidget != "":
                aoFilters["widget"] = sWidget
            if sReferenceDate != "":
                aoFilters["referenceDate"] = sReferenceDate
            if sTitle != "":
                aoFilters["title"] = sTitle

            oRetrievedResult = oCollection.find(aoFilters)

            if oRetrievedResult is None:
                print(f"WidgetInfoRepository.findByParams. no results retrieved from db")
                return None

            aoEntities = []
            for oRes in oRetrievedResult:
                aoEntities.append(WidgetInfo(**oRes))

            return aoEntities
        except Exception as oEx:
            logging.error(f"WidgetInfoRepository.findByParams. Exception: {str(oEx)}")

        return []    