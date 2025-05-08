import logging

from src.rise.business.WidgetInfo import WidgetInfo
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class WidgetInfoRepository(RiseMongoRepository):
    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "widget_infos"
        self.m_sEntityClassName = f"{WidgetInfo.__module__}.{WidgetInfo.__qualname__}"
    