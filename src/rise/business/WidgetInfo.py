import uuid
from datetime import datetime
from src.rise.business import Area
from src.rise.business.RiseEntity import RiseEntity


class WidgetInfo(RiseEntity):
    def __init__(self, **kwargs):
        self.id: str = None
        self.organizationId: str = None
        self.areaId: str = None
        self.widget: str = None
        self.bbox: str = None
        self.type: str = None
        self.icon: str = None
        self.title: str = None
        self.content: str = None
        self.referenceTime: float = None
        self.payload: dict[str, object] = {}

        for key, value in kwargs.items():
            setattr(self, key, value)        

    
    @staticmethod
    def createWidgetInfo(widget: str, oArea: Area, type: str, icon: str, title: str, content: str, referenceTime: str):
        oWidgetInfo = WidgetInfo()
        oWidgetInfo.id = str(uuid.uuid4())
        oWidgetInfo.organizationId = oArea.organizationId
        oWidgetInfo.areaId = oArea.id
        oWidgetInfo.widget = widget
        oWidgetInfo.bbox = oArea.bbox
        oWidgetInfo.type = type
        oWidgetInfo.icon = icon
        oWidgetInfo.title = title
        oWidgetInfo.content = content

        oDate = datetime.strptime(referenceTime, "%Y-%m-%d")
        # Set time to 12:00 PM (noon)
        oDate = oDate.replace(hour=12, minute=0, second=0, microsecond=0)  
        oWidgetInfo.referenceTime = oDate.timestamp()
        
        return oWidgetInfo
