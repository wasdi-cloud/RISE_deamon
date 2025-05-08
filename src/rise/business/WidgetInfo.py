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