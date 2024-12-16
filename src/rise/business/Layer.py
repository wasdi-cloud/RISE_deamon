from src.rise.business.RiseEntity import RiseEntity


class Layer(RiseEntity):

    def __init__(self, **kwargs):
        self.layerId = str()
        self.geoserverUrl = str()
        self.referenceDate = float()
        self.source = str()
        self.properties = dict()
        self.mapId = str()
        self.pluginId = str()
        self.areaId = str()
        self.id = str()
        self.published = False
        self.keepLayer = False

        for key, value in kwargs.items():
            setattr(self, key, value)
