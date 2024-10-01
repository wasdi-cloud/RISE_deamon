from src.rise.business.RiseEntity import RiseEntity


class Layer(RiseEntity):

    def __init__(self):
        self.link = str()
        self.referenceDate = float()
        self.source = str()
        self.profile = dict()
        self.mapId = str()
        self.pluginId = str()
        self.areaId = str()
        self.id = str()