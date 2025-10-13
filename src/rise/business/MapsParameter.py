from src.rise.business.RiseEntity import RiseEntity


class MapsParameter(RiseEntity):

    def __init__(self, **kwargs):
        self.id = str()
        self.areaId = str()
        self.pluginId = str()
        self.mapId = str()
        self.payload = str()
        self.creationUserId = str()
        self.creationTimestamp = float()
        self.lastModifyUserId = str()
        self.lastModifyTimestamp = float()

        for key, value in kwargs.items():
            setattr(self, key, value)
