from src.rise.business.RiseEntity import RiseEntity


class WasdiTask(RiseEntity):

    def __init__(self, **kwargs):
        self.id = str()
        self.areaId = str()
        self.mapId = str()
        self.pluginId = str()
        self.startDate = float()
        self.workspaceId = str()
        self.pluginPayload = {}

        for key, value in kwargs.items():
            setattr(self, key, value)
