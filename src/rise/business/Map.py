from src.rise.business.RiseEntity import RiseEntity


class Map(RiseEntity):

    def __init__(self, **kwargs):
        self.name = str()
        self.description = str()
        self.layerBaseName = str()
        self.icon = str()
        self.id = str()
        self.dateFiltered = True
        self.className = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
