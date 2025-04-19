from src.rise.business.RiseEntity import RiseEntity


class Event(RiseEntity):

    def __init__(self, **kwargs):
        self.name = str()
        self.type = str()
        self.bbox = str()
        self.startDate = float()
        self.endDate = float()
        self.peakDate = float()
        self.id = str()
        self.areaId = str()
        self.description = str()
        self.publicEvent = bool()
        self.inGoing = bool()
        self.markerCoordinates = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
