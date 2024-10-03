from src.rise.business.RiseEntity import RiseEntity


class Event(RiseEntity):

    def Event(self, **kwargs):
        self.name = str()
        self.type = str()
        self.bbox = str()
        self.startDate = float()
        self.endDate = float()
        self.peakDate = float()
        self.id = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
