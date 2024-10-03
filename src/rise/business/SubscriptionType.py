from src.rise.business.RiseEntity import RiseEntity


class SubscriptionType(RiseEntity):

    def __init__(self, **kwargs):
        self.description = str()
        self.stringCode = str()
        self.id = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
        