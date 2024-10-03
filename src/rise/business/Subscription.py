from src.rise.business.RiseEntity import RiseEntity


class Subscription(RiseEntity):

    def __init__(self, **kwargs):
        self.organizationId = str()
        self.name = str()
        self.type = str()
        self.description = str()
        self.creationDate = float()
        self.buyDate = float()
        self.valid = False
        self.expireDate = float()
        self.plugins = []
        self.paymentType = None
        self.price = float()
        self.currency = str()
        self.supportsArchive = False
        self.id = str()

        for key, value in kwargs:
            setattr(self, key, value)
