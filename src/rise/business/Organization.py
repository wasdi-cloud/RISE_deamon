from src.rise.business.RiseEntity import RiseEntity


class Organization(RiseEntity):

    def __init__(self, **kwargs):
        self.name = str()
        self.type = str()
        self.phone = str()
        self.county = str()
        self.city = str()
        self.street = str()
        self.number = str()
        self.postalCode = str()
        self.vat = str()
        self.creationDate = float()
        self.id = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
