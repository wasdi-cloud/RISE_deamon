from src.rise.business.RiseEntity import RiseEntity


class OTP(RiseEntity):

    def __init__(self, **kwargs):
        self.userId = str()
        self.secretCode = str()
        self.validated = False
        self.operation = str()
        self.timestamp = float()
        self.id = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
