from src.rise.business.RiseEntity import RiseEntity


class Session(RiseEntity):

    def __init__(self, **kwargs):
        self.token = str()
        self.userId = str()
        self.loginDate = float()
        self.lastTouch = float()

        for key, value in kwargs.items():
            setattr(self, key, value)
