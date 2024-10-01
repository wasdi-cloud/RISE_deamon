from src.rise.business.RiseEntity import RiseEntity


class Plugin(RiseEntity):

    def __init__(self):
        self.name = str()
        self.shortDescription = str()
        self.longDescription = str()
        self.supportArchive = False
        self.archivePrice = float()
        self.emergencyPrice = float()
        self.stringCode = str()
        self.maps = []
        self.id = str()