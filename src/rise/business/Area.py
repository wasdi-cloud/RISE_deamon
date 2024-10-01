from src.rise.business.RiseEntity import RiseEntity


class Area(RiseEntity):

    def __init__(self):
        self.name = str()
        self.description = str()
        self.plugins = []
        self.fieldOperators = []
        self.creationDate = float()
        self.subscriptionId = str()
        self.organizationId = str()
        self.bbox = str()
        self.markerCoordinates = str()
        self.shapeFileMask = str()
        self.supportArchive = False
        self.archiveStartDate = float()
        self.archiveEndDate = float()
        self.id = str()