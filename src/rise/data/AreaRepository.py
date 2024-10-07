from src.rise.business.Area import Area
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class AreaRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "areas"
        self.m_sEntityClassName = f"{Area.__module__}.{Area.__qualname__}"
