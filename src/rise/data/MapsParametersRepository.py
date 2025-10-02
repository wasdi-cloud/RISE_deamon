from src.rise.business.MapsParameter import MapsParameter
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class MapsParametersRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "maps"
        self.m_sEntityClassName = f"{MapsParameter.__module__}.{MapsParameter.__qualname__}"