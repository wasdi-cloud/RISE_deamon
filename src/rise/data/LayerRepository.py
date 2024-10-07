from src.rise.business.Layer import Layer
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class LayerRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "layers"
        self.m_sEntityClassName = f"{Layer.__module__}.{Layer.__qualname__}"
