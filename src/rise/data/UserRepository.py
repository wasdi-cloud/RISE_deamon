from src.rise.business.User import User
from src.rise.data.RiseMongoRepository import RiseMongoRepository

class UserRepository(RiseMongoRepository):

    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "users"
        self.m_sEntityClassName = f"{User.__module__}.{User.__qualname__}"