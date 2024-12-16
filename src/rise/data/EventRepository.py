import logging

from src.rise.business.Event import Event
from src.rise.data.RiseMongoRepository import RiseMongoRepository

class EventRepository(RiseMongoRepository):
    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "events"
        self.m_sEntityClassName = f"{Event.__module__}.{Event.__qualname__}"