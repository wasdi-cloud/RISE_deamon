import logging

from src.rise.data.MongoDBClient import MongoDBClient


# TODO: refine logging
class RiseMongoRepository:
    # name of the database connected to this repository
    s_sDB_NAME = "rise"  # TODO: define db name

    def __init__(self):
        self.m_sCollectionName = None

