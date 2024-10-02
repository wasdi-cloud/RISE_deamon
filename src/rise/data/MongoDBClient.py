from pymongo import MongoClient


class MongoDBClient:
    """
    Class dedicated to the creation of a single instance of the Mongo DB client
    """

    _s_oInstance = None

    def __new__(cls):
        if cls._s_oInstance is None:
            cls._s_oInstance = super(MongoDBClient, cls).__new__(cls)
            sConnectionString = cls._getConnectionString()
            cls._s_oInstance.client = MongoClient(sConnectionString)
        return cls._s_oInstance

    @staticmethod
    def _getConnectionString():
        # TODO: here we will need to deal with the connection to the DB retrieving relevan paramenters from the config, like username and password
        return "mongodb://localhost:27017"


