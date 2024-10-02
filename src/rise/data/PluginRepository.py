import logging

from src.rise.business.Plugin import Plugin
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class PluginRepository(RiseMongoRepository):
    
    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "plugins"
        
    
    def findPluginById(self, sPluginId):
        try:
            if sPluginId is None:
                logging.warning("PluginRepository.findPluginById. No plugin id specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"PluginRepository.findPluginById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sPluginId})

            if oRetrievedResult is None:
                logging.info(f"PluginRepository.findPluginById. no results retrieved from db")
                return None

            aoRetrievedPlugins = []
            for oResPlugin in oRetrievedResult:
                aoRetrievedPlugins.append(Plugin(**oResPlugin))

            if len(aoRetrievedPlugins) > 0:
                return aoRetrievedPlugins[0]
            else:
                return None

        except Exception as oEx:
            print(f"PluginRepository.findPluginById. Exception {oEx}")

        return None

    def listAllPlugins(self):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                logging.warning(f"PluginRepository.listAllPlugins. Collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({})

            if oRetrievedResult is None:
                logging.info(f"PluginRepository.listAllPlugins. No results retrieved from db")
                return None

            aoRetrievedPlugins = []
            for oResPlugin in oRetrievedResult:
                aoRetrievedPlugins.append(Plugin(**oResPlugin))

            logging.info(f"PluginRepository.listAllPlugins. found {len(aoRetrievedPlugins)} plugins")
            return aoRetrievedPlugins

        except Exception as oEx:
            logging.error(f"PluginRepository.listAllPlugins. Exception {oEx}")

        return None
