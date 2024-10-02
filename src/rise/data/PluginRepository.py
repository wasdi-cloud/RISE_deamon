from src.rise.business.Plugin import Plugin
from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.RiseMongoRepository import RiseMongoRepository


class PluginRepository(RiseMongoRepository):
    
    def __init__(self):
        super().__init__()
        self.m_sCollectionName = "plugins"
        
    
    def findPluginById(self, sPluginId):
        try:
            if sPluginId is None:
                print("PluginRepository.findPluginById. No plugin id specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                print(f"PluginRepository.findPluginById. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sPluginId})

            if oRetrievedResult is None:
                print(f"PluginRepository.findPluginById. no results retrieved from db")
                return None

            aoRetrievedPlugins = []
            for oResPlugin in oRetrievedResult:
                aoRetrievedPlugins.append(Plugin(**oResPlugin))

            if len(aoRetrievedPlugins) > 0:
                return aoRetrievedPlugins[0]
            else:
                return None

        except:
            print("PluginRepository.findPluginById. Exception")

        return None

    def listAllPlugins(self):
        try:
            oCollection = self.getCollection()

            if oCollection is None:
                print(f"PluginRepository.listAllPlugins. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({})

            if oRetrievedResult is None:
                print(f"PluginRepository.listAllPlugins. no results retrieved from db")
                return None

            aoRetrievedPlugins = []
            for oResPlugin in oRetrievedResult:
                aoRetrievedPlugins.append(Plugin(**oResPlugin))

            print(f"PluginRepository.listAllPlugins. found {len(aoRetrievedPlugins)} plugins")
            return aoRetrievedPlugins

        except:
            print("PluginRepository.listAllPlugins. Exception")

        return None

    def getMapsIdForPluging(self, sPluginId):
        try:
            if sPluginId is None:
                print("PluginRepository.getMapsIdForPluging. No plugin id specified")
                return None

            oCollection = self.getCollection()

            if oCollection is None:
                print(f"PluginRepository.getMapsIdForPluging. collection {self.m_sCollectionName} not found in {RiseMongoRepository.s_sDB_NAME} database")
                return None

            oRetrievedResult = oCollection.find({"id": sPluginId})

            if oRetrievedResult is None:
                print(f"PluginRepository.getMapsIdForPluging. no results retrieved from db")
                return None

            asRetrievedMapIds = []
            for oResPlugin in oRetrievedResult:
                oResPlugin = Plugin(**oResPlugin)
                asRetrievedMapIds.extend(oResPlugin.maps)

            print(f"PluginRepository.getMapsIdForPluging. Retrieved {len(asRetrievedMapIds)} maps for pluging {sPluginId}")

            return asRetrievedMapIds

        except:
            print("PluginRepository.getMapsIdForPluging. Exception")

        return None