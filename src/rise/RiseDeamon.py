import getopt
import json
import logging
import sys
from types import SimpleNamespace

import wasdi

from src.rise.business.Area import Area
from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.PluginRepository import PluginRepository


class RiseDeamon:
    def __init__(self, oConfig):
        self.m_oConfig = oConfig

    def run(self):
        logging.info("RiseDeamon.run: Rise deamon start")

        wasdi.setUser(self.m_oConfig.wasdiConfig.wasdiUser)
        wasdi.setPassword(self.m_oConfig.wasdiConfig.wasdiPassword)
        wasdi.setBaseUrl(self.m_oConfig.wasdiConfig.wasdiBaseUrl)

        if not wasdi.init():
            logging.error("RiseDeamon.run: There was an error initializing WASDI")

        oAreaRepository = AreaRepository()
        aoAreas = oAreaRepository.listAllAreas()

        if aoAreas is None:
            aoAreas = []

        aoNewAreas = []

        for oArea in aoAreas:
            if oArea.archiveEndDate < 0.0 and oArea.archiveStartDate < 0.0:
                aoNewAreas.append(oArea)

        if len(aoNewAreas) > 0:
            self.handleNewAreas(aoNewAreas)
        else:
            logging.info("RiseDeamon.run: no new area found")

        if len(aoAreas) > 0:
            self.handleDailyMaps(aoAreas)
        else:
            logging.info("RiseDeamon.run: no areas found")

        self.checkResultsAndPublishLayers()

        self.cleanLayers()

    def getConfig(self):
        return self.m_oConfig

    def getClass(self, sClassName):
        asParts = sClassName.split('.')
        oModule = ".".join(asParts[:-1])
        oType = __import__(oModule)
        for sComponent in asParts[1:]:
            oType = getattr(oType, sComponent)
        return oType

    def getRisePlugin(self, sPluginId, oArea):
        try:
            oPluginRepository = PluginRepository()
            aoPlugins = oPluginRepository.listAllPlugins()
            for oPluginMapping in aoPlugins:
                if oPluginMapping.id == sPluginId:
                    oPluginClass = self.getClass(oPluginMapping.className)
                    oPluginRepository = PluginRepository()
                    return oPluginClass(self.m_oConfig, oArea, oPluginRepository.findPluginById(sPluginId))
        except:
            logging.error("RiseDeamon.getRisePlugin: Error creating class for plugin " + sPluginId)

        return None

    def handleNewAreas(self, aoNewAreas):

        for oArea in aoNewAreas:

            logging.info("RiseDeamon.handleNewAreas: Trigger archives for new area " + str(oArea.name) + " ["+oArea.id + "]")

            for sPluginId in oArea.plugins:

                oRisePlugin = self.getRisePlugin(sPluginId, oArea)

                if oRisePlugin is None:
                    logging.warning("RiseDeamon.handleNewAreas: Jumping plugin " + sPluginId)
                    continue

                oRisePlugin.triggerNewAreaMaps()

        logging.info("RiseDeamon.handleNewAreas: All the new area have been processed")

    def handleDailyMaps(self, aoAreas):
        pass

    def checkResultsAndPublishLayers(self):
        pass

    def cleanLayers(self):
        pass
    @staticmethod
    def readConfigFile(sConfigFilePath):
        with open(sConfigFilePath, "r") as oConfigFile:
            sConfigContent = oConfigFile.read()

        # Get the config as an object
        oConfig = json.loads(sConfigContent, object_hook=lambda d: SimpleNamespace(**d))
        oConfig.myFilePath = sConfigFilePath
        return oConfig

if __name__ == '__main__':
    # Default configuration file Path
    sConfigFilePath = '/etc/rise/riseConfig.json'

    try:
        # Read the command line args
        aoOpts, asArgs = getopt.getopt(sys.argv[1:], "hc:", ["config="])
    except getopt.GetoptError:
        print('RISE Deamon: python RiseDeamon.py -c <configfile>')
        sys.exit(2)

    for sOpt, sArg in aoOpts:
        if sOpt == '-h':
            print('RISE Deamon: python RiseDeamon.py -c <configfile>')
            sys.exit()
        if sOpt in ("-c", "--config"):
            # Override the config file path
            sConfigFilePath = sArg

    # Get the config as an object
    oRiseConfig = RiseDeamon.readConfigFile(sConfigFilePath)

    MongoDBClient._s_oConfig = oRiseConfig

    # Set a defaulto log level
    if oRiseConfig.logLevel is None:
        oRiseConfig.logLevel = "INFO"

    # Basic configuration
    logging.basicConfig(format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.getLevelName(oRiseConfig.logLevel))
    logging.getLogger("pymongo").setLevel(logging.ERROR)

    try:
        # Create the Deamon class
        oDemon = RiseDeamon(oRiseConfig)

        # And start!
        oDemon.run()

        logging.info("RiseDeamon finished! bye bye")
    except Exception as oEx:
        logging.error("RiseDeamon exception: bye bye " + str(oEx))


