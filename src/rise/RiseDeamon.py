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
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.geoserver.GeoserverClient import GeoserverClient
from src.rise.utils import RiseUtils


class RiseDeamon:
    def __init__(self, oConfig):
        self.m_oConfig = oConfig
        self.m_aoPluginEntities = []

    def run(self):
        """
        Main Method of the Deamon.
        it initializes the wasdi lib, load areas, creates plugin engines
        and start the cycle:
            .Trigger New Area
            .Trigger Updated Maps
            .Check the result of tasks/publish layers
            .Clean old layers

        :return:
        """
        logging.info("RiseDeamon.run: Rise deamon start.")

        # Init the WASDI lib
        wasdi.setUser(self.m_oConfig.wasdiConfig.wasdiUser)
        wasdi.setPassword(self.m_oConfig.wasdiConfig.wasdiPassword)
        wasdi.setBaseUrl(self.m_oConfig.wasdiConfig.wasdiBaseUrl)

        if not wasdi.init():
            logging.error("RiseDeamon.run: There was an error initializing WASDI")
        else:
            logging.info("RiseDeamon.run: WASDI Initialized")

        # Get the list of areas
        oAreaRepository = AreaRepository()
        aoAreas = oAreaRepository.listAllEntities()

        # Safe programming: in the worst case an empty array
        if aoAreas is None:
            aoAreas = []

        # We start searching the new area
        aoNewAreas = []

        # That we recognize from the archive start and end date
        for oArea in aoAreas:
            if oArea.archiveEndDate < 0.0 and oArea.archiveStartDate < 0.0:
                aoNewAreas.append(oArea)

        if len(aoNewAreas) > 0:
            logging.info("RiseDeamon.run: handle new areas")
            self.handleNewAreas(aoNewAreas)
        else:
            logging.info("RiseDeamon.run: no new area found")

        if len(aoAreas) > 0:
            logging.info("RiseDeamon.run: handle daily maps")
            self.updateNewMaps(aoAreas)
        else:
            logging.info("RiseDeamon.run: no areas found")

        logging.info("RiseDeamon.run: chech the status of the processes scheduled")
        self.checkResultsAndPublishLayers()

        logging.info("RiseDeamon.run: Clean the old layers in geoserver")
        self.cleanLayers()

    def getConfig(self):
        """
        Get the RISE config object
        :return:
        """
        return self.m_oConfig

    def getRisePluginEngine(self, sPluginId, oArea):
        """
        Create the RISE Plugin Engines
        :param sPluginId: Unique Id of the Plugin
        :param oArea: Associated Area
        :return: The plugin Engine instance configured for this specific area
        """
        try:

            if len(self.m_aoPluginEntities)<=0:
                oPluginRepository = PluginRepository()
                self.m_aoPluginEntities = oPluginRepository.listAllEntities()

            for oPluginMapping in self.m_aoPluginEntities:
                if oPluginMapping.id == sPluginId:
                    oPluginClass = RiseUtils.getClass(oPluginMapping.className)
                    oPluginRepository = PluginRepository()
                    oPluginEngine = oPluginClass(self.m_oConfig, oArea, oPluginRepository.getEntityById(sPluginId))
                    return oPluginEngine

        except Exception as oEx:
            logging.error("RiseDeamon.getRisePlugin: Error creating class for plugin " + sPluginId + ": " +str(oEx))

        return None

    def handleNewAreas(self, aoNewAreas):
        """
        Trigger the execution of the processors for the new area
        :param aoNewAreas: List of new area to process
        :return:
        """
        # For all the new area
        for oArea in aoNewAreas:

            logging.info("RiseDeamon.handleNewAreas: Trigger archives for new area " + str(oArea.name) + " ["+oArea.id + "]")

            # For all the plugins activated
            for sPluginId in oArea.plugins:

                # Create instance of this plugin
                oRisePlugin = self.getRisePluginEngine(sPluginId, oArea)

                if oRisePlugin is None:
                    # We should find it!
                    logging.warning("RiseDeamon.handleNewAreas: Jumping plugin " + sPluginId)
                    continue

                # Ask the plugin to trigger the new operations
                oRisePlugin.triggerNewAreaMaps()

        logging.info("RiseDeamon.handleNewAreas: All the new area have been processed")

    def updateNewMaps(self, aoAreas):
        pass

    def checkResultsAndPublishLayers(self):
        logging.info("RiseDeamon.checkResultsAndPublishLayers: check the status of on-going processes")

        oTaskRepository = WasdiTaskRepository()
        aoTaskToProcess = oTaskRepository.getCreatedList()

        if aoTaskToProcess is None:
            logging.info("RiseDeamon.checkResultsAndPublishLayers: List of task is None, nothing to do")
            return

        oAreaRepository = AreaRepository()

        for oTask in aoTaskToProcess:

            oArea = oAreaRepository.getEntityById(oTask.areaId)
            oPluginEngine = self.getRisePluginEngine(oTask.pluginId, oArea)
            oPluginEngine.handleTask(oTask)


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
    GeoserverClient._s_oConfig = oRiseConfig

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


