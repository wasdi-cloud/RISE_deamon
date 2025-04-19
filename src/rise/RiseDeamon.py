import getopt
import json
import logging
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import wasdi

from src.rise.data.AreaRepository import AreaRepository
from src.rise.data.LayerRepository import LayerRepository
from src.rise.data.MongoDBClient import MongoDBClient
from src.rise.data.PluginRepository import PluginRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.geoserver.GeoserverClient import GeoserverClient
from src.rise.geoserver.GeoserverService import GeoserverService
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
            .Trigger New Area Archives
            .Trigger Updated Maps
            .Check the result of tasks/publish layers
            .Clean old layers

        :return:
        """

        logging.info("RiseDeamon.run: Rise deamon start v.1.0.6")

        logging.getLogger("requests").propagate = False
        logging.getLogger("urllib3").propagate = False

        # Init the WASDI lib
        wasdi.setUser(self.m_oConfig.wasdiConfig.wasdiUser)
        wasdi.setPassword(self.m_oConfig.wasdiConfig.wasdiPassword)
        wasdi.setBaseUrl(self.m_oConfig.wasdiConfig.wasdiBaseUrl)
        wasdi.setVerbose(self.m_oConfig.wasdiConfig.verbose)
        

        if not wasdi.init():
            logging.error("RiseDeamon.run: There was an error initializing WASDI")
        else:
            logging.info("RiseDeamon.run: WASDI Initialized")

        if self.m_oConfig.daemon.checkResults:
            logging.info("RiseDeamon.run: check the status of the processes scheduled")
            self.checkResultsAndPublishLayers()
        else:
            logging.info("RiseDeamon.run: checkResultsAndPublishLayers Disabled by config")

        # Get the list of areas
        oAreaRepository = AreaRepository()
        aoAreas = oAreaRepository.listActive(True)

        # Safe programming: in the worst case an empty array
        if aoAreas is None:
            aoAreas = []

        # We start searching the new area
        aoNewAreas = []
        aoOldAreas = []

        # That we recognize from the flag
        for oArea in aoAreas:
            if oArea.newCreatedArea:
                aoNewAreas.append(oArea)
            else:
                aoOldAreas.append(oArea)

        if len(aoNewAreas) > 0:
            logging.info("RiseDeamon.run: handle new areas found " + str(len(aoNewAreas)))
            if self.m_oConfig.daemon.newAreas:
                self.handleNewAreas(aoNewAreas)
            else:
                logging.info("RiseDeamon.run: New Areas Disabled by config")
        else:
            logging.info("RiseDeamon.run: no new area found")

        if len(aoOldAreas) > 0:
            if self.m_oConfig.daemon.updateNewMaps:
                logging.info("RiseDeamon.run: Update new maps")
                self.updateNewMaps(aoOldAreas)
            else:
                logging.info("RiseDeamon.run: updateNewMaps Disabled by config")
        else:
            logging.info("RiseDeamon.run: no areas found")

        if self.m_oConfig.daemon.cleanLayers:
            logging.info("RiseDeamon.run: Clean the old layers in geoserver")
            self.cleanLayers()
        else:
            logging.info("RiseDeamon.run: cleanLayers Disabled by config")

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
        oAreaRepository = AreaRepository()

        # For all the new area
        for oArea in aoNewAreas:

            logging.info("RiseDeamon.handleNewAreas: Trigger last maps for new area " + str(oArea.name) + " ["+oArea.id + "]")

            # For all the plugins activated
            for sPluginId in oArea.plugins:

                try:
                    # Create instance of this plugin
                    oRisePlugin = self.getRisePluginEngine(sPluginId, oArea)

                    if oRisePlugin is None:
                        # We should find it!
                        logging.warning("RiseDeamon.handleNewAreas: Jumping plugin " + sPluginId)
                        continue

                    # Ask the plugin to trigger the new operations
                    oRisePlugin.triggerNewAreaMaps()
                except Exception as oEx:
                    logging.warning("Error handling the new area " + oArea.name + " Plugin " + sPluginId + " - " + str(oEx))

            # Set the area as handled
            oArea.newCreatedArea = False
            oAreaRepository.updateEntity(oArea)

        # For all the new area
        for oArea in aoNewAreas:

            logging.info("RiseDeamon.handleNewAreas: Trigger archives for new area " + str(oArea.name) + " ["+oArea.id + "]")

            # For all the plugins activated
            for sPluginId in oArea.plugins:

                try:
                    # Create instance of this plugin
                    oRisePlugin = self.getRisePluginEngine(sPluginId, oArea)

                    if oRisePlugin is None:
                        # We should find it!
                        logging.warning("RiseDeamon.handleNewAreas: Jumping plugin " + sPluginId)
                        continue

                    # Ask the plugin to trigger the new operations
                    oRisePlugin.triggerNewAreaArchives()
                except Exception as oEx:
                    logging.warning("Error handling the new area " + oArea.name + " Plugin " + sPluginId + " - " + str(oEx))

            # Set the area as handled
            oArea.newCreatedArea = False
            oAreaRepository.updateEntity(oArea)

        logging.info("RiseDeamon.handleNewAreas: All the new area have been processed")



    def updateNewMaps(self, aoAreas):
        # For all the new areas
        for oArea in aoAreas:

            logging.info("RiseDeamon.updateNewMaps: Start new maps for area " + str(oArea.name) + " ["+oArea.id + "]")

            # For all the plugins activated
            for sPluginId in oArea.plugins:

                try:
                    # Create instance of this plugin
                    oRisePlugin = self.getRisePluginEngine(sPluginId, oArea)

                    if oRisePlugin is None:
                        # We should find it!
                        logging.warning("RiseDeamon.updateNewMaps: Jumping plugin " + sPluginId)
                        continue

                    # Ask the plugin to trigger the new operations
                    oRisePlugin.updateNewMaps()
                except Exception as oEx:
                    logging.warning("RiseDeamon.updateNewMaps: Error handling the new area " + oArea.name + " Plugin " + sPluginId + " - " + str(oEx))

        logging.info("RiseDeamon.updateNewMaps: new maps processed")

    def checkResultsAndPublishLayers(self):
        logging.info("RiseDeamon.checkResultsAndPublishLayers: check the status of on-going processes")

        # Take the list of our CREATED task
        oTaskRepository = WasdiTaskRepository()
        aoTaskToProcess = oTaskRepository.getCreatedList()

        # We need a list of task to proceed
        if aoTaskToProcess is None:
            logging.info("RiseDeamon.checkResultsAndPublishLayers: List of task is None, nothing to do")
            return

        oAreaRepository = AreaRepository()
        # For each task created
        for oTask in aoTaskToProcess:
            # Get the area
            oArea = oAreaRepository.getEntityById(oTask.areaId)

            if oArea is not None:
                # Create the Plugin Engine
                oPluginEngine = self.getRisePluginEngine(oTask.pluginId, oArea)
                # Handle this task!
                oPluginEngine.handleTask(oTask)
            else:
                logging.warning("RiseDeamon.checkResultsAndPublishLayers: Task " + oTask.id + " belong to a not anymore existing area. We delete it")
                oTaskRepository.deleteEntity(oTask.id)


    def cleanLayers(self):

        if self.m_oConfig is None:
            logging.error("RiseDeamon.cleanLayers. Config is none. No layer will be deleted")
            return

        if self.m_oConfig.daemon is None:
            logging.error("RiseDeamon.cleanLayers. No settings were found for the deamon. No layer will be deleted")
            return

        if self.m_oConfig.daemon.layersRetentionDays is None:
            logging.error("RiseDeamon.cleanLayers. No layers retention days specified. No layer will be deleted")
            return

        iRetentionDays = self.m_oConfig.daemon.layersRetentionDays

        try:
            iRetentionTimestampLimit = RiseUtils.getTimestampBackInDays(iRetentionDays)

            oLayerRepo = LayerRepository()
            aoLayerEntities = oLayerRepo.getLayersIdsOlderThanDate(iRetentionTimestampLimit)

            oGeoService = GeoserverService()
            aoDeletedEntitiesIds = []

            for oEntity in aoLayerEntities:
                sLayerId = oEntity.layerId

                if RiseUtils.isNoneOrEmpty(sLayerId):
                    logging.debug("RiseDeamon.cleanLayers: found an empty layer id")
                    continue

                if oGeoService.deleteLayer(sLayerId):
                    aoDeletedEntitiesIds.append(oEntity.id)
                    logging.debug(f"RiseDeamon.cleanLayers: layer {sLayerId} has been deleted from Geoserver")
                else:
                    if not oGeoService.existsLayer(sLayerId):
                        logging.info("RiseDeamon.cleanLayers: the layer " + sLayerId + " does not exists in Geoserver, we consider it deleted")
                        aoDeletedEntitiesIds.append(oEntity.id)

            iDeletedLayers = 0
            # to be sure that the Layer entities have not been updated while we were deleting the layers from Geoserver,
            # we reload the entities, before updating them
            if len(aoDeletedEntitiesIds)>0:
                aoDeletedLayers = oLayerRepo.getAllEntitiesById(aoDeletedEntitiesIds)
                list(map(lambda oLayer: setattr(oLayer, "published", False), aoDeletedLayers))
                iDeletedLayers = oLayerRepo.updateAllEntities(aoDeletedLayers)
            logging.info(f"RiseDeamon.cleanLayers: number of cleaned layers is equal to {iDeletedLayers}")

        except Exception as oEx:
            logging.error(f"RiseDeamon.cleanLayers: exception {oEx}")


    @staticmethod
    def readConfigFile(sConfigFilePath):
        with open(sConfigFilePath, "r") as oConfigFile:
            sConfigContent = oConfigFile.read()

        # Get the config as an object
        oConfig = json.loads(sConfigContent, object_hook=lambda d: SimpleNamespace(**d))
        oConfig.myFilePath = sConfigFilePath
        return oConfig
    
    @staticmethod
    def getPluginConfig(sPluginId, oConfig):
        try:
            oParentPath = Path(oConfig.myFilePath).parent
            oPluginConfigPath = oParentPath.joinpath(sPluginId + ".json")
            if os.path.isfile(oPluginConfigPath):
                oPluginConfig = RiseDeamon.readConfigFile(oPluginConfigPath)
                return oPluginConfig
        except Exception as oEx:
            logging.error("RiseDeamon.getPluginConfig: exception " + str(oEx))
        
        return None
    
    def getMapConfigFromPluginConfig(oPluginConfig, sMapId):
        try:
            for oMapConfig in oPluginConfig.maps:
                if oMapConfig.id == sMapId:
                    return oMapConfig
        except Exception as oEx:
            logging.error("RiseDeamon.getMapConfigFromPluginConfig: exception " + str(oEx))
        
        return None        


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

    # Set a defaul log level
    if oRiseConfig.logLevel is None:
        oRiseConfig.logLevel = "INFO"

    # Logger Basic configuration
    logging.basicConfig(format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.getLevelName(oRiseConfig.logLevel))
    logging.getLogger("pymongo").setLevel(logging.ERROR)

    try:
        # Create the Deamon class
        oDemon = RiseDeamon(oRiseConfig)

        # And start!
        oDemon.run()

        logging.info("RiseDeamon finished! bye bye")
    except Exception as oEx:
        logging.error("RiseDeamon exception: Error ->  " + str(oEx))


