import json
from types import SimpleNamespace
import sys, getopt
import logging
import wasdi

from src.rise.business.Area import Area
from src.rise.plugins.RisePlugin import RisePlugin
from src.rise.plugins.FloodPlugin import FloodPlugin


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

        self.runNewApplications()

        self.publishBands()

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

    def getRisePlugin(self, sPluginId):
        try:
            for oPluginMapping in self.m_oConfig.puglinClasses:
                if oPluginMapping.id == sPluginId:
                    oPluginClass = self.getClass(oPluginMapping.name)
                    return oPluginClass(self.m_oConfig)
        except:
            logging.error("RiseDeamon.getRisePlugin: Error creating class for plugin " + sPluginId)

        return None

    def runNewApplications(self):
        logging.info("RiseDeamon.runNewApplications: Run New Applications")

        logging.info("RiseDeamon.runNewApplications: Find new areas")
        oSampleArea = Area()
        oSampleArea.name = "Area 1"
        oSampleArea.bbox = "POLYGON ((1.839965 13.314794, 1.839965 13.771399, 2.339777 13.771399, 2.339777 13.314794, 1.839965 13.314794)))"
        oSampleArea.archiveEndDate = -1.0
        oSampleArea.archiveStartDate = -1.0
        oSampleArea.plugins = ["dc2f281d-cfa8-4b6e-b59c-609ea52fd6bf", "0245f5c2-3588-4ac5-94cb-edfb0a565142", "717464fe-3f67-4658-b868-16c3bd1f7e70", "bae09d65-6c1f-401e-9d8c-0e7f42c47d22"]

        aoNewAreas = [oSampleArea]

        for oArea in aoNewAreas:

            for sPluginId in oArea.plugins:
                oRisePlugin = self.getRisePlugin(sPluginId)

                if oRisePlugin is None:
                    logging.warning("RiseDeamon.runNewApplications: Jumping plugin " + sPluginId)

                oRisePlugin.runNewApplications()

        logging.info("RiseDeamon.runNewApplications: All the new area are processed")

    def publishBands(self):
        pass

    def cleanLayers(self):
        pass

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

    # We read the configuration file
    sConfigContent = ""

    with open(sConfigFilePath, "r") as oConfigFile:
        sConfigContent = oConfigFile.read()

    # Get the config as an object
    oRiseConfig = json.loads(sConfigContent, object_hook=lambda d: SimpleNamespace(**d))

    # Set a defaulto log level
    if oRiseConfig.logLevel is None:
        oRiseConfig.logLevel = "INFO"

    # Basic configuration
    logging.basicConfig(format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M", level=logging.getLevelName(oRiseConfig.logLevel))

    # Create the Deamon class
    oDemon = RiseDeamon(oRiseConfig)

    # And start!
    oDemon.run()


