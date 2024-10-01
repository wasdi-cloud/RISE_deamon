import json
from types import SimpleNamespace
import sys, getopt
import logging
import wasdi

from src.rise.business.Area import Area


class RiseDeamon:
    def __init__(self, oConfig):
        self.m_oConfig = oConfig

    def run(self):
        logging.info("Rise deamon start")

        wasdi.setUser(self.m_oConfig.wasdiConfig.wasdiUser)
        wasdi.setPassword(self.m_oConfig.wasdiConfig.wasdiPassword)
        wasdi.setBaseUrl(self.m_oConfig.wasdiConfig.wasdiBaseUrl)

        if not wasdi.init():
            logging.error("There was an error initializing WASDI")

        self.runNewApplications()

        self.publishBands()

        self.cleanLayers()

    def getConfig(self):
        return self.m_oConfig

    def runNewApplications(self):
        logging.info("Run New Applications")

        logging.info("Find new areas")
        oArea = Area()
        oArea.
        aoNewAreas = []
        aoNewAreas.append(Area())


        pass

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


