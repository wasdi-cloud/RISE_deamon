import logging

from src.rise.plugins.RisePlugin import RisePlugin


class FloodPlugin(RisePlugin):
    def __init__(self, oConfig):
        super().__init__(oConfig)

    def runNewApplications(self):
        logging.debug("FloodPlugin.runNewApplications")

