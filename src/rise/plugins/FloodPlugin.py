import logging

from src.rise.plugins.RisePlugin import RisePlugin


class FloodPlugin(RisePlugin):
    def __init__(self, oConfig, oArea):
        super().__init__(oConfig, oArea)

    def triggerNewAreaMaps(self):
        logging.debug("FloodPlugin.triggerNewAreaMaps")


