import logging

class RisePlugin:

    def __init__(self, oConfig, oArea):
        self.m_oConfig = oConfig
        self.m_oArea = oArea

    def triggerNewAreaMaps(self):
        logging.debug("RisePlugin.triggerNewAreaMaps")
