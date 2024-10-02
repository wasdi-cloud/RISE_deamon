import logging


class RisePlugin:

    def __init__(self, oConfig, oArea, oPlugin):
        self.m_oConfig = oConfig
        self.m_oArea = oArea
        self.m_oPlugin = oPlugin

    def triggerNewAreaMaps(self):
        logging.debug("RisePlugin.triggerNewAreaMaps")
