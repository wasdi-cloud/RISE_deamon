import logging

class RisePlugin:

    def __init__(self, oConfig):
        self.m_oConfig = oConfig

    def runNewApplications(self):
        logging.debug("RisePlugin.runNewApplications")
