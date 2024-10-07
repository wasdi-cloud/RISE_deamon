import logging
from datetime import datetime, timedelta

import wasdi

from src.rise.business.WasdiTask import WasdiTask
from src.rise.data.MapRepository import MapRepository
from src.rise.data.WasdiTaskRepository import WasdiTaskRepository
from src.rise.plugins.RisePlugin import RisePlugin


class FloodPlugin(RisePlugin):
    """

    """
    def __init__(self, oConfig, oArea, oPlugin):
        super().__init__(oConfig, oArea, oPlugin)
