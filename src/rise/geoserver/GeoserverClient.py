import logging

from geo.Geoserver import Geoserver

class GeoserverClient:
    """
    Class dedicated to the creation of a single instance of the Geoserver client
    """

    _s_oConfig = None
    _s_oInstance = None

    def __new__(cls):
        if cls._s_oInstance is None:
            cls._s_oInstance = super(GeoserverClient, cls).__new__(cls)
            sUrl, sUserName, sPassword = cls._getGeoserverConnectionParameters()
            try:
                cls._s_oInstance.client = Geoserver(sUrl, username=sUserName, password=sPassword)
            except Exception as oEx:
                logging.error(f"GeoserverClient.__new__: exception {oEx}")
        return cls._s_oInstance

    @staticmethod
    def _getGeoserverConnectionParameters():
        sUrl = 'http://127.0.0.1:8080/geoserver'
        sUserName = 'admin'
        sPassword = 'geoserver'
        return sUrl, sUserName, sPassword




