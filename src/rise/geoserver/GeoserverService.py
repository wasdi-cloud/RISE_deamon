import logging

from src.rise.geoserver.GeoserverClient import GeoserverClient


class GeoserverService:

    def getWorkspace(self, sWorkspaceName):

        if sWorkspaceName is None or sWorkspaceName == '':
            logging.warning(f"GeoserverService.getWorkspace. Workspace named {sWorkspaceName} not in Geoserver")
            return None

        try:
            oGeoClient = GeoserverClient().client
            oWorkspace = oGeoClient.get_workspace(sWorkspaceName)

            return oWorkspace
        except Exception as oEx:
            logging.error(f"GeoserverService.getWorkspace. Exception {oEx}")

        return None

    def publishRasterLayer(self, sRasterFilePath, sWorkspaceName, sLayerName = None):

        if sRasterFilePath is None or sRasterFilePath == '' \
                or sWorkspaceName is None or sWorkspaceName == '':
            logging.warning(f"GeoserverService.publishRasterLayer. Raster file path or workspace name not specified")
            return None

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.publishRasterLayer. Adding layer on Geoserver")
            oCoverageStore = oGeoClient.create_coveragestore(layer_name=sLayerName, path=sRasterFilePath, workspace=sWorkspaceName)

            if oCoverageStore is None:
                logging.warning("GeoserverService.publishRasterLayer. Layer not published on Geoserver")
                return None

            logging.info("GeoserverService.publishRasterLayer. Layer published on Geoserver")
            return oCoverageStore

        except Exception as oEx:
            logging.error(f"GeoserverService.publishRasterLayer. Exception {oEx}")


    def deleteRasterLayer(self, sLayerName, sWorkspace):
        if sWorkspace is None or sWorkspace == '' \
                or sLayerName is None or sLayerName == '':
            logging.warning("GeoserverService.deleteRasterLayer. One or more input parameters are empty")
            return None

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.deleteRasterLayer. Delete raster layer on Geoserver")
            print(oGeoClient.delete_coveragestore(coveragestore_name=sLayerName, workspace=sWorkspace))

            return True

        except Exception as oEx:
            logging.error(f"GeoserverService.deleteRasterLayer. Exception {oEx}")

        return False


    def publishShapeLayer(self, sShapeFilePath, sWorkspace, sDataStoreName):

        if sShapeFilePath is None or sShapeFilePath == '' \
                or sWorkspace is None or sWorkspace == '' \
                or sDataStoreName is None or sDataStoreName == '':
            logging.warning("GeoserverService.publishShapeLayer. One or more input parameters are empty")
            return None

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.publishShapeLayer. Adding shape layer on Geoserver")
            oShapeDatastore = oGeoClient.create_shp_datastore(sShapeFilePath, store_name=sDataStoreName, workspace=sWorkspace)

            if oShapeDatastore is None:
                logging.warning("GeoserverService.publishShapeLayer. Shape layer not published in Geoserver")

            logging.info("GeoserverService.publishShapeLayer Shape layer published in Geoserver")
            return oShapeDatastore

        except Exception as oEx:
            logging.error(f"GeoserverService.publishShapeLayer. Exception {oEx}")

        return None

    def deleteShapeLayer(self, sDataStoreName, sWorkspace):

        if sWorkspace is None or sWorkspace == '' \
                or sDataStoreName is None or sDataStoreName == '':
            logging.warning("GeoserverService.deleteShapeLayer. One or more input parameters are empty")
            return None

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.deleteShapeLayer. Delete shape layer on Geoserver")
            print(oGeoClient.delete_featurestore(featurestore_name=sDataStoreName, workspace=sWorkspace))

            return True

        except Exception as oEx:
            logging.error(f"GeoserverService.deleteShapeLayer. Exception {oEx}")

        return False



if __name__ == '__main__':
    oService = GeoserverService()
    sFilePath = "/path/to/tif/file"
    sWorkspaceName = "test_rise"
    sLayerName = "test_layer_rise"
    # oService.publishRasterLayer(sFilePath, sWorkspaceName, sLayerName)


    sLayerName = "test_shape_rise"
    sFilePath = "/path/to/zip/folder"
    # oService.publishShapeLayer(sFilePath, sWorkspaceName, sLayerName)

    oService.deleteShapeLayer(sLayerName, sWorkspaceName)




