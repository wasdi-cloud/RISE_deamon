import logging
import os

from src.rise.geoserver.GeoserverClient import GeoserverClient
from src.rise.utils import RiseUtils


class GeoserverService:

    def getWorkspace(self, sWorkspaceName):
        """
        Retrieve the workspace, given its name
        :param sWorkspaceName: the workspace name
        :return: the workspace with the given name, None if the workspace was not found
        """

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

    def createWorkspace(self, sWorkspaceName):
        """
        Create a workspace with a given name
        :param sWorkspaceName: the name
        :return: True if the workspace has been correctly created, False otherwise
        """

        if sWorkspaceName is None or sWorkspaceName == '':
            logging.warning("GeoserverService.createWorkspace. Workspace name is null or empty")
            return None
        try:
            oGeoClient = GeoserverClient().client
            oGeoClient.create_workspace(sWorkspaceName)

            # to make sure that the workspace has been correctly created, we try to retrieve it after creation
            oWorkspace = oGeoClient.get_workspace(sWorkspaceName)

            return oWorkspace is not None
        except Exception as oEx:
            logging.error(f"GeoserverService.createWorkspace. Exception {oEx}")

        return False

    def publishRasterLayer(self, sRasterFilePath, sWorkspaceName, sLayerName, sStyleName = None):
        """
        Publish a raster layer on Geoserver
        :param sRasterFilePath: path to the raster file
        :param sWorkspaceName: name of the workspace in Geoserver
        :param sLayerName: the name of the layer
        :return: the newly created storage
        """

        if sRasterFilePath is None or sRasterFilePath == '' \
                or sWorkspaceName is None or sWorkspaceName == '':
            logging.warning(f"GeoserverService.publishRasterLayer. Raster file path or workspace name not specified")
            return None

        if not os.path.exists(sRasterFilePath):
            logging.warning(f"GeoserverService.publishRasterLayer. Path does not exist: {sRasterFilePath}")
            return None

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.publishRasterLayer. Adding layer on Geoserver " + sLayerName)
            oCoverageStore = oGeoClient.create_coveragestore(layer_name=sLayerName, path=sRasterFilePath, workspace=sWorkspaceName)

            if oCoverageStore is None:
                logging.warning("GeoserverService.publishRasterLayer. Layer not published on Geoserver")
                return None

            logging.debug("GeoserverService.publishRasterLayer. Layer published on Geoserver")

            bStyleResult = self.publishStyle(sStyleName, sLayerName, sWorkspaceName)

            if not bStyleResult:
                logging.warning("GeoserverService.publishRasterLayer. No style added to the layer")

            return oCoverageStore

        except Exception as oEx:
            logging.error(f"GeoserverService.publishRasterLayer. Exception {oEx}")


    def deleteRasterLayer(self, sLayerName, sWorkspace):
        """
        Delete the raster layer and the corresponding storage from Geoserver
        :param sLayerName: the name of the layer to delete
        :param sWorkspace: the name of the workspace on Geoserver
        :return: True if the layer was successfully deleted, False otherwise
        """
        if sWorkspace is None or sWorkspace == '' \
                or sLayerName is None or sLayerName == '':
            logging.warning("GeoserverService.deleteRasterLayer. One or more input parameters are empty")
            return False

        try:
            oGeoClient = GeoserverClient().client
            logging.debug("GeoserverService.deleteRasterLayer. Delete raster layer on Geoserver")
            oResult = oGeoClient.delete_coveragestore(coveragestore_name=sLayerName, workspace=sWorkspace)
            # TODO: improve error handling
            logging.info(f"GeoserverService.deleteRasterLayer. Result of the deletion {oResult}")
            return True

        except Exception as oEx:
            logging.error(f"GeoserverService.deleteRasterLayer. Exception {oEx}")

        return False


    def publishShapeLayer(self, sShapeFilePath, sWorkspace, sDataStoreName, sStyleName=None):
        """
        Publish a layer starting from a shapefile in Geoserver
        :param sShapeFilePath: the path to the shapefile
        :param sWorkspace: the name of the workspace
        :param sDataStoreName: the name of the datastore
        :param sStyleName: the style for the layer
        :return: the newly created storage
        """

        if sShapeFilePath is None or sShapeFilePath == '' \
                or sWorkspace is None or sWorkspace == '' \
                or sDataStoreName is None or sDataStoreName == '':
            logging.warning("GeoserverService.publishShapeLayer. One or more input parameters are empty")
            return None

        if not sShapeFilePath.endswith(".zip"):
            logging.warning(f"GeoserverService.publishShapeLayer. Not a zip file: {sShapeFilePath}")
            return None

        if not os.path.exists(sShapeFilePath):
            logging.warning(f"GeoserverService.publishRasterLayer. Path does not exist: {sShapeFilePath}")
            return None

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.publishShapeLayer. Adding shape layer on Geoserver")
            oShapeDatastore = oGeoClient.create_shp_datastore(sShapeFilePath, store_name=sDataStoreName, workspace=sWorkspace)

            if oShapeDatastore is None:
                logging.warning("GeoserverService.publishShapeLayer. Shape layer not published in Geoserver")
                return None

            logging.info("GeoserverService.publishShapeLayer Shape layer published in Geoserver")

            if sStyleName is not None:
                sFileNameWithoutExtension = os.path.basename(sShapeFilePath)[:-4]
                bStyleResult = self.publishStyle(sStyleName, sLayerName=sFileNameWithoutExtension, sWorkspaceName=sWorkspace)

                if not bStyleResult:
                    logging.warning("GeoserverService.publishShapeLayer. There was an error adding the style to the layer")

            return oShapeDatastore

        except Exception as oEx:
            logging.error(f"GeoserverService.publishShapeLayer. Exception {oEx}")

        return None

    def deleteShapeLayer(self, sDataStoreName, sWorkspace):
        """
        Delete a vector layer from Geoserver
        :param sDataStoreName: the name of the datastore
        :param sWorkspace: the name of the workspace
        :return: True if the layer was correctly removed from Geoserver, False otherwise
        """

        if sWorkspace is None or sWorkspace == '' \
                or sDataStoreName is None or sDataStoreName == '':
            logging.warning("GeoserverService.deleteShapeLayer. One or more input parameters are empty")
            return False

        try:
            oGeoClient = GeoserverClient().client
            logging.info("GeoserverService.deleteShapeLayer. Delete shape layer on Geoserver")
            # TODO: improve the error handling
            oResult = oGeoClient.delete_featurestore(featurestore_name=sDataStoreName, workspace=sWorkspace)
            logging.info(f"GeoserverService.deleteShapeLayer. Deletion of the feature store: {oResult}")

            return True
        except Exception as oEx:
            logging.error(f"GeoserverService.deleteShapeLayer. Exception {oEx}")

        return False


    def deleteLayer(self, sLayerId):
        """
        :param sLayerId: layer id, following the pattern "workspace_id:storage_id"
        :return: True if the layer has been deleted successfully, False otherwise
        """
        if RiseUtils.isNoneOrEmpty(sLayerId):
            logging.warning(f"GeoserverService.deleteLayer. Layer id not specified")
            return False

        try:
            asLayerSplit = sLayerId.split(":")

            if len(asLayerSplit) == 2:
                sWorkspaceName = asLayerSplit[0]
                sDatastoreName = asLayerSplit[1]
            else:
                logging.warning(f"GeoserverService.deleteLayer: unrecognised pattern for layer id {sLayerId}")
                return False

            oGeoClient = GeoserverClient().client
            oLayer = oGeoClient.get_layer(layer_name=sLayerId)

            if 'layer' not in oLayer:
                logging.warning(f"GeoserverService.deleteLayer: object does not look like a layer {oLayer}")
                return False

            oLayerInfo = oLayer.get('layer')
            if 'type' not in oLayerInfo:
                logging.warning(
                    f"GeoserverService.deleteLayer. Impossible to identify the type of layer in {oLayerInfo}")
                return False

            sType = oLayerInfo.get('type')
            logging.debug(f"GeoserverService.deleteLayer: Type of layer: {sType}")

            if sType == 'VECTOR':
                return self.deleteShapeLayer(sDatastoreName, sWorkspaceName)
            elif sType == 'RASTER':
                return self.deleteRasterLayer(sDatastoreName, sWorkspaceName)

            else:
                logging.info(f"GeoserverService.deleteLayer. Unknown layer type")

        except Exception as oEx:
            logging.error(f"GeoserverService.deleteLayer. Exception {oEx}")

        return False


    def listAllLayers(self, sWorkspace):
        """
        List all the layers published in a given workspace
        :param sWorkspace: the name of the workspace
        :return: the list of layers in a given workspace
        """
        try:
            oGeoClient = GeoserverClient().client
            oLayers = oGeoClient.get_layers(workspace=sWorkspace)
            return oLayers

        except Exception as oEx:
            logging.error(f"GeoserverService.listAllLayers. Exception {oEx}")
        return None

    def existsLayer(self, sLayerName):
        """
        Check the existance of a layer in Geoserver
        :param sLayerName: the name of the layer
        :return: True if the layer with the existing name exists in Geoserver, False otherwise
        """

        if sLayerName is None or sLayerName == '':
            logging.warning("GeoserverService.existsLayer. Layer name not specified")
            return None

        try:
            oGeoClient = GeoserverClient().client
            oLayer = oGeoClient.get_layer(layer_name=sLayerName)

            return oLayer is not None

        except Exception as oEx:
            logging.debug(f"GeoserverService.existsLayer. Exception {oEx}")

        return None

    def publishStyle(self, sStyleName, sLayerName, sWorkspaceName):
        # TODO: so far we assume that the style is already in geoserver. We should handle the case where the style it is not there
        """
        Add a style to a layer
        :param sStyleName: the style name (should be already in Geoserver)
        :param sLayerName: the name of the layer
        :param sWorkspaceName: the name of the workspace
        :return: True if the style was added successfully, False otherwise
        """

        if sStyleName is None:
            logging.warning(f"GeoserverService.publishStyle. Style name not specified")
            return None

        if sLayerName is None:
            logging.warning(f"GeoserverService.publishStyle. Layer name not specified")
            return None

        if sWorkspaceName is None:
            logging.warning(f"GeoserverService.publishStyle. Workspace name not specified")
            return None

        try:
            oGeoClient = GeoserverClient().client
            iStatusCode = oGeoClient.publish_style(layer_name=sLayerName, style_name=sStyleName, workspace=sWorkspaceName)

            if 200 <= iStatusCode <= 299:
                return True

            return False

        except Exception as oEx:
            logging.error(f"GeoserverService.publishStyle. Exception {oEx}")

        return False


if __name__ == '__main__':
    oService = GeoserverService()
    sFilePath = "path/to/TIFF/file"
    sWorkspaceName = "rise_test_workspace"
    sLayerName = "test_layer_to_be_deleted"
    # print(oService.publishRasterLayer(sFilePath, sWorkspaceName, sLayerName))


    sLayerName = "test_shape_rise"
    sFilePath = "/path/to/zip/folder"
    # oService.publishShapeLayer(sFilePath, sWorkspaceName, sLayerName)

    """
    print(oService.getWorkspace(sWorkspaceName))

    for oLayer in oService.listAllLayers(None):
        print(oLayer)
    """

    # oService.deleteLayer("rise_test_workspace:test_layer_to_be_deleted")



