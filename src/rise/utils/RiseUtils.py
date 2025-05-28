import glob
import logging
import os

from mailjet_rest import Client

from datetime import datetime, timedelta, timezone

import wasdi
import geopandas as gpd
import pandas as pd

def getClass(sClassName):
    asParts = sClassName.split('.')
    oModule = ".".join(asParts[:-1])
    oType = __import__(oModule)
    for sComponent in asParts[1:]:
        oType = getattr(oType, sComponent)
    return oType


def isNoneOrEmpty(sString):
    return sString is None or sString == ''


def sendEmailMailJet(oRiseConfig, sSender, sRecipient, sTitle, sMessage, bAddAdminToRecipient):

    if oRiseConfig is None:
        logging.warning("RiseUtils.sendEmailMailJet. Configuration is none. Mail not sent")
        return False

    if isNoneOrEmpty(sSender) or isNoneOrEmpty(sRecipient):
        logging.warning("RiseUtils.sendEmailMailJet. Sender or recipient of the mail not specified. Mail not sent")
        return False

    if bAddAdminToRecipient is None:
        bAddAdminToRecipient = False

    aoRecipients = list()
    aoRecipients.append(_getJetmailUserObject(sRecipient))

    if bAddAdminToRecipient:
        sRiseAdminMail = oRiseConfig.notifications.riseAdminMail
        aoRecipients.append(_getJetmailUserObject(sRiseAdminMail))

    oSender = _getJetmailUserObject(sSender)

    oMessage = {
        'Messages': [
            {
                "From": oSender,
                "To": aoRecipients,
                "Subject": sTitle,
                "HTMLPart": sMessage
            }
        ]
    }

    try:
        sApiKey = oRiseConfig.notifications.mailJetUser
        sApiSecret = oRiseConfig.notifications.mailJetPassword
        oMailjetService = Client(auth=(sApiKey, sApiSecret), version='v3.1')
        oMailjetService.send.create(data=oMessage)

        return True

    except Exception as oEx:
        logging.error(f"RiseUtils.sendEmailMailJet. Exception {oEx}")

    return False


def _getJetmailUserObject(sEmail):
    if isNoneOrEmpty(sEmail):
        return None

    return {
        "Email": sEmail,
        "Name": sEmail
    }


def getTimestampBackInDays(iDaysCount):
    """
    Calculates the timestamp (in seconds) for the date that is a given number of days before the current date and time.
    :iDaysCount: number of days back in time
    :return: Calculates the timestamp (in seconds) representing the date that is iDaysCount days in the past from the
    current date and time.
    """

    if iDaysCount < 0:
        logging.error("RiseUtils.getTimestampBackInDays. Number of days should be a positive number")
        return -1

    oUTCNow = datetime.now(timezone.utc)  # Current UTC datetime
    oUTCPast = oUTCNow - timedelta(days=iDaysCount)  # go back in time of the specified amount of days
    return int(oUTCPast.timestamp())


def mergeShapeFiles(asShapeFiles, sOutputFileName, sStyle=""):

    try:
        asFullPaths = []

        for sFile in asShapeFiles:
            asFullPaths.append(wasdi.getPath(sFile))

        # Read and merge shapefiles
        aoShapeDataFrames = [gpd.read_file(sShapeFullPath) for sShapeFullPath in asFullPaths]

        for i in range(len(aoShapeDataFrames)):
            aoShapeDataFrames[i]["ID"] = aoShapeDataFrames[i]["ID"].astype(str)        
        
        oMergedShape = gpd.GeoDataFrame(pd.concat(aoShapeDataFrames, ignore_index=True))

        # Ensure the CRS is set to EPSG:4326
        oMergedShape = oMergedShape.set_crs("EPSG:4326", allow_override=True)

        # Clean the file from wasdi, if it exists
        if wasdi.fileExistsOnWasdi(sOutputFileName):
            wasdi.deleteProduct(sOutputFileName)

        # Now get the full path
        sOutputFullPath = wasdi.getPath(sOutputFileName)

        # And clean also the local copy if exists:
        deleteShapeFile(sOutputFullPath);

        # Save the merged shapefile
        oMergedShape.to_file(sOutputFullPath)

        #Re - add the file to wasdi: note the upload of shape is not working in reality
        wasdi.addFileToWASDI(sOutputFileName, sStyle=sStyle)
    except Exception as oEx:
        logging.error("RiseUtils.mergeShapeFiles. Exception " + str(oEx))
        return False

    return True

def mergeShapeFiles2(asShapeFiles, sOutputFileName):

    try:
        asFullPaths = []

        for sFile in asShapeFiles:
            asFullPaths.append(sFile)

        # Read and merge shapefiles
        aoShapeDataFrames = [gpd.read_file(sShapeFullPath) for sShapeFullPath in asFullPaths]

        for i in range(len(aoShapeDataFrames)):
            aoShapeDataFrames[i]["ID"] = aoShapeDataFrames[i]["ID"].astype(str)        
        
        oMergedShape = gpd.GeoDataFrame(pd.concat(aoShapeDataFrames, ignore_index=True))

        # Ensure the CRS is set to EPSG:4326
        oMergedShape = oMergedShape.set_crs("EPSG:4326", allow_override=True)

        # Save the merged shapefile
        oMergedShape.to_file(sOutputFileName)

    except Exception as oEx:
        logging.error("RiseUtils.mergeShapeFiles. Exception " + str(oEx))
        return False

    return True

def deleteShapeFile(sShapeFileFullPath):
    """
    Delete a shapefile and its associated files (e.g., .shx, .dbf) from the filesystem.
    :param sShapeFileFullPath: Full path to the shapefile (without extension).
    """
    try:
        asFiles = glob.glob(sShapeFileFullPath.replace(".shp","*"))
        
        for sFile in asFiles:
            os.remove(sFile)

    except Exception as oEx:
        logging.error("RiseUtils.deleteShapeFile. Exception " + str(oEx))