import logging

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
        oMergedShape = gpd.GeoDataFrame(pd.concat(aoShapeDataFrames, ignore_index=True))

        # Save the merged shapefile
        sOutputFullPath = wasdi.getPath(sOutputFileName)
        oMergedShape.to_file(sOutputFullPath)

        wasdi.addFileToWASDI(sOutputFileName, sStyle=sStyle)
    except Exception as oEx:
        logging.error("RiseUtils.mergeShapeFiles. Exception " + str(oEx))
        return False

    return True
