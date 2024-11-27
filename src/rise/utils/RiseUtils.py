import logging

from mailjet_rest import Client

from src.rise.data.LayerRepository import LayerRepository
from src.rise.geoserver.GeoserverService import GeoserverService


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


def cleanLayers():
    try:
        fTimeStamp = 1448755200     #TODO: management of the timestamp still missing

        oLayerRepo = LayerRepository()
        aoLayerEntities = oLayerRepo.getLayersIdsOlderThanDate(fTimeStamp)

        oGeoService = GeoserverService()
        aoDeletedEntitiesIds = []

        for oEntity in aoLayerEntities:
            sLayerId = oEntity.layerId
            print("Layer id: " + sLayerId)

            if isNoneOrEmpty(sLayerId):
                logging.info("RiseUtils.cleanLayers: found an empty layer id")
                continue

            if oGeoService.deleteLayer(sLayerId):
                aoDeletedEntitiesIds.append(oEntity.id)
                logging.info(f"RiseUtils.cleanLayers: layer {sLayerId} has been deleted from Geoserver")

        # to be sure that the Layer entities have not been updated while we were deleting the layers from Geoserver,
        # we reload the entities, before updating them
        aoDeletedLayers = oLayerRepo.getAllEntitiesById(aoDeletedEntitiesIds)
        list(map(lambda oLayer: setattr(oLayer, "published", False), aoDeletedLayers))
        iDeletedLayers = oLayerRepo.updateAllEntities(aoDeletedLayers)
        logging.info(f"RiseUtils.cleanLayers: number of cleaned layers is equal to {iDeletedLayers}")

    except Exception as oEx:
        logging.error(f"RiseUtils.cleanLayers: exception {oEx}")

