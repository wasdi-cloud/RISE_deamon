import logging

from mailjet_rest import Client

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

    try :
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



