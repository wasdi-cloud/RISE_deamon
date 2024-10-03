from src.rise.business.RiseEntity import RiseEntity


class User(RiseEntity):

    def __init__(self, **kwargs):
        self.userId = str()
        self.email = str()
        self.name = str()
        self.surname = str()
        self.mobile = str()
        self.role = str()
        self.registrationDate = float()
        self.confirmationDate = float()
        self.acceptedTermsAndConditions = False
        self.termsAndConditionAcceptedDate = float()
        self.acceptedPrivacy = False
        self.privacyAcceptedDate = float()
        self.lastPasswordUpdateDate = float()
        self.lastLoginDate = float()
        self.lastResetPasswordRequest = float()
        self.notifyNewsletter = False
        self.notifyMaintenance = False
        self.notifyActivities = False
        self.defaultLanguage = str()
        self.organizationId = str()
        self.confirmationCode = str()
        self.password = str()

        for key, value in kwargs.items():
            setattr(self, key, value)
