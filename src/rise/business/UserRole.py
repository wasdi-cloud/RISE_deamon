from enum import Enum
from src.rise.business.User import User

class UserRole(Enum):

    RISE_ADMIN = "RISE_ADMIN"
    ADMIN = "ADMIN"
    HQ = "HQ"
    FIELD = "FIELD"

    def __init__(self, sValue):
        self._value = sValue

    def getString(self):
        return self._value

    @staticmethod
    def isValid(sValue):
        if sValue is None or sValue == '':
            return False

        if sValue == UserRole.RISE_ADMIN.getString():
            return True

        if sValue == UserRole.ADMIN.getString():
            return True

        if sValue == UserRole.HQ.getString():
            return True

        if sValue == UserRole.FIELD.getString():
            return True

        return False

    @staticmethod
    def isAdmin(oUser):
        if oUser is None or not isinstance(oUser, User):
            return False

        if not UserRole.isValid(oUser.role.getString()):
            return False

        if oUser.role == UserRole.ADMIN:
            return True

        return False

    @staticmethod
    def isRiseAdmin(oUser):
        if oUser is None or not isinstance(oUser, User):
            return False

        if not UserRole.isValid(oUser.role.getString()):
            return False

        if oUser.role == UserRole.RISE_ADMIN:
            return True

        return False


