from enum import Enum

class PaymentType(Enum):

    MONTH = "MONTH"
    YEAR = "YEAR"

    def __init__(self, sValue):
        self._value = sValue

    def getString(self):
        return self._value

    @staticmethod
    def isValid(sValue):
        if sValue is None or sValue == '':
            return False

        if sValue == PaymentType.MONTH.getString():
            return True

        if sValue == PaymentType.YEAR.getString():
            return True

        return False
