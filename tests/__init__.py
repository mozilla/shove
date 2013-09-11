import json


class CONTAINS(object):
    """Helper object that is equal to any object that contains a specific value."""
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return self.value in other

    def __ne__(self, other):
        return self.value not in other


class JSON(object):
    """Helper object that is equal to any JSON string representing a specific value."""
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        try:
            return self.value == json.loads(other)
        except ValueError:
            return False

    def __ne__(self, other):
        try:
            return self.value != json.loads(other)
        except ValueError:
            return True
