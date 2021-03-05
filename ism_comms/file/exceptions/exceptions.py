"""Custom Exceptions for the state machine ism_comms.file actions"""


class MissingPropertyKey(EnvironmentError):

    def __init(self, message='Property not found in properties file:'):
        self.message = message
        super().__init__(self.message)
