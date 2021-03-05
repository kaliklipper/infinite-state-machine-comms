"""This module tests the file ism_comms.file action pack for the python state machine.


"""

# Standard library imports
import json
import os
import unittest
import yaml

# Local application imports
from time import sleep
from ism.ISM import ISM


class TestIsmIoFile(unittest.TestCase):

    path_sep = os.path.sep
    dir = os.path.dirname(os.path.abspath(__file__))
    sqlite3_properties = f'{dir}{path_sep}resources{path_sep}sqlite3_properties.yaml'
    mysql_properties = f'{dir}{path_sep}resources{path_sep}mysql_properties.yaml'

    @staticmethod
    def get_properties(properties_file) -> dict:
        """Read in the properties file"""
        with open(properties_file, 'r') as file:
            return yaml.safe_load(file)

    @staticmethod
    def send_test_support_msg(msg, inbound):

        sender_id = msg['payload']['sender_id']

        if not os.path.exists(inbound):
            os.makedirs(inbound)

        with open(f'{inbound}{os.path.sep}{sender_id}.json', 'w') as message:
            message.write(json.dumps(msg))
        with open(f'{inbound}{os.path.sep}{sender_id}.smp', 'w') as semaphore:
            semaphore.write('')

    @staticmethod
    def wait_for_test_message_reply(sender_id, outbound, retries=10) -> bool:
        """Wait for an expected reply to a test support message"""

        expected_file = f'{outbound}{os.path.sep}{sender_id}.json'

        while retries > 0:
            if os.path.exists(expected_file):
                break
            retries -= 1
            sleep(1)

        if retries == 0:
            return False

        return True

    def test_action_import_comms_file_actions(self):
        """Test that the ism imports the file based comms actions.

        Use the test support package from the ISM to query that the
        action table contains entries for them.
        """

        inbound = self.get_properties(self.mysql_properties)['test']['support']['inbound']
        outbound = self.get_properties(self.mysql_properties)['test']['support']['outbound']

        # Create an instance of the state machine
        args = {
            'properties_file': self.sqlite3_properties
        }
        ism = ISM(args)

        # Import the actions and also the test support actions
        ism.import_action_pack('ism.tests.support')
        ism.import_action_pack('ism_comms.file.actions')

        # Start the ISM
        ism.start()

        # Drop a test message into the support pack's inbound directory.
        message = {
            "action": "ActionRunSqlQuery",
            "payload": {
                "sql": "SELECT execution_phase, active FROM actions WHERE action = 'ActionIoFileBefore';",
                "sender_id": 1
            }
        }
        self.send_test_support_msg(message, inbound)

        # Wait for the reply
        self.assertTrue(
            self.wait_for_test_message_reply(message['payload']['sender_id'], outbound),
            'Failed to find expected reply to test support message.'
        )

        # End the test
        ism.stop()

    def test_action_import_mysql(self):
        """Test that the ism imports the core actions and runs in the background
        as a daemon.

        """
        args = {
            'properties_file': self.mysql_properties,
            'database': {
                'password': 'wbA7C2B6R7'
            }
        }
        ism = ISM(args)
        self.assertEqual('STARTING', ism.get_execution_phase())
        ism.start()
        sleep(1)
        ism.stop()
        sleep(1)
        self.assertEqual('RUNNING', ism.get_execution_phase())
