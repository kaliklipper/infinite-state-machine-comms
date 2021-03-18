"""This module tests the file ism_comms.file action pack for the python state machine.


"""

# Standard library imports
import json
import ntpath
import os
import shutil
import unittest
import yaml

# Local application imports
from time import sleep
from ism.ISM import ISM


class TestIsmIoFile(unittest.TestCase):
    """This action pack implements file based IO.

    e.g. Message files in JSON format.
    """
    path_sep = os.path.sep
    dir = os.path.dirname(os.path.abspath(__file__))
    sqlite3_properties = f'{dir}{path_sep}resources{path_sep}sqlite3_properties.yaml'
    mysql_properties = f'{dir}{path_sep}resources{path_sep}mysql_properties.yaml'
    msg1 = f'{dir}{path_sep}resources{path_sep}msg1.json'

    def setUp(self):
        self.properties = self.get_properties(self.mysql_properties)
        self.test_inbound = self.properties['test']['support']['inbound']
        self.test_outbound = self.properties['test']['support']['outbound']
        self.test_archive = self.properties['test']['support']['archive']

    def tearDown(self):
        self.clear_test_files(self.test_inbound)
        self.clear_test_files(self.test_outbound)
        self.clear_test_files(self.test_archive)

    @staticmethod
    def clear_test_files(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    @staticmethod
    def get_properties(properties_file) -> dict:
        """Read in the properties file"""
        with open(properties_file, 'r') as file:
            return yaml.safe_load(file)

    def send_test_support_msg(self, msg):

        sender_id = msg['payload']['sender_id']

        with open(f'{self.test_inbound}{os.path.sep}{sender_id}.json', 'w') as message:
            message.write(json.dumps(msg))
        with open(f'{self.test_inbound}{os.path.sep}{sender_id}.smp', 'w') as semaphore:
            semaphore.write('')

    @staticmethod
    def send_inbound_msg_file(msg_file: str, properties: dict):

        # The action ActionBeforeIoFile probably hasn't run yet so directory not created
        tries = 10
        while not os.path.exists(properties['comms']['file']['inbound']):
            sleep(.01)

        # Copy the file to the inbound dir
        inbound = properties['comms']['file']['inbound']
        head, tail = ntpath.split(msg_file)
        file_name = tail or ntpath.basename(head)
        target = f'{inbound}{os.path.sep}{file_name}'
        shutil.copy(msg_file, target)
        # Create a semaphore file to show write is complete
        with open(f'{inbound}{os.path.sep}{os.path.splitext(file_name)[0]}.smp', 'w') as semaphore:
            semaphore.write('')

    def wait_for_test_message_reply(self, sender_id, retries=10) -> bool:
        """Wait for an expected reply to a test support message"""

        expected_file = f'{self.test_outbound}{os.path.sep}{sender_id}.json'

        while retries > 0:
            if os.path.exists(expected_file):
                break
            retries -= 1
            sleep(1)

        if retries == 0:
            return False

        return True

    def wait_for_message_archive(self, file_name, properties, retries=10):
        """Waiting for the message file to be archived shows that it's been picked up by ActionIoFileInbound"""

        expected_file = f'{properties["comms"]["file"]["archive"]}{os.path.sep}{file_name}.smp'

        while retries > 0:
            if os.path.exists(expected_file):
                break
            retries -= 1
            sleep(1)

        if retries == 0:
            return False

        return True

    def test_import_comms_before_file_actions_sqlite3(self):
        """Test that the ism imports the file based comms actions.

        Use the test support package from the ISM to query that the
        action table contains entries for them. Starting with the setup
        action - ActionBeforeIoFile which should be disabled by the time we
        run our test support query against the DB.
        """

        sender_id = 1

        # Create an instance of the state machine
        args = {
            'properties_file': self.sqlite3_properties
        }
        ism = ISM(args)

        # Import the actions and also the test support actions
        ism.import_action_pack('ism.tests.support')
        ism.import_action_pack('ism_comms.file.actions')

        # Start the ISM as a background daemon so we don't wait for it in the main thread.
        ism.start()

        # Drop a test message into the support pack's inbound directory.
        message = {
            "action": "ActionRunSqlQuery",
            "payload": {
                "sql": "SELECT active FROM actions WHERE action = 'ActionBeforeIoFile'",
                "sender_id": sender_id
            }
        }
        self.send_test_support_msg(message)

        # Wait for the reply. Test support actions run in the RUNNING phase, so the fact that we get back
        # a message is itself proof that the Before action deactivated itself on completion. But we shall assert
        # that it is flagged as inactive and check for the existence of the messaging directories.
        self.assertTrue(
            self.wait_for_test_message_reply(message['payload']['sender_id']),
            'Failed to find expected reply to test support message.'
        )

        # Test if the Before action deactivated itself
        with open(f'{self.test_outbound}{os.path.sep}1.json', 'r') as file:
            action = json.loads(file.read())
            self.assertEqual(0, action['query_result'][0][0],
                             'expected action ActionIoFileBefore to be deactivated')

        # Test if the messaging directories were created
        self.assertTrue(os.path.exists(ism.properties['comms']['file']['inbound']))
        self.assertTrue(os.path.exists(ism.properties['comms']['file']['outbound']))
        self.assertTrue(os.path.exists(ism.properties['comms']['file']['archive']))

        # End the test
        ism.stop()

    def test_import_comms_before_file_actions_mysql(self):
        """Test that the ism imports the file based comms actions.

        Use the test support package from the ISM to query that the
        action table contains entries for them. Starting with the setup
        action - ActionBeforeIoFile which should be disabled by the time we
        run our test support query against the DB.
        """

        sender_id = 2

        # Create an instance of the state machine
        args = {
            'properties_file': self.mysql_properties,
            'database': {
                'password': 'wbA7C2B6R7'
            }
        }
        ism = ISM(args)

        # Import the actions and also the test support actions
        ism.import_action_pack('ism.tests.support')
        ism.import_action_pack('ism_comms.core')
        ism.import_action_pack('ism_comms.file.actions')

        # Start the ISM as a background daemon so we don't wait for it in the main thread.
        ism.start()

        # Drop a test message into the support pack's inbound directory.
        message = {
            "action": "ActionRunSqlQuery",
            "payload": {
                "sql": "SELECT active FROM actions WHERE action = 'ActionBeforeIoFile'",
                "sender_id": sender_id
            }
        }
        self.send_test_support_msg(message)

        # Wait for the reply. Test support actions run in the RUNNING phase, so the fact that we get back
        # a message is itself proof that the Before action deactivated itself on completion. But we shall assert
        # that it is flagged as inactive and check for the existence of the messaging directories.
        self.assertTrue(
            self.wait_for_test_message_reply(message['payload']['sender_id']),
            'Failed to find expected reply to test support message.'
        )

        # Test if the Before action deactivated itself
        with open(f'{self.test_outbound}{os.path.sep}{sender_id}.json', 'r') as file:
            action = json.loads(file.read())
            self.assertEqual(0, action['query_result'][0][0],
                             'expected action ActionBeforeIoFile to be deactivated')

        # Test if the messaging directories were created
        self.assertTrue(os.path.exists(ism.properties['comms']['file']['inbound']))
        self.assertTrue(os.path.exists(ism.properties['comms']['file']['outbound']))
        self.assertTrue(os.path.exists(ism.properties['comms']['file']['archive']))

        # End the test
        ism.stop()

    def test_inbound_msg_file_sqlite3(self):
        """Test that the ism comms action ActionIoFileInbound picks up a new message.

        Action should insert the message into the messages table and then archive the
        message file before activating the action addressed in the message.
        """

        sender_id = 3

        # Create an instance of the state machine
        args = {
            'properties_file': self.sqlite3_properties
        }
        ism = ISM(args)

        # Import the actions and also the test support actions
        ism.import_action_pack('ism.tests.support')
        ism.import_action_pack('ism_comms.file.actions')

        # Start the ISM as a background daemon so we don't wait for it in the main thread.
        ism.start()

        # Drop a message file into the messaging inbound directory
        self.send_inbound_msg_file(self.msg1, ism.properties)

        # Message is written to DB before msg file is archived. So good to wait for it to be moved
        # before sending the database query.
        self.wait_for_message_archive('msg1', ism.properties)

        # Use the test support pack to query if the message is added to the DB
        message = {
            "action": "ActionRunSqlQuery",
            "payload": {
                "sql": "SELECT * FROM messages",
                "sender_id": sender_id
            }
        }
        self.send_test_support_msg(message)

        # Wait for the reply.
        self.assertTrue(
            self.wait_for_test_message_reply(sender_id),
            'Failed to find expected reply to test support message.'
        )

        with open(f'{self.test_outbound}{os.path.sep}{sender_id}.json', 'r') as file:
            result = json.loads(file.read()).get('query_result', {})

        if result[0]:
            self.assertEqual('test_inbound_msg_file', result[0][1])

        ism.stop()


if __name__ == '__main__':
    unittest.main()
