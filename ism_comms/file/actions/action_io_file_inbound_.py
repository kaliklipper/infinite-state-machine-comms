"""Action finds and loads any inbound message files, then archives them"""

# Standard library imports
import json
import os

# Application imports
from ism.core.base_action import BaseAction
from ism.exceptions.exceptions import OrphanedSemaphoreFile


class ActionIoFileInbound(BaseAction):
    """Scan the inbound message directory and read any
    found messages into the database messages table.

    MSG Format:

    CREATE TABLE messages (
        message_id INTEGER NOT NULL PRIMARY KEY, -- Record ID in recipient messages table
        sender TEXT NOT NULL, -- Return address of sender
        sender_id INTEGER NOT NULL, -- Record ID in sender messages table
        recipient TEXT NOT NULL, -- Address of recipient
        action TEXT NOT NULL, -- Name of the action that handles this message
        payload TEXT, -- Json body of msg payload
        sent TEXT NOT NULL, -- Timestamp msg sent by sender
        received TEXT NOT NULL DEFAULT (strftime('%s', 'now')), -- Timestamp ism loaded message into database
        direction TEXT NOT NULL DEFAULT 'inbound', -- In or outbound message
        processed BOOLEAN NOT NULL DEFAULT '0' -- Has the message been processed
    );

    """

    def execute(self):

        if self.active():

            #  Get the directory paths from the properties
            try:
                inbound = str(self.properties['comms']['file'].get('inbound'))
                archive = self.properties['comms']['file']['archive']
                smp = self.properties['comms']['file']['semaphore_extension']
                msg = self.properties['comms']['file']['message_extension']
            except KeyError as e:
                self.logger.error(f'Failed to read [comms][file] entries from properties. KeyError ({e})')
                raise

            # Are there any inbound files?
            for file in os.listdir(inbound):
                if file.endswith(smp):
                    file_name = os.path.splitext(file)[0]
                    smp_file = f'{inbound}{os.path.sep}{file_name}{smp}'
                    msg_file = f'{inbound}{os.path.sep}{file_name}{msg}'
                    if not os.path.exists(msg_file):
                        raise OrphanedSemaphoreFile(f'Semaphore file ({file}) without associated message file.')

                    # Message file found so read it into the DB test messages table
                    with open(msg_file, 'r') as message_file:
                        message = json.loads(message_file.read())
                        sql = self.dao.prepare_parameterised_statement(
                            f'INSERT INTO messages '
                            f'(message_id, sender, sender_id, action, payload, sent)'
                            f' VALUES (?, ?, ?, ?, ?, ?)'
                        )
                        self.dao.execute_sql_statement(
                            sql,
                            (
                                message['message_id'],
                                message['sender'],
                                message['sender_id'],
                                message['action'],
                                json.dumps(message['payload']),
                                message['sent']
                            )
                        )

                    # Archive the file so we don't process it again
                    destination_path = f'{archive}{os.path.sep}{file_name}'
                    os.rename(msg_file, f'{destination_path}{msg}')
                    os.rename(smp_file, f'{destination_path}{smp}')
