# Standard library imports
import time
import json
import os

# Application imports
from ism.core.base_action import BaseAction


class ActionIoFileOutbound(BaseAction):
    """Scan the messages table in the control DB for
    outbound messages and create an outbound file if any found.

    MSG Format:

        CREATE TABLE messages (
            message_id INTEGER NOT NULL PRIMARY KEY, -- Record ID in recipient messages table
            sender TEXT NOT NULL, -- Return address of sender
            sender_id INTEGER NOT NULL, -- Record ID in sender messages table
            action TEXT NOT NULL, -- Name of the action that handles this message
            payload TEXT, -- Json body of msg payload
            sent TEXT NOT NULL, -- Timestamp msg sent by sender
            received TEXT NOT NULL DEFAULT (strftime('%s', 'now')), -- Timestamp ism loaded message into database
            direction TEXT NOT NULL DEFAULT 'inbound', -- In or outbound message
            processed BOOLEAN NOT NULL DEFAULT '0' -- Has the message been processed
        );

    File Name Format:
        <recipient>_<sender_id>.json
    """

    def execute(self):

        if self.active():

            #  Get the directory paths from the properties
            try:
                outbound = str(self.properties['comms']['file'].get('outbound'))
                smp = self.properties['comms']['file']['semaphore_extension']
                msg = self.properties['comms']['file']['message_extension']
            except KeyError as e:
                self.logger.error(f'Failed to read [comms][file] entries from properties. KeyError ({e})')
                raise

            # Query the messages table for outbound messages that aren't 'processed'
            sql = self.dao.prepare_parameterised_statement(
                f'SELECT message_id, recipient, sender, sender_id, action, payload '
                f'FROM messages WHERE processed = ? AND  direction = ?'
            )
            results = self.dao.execute_sql_query(
                sql,
                (
                    0,
                    'outbound'
                )
            )

            if not results:
                return

            # Create the message files in the outbound directory
            for record in results:
                # Create a dict of the values
                send_time = int(time.time())
                message = {
                    "message_id": record[0],
                    "recipient": record[1],
                    "sender": record[2],
                    "sender_id": record[3],
                    "action": record[4],
                    "payload": json.dumps(record[5]),
                    "sent": send_time
                }

                # Create the file
                with open(f'{outbound}{os.path.sep}{record[1]}_{record[3]}{msg}', 'w') as file:
                    file.write(json.dumps(message))
                # Create the semaphore
                with open(f'{outbound}{os.path.sep}{record[1]}_{record[3]}{smp}', 'w') as file:
                    file.write('')

                # Mark the message as processed and update the sent field with timestamp of epoch seconds
                sql = self.dao.prepare_parameterised_statement(
                    'UPDATE messages SET sent = ?, processed = ?'
                )
                self.dao.execute_sql_statement(
                    sql,
                    (
                        send_time,
                        1
                    )
                )
