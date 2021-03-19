"""Check to see if any of the comms actions have added an inbound message to the messages table.

If they have then enable the action addressed by the message and set its payload.
"""

# Standard library imports
import json

# Application imports
from ism.core.base_action import BaseAction


class ActionIoCheckMsgTable(BaseAction):

    def execute(self):

        if self.active():

            # Look in the messages table
            sql = self.dao.prepare_parameterised_statement(
                'SELECT message_id, action, payload FROM messages WHERE processed = ?'
            )
            msgs = self.dao.execute_sql_query(
                sql,
                (
                    0,
                )
            )

            for msg in msgs:

                # Update the action's payload
                sql = self.dao.prepare_parameterised_statement(
                    'UPDATE actions SET payload = ? WHERE action = ?'
                )
                self.dao.execute_sql_statement(
                    sql,
                    (
                        json.dumps(msg['payload']),
                        msg['action']
                    )
                )
                # Enable the test action
                self.activate(msg['action'])

                # Mark the message as processed
                sql = self.dao.prepare_parameterised_statement(
                    'UPDATE messages SET processed = ? WHERE message_id = ?'
                )
                self.dao.execute_sql_statement(
                    sql,
                    (
                        True,
                        msg['message_id']
                    )
                )
