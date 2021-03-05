"""Create the messaging directories before running the messaging actions"""

# Standard library imports
import os
from pathlib import Path

# Application imports
from ism.core.base_action import BaseAction


class ActionIoFileBefore(BaseAction):
    """Create the inbound and outbound directories for the file based messages

    Directories are defined in the properties file.
    If:
        1) The paths are absolute, then they are created as defined.
        2) The paths are relative, then they are created under the run root.
    """

    def execute(self):

        if self.active():

            #  Get the directory paths from the properties file
            try:
                paths = {
                    'inbound': self.properties['comms']['file']['inbound'],
                    'outbound': self.properties['comms']['file']['outbound'],
                    'archive': self.properties['comms']['file']['archive']
                }
            except KeyError as e:
                self.logger.error(f'Failed to read directory entries from properties file. KeyError ({e})')
                raise

            # Are they relative or absolute?
            path_type = 'Abs'
            for path in paths:
                if not os.path.isabs(path):
                    path_type = 'Rel'

            # Create the directories
            try:
                if path_type == 'Rel':
                    run_dir = self.properties["runtime"]["run_dir"]
                    sep = os.path.sep
                    for path in paths:
                        Path(f'{run_dir}{sep}comms{sep}file{sep}{path}').mkdir(parents=True)
                else:
                    for path in paths:
                        Path(path).mkdir(parents=True, exist_ok=True)
            except OSError as err:
                self.logger.error(f'Error creating directory for ({path}). Error message: ({err})')
                raise

            #  Job done so disable this action
            self.deactivate()
