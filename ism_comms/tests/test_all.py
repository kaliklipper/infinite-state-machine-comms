
# Standard library imports
import unittest

# Local application imports
from ism_comms.tests.test_ism_io_file import TestIsmIoFile


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(TestIsmIoFile('test_import_comms_before_file_actions_sqlite3'))
    test_suite.addTest(TestIsmIoFile('test_import_comms_before_file_actions_mysql'))
    test_suite.addTest(TestIsmIoFile('test_inbound_msg_file_sqlite3'))

    return test_suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
