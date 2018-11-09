
import unittest
import datetime

import utilities


class TestUtilityFunctions(unittest.TestCase):

    def setUp(self):
        pass

    # Returns TRUE if the date returned is formatted to the expected string
    def test_format_datetime1(self):

        print('Testing date format YYYY-MM-DD')
        # self.assertEqual(1, 2, '1 not = to 1')
        self.assertEqual(utilities.format_datetime(datetime.datetime.now(), 'yyyy-mm-dd'), '2018-11-09')

    # Returns TRUE if the date returned is formatted to the expected string
    def test_format_datetime2(self):
        print('Testing data format yyyy-mm-dd hh:mi:ss')
        # self.assertEqual(1, 2, '1 not = to 1')
        self.assertEqual(utilities.format_datetime(datetime.datetime.now(), 'yyyy-mm-dd hh:mi:ss'), '2018-11-09')

    # Returns TRUE if the date returned is formatted to the expected string
    def test_format_datetime3(self):
        print('Testing data format yyyy-mm-dd hh:mi:ss')
        self.assertIsNone(utilities.format_datetime(None, 'yyyy-mm-dd'))


if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()
