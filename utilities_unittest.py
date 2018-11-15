
import unittest
import datetime

import utilities


class TestDatetimeFunctions(unittest.TestCase):

    def setUp(self):
        pass

    # Returns TRUE if the date returned is formatted to the expected string
    def test_format_datetime1(self):
        self.assertEqual(utilities.format_datetime(datetime.datetime.now(), 'yyyy-mm-dd'), datetime.date.today().strftime('%Y-%m-%d'))


    # Returns TRUE if the date returned is formatted to the expected string
    def test_format_datetime2(self):
        self.assertEqual(utilities.format_datetime(datetime.datetime.now(), 'yyyy-mm-dd hh:mi:ss'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


    # Returns TRUE if the date returned is formatted to the expected string
    def test_format_datetime3(self):
        self.assertIsNone(utilities.format_datetime(None, 'yyyy-mm-dd'))


class TestDetectByBom(unittest.TestCase):

    def setUp(self):
        # Create UTF8, UTF16LE and ascii encoded files for testing
        import codecs

        file = codecs.open("testfile_u8s.txt", mode="w", encoding="utf-8-sig")
        file.write('Test File Text.')
        file.close()
        file16 = codecs.open("testfile_u16le.txt", mode="w", encoding="utf-16")
        file16.write('Test File Text.')
        file16.close()
        s = '12345'
        asctext = s.encode('ascii')
        with open('testfile_ascii.txt', 'wb') as f:
            f.write(asctext)


    def test_detect8(self):
        self.assertEqual(utilities.detect_by_bom('testfile_u8s.txt', None), 'utf-8-sig')


    def test_detect16(self):
        self.assertEqual(utilities.detect_by_bom('testfile_u16le.txt', None), 'utf-16')


    def test_detectascii(self):
        self.assertEqual(utilities.detect_by_bom('testfile_ascii.txt', None), 'utf-8')


    def tearDown(self):
        # Delete test files created in setUp
        import os
        os.remove('testfile_u8s.txt')
        os.remove('testfile_u16le.txt')
        os.remove('testfile_ascii.txt')


class TestFetchArraySize(unittest.TestCase):

    # Returns TRUE if the date returned is formatted to the expected string
    def test_fetcharraysize(self):
        self.assertIsInstance(utilities.fetcharraysize(), int)


if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()
