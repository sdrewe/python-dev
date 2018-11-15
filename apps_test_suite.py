import unittest
import utilities_unittest

suite_loader = unittest.TestLoader()
suite1 = suite_loader.loadTestsFromTestCase(utilities_unittest.TestDatetimeFunctions)
suite2 = suite_loader.loadTestsFromTestCase(utilities_unittest.TestDetectByBom)
suite3 = suite_loader.loadTestsFromTestCase(utilities_unittest.TestFetchArraySize)
suite = unittest.TestSuite([suite1, suite2, suite3])
unittest.TextTestRunner(verbosity=2).run(suite)
