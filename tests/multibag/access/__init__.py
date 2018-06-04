from unittest import TestLoader, TestSuite

from . import test_bagit_imported, test_bagit_fs

def additional_tests():
    suites = [TestLoader().loadTestsFromModule(m[1])
                     for m in globals().items() if m[0].startswith("test_")]
    return TestSuite(suites)
