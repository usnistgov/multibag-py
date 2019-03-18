from __future__ import absolute_import
from unittest import TestLoader, TestSuite

def additional_tests():
    import tests.multibag.access as access
    import tests.multibag.validate as validate
    import tests.multibag.testing as testing
    import tests.multibag as multibag

    suites = [TestLoader().loadTestsFromModule(m[1])
                     for m in globals().items() if m[0].startswith("test_")]
    
    suites.extend( [access.additional_tests(), validate.additional_tests(),
                    multibag.additional_tests(), testing.additional_tests()] )
    return TestSuite(suites)
