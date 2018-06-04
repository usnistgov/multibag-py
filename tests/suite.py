from unittest import TestLoader, TestSuite
# import tests.multibag as multibag
import tests.multibag.access as access
import tests.multibag as multibag

def additional_tests():
    suites = [access.additional_tests(), multibag.additional_tests()]
    return TestSuite(suites)
