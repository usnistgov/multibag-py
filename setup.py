import os, sys, subprocess, unittest
from setuptools import setup

import tests
print(str(dir(tests)))

setup(name='multibag',
      version='0.1',
      description="multibag: a Python-based reference implementation of the Multibag BagIt profile",
      author="Ray Plante",
      author_email="raymond.plante@nist.gov",
      url='https://github.com/usnistgov/multibag-py',
      scripts=[ ],
      packages=['multibag', 'multibag.access'],
      test_suite="tests.suite",
      test_runner="unittest:TextTestRunner"
)
