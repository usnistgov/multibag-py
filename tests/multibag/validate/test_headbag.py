# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, json
import tempfile, shutil
import unittest as test

import multibag.validate.headbag as bagv
import multibag.validate.base as val
from multibag.access.bagit import open_bag

datadir=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                     "access", "data")

class TestHeadBagValidator(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_validate_version(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_version()
        self.assertEqual(results.count_applied(), 3)
        self.assertTrue(results.ok())
        
        results = valid8r.validate_version(version="0.2")
        self.assertEqual(results.count_applied(), 3)
        self.assertTrue(not results.ok())
        
    def test_validate_reference(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_reference()
        self.assertEqual(results.count_applied(), 3)
        self.assertTrue(results.ok())


        

if __name__ == '__main__':
    test.main()
    

