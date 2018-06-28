# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, json
import tempfile, shutil
import unittest as test

import multibag.validate.member as bagv
import multibag.validate.base as val
from multibag.access.bagit import open_bag

datadir=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                     "access", "data")

class TestMemberBagValidator(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)


    def test_validate_bagname(self):
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertTrue(results.ok())

        newname = os.path.join(os.path.dirname(self.bagdir), "sample\tbag ")
        os.rename(self.bagdir, newname)
        self.bagdir = newname
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 2)
        self.assertTrue(not results.ok())

        newname = os.path.join(os.path.dirname(self.bagdir), " samplebag")
        os.rename(self.bagdir, newname)
        self.bagdir = newname
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "2.1b-name-wsp")
        self.assertTrue(not results.ok())

        newname = os.path.join(os.path.dirname(self.bagdir), "sample\tbag")
        os.rename(self.bagdir, newname)
        self.bagdir = newname
        valid8r = bagv.MemberBagValidator(self.bagdir)
        results = valid8r.validate_bagname()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "2.1a-name-TAB")
        self.assertTrue(not results.ok())


        

if __name__ == '__main__':
    test.main()
    

