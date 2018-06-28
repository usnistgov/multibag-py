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
        self.bagdir = os.path.join(self.tempdir, "samplembag")
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

    def test_validate_tag_directory(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_tag_directory()
        self.assertEqual(results.count_applied(), 3)
        self.assertTrue(results.ok())

        os.rename(os.path.join(self.bagdir, "multibag"),
                  os.path.join(self.bagdir, "goober"))
        results = valid8r.validate_tag_directory()
        self.assertEqual(results.count_applied(), 3)
        self.assertEqual(results.count_failed(), 1)
        self.assertTrue(not results.ok())
        
    def test_validate_head_version(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_head_version()
        self.assertEqual(results.count_applied(), 2)
        self.assertTrue(results.ok())

        valid8r.bag.info['Multibag-Head-Version'] = ""
        results = valid8r.validate_head_version()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "3-Head-Version_nonempty")
        self.assertTrue(not results.ok())
        
        valid8r.bag.info['Multibag-Head-Version'] = ["1.0", "2.0"]
        results = valid8r.validate_head_version()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "3-Head-Version_single")
        self.assertTrue(not results.ok())
        
        valid8r.bag.info['Multibag-Head-Version'] = ["1.0", ""]
        results = valid8r.validate_head_version()
        self.assertEqual(results.count_applied(), 2)
        self.assertEqual(results.count_failed(), 2)
        self.assertTrue(not results.ok())
        
        del valid8r.bag.info['Multibag-Head-Version']
        results = valid8r.validate_head_version()
        self.assertEqual(results.count_applied(), 1)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "3-Head-Version")
        self.assertTrue(not results.ok())

    def test_validate_head_deprecates(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_head_deprecates()
        self.assertEqual(results.count_applied(), 0)
        self.assertTrue(results.ok())

        with open(os.path.join(self.bagdir, "bag-info.txt"), "a") as fd:
            fd.write("Multibag-Head-Deprecates: 0.1\n")

        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_head_deprecates()
        self.assertEqual(results.count_applied(), 4)
        self.assertTrue(results.ok())

        with open(os.path.join(self.bagdir, "bag-info.txt"), "a") as fd:
            fd.write("Multibag-Head-Deprecates: 0.2, goober\n")

        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_head_deprecates()
        self.assertEqual(results.count_applied(), 4)
        self.assertTrue(results.ok())

        with open(os.path.join(self.bagdir, "bag-info.txt"), "a") as fd:
            fd.write("Multibag-Head-Deprecates: 0.3, samplembag\n")

        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_head_deprecates()
        self.assertEqual(results.count_applied(), 4)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label,"3-Head-Deprecates_notselfdep")
        self.assertTrue(not results.ok())

        valid8r.bag.info['Multibag-Head-Deprecates'] = ["0.1", "1.0, goober"]
        results = valid8r.validate_head_deprecates()
        self.assertEqual(results.count_applied(), 4)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label,"3-Head-Deprecates_notselfdep")
        self.assertTrue(not results.ok())
        
    def test_validate_baginfo_recs(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_baginfo_recs()
        self.assertEqual(results.count_applied(), 6)
        self.assertTrue(results.ok())

        del valid8r.bag.info['Internal-Sender-Identifier']
        results = valid8r.validate_baginfo_recs()
        self.assertEqual(results.count_applied(), 5)
        self.assertEqual(results.count_failed(), 1)
        self.assertEqual(results.failed()[0].label, "3-2")
        self.assertTrue(not results.ok())

    def test_validate_member_bags(self):
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_member_bags()
        self.assertEqual(results.count_applied(), 7)
        self.assertTrue(results.ok())

        with open(os.path.join(self.bagdir,"multibag/member-bags.tsv"),"a") as fd:
            fd.write("goober\n")
        valid8r = bagv.HeadBagValidator(self.bagdir)
        results = valid8r.validate_member_bags()
        self.assertEqual(results.count_applied(), 7)
        self.assertEqual(results.count_failed(), 1)
        self.assertTrue(not results.ok())
            

        
        

if __name__ == '__main__':
    test.main()
    
