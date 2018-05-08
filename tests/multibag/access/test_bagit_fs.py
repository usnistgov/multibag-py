# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import unittest as test

import multibag.access.bagit as bagit

logging.basicConfig(filename='test.log', level=logging.DEBUG)

# But we do want any exceptions raised in the logging path to be raised:
logging.raiseExceptions = True

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")

import fs.osfs

class TestPath(test.TestCase):

    def setUp(self):
        self.fs = fs.osfs.OSFS(datadir)
        self.file = "samplembag"
        self.path = bagit.Path(self.fs, self.file, "testdata:")

    def test_ctor(self):
        self.assertIs(self.path.fs, self.fs)
        self.assertEqual(self.path.path, "samplembag")
        self.assertEqual(self.path._pfx, "testdata:")

    def test_str(self):
        self.assertEqual(str(self.path), "testdata:samplembag")

    def test_repr(self):
        self.assertEqual(repr(self.path), repr(self.path.fs)+':samplembag')

    def test_filetests(self):
        path = self.path
        self.assertTrue(path.exists())
        self.assertTrue(path.isdir())
        self.assertFalse(path.isfile())

        path = bagit.Path(self.fs, self.file+'/bagit.txt', "testdata:")
        self.assertTrue(path.exists())
        self.assertTrue(not path.isdir())
        self.assertFalse(not path.isfile())

        path = bagit.Path(self.fs, self.file+'/goober', "testdata:")
        self.assertFalse(path.exists())
        self.assertFalse(path.isdir())
        self.assertFalse(path.isfile())

    def test_relpath(self):
        subpath = self.path.relpath("bagit.txt")
        self.assertEqual(str(subpath), "testdata:samplembag/bagit.txt")

        path = bagit.Path(fs.osfs.OSFS(os.path.join(datadir,"samplembag","data")), "", "payload:")

        subpath = path.relpath("trial1.json")
        self.assertEqual(str(subpath), "payload:trial1.json")
        self.assertTrue(subpath.isfile())

        subpath = path.relpath("trial3")
        self.assertEqual(str(subpath), "payload:trial3")
        self.assertTrue(subpath.isdir())

        subpath = path.relpath("goober")
        self.assertEqual(str(subpath), "payload:goober")
        self.assertTrue(not subpath.exists())

    def test_subfspath(self):
        self.assertEqual(str(self.path), "testdata:samplembag")

        subpath = self.path.subfspath()
        self.assertEqual(subpath.path, "")
        self.assertEqual(str(subpath), "testdata:samplembag/")
        self.assertIn("bagit.txt", subpath.fs.listdir("."))
        self.assertTrue(subpath.fs.isdir("data"))

        subpath = self.path.subfspath("data")
        self.assertEqual(subpath.path, "")
        self.assertEqual(str(subpath), "testdata:samplembag/data/")
        self.assertIn("trial1.json", subpath.fs.listdir("."))
        self.assertTrue(subpath.fs.isdir("trial3"))

        subpath = subpath.subfspath("trial3")
        self.assertEqual(subpath.path, "")
        self.assertEqual(str(subpath), "testdata:samplembag/data/trial3/")
        self.assertIn("trial3a.json", subpath.fs.listdir("."))


class TestReadonlyBag(test.TestCase):

    def setUp(self):
        self.root = os.path.join(datadir, "samplebag")
        self.bag = bagit.ReadOnlyBag(self.root)






if __name__ == '__main__':
    test.main()
