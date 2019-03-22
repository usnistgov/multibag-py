# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging, random, math
import tempfile, shutil
import unittest as test

import multibag
import multibag.testing.mkdata as mkdata
import bagit

class TestCreateTestData(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_mkdataset(self):
        datadir = os.path.join(self.tempdir, "datadir")
        self.assertTrue(not os.path.exists(datadir))
        
        mkdata.mkdataset(datadir, totalsize=10000000, filecount=20)

        self.assertTrue(os.path.exists(datadir))
        files = os.listdir(datadir)
        self.assertEqual(len(files), 20)
        self.assertTrue(all([(os.stat(os.path.join(datadir, f)).st_size==500000)
                             for f in files]))

    def test_mkdataset2(self):
        datadir = os.path.join(self.tempdir, "datadir2")
        self.assertTrue(not os.path.exists(datadir))
        
        mkdata.mkdataset(datadir, totalsize=10000000, filecount=20,
                         plan={ 'files': [
                                   { "totalsize": 500000, "totalfiles": 2 }
                                ],
                                'dirs': [
                                   { "totalsize": 500000, "totalfiles": 10 },
                                   { "totalfiles": 90 }
                                ]} )

        self.assertTrue(os.path.exists(datadir))
        files = os.listdir(datadir)
        self.assertEqual(len(files), 4)

        dirs = [f for f in files
                  if os.path.isdir(os.path.join(datadir,f))]
        self.assertEqual(len(dirs), 2)

        files = [f for f in files if f not in dirs]
        self.assertEqual(len(files), 2)
        self.assertTrue(all([(os.stat(os.path.join(datadir, f)).st_size==250000)
                             for f in files]))

        for subdir in dirs:
            subdir = os.path.join(datadir, subdir)
            files = os.listdir(subdir)
            self.assertTrue(len(files) == 10 or len(files) == 90)
            if len(files) == 10:
                self.assertTrue(
                    all([(os.stat(os.path.join(subdir, f)).st_size==50000)
                         for f in files]))
            else:
                self.assertTrue(
                    all([(os.stat(os.path.join(subdir, f)).st_size==100000)
                         for f in files]))
                
    def test_make_bag(self):
        datadir = os.path.join(self.tempdir, "datadir")
        self.assertTrue(not os.path.exists(datadir))
        
        mkdata.mkdataset(datadir, totalsize=10000000, filecount=20)

        self.assertTrue(os.path.exists(datadir))
        files = os.listdir(datadir)
        self.assertEqual(len(files), 20)
        self.assertTrue(not os.path.exists(os.path.join(datadir, "bagit.txt")))

        bagit.make_bag(datadir)
        self.assertTrue(os.path.isfile(os.path.join(datadir, "bagit.txt")))
        self.assertTrue(os.path.isfile(os.path.join(datadir, "bag-info.txt")))
        self.assertTrue(os.path.isdir(os.path.join(datadir, "data")))
        self.assertTrue(os.path.isfile(os.path.join(datadir,
                                                    "manifest-sha256.txt")))
        for f in files:
            self.assertTrue(os.path.isfile(os.path.join(datadir,"data",f)))
        self.assertEqual(len(files), len(os.listdir(os.path.join(datadir,"data"))))

    def test_make_single_bag(self):
        bagdir = os.path.join(self.tempdir, "bagdir")
        mkdata.mkdataset(bagdir, totalsize=10000000, filecount=20)
        bagit.make_bag(bagdir)
        self.assertTrue(not os.path.isdir(os.path.join(bagdir,"multibag")))

        multibag.make_single_multibag(bagdir)
        self.assertTrue(os.path.isdir(os.path.join(bagdir,"multibag")))
        
    def test_split_bag(self):
        bagdir = os.path.join(self.tempdir, "bagdir")
        mkdata.mkdataset(bagdir, totalsize=10000000, filecount=20)
        bagit.make_bag(bagdir)
        self.assertTrue(not os.path.isdir(os.path.join(bagdir,"multibag")))

        bags = multibag.split_bag("mybag", "mymultibag", 10000001)
        self.assertEqual(len(bags), 11)
        self.assertTrue(all([re.match(r'mymultibag_\d+$', os.path.basename(b))
                             for b in bags]))
        self.assertIn(bagdir+"_11", bags)
        self.assertNotIn(bagdir+"_12", bags)
        self.assertNotIn(bagdir+"_0", bags)

        self.assertTrue(all([os.stat(b).st_size < 10000001 for b in bags]))


        
        
        
        

        
if __name__ == '__main__':
    test.main()
