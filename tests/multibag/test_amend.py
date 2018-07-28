# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test
from functools import cmp_to_key

import multibag.amend as amend
import multibag.access.bagit as bagit
import multibag.validate as valid8
import tests.multibag.access.mkdata as mkdata

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "access", "data")

class TestSingleMutlibagMaker(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
        shutil.rmtree(os.path.join(self.bagdir, "multibag"))
        bag = bagit.Bag(self.bagdir)
        rmtag = []
        for tag in bag.info:
            if tag.startswith('Multibag-'):
                rmtag.append(tag)
        for tag in rmtag:
            del bag.info[tag]
        bag.save()
                
        self.mkr = amend.SingleMultibagMaker(self.bagdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        self.assertEqual(self.mkr.bagdir, self.bagdir)
        self.assertEqual(self.mkr.tagdir, "multibag")

        self.mkr = amend.SingleMultibagMaker(self.bagdir, "goober")
        self.assertEqual(self.mkr.tagdir, "goober")

    def test_format_bytes(self):
        self.assertEqual(self.mkr._format_bytes(108), "108 B")
        self.assertEqual(self.mkr._format_bytes(34569), "34.57 kB")
        self.assertEqual(self.mkr._format_bytes(9834569), "9.835 MB")
        self.assertEqual(self.mkr._format_bytes(19834569), "19.83 MB")
        self.assertEqual(self.mkr._format_bytes(14419834569), "14.42 GB")

    def test_update_info(self):
        # test assumption
        bag = bagit.Bag(self.bagdir)
        for tag in bag.info:
            self.assertFalse(tag.startswith('Multibag-'))

        self.mkr.update_info()
        bag = bagit.Bag(self.bagdir)
        self.assertEqual(bag.info.get('Multibag-Version'),
                         amend.CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         amend.CURRENT_REFERENCE)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")

        self.assertTrue(isinstance(bag.info.get('Internal-Sender-Description'), list))
        self.assertEqual(len(bag.info.get('Internal-Sender-Description')),2)
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description')[1])

        self.assertEqual(bag.info['Bag-Size'], "4.875 kB")

        self.mkr = amend.SingleMultibagMaker(self.bagdir, "goober")
        self.mkr.update_info("1.0", "1.1")
        bag = bagit.Bag(self.bagdir)

        self.assertEqual(bag.info.get('Multibag-Version'), "1.1")
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1.0")
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "goober")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         amend.CURRENT_REFERENCE)

        del bag.info['Internal-Sender-Description']
        bag.save()
        self.mkr.update_info()
        bag = bagit.Bag(self.bagdir)
        
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1")
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "goober")
        
        self.assertFalse(isinstance(bag.info.get('Internal-Sender-Description'),
                                    list))
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description'))

    def test_write_member_bags(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.write_member_bags()
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))

        with open(mbfile) as fd:
            lines = fd.readlines()

        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0].strip(), os.path.basename(self.bagdir))

    def test_write_member_bags_alt(self):
        self.mkr = amend.SingleMultibagMaker(self.bagdir, "goober")
        mbdir = os.path.join(self.bagdir,"goober")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.write_member_bags("doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))
        
        with open(mbfile) as fd:
            lines = fd.readlines()

        self.assertEqual(len(lines), 1)
        parts = lines[0].strip().split("\t")
        self.assertEqual(parts[0], os.path.basename(self.bagdir))
        self.assertEqual(parts[1], "doi:XXXX/11111")

    def test_write_file_lookup(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'file-lookup.tsv')
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.write_file_lookup()
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))

        bagn = os.path.basename(self.bagdir)
        lu = {}
        with open(mbfile) as fd:
            i = 0
            for line in fd:
                parts = line.strip().split('\t')
                i += 1
                self.assertEqual(len(parts), 2,
                   "Expecting each line from file-lookup.tsv to have 2 fields: "+
                                 line.strip())
                self.assertEqual(parts[1], bagn)
                self.assertTrue(os.path.exists(os.path.join(self.bagdir,
                                                            parts[0])))
                self.assertTrue(parts[0].startswith("data/"))
        self.assertEqual(i, 3)

        self.mkr.write_file_lookup("metadata about.txt junk.json data/trial1.json".split())
        self.assertTrue(os.path.exists(mbdir))
        self.assertTrue(os.path.exists(mbfile))

        with open(mbfile) as fd:
            lines = fd.readlines()

        self.assertIn("about.txt\t"+bagn+"\n", lines)
        self.assertIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertNotIn("junk.json\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 5)

        self.mkr.write_file_lookup("data metadata/pod.json".split(),
                                   "data/trial1.json metadata".split(),
                                   trunc=True)
        with open(mbfile) as fd:
            lines = fd.readlines()
        self.assertNotIn("data/trial1.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial2.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial3/trial3a.json\t"+bagn+"\n", lines)
        self.assertIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 3)

    def test_convert(self):
        mbdir = os.path.join(self.bagdir,"multibag")
        mbfile = os.path.join(mbdir,'member-bags.tsv')
        flfile = os.path.join(mbdir,'file-lookup.tsv')
        bagn = os.path.basename(self.bagdir)
        self.assertTrue(not os.path.exists(mbdir))

        self.mkr.convert("1.5", "doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))

        # test for member-bags.tsv
        self.assertTrue(os.path.exists(mbfile))
        with open(mbfile) as fd:
            lines = fd.readlines()
        self.assertEqual(len(lines), 1)
        parts = lines[0].strip().split("\t")
        self.assertEqual(parts[0], bagn)
        self.assertEqual(parts[1], "doi:XXXX/11111")

        # test for file-lookup.tsv
        self.assertTrue(os.path.exists(flfile))
        with open(flfile) as fd:
            lines = fd.readlines()
        self.assertIn("data/trial1.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial2.json\t"+bagn+"\n", lines)
        self.assertIn("data/trial3/trial3a.json\t"+bagn+"\n", lines)
        self.assertNotIn("metadata/pod.json\t"+bagn+"\n", lines)
        self.assertNotIn("about.txt\t"+bagn+"\n", lines)
        self.assertEqual(len(lines), 3)

        # test info tag data
        bag = bagit.Bag(self.bagdir)
        self.assertEqual(bag.info.get('Multibag-Version'),
                         amend.CURRENT_VERSION)
        self.assertEqual(bag.info.get('Multibag-Head-Version'), "1.5")
        self.assertEqual(bag.info.get('Multibag-Reference'),
                         amend.CURRENT_REFERENCE)
        self.assertEqual(bag.info.get('Multibag-Tag-Directory'), "multibag")

        self.assertTrue(isinstance(bag.info.get('Internal-Sender-Description'), list))
        self.assertEqual(len(bag.info.get('Internal-Sender-Description')),2)
        self.assertIn("Multibag-Reference",
                      bag.info.get('Internal-Sender-Description')[1])

        self.assertEqual(bag.info['Bag-Size'], "5.171 kB")

    def test_convert_new(self):
        # create the data
        self.bagdir = os.path.join(self.tempdir, "sampledata")
        self.assertTrue(not os.path.isdir(self.bagdir))
        dm = mkdata.DatasetMaker(self.bagdir,
                                 { 'totalsize': 15, 'totalfiles': 3,
                                   'files': [{
                                       'totalsize': 10, 'totalfiles': 2
                                   }], 'dirs': [{
                                       'totalsize': 5, 'totalfiles': 1
                                   }]
                                 })
        dm.fill()
        self.assertTrue(os.path.isdir(self.bagdir))

        # turn it into a bag
        bag = bagit.make_bag(self.bagdir)
        self.assertTrue(bag.validate())

        mbdir = os.path.join(self.bagdir,'multibag')
        self.assertTrue(not os.path.exists(mbdir))

        # convert it to a multibag
        self.mkr = amend.SingleMultibagMaker(self.bagdir)
        self.mkr.convert("1.5", "doi:XXXX/11111")
        self.assertTrue(os.path.exists(mbdir))

        # validate it as a head bag
        valid8.validate_headbag(self.bagdir)
        

if __name__ == '__main__':
    test.main()
