# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test
from collections import OrderedDict

import fs
from fs import open_fs

import multibag.access.multibag as mb
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag
from multibag.constants import CURRENT_VERSION

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
samplembag = os.path.join(datadir, "samplembag")

class TestMemberInfo(test.TestCase):

    def test_ctor(self):
        mi = mb.MemberInfo("goob er")
        self.assertEqual(mi.name, "goob er")
        self.assertIsNone(mi.uri)
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        mi = mb.MemberInfo("g urn", "ivo://ncsa.vo/gurn")
        self.assertEqual(mi.name, "g urn")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/gurn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        mi = mb.MemberInfo("fooman", "ivo://ncsa.vo/fooman", "Hey!")
        self.assertEqual(mi.name, "fooman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/fooman")
        self.assertEqual(mi.comment, "Hey!")
        self.assertEqual(mi.info, [])
        
        mi = mb.MemberInfo("barman", "ivo://ncsa.vo/barman", "You!",
                           "goob=gurn", "blueberry", "https://example.com")
        self.assertEqual(mi.name, "barman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/barman")
        self.assertEqual(mi.comment, "You!")
        self.assertEqual(mi.info,
                         ["goob=gurn", "blueberry", "https://example.com"])

    def test_parse_line_03(self):
        # note that the purpose for TSV format is to support spaces in names

        line = "goob er\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "goob er")
        self.assertIsNone(mi.uri)
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "g urn\tivo://ncsa.vo/gurn\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "g urn")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/gurn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])

        line = "fooman\tivo://ncsa.vo/fooman\t# Hey!\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "fooman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/fooman")
        self.assertEqual(mi.comment, "Hey!")
        self.assertEqual(mi.info, [])

        line = "fooman\tivo://ncsa.vo/fooman\t Hey!\n"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "fooman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/fooman")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [" Hey!"])
        
        line = "barman\tivo://ncsa.vo/barman\tgoob=gurn\tblueberry\thttps://example.com\t# You!"
        mi = mb.MemberInfo.parse_line_03(line)
        self.assertEqual(mi.name, "barman")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/barman")
        self.assertEqual(mi.comment, "You!")
        self.assertEqual(mi.info,
                         ["goob=gurn", "blueberry", "https://example.com"])

        line = "\n"
        with self.assertRaises(mb.MultibagError):
            mi = mb.MemberInfo.parse_line_03(line)
        
    def test_parse_line_02(self):

        line = "goober\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "goober")
        self.assertIsNone(mi.uri)
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "goob er\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "goob")
        self.assertEqual(mi.uri, "er")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "gurn ivo://ncsa.vo/gurn\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "gurn")
        self.assertEqual(mi.uri, "ivo://ncsa.vo/gurn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])
        
        line = "g urn ivo://ncsa.vo/gurn\n"
        mi = mb.MemberInfo.parse_line_02(line)
        self.assertEqual(mi.name, "g")
        self.assertEqual(mi.uri, "urn")
        self.assertIsNone(mi.comment)
        self.assertEqual(mi.info, [])

        line = "\n"
        with self.assertRaises(mb.MultibagError):
            mi = mb.MemberInfo.parse_line_02(line)

    def test_parse_file(self):
        mbf = os.path.join(samplembag, "multibag", "member-bags.tsv")
        lu = OrderedDict()
        with open(mbf) as fd:
            for line in fd:
                if line.split():
                    mi = mb.MemberInfo.parse_line_03(line)
                    lu[mi.name] = mi

        self.assertEqual(list(lu.keys()), ["samplembag"])
        self.assertIsNone(lu["samplembag"].uri)
        self.assertIsNone(lu["samplembag"].comment)
        self.assertEqual(lu["samplembag"].info, [])
        
        
class TestParseFileLookup(test.TestCase):

    def test_parse_file_lookup_line_03(self):
        # note that the purpose for TSV format is to support spaces in names

        line = "data/goo ber.txt\theadbag\n"
        pair = mb.parse_file_lookup_line_03(line)
        self.assertEqual(pair, ("data/goo ber.txt", "headbag"))

    def test_parse_group_directory_line(self):
        
        line = "data/goober.txt headbag\n"
        pair = mb.parse_group_directory_line(line)
        self.assertEqual(pair, ("data/goober.txt", "headbag"))

        line = "data/goo ber.txt\theadbag\n"
        pair = mb.parse_group_directory_line(line)
        self.assertEqual(pair, ("data/goo", "ber.txt\theadbag"))

    def test_parse_file(self):
        mbf = os.path.join(samplembag, "multibag", "file-lookup.tsv")
        lu = OrderedDict()
        with open(mbf) as fd:
            for line in fd:
                if line.split():
                    mi = mb.parse_file_lookup_line_03(line)
                    lu[mi[0]] = mi[1]

        files = list(lu.keys())
        self.assertEqual(files, [
            "data/trial1.json", "data/trial2.json", "data/trial3/trial3a.json",
            "metadata/pod.json", "metadata/nerdm.json"
        ])
        for f in files[:4]:
            self.assertEqual(lu[f], "samplembag")
        self.assertEqual(lu[files[-1]], "samplembag2")
    
class TestReadOnlyHeadBag(test.TestCase):

    def setUp(self):
        self.bagfile = os.path.join(datadir, "samplembag.zip")
        self.fs = fs.zipfs.ZipFS(self.bagfile)
        self.path = Path(self.fs, "samplembag", "samplembag.zip:samplembag/")
        self.bag = mb.ReadOnlyHeadBag(self.path)
        
    def test_ctor(self):
        self.assertTrue(isinstance(self.bag, mb.HeadBagReadMixin))
        self.assertTrue(self.bag.is_readonly)
        self.assertTrue(self.bag.is_head_multibag())

    def test_version(self):
        self.assertEqual(self.bag.head_version, "1.0")

    def test_profile_version(self):
        self.assertEqual(self.bag.profile_version, "0.4")

    def test_missing_req_item(self):
        with self.assertRaises(mb.MultibagError):
            self.bag._get_required_info_item('Goober-Name')

        self.bag.info['Goober-Name'] = []
        with self.assertRaises(mb.MultibagError):
            self.bag._get_required_info_item('Goober-Name')

        self.bag.info['Goober-Name'] = ['1', '2']
        self.assertEqual(self.bag._get_required_info_item('Goober-Name'), '2')

    def test_iter_member_bags(self):
        mis = list(self.bag.iter_member_bags())
        self.assertEqual(len(mis), 1)
        self.assertEqual(mis[0].name, 'samplembag')
        self.assertIsNone(mis[0].uri)
        self.assertIsNone(mis[0].comment)
        self.assertEqual(mis[0].info, [])
        
    def test_member_bags(self):
        mis = self.bag.member_bags()
        self.assertEqual(len(mis), 1)
        self.assertEqual(mis[0].name, 'samplembag')
        self.assertIsNone(mis[0].uri)
        self.assertIsNone(mis[0].comment)
        self.assertEqual(mis[0].info, [])

        self.bag._memberbags.append(mb.MemberInfo("goob"))
        mis = self.bag.member_bags()
        self.assertEqual(len(mis), 2)
        mis = self.bag.member_bags(True)
        self.assertEqual(len(mis), 1)
        
        
    def test_member_bag_names(self):
        names = self.bag.member_bag_names
        self.assertEqual(names, ['samplembag'])

    def test_iter_file_lookup(self):
        lu = OrderedDict(self.bag.iter_file_lookup())
        files = list(lu.keys())
        self.assertEqual(files, [
            "data/trial1.json", "data/trial2.json", "data/trial3/trial3a.json",
            "metadata/pod.json", "metadata/nerdm.json"
        ])
        for f in files[:4]:
            self.assertEqual(lu[f], "samplembag")
        self.assertEqual(lu[files[-1]], "samplembag2")

    def test_lookup_file(self):
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag")
        self.assertEqual(self.bag.lookup_file("metadata/nerdm.json"),
                         "samplembag2")
        self.bag._filelu["data/trial1.json"] = "samplembag2"
        self.assertEqual(self.bag.lookup_file("data/trial1.json"), "samplembag2")
        self.assertEqual(self.bag.lookup_file("data/trial1.json", True),
                         "samplembag")


if __name__ == '__main__':
    test.main()
