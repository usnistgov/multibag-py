# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, logging
import tempfile, shutil
import unittest as test
from functools import cmp_to_key

import multibag.split as split
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "access", "data")

def ishardlink(path):
    return os.stat(path).st_nlink > 1

class TestLocalDirProgenitorBag(test.TestCase):

    def setUp(self):
        self.bagdir = os.path.join(datadir, "samplembag")
        self.bag = split.asProgenitor(Bag(self.bagdir))

    def test_ctor(self):
        self.assertEqual(self.bag._bagdir, self.bagdir)
        self.assertEqual(self.bag._bagname, "samplembag")

    def test_exists(self):
        self.assertTrue(self.bag.exists("bagit.txt"))
        self.assertTrue(self.bag.exists("data/trial1.json"))
        self.assertFalse(self.bag.exists("data/goober"))
        self.assertFalse(self.bag.exists("data/trial3/goober"))

        self.assertTrue(os.path.exists(os.path.join(self.bagdir,
                                                    "data/../../../data")))
        self.assertFalse(self.bag.exists("data/../../../data"))

    def test_isdir(self):
        self.assertFalse(self.bag.isdir("bagit.txt"))
        self.assertTrue(self.bag.isdir("data"))
        self.assertFalse(self.bag.isdir("data/trial1.json"))
        self.assertFalse(self.bag.isdir("data/goober"))
        self.assertTrue(self.bag.isdir("data/trial3"))
        self.assertFalse(self.bag.isdir("data/trial3/goober"))

        self.assertTrue(os.path.exists(os.path.join(self.bagdir,
                                                    "data/../../../data")))
        self.assertFalse(self.bag.isdir("data/../../../data"))

    def test_isfile(self):
        self.assertTrue(self.bag.isfile("bagit.txt"))
        self.assertFalse(self.bag.isfile("data"))
        self.assertTrue(self.bag.isfile("data/trial1.json"))
        self.assertFalse(self.bag.isfile("data/goober"))
        self.assertFalse(self.bag.isfile("data/trial3"))
        self.assertFalse(self.bag.isfile("data/trial3/goober"))

    def test_bag(self):
        # test that self.bag behaves like a bagit.Bag
        self.assertEqual(self.bag.algs, ["sha256"])
        self.assertEqual(list(self.bag.manifest_files()),
                         [os.path.join(self.bagdir, "manifest-sha256.txt")])

    def test_walk(self):
        contents = list(self.bag.walk())

        self.assertEqual(contents[0][0], "") # the bag's base dir
        dirs = contents[0][1]
        self.assertIn("data", dirs)
        self.assertIn("multibag", dirs)
        self.assertIn("metadata", dirs)
        self.assertEqual(len(dirs), 3)
        files = contents[0][2]
        self.assertIn("bagit.txt", files)
        self.assertIn("bag-info.txt", files)
        self.assertIn("manifest-sha256.txt", files)
        self.assertIn("fetch.txt", files)
        self.assertIn("about.txt", files)
        self.assertEqual(len(files), 5)

        sub = [t for t in contents if t[0] == "data"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertIn("trial3", dirs)
        self.assertEqual(len(dirs), 1)
        files = sub[2]
        self.assertIn("trial1.json", files)
        self.assertIn("trial2.json", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "multibag"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("member-bags.tsv", files)
        self.assertIn("file-lookup.tsv", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "metadata"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("pod.json", files)
        self.assertEqual(len(files), 1)

        sub = [t for t in contents if t[0] == os.path.join("data","trial3")]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("trial3a.json", files)
        self.assertEqual(len(files), 1)

        self.assertEqual(len(contents), 5)

    def test_nonstandard(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            os.mkdir(os.path.join(self.bagdir, "metadata", "trial3"))

            self.bag = split.asProgenitor(Bag(self.bagdir))

            contents = list(self.bag.nonstandard())
            self.assertIn("about.txt", contents)
            self.assertIn(os.path.join("metadata","pod.json"), contents)
            self.assertIn(os.path.join("metadata","trial3"), contents)
            self.assertIn(os.path.join("data","trial1.json"), contents)
            self.assertIn(os.path.join("data","trial2.json"), contents)
            self.assertIn(os.path.join("data","trial3/trial3a.json"), contents)
            self.assertIn(os.path.join("multibag","member-bags.tsv"), contents)
            self.assertIn(os.path.join("multibag","file-lookup.tsv"), contents)
            self.assertEqual(len(contents), 8)

        finally:
            shutil.rmtree(tempdir)

    def test_replicate(self):
        self.bag.replicate_with_hardlink = False
        tempdir = tempfile.mkdtemp()
        try:
            self.assertTrue(os.path.exists(os.path.join(self.bag._bagdir,
                                                        "bagit.txt")))
            self.assertFalse(os.path.exists(os.path.join(tempdir, "bagit.txt")))
                                                         
            self.bag.replicate("bagit.txt", tempdir)
            self.assertTrue(os.path.exists(os.path.join(tempdir, "bagit.txt")))
            self.assertFalse(ishardlink(os.path.join(tempdir, "bagit.txt")))

        finally:
            shutil.rmtree(tempdir)
    
    def test_replicate_withlink(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            os.mkdir(os.path.join(self.bagdir, "metadata", "trial3"))

            self.bag = split.asProgenitor(Bag(self.bagdir))
            self.bag.replicate_with_hardlink = True
            self.assertTrue(os.path.exists(os.path.join(self.bag._bagdir,
                                                        "bagit.txt")))
            outdir = os.path.join(tempdir, "otherbag")
            self.assertFalse(os.path.exists(os.path.join(outdir, "bagit.txt")))
                                                         
            self.bag.replicate("bagit.txt", outdir)
            self.assertTrue(os.path.exists(os.path.join(outdir, "bagit.txt")))
            self.assertTrue(ishardlink(os.path.join(outdir, "bagit.txt")))

        finally:
            shutil.rmtree(tempdir)
    
    def test_replicate_dir(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            path = os.path.join("metadata", "trial3")
            srcpath = os.path.join(self.bagdir, path)
            os.mkdir(srcpath)

            self.bag = split.asProgenitor(Bag(self.bagdir))
            self.bag.replicate_with_hardlink = True
            self.assertTrue(os.path.exists(os.path.join(self.bag._bagdir, path)))
            outdir = os.path.join(tempdir, "otherbag")
            self.assertFalse(os.path.exists(os.path.join(outdir, path)))
                                                         
            self.bag.replicate(path, outdir)
            self.assertTrue(os.path.exists(os.path.join(outdir, path)))
            self.assertTrue(os.path.isdir(os.path.join(outdir, path)))

        finally:
            shutil.rmtree(tempdir)
    
class TestReadOnlyProgenitorBag(test.TestCase):

    def setUp(self):
        self.bagroot = os.path.join(datadir, "samplembag.zip")
        self.bag = split.asProgenitor(open_bag(self.bagroot))

    def test_ctor(self):
        self.assertTrue(isinstance(self.bag, ReadOnlyBag))
        self.assertTrue(isinstance(self.bag, split.ProgenitorMixin))
        self.assertTrue(isinstance(self.bag._root, Path))

    def test_exists(self):
        self.assertTrue(self.bag.exists("bagit.txt"))
        self.assertTrue(self.bag.exists("data/trial1.json"))
        self.assertFalse(self.bag.exists("data/goober"))
        self.assertFalse(self.bag.exists("data/trial3/goober"))

    def test_isdir(self):
        self.assertFalse(self.bag.isdir("bagit.txt"))
        self.assertTrue(self.bag.isdir("data"))
        self.assertFalse(self.bag.isdir("data/trial1.json"))
        self.assertFalse(self.bag.isdir("data/goober"))
        self.assertTrue(self.bag.isdir("data/trial3"))
        self.assertFalse(self.bag.isdir("data/trial3/goober"))

    def test_isfile(self):
        self.assertTrue(self.bag.isfile("bagit.txt"))
        self.assertFalse(self.bag.isfile("data"))
        self.assertTrue(self.bag.isfile("data/trial1.json"))
        self.assertFalse(self.bag.isfile("data/goober"))
        self.assertFalse(self.bag.isfile("data/trial3"))
        self.assertFalse(self.bag.isfile("data/trial3/goober"))

    def test_bag(self):
        # test that self.bag behaves like a bagit.Bag
        # Note that this is a little different, as bagit.Bag will include the
        # full path.  
        self.assertEqual(self.bag.algs, ["sha256"])
        self.assertEqual(list(self.bag.manifest_files()),["manifest-sha256.txt"])

    def test_walk(self):
        contents = list(self.bag.walk())

        self.assertEqual(contents[0][0], "") # the bag's base dir
        dirs = contents[0][1]
        self.assertIn("data", dirs)
        self.assertIn("multibag", dirs)
        self.assertIn("metadata", dirs)
        self.assertEqual(len(dirs), 3)
        files = contents[0][2]
        self.assertIn("bagit.txt", files)
        self.assertIn("bag-info.txt", files)
        self.assertIn("manifest-sha256.txt", files)
        self.assertIn("fetch.txt", files)
        self.assertIn("about.txt", files)
        self.assertIn("preserv.log", files)
        self.assertEqual(len(files), 6)

        sub = [t for t in contents if t[0] == "data"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertIn("trial3", dirs)
        self.assertEqual(len(dirs), 1)
        files = sub[2]
        self.assertIn("trial1.json", files)
        self.assertIn("trial2.json", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "multibag"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("member-bags.tsv", files)
        self.assertIn("file-lookup.tsv", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == "metadata"]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 3)
        self.assertIn("trial1.json", dirs)
        self.assertIn("trial2.json", dirs)
        self.assertIn("trial3", dirs)
        files = sub[2]
        self.assertIn("pod.json", files)
        self.assertIn("nerdm.json", files)
        self.assertEqual(len(files), 2)

        sub = [t for t in contents if t[0] == os.path.join("data","trial3")]
        self.assertEqual(len(sub), 1)
        sub = sub[0]
        dirs = sub[1]
        self.assertEqual(len(dirs), 0)
        files = sub[2]
        self.assertIn("trial3a.json", files)
        self.assertEqual(len(files), 1)

        self.assertEqual(len(contents), 9)

    def test_nonstandard(self):
        tempdir = tempfile.mkdtemp()

        contents = list(self.bag.nonstandard())
        self.assertIn("about.txt", contents)
        self.assertIn(os.path.join("metadata","pod.json"), contents)
        self.assertIn(os.path.join("data","trial1.json"), contents)
        self.assertIn(os.path.join("data","trial2.json"), contents)
        self.assertIn(os.path.join("data","trial3/trial3a.json"), contents)
        self.assertIn(os.path.join("multibag","member-bags.tsv"), contents)
        self.assertIn(os.path.join("multibag","file-lookup.tsv"), contents)
        self.assertEqual(len(contents), 13)

    def test_replicate(self):
        tempdir = tempfile.mkdtemp()
        try:
            self.assertTrue(self.bag.exists("bagit.txt"))
            self.assertFalse(os.path.exists(os.path.join(tempdir, "bagit.txt")))
                                                         
            self.bag.replicate("bagit.txt", tempdir)
            self.assertTrue(os.path.exists(os.path.join(tempdir, "bagit.txt")))
            self.assertFalse(ishardlink(os.path.join(tempdir, "bagit.txt")))

        finally:
            shutil.rmtree(tempdir)
    

        


class TestSplitPlan(test.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.bagdir = os.path.join(self.tempdir, "samplebag")
        shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
        shutil.rmtree(os.path.join(self.bagdir, "multibag"))
        os.mkdir(os.path.join(self.bagdir, "metadata", "trial3"))

        self.bag = split.asProgenitor(Bag(self.bagdir))
        self.plan = split.SplitPlan(self.bag)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ctor(self):
        self.assertEqual(self.plan._manifests, [])
        self.assertIs(self.plan.progenitor, self.bag)

        with self.assertRaises(ValueError):
            split.SplitPlan(self.bagdir)

    def test_is_complete(self):
        self.assertFalse(self.plan.is_complete())

        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)
        self.assertFalse(self.plan.is_complete())

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)
        self.assertTrue(self.plan.is_complete())

    def test_required(self):
        exp = "about.txt data/trial1.json data/trial2.json " + \
              "data/trial3/trial3a.json metadata/pod.json metadata/trial3"
        exp = exp.split()
        
        required = set(self.plan.required())
        for p in exp:
            self.assertIn(p, required)
        self.assertEqual(len(required), 6)

    def test_find_destination(self):
        self.assertIsNone(self.plan.find_destination("about.txt"))
        
        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)

        self.assertEqual(self.plan.find_destination("about.txt")['name'],
                         "goob_1.bag")
        self.assertEqual(self.plan.find_destination("metadata/trial3")['name'],
                         "goob_1.bag")
        self.assertEqual(self.plan.find_destination("data/trial2.json")['name'],
                         "goob_2.bag")
        self.assertIsNone(self.plan.find_destination("goober.txt"))
        

    def test_missing(self):
        exp = "about.txt data/trial1.json data/trial2.json " + \
              "data/trial3/trial3a.json metadata/pod.json metadata/trial3"
        exp = exp.split()
        
        missing = set(self.plan.missing())
        for p in exp:
            self.assertIn(p, missing)
        self.assertEqual(len(missing), 6)

        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)
        missing = set(self.plan.missing())
        for p in "data/trial1.json data/trial2.json data/trial3/trial3a.json".split():
            self.assertIn(p, missing)
        self.assertEqual(len(missing), 3)

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)
        missing = set(self.plan.missing())
        self.assertEqual(len(missing), 0)

    def test_complete_plan(self):
        willmiss = "data/trial1.json data/trial2.json data/trial3/trial3a.json"
        willmiss = willmiss.split()
        
        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)
        self.assertEqual(len(self.plan._manifests), 1)
        missing = set(self.plan.missing())
        for p in willmiss:
            self.assertIn(p, missing)
        self.assertEqual(len(missing), 3)

        self.plan.complete_plan()
        self.assertEqual(len(self.plan._manifests), 2)
        for p in willmiss:
            self.assertIn(p, self.plan._manifests[-1]["contents"])
        self.assertEqual(len(self.plan._manifests[-1]["contents"]), 3)
        missing = set(self.plan.missing())
        self.assertEqual(len(missing), 0)

    def test_name_output_bags(self):

        class nameiter(object):
            lim = 10
            def __init__(self):
                self.nxt = 0
            def __iter__(self):
                return self
            def __next__(self):
                self.nxt += 1
                return "mbag_"+str(self.nxt-1)
            def next(self):
                return self.__next__()

        manifest = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest)

        manifest = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest)

        ni = nameiter()
        self.plan.name_output_bags(ni)
        self.assertEqual(self.plan._manifests[0]['name'], "mbag_0")
        self.assertEqual(self.plan._manifests[1]['name'], "mbag_1")
        self.plan.name_output_bags(ni, True)
        self.assertEqual(self.plan._manifests[0]['name'], "mbag_3")
        self.assertEqual(self.plan._manifests[1]['name'], "mbag_2")

    def test_apply_iter(self):
        manifest1 = {
           'contents': set("about.txt metadata/pod.json metadata/trial3".split()),
           'name': "goob_1.bag"
        }
        self.plan._manifests.append(manifest1)

        manifest2 = {
           'contents': set("data/trial1.json data/trial2.json data/trial3/trial3a.json".split()),
           'name': "goob_2.bag"
        }
        self.plan._manifests.append(manifest2)

        iter = self.plan.apply_iter(self.tempdir)
        mbag = next(iter)
        mbagdir = os.path.join(self.tempdir, "goob_1.bag")
        self.assertEqual(mbag, mbagdir)
        for member in manifest1['contents']:
            self.assertTrue(os.path.exists(os.path.join(mbagdir, member)))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bagit.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bag-info.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir,
                                                    "manifest-sha256.txt")))

        bag = Bag(mbagdir)
        self.assertTrue(bag.validate())
        self.assertTrue(bag.is_valid())

        mbag = next(iter)
        mbagdir = os.path.join(self.tempdir, "goob_2.bag")
        self.assertEqual(mbag, mbagdir)
        for member in manifest2['contents']:
            self.assertTrue(os.path.exists(os.path.join(mbagdir, member)))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bagit.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir, "bag-info.txt")))
        self.assertTrue(os.path.exists(os.path.join(mbagdir,
                                                    "manifest-sha256.txt")))

        bag = Bag(mbagdir)
        self.assertTrue(bag.validate())
        self.assertTrue(bag.is_valid())


class TestNeighborlySplitter(test.TestCase):

    files = {
        "/a":   14,
        "/b": 1598,
        "/c":  350,
        "/d":  350,
        "/e/a":    860,
        "/e/b":    860,
        "/e/c":    860,
        "/e/d/a":    1100,
        "/e/d/b":      58,
        "/e/d/c":      58,
        "/e/d/d":      58,
        "/e/d/3":      58,
        "/f/a":    860,
        "/f/b":    860,
        "/f/c":    860,
        "/f/d/a":    1100,
        "/f/d/b":      58,
        "/f/d/c":      58,
        "/f/d/d":      58,
        "/f/d/3":      58,
        "/g": 0,
        "/h": 10,
        "/bagit.txt": 43,
        "/fetch.txt":  0
    }

    def setUp(self):
        self.bagdir = os.path.join(datadir, "samplembag")
        self.info = self.mkinfo(self.files)

    def mkinfo(self, files):
        out = []
        for f in files:
            out.append({ "path": f, "size": files[f], "name": f.split('/')[-1] })
        return out

    def test_is_special(self):
        self.spltr = split.NeighborlySplitter()
        specials = [n for n in self.files.keys() if self.spltr._is_special(n)]
        self.assertIn("/fetch.txt", specials)
        self.assertIn("/bagit.txt", specials)
        self.assertEqual(len(specials), 2)

    def test_cmp_by_size(self):
        self.spltr = split.NeighborlySplitter()
        self.info.sort(key=cmp_to_key(self.spltr._cmp_by_size))
        sz = 0
        for fi in reversed(self.info):
            self.assertGreaterEqual(fi['size'], sz)
            sz = fi['size']

    def test_apply_algorithm(self):
        self.spltr = split.NeighborlySplitter(2200, 2000)
        self.info = [f for f in self.info
                       if not self.spltr._is_special(f['path'])]
        self.info.sort(key=cmp_to_key(self.spltr._cmp_by_size))
        bag = ReadOnlyBag(self.bagdir)
        plan = split.SplitPlan(bag)
        self.spltr._apply_algorithm(self.info, plan)

        self.assertGreater(len(plan._manifests), 0)
        # self.assertEqual(len(plan._manifests), 1)

        mf = plan._manifests[0]
        self.assertIn("b", mf['contents'])
        self.assertIn("c", mf['contents'])
        self.assertIn("a", mf['contents'])
        self.assertIn("g", mf['contents'])
        self.assertIn("h", mf['contents'])
        # self.assertIn("e/d/3", mf['contents'])
        self.assertEqual(len(mf['contents']), 6)
        self.assertGreater(2200, mf['totalsize'])

        for i in range(6):
            self.assertGreater(2200, plan._manifests[i]['totalsize'])

    def test_plan(self):
        self.spltr = split.NeighborlySplitter(500)
        plan = self.spltr.plan(self.bagdir)
        self.assertTrue(plan.is_complete())
        mfs = list(plan.manifests())
        self.assertEqual(len(list(plan.manifests())), 3)
        self.assertIn("metadata/pod.json",        mfs[0]['contents'])
        self.assertEqual(len(mfs[0]['contents']), 1)
        self.assertEqual("samplembag_1.mbag",      mfs[0]['name'])
        
        self.assertIn("about.txt", mfs[1]['contents'])
        self.assertIn("data/trial1.json",         mfs[1]['contents'])
        self.assertIn("multibag/member-bags.tsv", mfs[1]['contents'])
        self.assertEqual(len(mfs[1]['contents']), 3)
        self.assertEqual("samplembag_2.mbag",      mfs[1]['name'])
        
        self.assertIn("multibag/file-lookup.tsv", mfs[2]['contents'])
        self.assertIn("data/trial2.json",         mfs[2]['contents'])
        self.assertIn("data/trial3/trial3a.json", mfs[2]['contents'])
        self.assertEqual(len(mfs[2]['contents']), 3)
        self.assertEqual("samplembag_3.mbag",      mfs[2]['name'])

    def test_split(self):
        from multibag.validate import HeadBagValidator, MemberBagValidator
        
        self.tempdir = tempfile.mkdtemp()
        try:
            self.bagdir = os.path.join(self.tempdir, "samplebag")
            shutil.copytree(os.path.join(datadir, "samplembag"), self.bagdir)
            shutil.rmtree(os.path.join(self.bagdir, "multibag"))
            
            self.spltr = split.NeighborlySplitter(500)
            self.spltr.split(self.bagdir, self.tempdir)

            mbags = [d for d in os.listdir(self.tempdir) if d.endswith(".mbag")]
            self.assertIn("samplebag_1.mbag", mbags)
            self.assertIn("samplebag_2.mbag", mbags)
            self.assertIn("samplebag_3.mbag", mbags)
            self.assertEqual(len(mbags), 3)

            # run validator on output files!
            MemberBagValidator(os.path.join(self.tempdir,
                                            "samplebag_1.mbag")).ensure_valid()
            MemberBagValidator(os.path.join(self.tempdir,
                                            "samplebag_2.mbag")).ensure_valid()
            HeadBagValidator(os.path.join(self.tempdir,
                                          "samplebag_3.mbag")).ensure_valid()

        finally:
            shutil.rmtree(self.tempdir)
            
        

class TestWellPackedSplitter(test.TestCase):

    files = {
        "/a":   14,
        "/b": 1598,
        "/c":  350,
        "/d":  350,
        "/e/a":    860,
        "/e/b":    860,
        "/e/c":    860,
        "/e/d/a":    1100,
        "/e/d/b":      58,
        "/e/d/c":      58,
        "/e/d/d":      58,
        "/e/d/3":      58,
        "/f/a":    860,
        "/f/b":    860,
        "/f/c":    860,
        "/f/d/a":    1100,
        "/f/d/b":      58,
        "/f/d/c":      58,
        "/f/d/d":      58,
        "/f/d/3":      58,
        "/g": 0,
        "/h": 10,
        "/bagit.txt": 43,
        "/fetch.txt":  0
    }

    def mkinfo(self, files):
        out = []
        for f in files:
            out.append({ "path": f, "size": files[f], "name": f.split('/')[-1] })
        return out

    def setUp(self):
        self.bagdir = os.path.join(datadir, "samplembag")
        self.spltr = split.WellPackedSplitter(2500)
        self.info = self.mkinfo(self.files)

    def test_apply_algorithm(self):
        self.info = [f for f in self.info
                       if not self.spltr._is_special(f['path'])]
        self.info.sort(key=cmp_to_key(self.spltr._cmp_by_size))
        bag = ReadOnlyBag(self.bagdir)
        plan = split.SplitPlan(bag)
        self.spltr._apply_algorithm(self.info, plan)

        self.assertGreater(len(plan._manifests), 0)
        # self.assertEqual(len(plan._manifests), 1)

        mf = plan._manifests[0]
        self.assertIn("b", mf['contents'])
        self.assertIn("e/a", mf['contents'])
        self.assertIn("a", mf['contents'])
        self.assertIn("g", mf['contents'])
        self.assertIn("h", mf['contents'])
        self.assertEqual(len(mf['contents']), 5)
        self.assertGreater(2500, mf['totalsize'])

        for i in range(5):
            self.assertGreater(2500, plan._manifests[i]['totalsize'])


class TestSimpleNamer(test.TestCase):

    def test_iter(self):
        niter = split.SimpleNamer("goober")
        self.assertEqual(niter.next(), "goober_1.mbag")
        self.assertEqual(niter.next(), "goober_2.mbag")
        self.assertEqual(niter.next(), "goober_3.mbag")
        self.assertEqual(niter.next(), "goober_4.mbag")
        self.assertEqual(niter.next(), "goober_5.mbag")



if __name__ == '__main__':
    test.main()
