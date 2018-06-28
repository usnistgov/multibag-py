"""
This module provides the validator implementation for validating head bags.
"""
import os
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from .base import (Validator, ValidationIssue, ValidationResults, 
                   ALL, ERROR, WARN, REC, PROB, CURRENT_VERSION)
from ..access.bagit import BagValidationError, BagError, open_bag

class HeadBagValidator(Validator):
    """
    A validator that tests whether a given Bag (serialized or otherwise) complies
    with the Multibag requirements for serving as a head bag.
    """

    def __init__(self, bagpath):
        """
        initialize the validator for the bag with a given path.  

        :param str bagpath:  the target bag, either as a directory for an 
                             unserialized bag or a file for a serialized one
        """
        super(HeadBagValidator, self).__init__(bagpath)
        self.bag = open_bag(bagpath)

    def validate_version(self, want=ALL, results=None, version=CURRENT_VERSION):
        """
        ensure that the version information is correct
        """
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        data = self.bag.info

        t = self._issue("3-Version",
              "bag-info.txt field must have required element: Multibag-Version")
        out._err(t, "Multibag-Version" in data and bool(data["Multibag-Version"]))
        if t.failed():
            return out

        t = self._issue("3-Version",
                "bag-info.txt field, Multibag-Version, should only appear once")
        out._warn(t, not isinstance(data["Multibag-Version"], (list, tuple)))

        vers = data["Multibag-Version"]
        if isinstance(data["Multibag-Version"], (list, tuple)):
            version = data["Multibag-Version"][-1]

        t = self._issue("3-Version-val",
                        "Multibag-Version must be set to '{0}'".format(version))
        out._err(t, vers == version)
        
        return out

    def validate_reference(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        data = self.bag.info
        
        t = self._issue("3-Reference",
                        "bag-info.txt should include field: Multibag-Reference")
        out._rec(t, "Multibag-Reference" in data and data["Multibag-Reference"])
        if t.failed():
            return out

        t = self._issue("3-Reference-val",
                        "Multibag-Reference value must be an absolute URL " +
                        "(not an empty value)")
        url = data["Multibag-Reference"]
        if isinstance(url, list):
            url = url[-1]
        out._err(t, bool(url))

        t = self._issue("3-Reference-val",
                        "Multibag-Reference value must be an absolute URL")
        url = urlparse(url)
        out._err(t, url.scheme and url.netloc)

        return out

    def validate_tag_directory(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        data = self.bag.info
        
        if "Multibag-Tag-Directory" in data:
            mdir = data["Multibag-Tag-Directory"]
            if not isinstance(mdir, list):
                mdir = [mdir]

            t = self._issue("2-Tag-Directory",
                            "bag-info.txt: Value for Multibag-Tag-Directory "+
                            "should not be empty")
            out._err(t, len(mdir) > 0 and mdir[-1])
            if t.failed():
                return out

            t = self._issue("2-Tag-Directory",
                            "bag-info.txt: Multibag-Tag-Directory element "+
                            "should appear no more than once")
            out._err(t, len(mdir) == 1)

            t = self._issue("2-Tag-Directory",
                            "Multibag-Tag-Directory must exist as directory")
            out._err(t, self.bag.isdir(mdir[-1]))

        else:
            t = self._issue("2-Tag-Directory",
                            "Default Multibag-Tag-Directory, multibag, must "+
                            "exist as a directory")
            out._err(t, self.bag.isdir("multibag"))

        return out

    def validate_head_version(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        data = self.bag.info
        if "Multibag-Head-Version" in data:
            value = data["Multibag-Head-Version"]
            if not isinstance(value, list):
                value = [value]

            t = self._issue("3-Head-Version_nonempty",
                            "bag-info.txt: Value for Multibag-Head-Version "+
                            "should not be empty")
            out._warn(t, len(value) > 0 and value[-1])
            if len(value) == 0:
                return out

            t = self._issue("3-Head-Version_single",
                            "bag-info.txt: Multibag-Head-Version element "+
                            "should only appear once")
            out._warn(t, len(value) == 1)

        else:
            t = self._issue("3-Head-Version",
                            "Head bag: bag-info.txt must have "+
                            "Multibag-Head-Version element")
            out._err(t, False)

        return out

    def validate_head_deprecates(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        data = self.bag.info
        if "Multibag-Head-Deprecates" not in data:
            return out

        headver = data.get("Multibag-Head-Version")
        if isinstance(headver, list) and len(headver) > 0:
            headver = headver[-1]
        assert headver

        values = data["Multibag-Head-Deprecates"]
        if not isinstance(values, list):
            values = [values]

        t = self._issue("3-Head-Deprecates_notempty",
           "bag-info.txt: Value for Multibag-Head-Deprecates should not be empty")
        out._warn(t, len(values) > 0 and values[0])
        if t.failed():
            return out

        empty = True
        selfdeprecating = False
        badfmt = []
        for val in values:
            if val:
                empty = False
            parts = [p.strip() for p in val.split(',')]
            if len(parts) > 2:
                badfmt.append(val)
            selfdeprecating = selfdeprecating or parts[0] == headver or \
                              (len(parts) > 1 and parts[1] == self.bag._name)

        t = self._issue("3-Head-Deprecates_format",
                        "bag-info.txt: Multibag-Head-Deprecates value must "+
                        "match format: VERSION[, BAGNAME]")
        comm = None
        if len(badfmt) > 0:
            comm = list(badfmt)
        out._err(t, len(badfmt) == 0, comm)
        
        t = self._issue("3-Head-Deprecates_notempty",
                        "bag-info.txt: Value for Multibag-Head-Deprecates "+
                        "should not be empty")
        out._warn(t, not empty)

        t = self._issue("3-Head-Deprecates_notselfdep",
                        "bag-info.txt: Multibag-Head-Deprecates should not "+
                        "deprecate itself")
        out._warn(t, not selfdeprecating)

        return out

    def validate_baginfo_recs(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        data = self.bag.info

        for el in ["Internal-Sender-Identifier",
                   "Internal-Sender-Description", "Bag-Group-Identifier"]:
            t = self._issue("3-2", "Recommed adding value for "+el+ 
                            " into bag-info.txt file")
            out._rec(t, el in data and len(data[el]) > 0 and data[el][-1])
            if t.failed():
                continue
            t = self._issue("3-2", "bag-info.txt: "+el+" element should not "+
                            "have empty values")
            out._err(t, all(data[el]))

        return out

    def validate_member_bags(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        ishead = self.bag.is_head_multibag()
        mdir = self.bag.info.get("Multibag-Tag-Directory")
        if not mdir:
            mdir = "multibag"
        if not isinstance(mdir, list):
            mdir = [mdir]
        mdir = mdir[-1]

        assert mdir
        assert ishead

        t = self._issue("2-Tag-Directory",
                        "Multibag-Tag-Directory must exist as directory")
        out._err(t, self.bag.isdir(mdir))
        if t.failed():
            return out

        mbemf = "/".join([mdir, "member-bags.tsv"])
        t = self._issue("3.0-1", "Multibag tag directory must contain a "+
                        "member-bags.tsv file")
        out._err(t, self.bag.isfile(mbemf))
        if t.failed():
            return out

        badfmt = []
        badurl = []
        replicated = []
        found = set()
        foundme = False
        last = None
        with self.bag.open_text_file(mbemf) as fd:
            i = 0
            for line in fd:
                if not line.strip():
                    continue
                i += 1
                parts = [f.strip() for f in line.strip().split('\t')]
                last = parts[0]
                if last == self.bag._name:
                    foundme = True
                if last in found:
                    replicated.append(i)
                else:
                    found.add(last)
                if len(parts) > 1 and len(parts[1]) > 0:
                    url = urlparse(parts[1])
                    if not url.scheme or url.netloc:
                        badurl.append(i)

        t = self._issue("3.1-1", "member-bags.tsv lines must match "+
                        "format, BAGNAME[\tURL][\t...]")
        comm = None
        if badfmt:
            s = (len(badfmt) > 1 and "s") or ""
            if len(badfmt) > 4:
                badfmt[3] = '...'
                badfmt = badfmt[:4]
            comm = "line{0} {1}".format(s, ", ".join([str(b) for b in badfmt]))
        out._err(t, len(badfmt) == 0, comm)

        t = self._issue("3.1-2", "member-bags.txt: URL field must be an "+
                        "absolute URL")
        comm = None
        if badurl:
            s = (len(badurl) > 1 and "s") or ""
            if len(badurl) > 4:
                badurl[3] = '...'
                badurl = badurl[:4]
            comm = "line{0} {1}".format(s, ", ".join([str(b) for b in badurl]))
        out._err(t, len(badurl) == 0, comm)

        t = self._issue("3.1-3", "member-bags.tsv must list current bag name")
        out._err(t, foundme)

        t = self._issue("3.1-4", "member-bags.tsv: Head bag must be "+
                        "listed last")
        out._err(t, last == self.bag._name)

        t = self._issue("3.1-5", "member-bags.tsv: a bag name should only be "+
                        "listed once")
        comm = None
        if len(replicated) > 0:
            s = (len(replicated) > 1 and "s") or ""
            if len(replicated) > 4:
                replicated[3] = '...'
                replicated = replicated[:4]
            comm= "line{0} {1}".format(s,", ".join([str(b) for b in replicated]))
        out._warn(t, len(replicated) == 0, comm)

        return out

   
        