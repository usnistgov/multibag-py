"""
This module provides the validator implementation for validating head bags.
"""
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

        
