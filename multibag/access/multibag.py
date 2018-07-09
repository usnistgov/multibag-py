"""
This module provides access to the informational content of a multibag
aggregation.  In particular, it provides the read-only HeadBag class.
"""
from __future__ import absolute_import

from .bagit import ReadOnlyBag, open_bag
# from .validate import MultibagValidationError

class HeadBag(ReadOnlyBag):
    """
    an interface to the informational content in the head bag of a multibag 
    aggregation.
    """

    @property
    def version(self):
        """
        the version of the aggregation described by this head bag (i.e. the 
        value of the Multibag-Head-Version info tag).
        """
        try:
            return self.info['Multibag-Head-Version']
        except KeyError:
            raise MultibagValidationError("Missing required 'Multibag-Head-" +
                                          "Version' info tag")

