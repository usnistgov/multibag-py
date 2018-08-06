"""
This module provides access to the informational content of a multibag
aggregation.  In particular, it provides the read-only HeadBag class.
"""
from __future__ import absolute_import
import re
from collections import OrderedDict

from .bagit import ReadOnlyBag, open_bag
from .exceptions import MultibagError
from .extended import (ExtendedReadMixin, ExtendedReadOnlyBag,
                       ExtendedReadWritableBag)
from ..constants import Version

_spre = re.compile(r' +')

class MemberInfo(object):
    """
    a description of a member bag of a multibag aggregation as given by a 
    line in a member-bags.tsv file.
    """

    def __init__(self, name, uri=None, comment=None, *info):
        """
        initialize the information about a member bag
        :param str name:   the name of the member bag
        :param str uri:    a URI for the member bag, providing a global identity
        :param str comment:  a comment about the bag entry
        :param list info:  extra profile-specific metadata about the 
        
        """
        if info is None:
            info = []
        self.name = name
        self.uri = uri
        self.comment = comment
        self.info = list(info)

    @classmethod
    def parse_line_03(self, line):
        """
        parse a line from a member-bags.tsv file according to the multibag
        profile version 0.4, returning a MemberInfo object
        """
        fields = line.strip().split('\t')
        if len(fields) < 1 or not fields[0]:
            raise MultibagError("Multibag syntax error in member-bags.tsv: "+
                                "missing fields: " + line.strip())
        name = fields.pop(0)
        comm = None
        uri = None
        if len(fields) > 0:
            if fields[-1].startswith('#'):
                comm = fields.pop(-1).lstrip('#').strip()
        if len(fields) > 0:
            uri = fields.pop(0)
        
        return MemberInfo(name, uri, comm, *fields)

    @classmethod
    def parse_line_02(self, line):
        """
        parse a line from a member-bags.tsv file according to the multibag
        profile version 0.4, returning a MemberInfo object
        """
        fields = _spre.split(line.strip())
        if len(fields) < 1 or not fields[0]:
            raise MultibagError("Multibag syntax error in group-members.txt: "+
                                "missing fields: " + line.strip())
                                
        name = fields.pop(0)
        uri = None
        if len(fields) > 0:
            uri = fields.pop(0)
        
        return MemberInfo(name, uri)

def parse_file_lookup_line_03(line):
    """
    parse a line from the file_lookup.tsv file according to version 0.3 of 
    the multibag profile specification

    :rtype:  a 2-tuple containing the bag file path and the name of the 
             bag containing that file.  
    """
    out = line.strip().split('\t')
    if len(out) < 2:
        raise MultibagError("Multibag syntax error in file-lookup.tsv: "+
                            "missing bagname field: " + line.strip())
    return (out[0], out[1])
        
def parse_group_directory_line(line):
    """
    parse a line from the file_lookup.tsv file according to version 0.3 of 
    the multibag profile specification

    :rtype:  a 2-tuple containing the bag file path and the name of the 
             bag containing that file.  
    """
    out = _spre.split(line.strip())
    if len(out) < 2:
        raise MultibagError("Multibag syntax error in file-lookup.tsv: "+
                            "missing bagname field: " + line.strip())
    return (out[0], out[1])
    

class HeadBagReadMixin(ExtendedReadMixin):
    """
    an interface to the informational content in the head bag of a multibag 
    aggregation.
    """

    def __init__(self):
        """
        Initialize the extended interface.  
        """
        # subclasses are expected to initialize the ExtendedReadMixin separately;
        # if they do not, NotImplementedError exceptions will be raised when
        # calling certain methods.
        self._memberbags = None
        self._filelu = None

    @property
    def head_version(self):
        """
        the version of the aggregation described by this head bag (i.e. the 
        value of the Multibag-Head-Version info tag).
        """
        return self._get_required_info_item('Multibag-Head-Version')

    @property
    def profile_version(self):
        """
        the version of the multbag profile specification that this head bag 
        claims to adhere to.  
        """
        return self._get_required_info_item('Multibag-Version')

    def _get_required_info_item(self, name):
        try:
            out = self.info[name]
            if isinstance(out, list):
                out = out[-1]
            return out
        except KeyError:
            raise MultibagError("Missing required '" + name + "' info tag")
        except IndexError:
            raise MultibagError("Empty value for '" + name + "' info tag")
        

    def iter_member_bags(self):
        """
        iterate through the contents of the multibag/member_bags.tsv file.  This
        opens the file anew and returns the contents via an iterator that returns
        MemberInfo instances.
        """
        mbdir = self.info.get('Multibag-Tag-Directory', 'multibag')
        vers = Version(self.version)
        membagfile = (vers < "0.3" and "group-members.txt") \
                      or "member-bags.tsv"
        membagpath = "/".join([mbdir, membagfile])
        
        if not self.isfile(membagpath):
            raise MultibagError("Missing "+membagfile+" file")

        parseline = (vers < "0.3" and MemberInfo.parse_line_02) \
                    or MemberInfo.parse_line_03

        with self.open_text_file(membagpath) as fd:
            for line in fd:
                if line.strip():
                    yield parseline(line)

    def member_bags(self, reread=False):
        """
        return the ordered list of bags that make up this multibag aggregation
        as a list of MemberInfo objects
        """
        if reread or not self._memberbags:
            self._memberbags = [m for m in self.iter_member_bags()]
        return self._memberbags

    @property
    def member_bag_names(self):
        """
        the ordered list of bag names that make up this multibag aggregation.
        """
        return [m.name for m in self.member_bags()]

    def iter_file_lookup(self):
        """
        iterate through the contents of the multibag/file-lookup.tsv
        """
        mbdir = self.info.get('Multibag-Tag-Directory', 'multibag')
        vers = Version(self.version)
        membagfile = (vers < "0.3" and "group-directory.txt") \
                      or "file-lookup.tsv"
        membagpath = "/".join([mbdir, membagfile])
        
        if not self.isfile(membagpath):
            raise MultibagError("Missing "+membagfile+" file")

        parseline = (vers < "0.3" and parse_group_directory_line) \
                    or parse_file_lookup_line_03

        with self.open_text_file(membagpath) as fd:
            for line in fd:
                if line.strip():
                    yield parseline(line)

    def lookup_file(self, filepath, reread=False):
        """
        return the name of the bag containing the given file 

        :param str  filepath:  the bag-root-relative path to the desired file
        :param bool reread:    If True, re-cache the lookup table from the 
                               contents of the multibag/file-lookup.tsv file.
        """
        if reread or not self._filelu:
            self._filelu = OrderedDict(self.iter_file_lookup())
        return self._filelu.get(filepath)

    def iter_deleted(self):
        """
        iterate through the file paths listed in the multibag/deleted.txt file.
        """
        raise NotImplementedError()

    def deleted_paths(self, reread=False):
        """
        return the file paths that should be deleted when the multibag 
        aggregation is recombined into a single bag.
        """
        if reread or self._deleted is None:
            self._deleted = set(self.iter_deleted())
        return self._deleted

class HeadBagUpdateMixin(ExtendedReadMixin):
    """
    an interface for updating the informational content in a head bag of a 
    multibag aggregation.
    """

    def __init__(self):
        """
        Initialize the extended interface.  
        """
        # subclasses are expected to initialize the ExtendedReadMixin separately;
        # if they do not, NotImplementedError exceptions will be raised when
        # calling the methods.
        pass

    

class ReadOnlyHeadBag(ExtendedReadOnlyBag, HeadBagReadMixin):
    """
    A representation of head bag for a multibag aggregation, opened in 
    a read-only state.  

    The underlying bag may be in a serialized form.  To open a serialized bag, 
    the factory function open_headbag() is recommended instead of instantiating 
    this class directly.
    """

    def __init__(self, bagpath, name=None):
        """
        open the bag with the given location
        :param bagpath:  either a Path instance or a filepath to the bag's 
                         root directory.  A Path instance must be used if the 
                         bag is in a serialized form.  
        :type bagpath:   str or Path
        """
        super(ReadOnlyHeadBag, self).__init__(bagpath, name)
        HeadBagReadMixin.__init__(self)

class _NoUpdateHeadBag(ExtendedReadWritableBag, HeadBagReadMixin):
    """
    A representation of head bag for a multibag aggregation, opened in 
    a un-updateable state.  

    This class is usually not directly instantiated by the user.  It is 
    provided to add only the head bag read interface to a bag that is 
    otherwise updateable.
    """

    def __init__(self, bagpath):
        """
        open the bag with the given root directory

        :param str bagpath:  a filepath to the bag's root directory.  
        """
        super(HeadBag, self).__init__(bagpath)
        HeadBagReadMixin.__init__(self)

    
class HeadBag(ExtendedReadWritableBag, HeadBagReadMixin, HeadBagUpdateMixin):
    """
    A representation of head bag for a multibag aggregation, opened in 
    a updateable state.  

    The underlying bag can not be in a serialized form as such is supported 
    only in a read-only state.  To open a serialized bag, use the 
    the factory function open_headbag().
    """

    def __init__(self, bagpath):
        """
        open the bag with the given root directory

        :param str bagpath:  a filepath to the bag's root directory.  
        """
        super(HeadBag, self).__init__(bagpath)
        HeadBagReadMixin.__init__(self)
        HeadBagUpdateMixin.__init__(self)

def is_head_bag(bag):
    """
    return True if this bag is a designated as the head bag of a multibag
    aggregation.  This implementation returns True if the 
    'Multibag-Head-Version' is set.  
    """
    return 'Multibag-Head-Version' in bag.info

def as_headbag(bag, readonly=False):
    """
    extend the interface of the given bag instance with methods for managing
    head bags.  The input bag instance is returned with its class updated.  

    Note that it is not an error to apply this method to a bag (serialized 
    or otherwise) that is not designated as such according to the Multibag 
    profile spec.  In such a case, head bag read methods may raise a 
    MultibagError.

    :param Bag bag:        the Bag instance to extend
    :param bool readonly:  do not include methods for updating the head bag
                           data.  Note that if the input bag is of type 
                           ReadOnlyBag (e.g. it is in a serialized form), 
                           the update methods will not be included.
    """
    if not isinstance(bag, Bag):
        raise ValueError("as_headbag(): input not of type Bag: " + str(bag))

    if not isinstance(bag, ExtendedReadMixin):
        as_extended(bag)

    if not isinstance(bag, HeadBagReadMixin):
        if isinstance(bag, ExtendedReadOnlyBag):
            bag.__class__ = ReadOnlyHeadBag
        else:
            # isinstance(bag, ExtendedReadWritableBag) == True
            bag.__class__ = _NoUpdateHeadBag
        HeadBagReadMixin.__init__()

    if not isinstance(bag, HeadBagUpdateMixin) and not readonly and \
       not isinstance(ExtendedReadOnlyBag):
        bag.__class__ = HeadBag
        HeadBagUpdateMixin.__init__()

    return bag


