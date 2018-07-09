"""
This module supports use of the multibag profile for amending an existing bag
or multibag aggregation.   It also can update an existing bag to convert it to
a head bag for a single-bag aggregation.
"""
import os, sys, re
from collections import OrderedDict

from .constants import CURRENT_VERSION, CURRENT_REFERENCE
from .access.bagit import Bag

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

ABOUT_MBAG = "This bag complies with the Multibag BagIt profile.  For more information, refer to the URL given by Multibag-Reference tag."


class SingleMultibagMaker(object):
    """
    This class collects operations for turning a standard bag into a head 
    bag for a single-bag aggregation.
    """
    def __init__(self, bagdir, multibag_tagdir="multibag"):
        """
        Initialize the object that will convert a bag with a given root 
        directory.
        """
        self.tagdir = multibag_tagdir
        if not os.path.exists(bagdir):
            raise OSError(2, "Directory not found: "+bagdir, bagdir)
        self.bagdir = bagdir

    def update_info(self, version="1", profver=CURRENT_VERSION):
        """
        update the bag info tag data to include head bag information.

        :param str version:  a version string to set as the version of the 
                             aggregation.  
        """
        bag = Bag(self.bagdir)

        bag.info['Multibag-Version'] = profver
        bag.info['Multibag-Tag-Directory'] = self.tagdir
        bag.info['Multibag-Head-Version'] = version
        bag.info['Multibag-Reference'] = CURRENT_REFERENCE

        if 'Internal-Sender-Description' in bag.info:
            if not isinstance(bag.info['Internal-Sender-Description'], list):
                bag.info['Internal-Sender-Description'] = \
                     [ bag.info['Internal-Sender-Description'] ]
            bag.info['Internal-Sender-Description'].append( ABOUT_MBAG )
        else:
            bag.info['Internal-Sender-Description'] = ABOUT_MBAG

        if 'Bag-Count' in bag.info:
            del bag.info['Bag-Count']
        if 'Bag-Size' in bag.info:
            del bag.info['Bag-Size']
        bag.save()
        bag.info['Bag-Size'] = self._bag_size()
        bag.save()

    def ensure_tagdir(self):
        """
        make sure the tag directory for the special multibag files exists
        """
        mbdir = os.path.join(self.bagdir, self.tagdir)
        if not os.path.exists(mbdir):
            os.mkdir(mbdir)

    def write_member_bags(self, pid=None):
        """
        write the member-bags.tsv file for a single-bag aggregation.  The 
        PID, if provided, should be set to resolve to this bag (usually a
        serialized form for it).
        :param str pid:  Persistent ID to associate with this bag.
        :raises OSError: if there is a failure to open the output file or
                         otherwise write the data.  
        """
        self.ensure_tagdir()

        tagfile = os.path.join(self.bagdir, self.tagdir, 'member-bags.tsv')
        bagname = os.path.basename(self.bagdir)
        with open(tagfile, 'w') as fd:
            fd.write(bagname)
            if pid:
                fd.write('\t'+pid)
            fd.write('\n')

    def write_file_lookup(self, include=None, exclude=None, trunc=False):
        """
        write the file-lookup.tsv file.  This function can be called multiple
        times as the file paths will be appended to the existing file-lookup.tsv
        unless trunc=True.  

        :param list include:  a list of directories or files that should be 
                              included.  If not provided, a default value of 
                              ['data'] will be assumed.  Each element must be 
                              a path relative to the bag's root directory.  If 
                              an entry is a directory, all files under that 
                              directory (recursively) will be included except
                              any whose path matches an entry in the exclude
                              list parameter.  
        :param list exclude:  a list of files or directories to exclude.  The
                              include list will over-ride those listed in this
                              list, unless the corresponding include entry is a
                              directory.
        :param boolean trunc: if False, append specified file paths to the end
                              of an existing file-lookup.tsv (creating the 
                              file if necessary); if True, discard any existing
                              content of the file-lookup.tsv before adding paths.
        """
        if include is None:
            include = ['data']
        elif isinstance(include, (str,_unicode)):
            include = [include]
        elif not isinstance(include, list):
            raise TypeError("write_file_lookup(): include not a type: "+
                            str(include))
        if exclude is None:
            exclude = []
        elif isinstance(exclude, (str,_unicode)):
            include = [exclude]
        elif not isinstance(exclude, list):
            raise TypeError("write_file_lookup(): include not a type: "+
                            str(include))

        self.ensure_tagdir()

        lu = self._load_file_lookup()
        if trunc:
            lu.clear()

        bagname = os.path.basename(self.bagdir)
        for incl in include:
            # skip this include item if it explicitly excluded
            if incl in exclude:
                continue
            
            root = os.path.join(self.bagdir, incl)
            if os.path.isfile(root):
                lu[incl] = [bagname]

            elif os.path.isdir(root):
                for d, subdirs, files in os.walk(root):
                    path = d[len(self.bagdir)+1:]
                    if path and path in exclude:
                        for i in range(len(subdirs)):
                            subdirs.pop(0)
                        continue
                    for f in files:
                        f = os.path.join(path, f)
                        if f in exclude:
                            continue
                        lu[f] = [bagname]
                        
            # else does not exist in bag; skip it.

        self._write_file_lookup(lu)

    def _load_file_lookup(self):
        # Return an OrderedDict that contains the content of the
        # file-lookup.tsv file.  The keys are the paths to the files, and
        # values are lists containing the remaining fields of the file.
        # The first value in that array is the bagname.
        out = OrderedDict()
        mbfile = os.path.join(self.bagdir, self.tagdir, "file-lookup.tsv")
        if not os.path.exists(mbfile):
            return out

        with open(mbfile) as fd:
            for line in fd:
                line = line.strip()
                if not line:
                    continue
                flds = [f.strip() for f in line.split('\t')]
                if len(flds) < 2:
                    # bad format; skip (for now)
                    continue
                out[flds[0]] = flds[1:]

        return out

    def _write_file_lookup(self, ludata):
        mbfile = os.path.join(self.bagdir, self.tagdir, "file-lookup.tsv")
        with open(mbfile, 'w') as fd:
            for path in ludata:
                fd.write(path)
                fd.write("\t")
                fd.write("\t".join(ludata[path]))
                fd.write("\n")

    def convert(self, version="1", pid=None):
        """
        convert the attached bag to single multibag aggregation.  This calls, 
        in succession, write_member_bags(), write_file_lookup(), 
        and update_info() to complete the conversion.  

        :param str version:  a version string to set as the version of the 
                             aggregation.  
        :param bool payload_only:  if True, the file-lookup.tsv file will only 
                                   contain the payload files.
        """
        self.write_member_bags(pid)
        self.write_file_lookup(trunc=True)

        # This (re-)calculates Bag-Size due to the new multibag tag files
        self.update_info(version)

    def _bag_size(self):
        size = 0
        for root, subdirs, files in os.walk(self.bagdir):
            for f in files:
                size += os.stat(os.path.join(root,f)).st_size

        out = self._format_bytes(size)
        size += len("Bag-Size: {0}".format(size))
        return self._format_bytes(size)

    def _format_bytes(self, nbytes):
        prefs = ["", "k", "M", "G", "T"]
        ordr = 0
        while nbytes >= 1000.0 and ordr < 4:
            nbytes /= 1000.0
            ordr += 1
        pref = prefs[ordr]
        ordr = 0
        while nbytes >= 10.0:
            nbytes /= 10.0
            ordr += 1
        nbytes = str(round(nbytes, 3) * 10**ordr)
        if '.' in nbytes:
            nbytes = re.sub(r"0+$", "", nbytes)
        if nbytes.endswith('.'):
            nbytes = nbytes[:-1]    
        return "{0} {1}B".format(nbytes, pref)

        
