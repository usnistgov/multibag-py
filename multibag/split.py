"""
Tools for splitting a single bag (a ProgenitorBag) into a set of 
Multibag-compliant bags.
"""
import os, sys, re, shutil, codecs
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from copy import deepcopy
from functools import cmp_to_key

from .access.bagit import Bag, ReadOnlyBag
from bagit import _parse_tags

from fs.copy import copy_file
from fs import open_fs

_bagsepre = re.compile(r'/')
_ossepre = re.compile(os.sep)

MBAG_VERSION = "0.3"

MBAG_INTERNAL_SENDER_DESC = \
"""This bag is part of a Multibag aggregation. (See Multibag-Reference for a 
   description of the Multibag profile.)  The aggregation was formed by splitting
   the source bag (whose ID is set as the Bag-Group-Identifier) into multibag 
   member bags."""

PMAN = "manifest"
TMAN = "tagmanifest"

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

class ProgenitorMixin(object):
    """
    a complete bag that can be split into set of multibag-compliant bags.

    A ProgenitorBag gives access that the source bag content, and so is 
    read-only.  
    """
    __metaclass__ = ABCMeta

    _is_readonly = True

    def __init__(self):
        super(ProgenitorMixin, self).__init__()

    @property
    def is_readonly(self):
        """
        returns True if this bag is set to be read-only.  
        """
        return self._is_readonly

    @property
    def name(self):
        """
        the name of the root directory of the bag (without any parent path
        included).
        """
        return self._get_bag_name()

    @abstractmethod
    def _get_bag_name(self):
        raise NotImplementedError()

    @abstractmethod
    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplementedError()

    @abstractmethod
    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplementedError()

    @abstractmethod
    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplementedError()

    @abstractmethod
    def walk(self):
        """
        Walk the source bag contents returning the triplets returned by
        os.path.  The difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.
        """
        raise NotImplementedError()

    @abstractmethod
    def replicate(self, path, destdir):
        """
        copy a file from the source bag to the same location in an output bag.

        :param str path:  the path, relative to the source bag's root directory,
                          to the file to be replicated.
        :param str destdir:  the destination directory.  This is usually the 
                          root directory of another bag.  
        """
        raise NotImplementedError()

    @abstractmethod
    def open_text_file(self, path, encoding='utf-8-sig'):
        """
        return a open, read-only file object on the file from the source bag 
        with the given path.

        :param str path:     the path to the file to open, relative to the source
                             bag's root directory.
        :param str encoding  a label indicating the encoding to expect in the 
                             file.  (Default: "utf-8-sig").
        """
        raise NotImplementedError()

    def nonstandard(self):
        """
        iterate through the non-standard files in this bag.  This will 
        exclude the special files defined by the BagIt standard (bagit.txt,
        bag-info.txt, fetch.txt and the manifest files).  It will include
        any directory in the bag that is empty (to allow it to be replicated
        in output multibags).
        """
        special = [re.compile(r) for r in
                       r"^bagit.txt$ ^bag-info.txt$ ^fetch.txt$".split() +
                       r"^(tag)?manifest-(\w+).txt$".split()]

        for dir, subdirs, files in self.walk():
            if len(subdirs) == 0 and len(files) == 0:
                # spit out a directory if it is empty
                yield dir
            for file in files:
                # don't spit out a file is it's one of the special ones
                if not any([bool(spf.match(file)) for spf in special]):
                    yield os.path.join(dir, file)

    def _load_bag_info(self):
        # this (re-)reads the bag-info data, loading it into an OrderedDict

        tags = OrderedDict()
        with self.open_text_file(self.tag_file_name) as fd:
            for name, value in _parse_tags(fd):
                if name not in tags:
                    tags[name] = value
                    continue

                if not isinstance(tags[name], list):
                    tags[name] = [tags[name], value]
                else:
                    tags[name].append(value)

        self.info = tags
        

class LocalDirProgenitor(ProgenitorMixin):
    """
    An implementation of the ProgenitorMixin for a Bag that is stored as 
    a directory on a local filesystem.  
    """
    is_readonly = False

    def __init__(self, bagdir):
        self._bagdir = bagdir.rstrip(os.sep)
        self._bagname = os.path.basename(bagdir)
        self._replwhl = True
        super(LocalDirProgenitor, self).__init__()

    @property
    def replicate_with_hardlink(self):
        """
        a flag indicating, if True, that requests to replicate a file will be 
        by attempted by creating a hard link at the destination to the original
        file.  Doing so will save space on disk because bytes are not replicated.
        This is not supported on Windows.  On UNIX-like systems, hard links are 
        only possible when the source and destinations are on the same filesystem.
        When this is not the case, replicate falls back to a true copy.  
        """
        return self._replwhl

    @replicate_with_hardlink.setter
    def replicate_with_hardlink(self, yes):
        """
        set the flag with a True or False
        """
        self._replwhl = bool(yes)

    def _get_bag_name(self):
        return self._bagname

    def _canon_path(self, path):
        if os.sep != '/':
            path = _bagsepre.sub(os.sep, path)
        path = os.path.normpath(os.path.join(self._bagdir, path))
        if not path.startswith(os.path.normpath(self._bagdir)):
            return None
        return path

    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        path = self._canon_path(path)
        if path is None:
            return False
        return os.path.exists(path)

    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        path = self._canon_path(path)
        if path is None:
            return False
        return os.path.isdir(path)

    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        path = self._canon_path(path)
        if path is None:
            return False
        return os.path.isfile(path)

    def replicate(self, path, destdir, logger=None):
        """
        copy a file from the source bag to the same location in an output bag.

        This implementation will replicate the file as a hard link if possible 
        when self.replicate_with_hardlink is True.  Otherwise, a normal file 
        copy is done.  

        :param str path:  the path, relative to the source bag's root directory,
                          to the file to be replicated.
        :param str destdir:  the destination directory.  This is usually the 
                          root directory of another bag.  
        :param Logger logger: a logger instance to send messages to.
        :raises ValueError:  if the given file path doesn't exist in the source
                          bag.
        """
        if not self.exists(path):
            raise ValueError("replicate: file/dir does not exist in this bag: " +
                             path)
        
        destpath = os.path.join(destdir, path)
        if self.isdir(path):
            if not os.path.isdir(destpath):
                if logger:
                    logger.info("Creating matching directory in output bag: "
                                     +path)
                os.makedirs(destpath)
            return
        
        parent = os.path.dirname(destpath)
        hardlink = self.replicate_with_hardlink
        source = os.path.join(self._bagdir, path)
        
        if not os.path.exists(parent):
            if logger:
                logger.debug("Creating output file's parent directory: " +
                                  parent)
            os.makedirs(parent)

        if hardlink:
            try:
                os.link(source, destpath)
            except OSError as ex:
                hardlink = False
                if logger:
                    msg = "Unable to create hard link for data file (" + path + \
                          "): " + str(ex) + "; swithching to copy mode."
                    self.logger.warning(msg)

        if not hardlink:
            try:
                shutil.copy(source, destpath)
            except Exception as ex:
                if logger:
                    msg = "Unable to copy data file (" + path + \
                          ") into bag (" + destdir + "): " + str(ex)
                    self.log.exception(msg, exc_info=True)
        
    def open_text_file(self, path, encoding='utf-8-sig'):
        """
        return a open, read-only file object on the file from the source bag 
        with the given path.

        :param str path:     the path to the file to open, relative to the source
                             bag's root directory.
        :param str encoding  a label indicating the encoding to expect in the 
                             file.  (Default: "utf-8-sig").
        """
        path = self._canon_path(path)
        return codecs.open(path, encoding=encoding);

        
    def walk(self):
        """
        Walk the source bag contents returning the triplets returned by
        os.path with two differences.  

        The first difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.

        The second difference is that the path separator will always be a 
        forward slash ('/'), consistent with the BagIt standard.
        """
        for dir, subdirs, files in os.walk(self._bagdir):
            # make dir relative to bag's root directory
            if dir.startswith(self._bagdir):
                dir = dir[len(self._bagdir):].lstrip(os.sep)
            dir = _ossepre.sub('/', dir)
            
            yield dir, subdirs, files

# _ProgenitorBagCls = type("ProgenitorBag", (Bag, LocalDirProgenitor), {})
# _ROProgenitorBagCls = type("ROProgenitorBag", (Bag, FSProgenitor), {})

class _LocalProgenitorBag(Bag, LocalDirProgenitor):
    """
    A BagIt bag representing the progenitor bag for a multibag aggregation

    This class should not be instantiated directly; rather asProgenitor()
    should be called.
    """
    pass

# class ReadOnlyProgenitorBag(ReadOnlyBag, FSProgenitor):

class ReadOnlyProgenitor(ProgenitorMixin):
    """
    An implementation of the ProgenitorMixin for a Bag that is accessible 
    only via the ReadOnlyBag interface.  
    """
    is_readonly = True

    def __init__(self):
        super(ReadOnlyProgenitor, self).__init__()

    def _get_bag_name(self):
        return self._name

    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        if path is None:
            return False
        return self._root.relpath(path).exists()

    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        if path is None:
            return False
        return self._root.relpath(path).isdir()

    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param str path:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        if path is None:
            return False
        return self._root.relpath(path).isfile()

    def replicate(self, path, destdir, logger=None):
        """
        copy a file from the source bag to the same location in an output bag.

        :param str path:  the path, relative to the source bag's root directory,
                          to the file to be replicated.
        :param str destdir:  the destination directory.  This is usually the 
                          root directory of another bag.  
        :param Logger logger: a logger instance to send messages to.
        :raises ValueError:  if the given file path doesn't exist in the source
                          bag.
        """
        path = _unicode(path)
        if not self.exists(path):
            raise ValueError("replicate: file/dir does not exist in this bag: " +
                             path)

        destfs = open_fs(destdir)
        if self.isdir(path):
    
            if not destfs.isdir(path):
                if logger:
                    logger.info("Creating matching directory in output bag: "
                                     +path)
                destfs.makedirs(path, recreate=True)
            return

        parent = os.path.dirname(path)
        if parent and not destfs.exists(parent):
            if logger:
                logger.debug("Creating output file's parent directory: " +
                             parent)
            destfs.makedirs(parent)

        copy_file(self._root.fs, path, destfs, path)

    def open_text_file(self, path, encoding='utf-8-sig'):
        """
        return a open, read-only file object on the file from the source bag 
        with the given path.

        :param str path:     the path to the file to open, relative to the source
                             bag's root directory.
        :param str encoding  a label indicating the encoding to expect in the 
                             file.  (Default: "utf-8-sig").
        """
        return self._root.fs.open(path, encoding=encoding)

    def walk(self):
        """
        Walk the source bag contents returning the triplets returned by
        os.path with two differences.  

        The first difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.

        The second difference is that the path separator will always be a 
        forward slash ('/'), consistent with the BagIt standard.
        """
        witer = self._root.fs.walk.walk()
        while True:
            root, dirs, files = next(witer)
            root = root.lstrip('/')
            yield root, [d.name for d in dirs], [f.name for f in files]

class _ReadOnlyProgenitorBag(ReadOnlyBag, ReadOnlyProgenitor):
    """
    A ReadOnlyBag representing a progenitor bag for a multibag aggregation

    This class should not be instantiated directly; rather asProgenitor()
    should be called.
    """
    pass
    
def asProgenitor(bag):
    """
    add the ProgenitorMixin interface to the given bag.  The input bag instance 
    is returned with its class updated.
    """
    if not isinstance(bag, Bag):
        raise ValueError("asProgenitor(): input not of type Bag: " + str(bag))

    # already a progenitor; just return it
    if isinstance(bag, ProgenitorMixin):
        return bag

    # input is a ReadOnlyBag, access via an fs instance (e.g. zip file, tar ball,
    # ...).  Convert to _ReadOnlyProgenitorBag.
    if isinstance(bag, ReadOnlyBag):
        bag.__class__ = _ReadOnlyProgenitorBag
        ReadOnlyProgenitor.__init__(bag)
        return bag

    # input is a normal bag
    if not os.path.exists(os.path.join(bag.path, "bagit.txt")):
        raise ValueError("Unsupported Bag implementation: "+str(type(bag)))

    bag.__class__ = _LocalProgenitorBag
    LocalDirProgenitor.__init__(bag, bag.path)
    return bag
    

class SplitPlan(object):
    """
    a description of how to distribute the payload and metadata files 
    from a progenitor bag across multiple output multibags.  

    The plan is specific to a given source bag, and when it's complete, it 
    can be applied to the source bag to create the output multibags via 
    the `apply_iter()` function.  
    """

    def __init__(self, source):
        """
        create an empty plan for splitting a given source bag
        """
        if not isinstance(source, Bag):
            raise ValueError("SplitPlan(): source is not a Bag instance")
        if not isinstance(source, ProgenitorMixin):
            source = asProgenitor(source)
        self.progenitor = source
        self._manifests = []
        self.head_version = "1"
        self._deprecates = []

    def manifests(self):
        """
        return an iterator for iterating over the manifest descriptions that 
        prescribe the contents of each output multibag.
        """
        if len(self._manifests) == 0:
            raise StopIteration()
        
        i = 0
        while i < len(self._manifests):
            out = deepcopy(self._manifests[i])
            out['ishead'] = (i == len(self._manifests)-1)
            yield out
            i += 1

    def is_complete(self):
        """
        return True if the plan is set to produce output multibags that 
        will include all files from the source bag.  
        """
        try:
            next(self.missing())
            return False
        except StopIteration:
            return True

    def missing(self):
        """
        return an iterator to the paths (relative to the base directory) of the 
        files and directories in the source bag that are not yet included in the 
        plan (i.e. any of the output multibags).  A directory is included in 
        the list only if it appears in the source bag as an empty directory.  
        """
        for file in self.required():
            outbag = self.find_destination(file)
            if not outbag:
                yield file

    def required(self):
        """
        return an iterator to the paths (relative to the base directory) of the
        files and directories in the source bag that should be part of a split 
        plan.  A directory is included in the list only if it appears in the 
        source bag as an empty directory.  
        """
        return self.progenitor.nonstandard()

    def _path_in(self, path, manifest):
        # return true if the path can be found in the given plan manifest
        return path in manifest['contents']

    def find_destination(self, path):
        """
        return the dictionary describing the output multibag that will 
        contain the file (or empty directory) with the given path.  If 
        this plan has not assigned this path to an output file, then 
        None is returned.  If the path is listed for more than one output
        bag, the last one it appears in will be returned.  
        """
        for m in reversed(self._manifests):
            if self._path_in(path, m):
                return m
        return None

    def name_output_bags(self, naming_iter, reverse=False):
        """
        set the names of the output bags using the given naming iterator.  
        A naming iterator is an iterator that emits a sequence of bag names
        that should be given to the output multibag names.  Thse names will
        be attached to the manifests.  
        :param iter naming_iter:   an iterator whose next() method emits a 
                                   sequence of bag names.
        :param bool reverse:       if True, the names should be assigned to 
                                   the manifests in reverse order (i.e. with
                                   the head bag getting the first name in 
                                   the sequence).  
        """
        use = self._manifests
        if reverse:
            use = reversed(use)
        try:
            for m in use:
                name = next(naming_iter)
                self._set_manifest_name(m, name)
        except StopIteration as ex:
            after = (name and (" (after %s)" % name)) or ""
            raise RuntimeError("Naming iterator exhausted prematurely"+after)

    def _set_manifest_name(self, manifest, name):
        manifest['name'] = name
        return manifest

    def complete_plan(self):
        """
        if there are any files missing from the current set of manifests (as 
        determined by missing()), add one or more additional manifests to the 
        plan to capture the missing files.  If no files are missing, this 
        method returns without changing the current plan; thus, there is no 
        harm in calling it.

        This implementation will just create a single additional output manifest 
        if there are any missing files.  Subclasses may split the missing files 
        over multiple additional output bags.  
        """
        man = {
            "name": "{0}_{1}.mbag".format(self.progenitor.name,
                                          len(self._manifests)),
            "contents": list(self.missing())
        }
        if man['contents']:
            self._manifests.append(man)

    def apply_iter(self, outdir, naming_iter=None, logger=None):
        """
        iteratively apply the plan by writing split bags to a given directory.
        This method returns an iterator that iterates through the plan 
        manifests; with each call to its next() method, it writes out the next
        multibag member bag to the output directory and returns a full path to 
        the member.  Each member bag is given a name set either by default, 
        or as set by a previous call to name_output_bags(), or via a naming 
        iterator provided as an argument to this method.  

        The caller is allowed to modify the newly written bag between calls to
        the iterator--e.g. to serialize it or move it to other storage.  

        :param str outdir: the directory to write the split bags to.  
        :param iter naming_iter:  a naming iterator to use to name the bags.  If
                           None, the output bags will retain their currently set
                           names (which may have been set by default or via 
                           name_output_bags()).  See name_output_bags() for 
                           the requirements of a naming iterator.  
        :param Logger logger: a logger instance to send messages to.
        :return iterator: an iterator whose next() method will write the next
                          member bag and returns its output path.  StopIteration
                          is raised when there are no more output bags to write.
        """
        if not self.manifests:
            if logger:
                logger.warn("Requested plan execution, but no manifests are set")
            raise RuntimeError("No manifests set for output bags")

        def _write_item(fd, key, vals):
            if not isinstance(vals, list):
                vals = [vals]
            for v in vals:
                fd.write("%s: %s\n" % (key,v))
            

        filedest = OrderedDict()
        memberbags = []
        
        for m in self.manifests():
            bagname = m.get('name')
            if naming_iter:
                try:
                    bagname = naming_iter.next()
                except StopIteration as ex:
                    if logger and not m.get('ishead'):
                        logger.warn("naming iterator ran out of names before "+
                                    "output bags")
                    naming_iter = None
            memberbags.append(bagname)

            # create the bag directory and its data subdirectory
            bagdir = os.path.join(outdir, bagname)
            os.mkdir(bagdir)
            os.mkdir(os.path.join(bagdir,"data"))

            # create empty manifest files
            for mf in self.progenitor.manifest_files():
                with open(os.path.join(bagdir, os.path.basename(mf)), 'w') as fd:
                    pass

            self.progenitor.replicate("bagit.txt", bagdir)

            payload_count = 0
            payload_size = 0
            for file in m['contents']:

                if not self.progenitor.exists(file):
                    if logger:
                        logger.warn("Plan for %s calls for non-existent file, %s",
                                    bagname, file)
                    continue

                opath = os.path.join(bagdir, file)
                if self.progenitor.isdir(file):
                    if not os.path.exists(opath):
                        if logger:
                            logger.info("Creating (empty) directory, %s", file)
                        os.makedirs(os.path.join(bagdir, file))

                else:
                    if logger:
                        logger.info("Replicating file, %s", file)

                    # copy the file to the output bag
                    parent = os.path.dirname(opath)
                    if not os.path.exists(parent):
                        os.makedirs(parent)
                    self.progenitor.replicate(file, bagdir)
                    if not os.path.isfile(os.path.join(bagdir,file)):
                        raise RuntimeError("Failed to replicate file in output "+
                                           "bag, "+bagname+": "+file)

                    # copy its hash, if it has one recorded
                    if file.startswith("data"+os.sep):
                        payload_count += 1
                        payload_size += \
                                os.stat(os.path.join(bagdir,file)).st_size
                        hd = self.progenitor.payload_entries()
                        mantype = PMAN
                    else:
                        hd = self.progenitor.tagfile_entries()
                        mantype = TMAN
                    if file in hd:
                        hashes = hd[file]
                        for alg in hashes:
                            self._record_hash(bagdir, alg, hashes[alg],
                                              file, mantype)
                        
                    filedest[file] = bagname
                    
            # create the bag-info file
            outinfo = os.path.join(bagdir, "bag-info.txt")
            with open(outinfo, 'w') as fd:
                _write_item(fd, 'Multibag-Version', MBAG_VERSION)
                for name, vals in self.progenitor.info.items():
                    if name.startswith('Multibag-'):
                        continue
                    
                    if not isinstance(vals, list):
                        vals = [vals]
                    if name == 'Internal-Sender-Identifier':
                        _write_item(fd, name, bagname)
                        name = 'Multibag-Source-'+name
                    elif name == 'Internal-Sender-Description':
                        _write_item(fd, name, m.get(name,
                                                    MBAG_INTERNAL_SENDER_DESC))
                        name = 'Multibag-Source-'+name
                    elif name == 'External-Identifier':
                        _write_item(fd, 'Multibag-Source-'+name, vals)
                        _write_item(fd, name, vals[0]+'/mbag:'+bagname)
                        name = 'Bag-Group-Identifier'
                    elif name == 'Payload-Oxum':
                        vals = ["%s.%s" % (payload_size, payload_count)]

                    _write_item(fd, name, vals)

                _write_item(fd, "Multibag-Tag-Directory", "multibag")
                if m.get('ishead'):
                    # this is the head bag!
                    _write_item(fd, "Multibag-Head-Version", self.head_version)
                    if self._deprecates:
                        name = "Multibag-Head-Deprecates"
                        for p in self._deprecates:
                            v = p[0]
                            if len(p) > 1:
                                v += ",%s" % p[1]
                            _write_item(fd, name, v)

            # create the multibag files
            if m.get('ishead'):
                mbagdir = os.path.join(bagdir, "multibag")
                os.mkdir(mbagdir)

                with open(os.path.join(mbagdir, "member-bags.tsv"), 'w') as fd:
                    for bag in memberbags:
                        fd.write(bag)
                        fd.write('\n')
                with open(os.path.join(mbagdir, "file-lookup.tsv"), 'w') as fd:
                    for f in filedest:
                        fd.write("%s\t%s\n" % (f, filedest[f]))
                with open(os.path.join(mbagdir, "aggregation-info.txt"), 'w') as fd:
                    with self.progenitor.open_text_file("bag-info.txt") as ifd:
                        for f in ifd:
                            fd.write(f)
                

            yield bagdir


    def _record_hash(self, outdir, alg, hash, path, mantype):
        if mantype not in "manifest tagmanifest".split():
            raise ValueError("Unknown manifest type: "+mantype)

        manfile = "{0}-{1}.txt".format(mantype, alg)
        manpath = os.path.join(outdir, manfile)
        if not os.path.exists(manpath):
            open(manpath, 'w').close()
        with open(manpath, 'a') as fd:
            fd.write(hash)
            fd.write(' ')
            fd.write(path)
            fd.write('\n')

class Splitter(object):
    """
    an abstract class for algorithms that can create a SplitPlan given a 
    source bag.  

    A Splitter subclass captures a particular strategy for distributing the 
    files found in a source bag into a set of output multibags.  To create a
    Splitter implementation, one would subclass this abstract base class and 
    over-ride the abstract `_create_plan()` function.  This function captures
    the essential logic of the strategy and uses it to create SplitPlan tailored
    to a given source bag.  
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        initialize the common internal planner data
        """
        pass

    @abstractmethod
    def _create_plan(self, bagpath):
        """
        Create a SplitPlan for the given source bag and apply this splitter's 
        strategy for spliting it into multibags.  Not intended for calling 
        publicly, this function is called by plan() and is intended to contain
        the logic behind the split strategy.  

        :param str bagpath:  the path to the the source bag's root directory
        :return SplitPlan:   a plan that describes how the given bag should 
                             be split into multibags.  
        """
        raise NotImplementedError()

    def plan(self, bagpath, namebasis=None):
        """
        Apply this splitter's strategy for spliting a source bag into multibags
        and return the result as a SplitPlan instance.  

        :param str bagpath:  the path to the the source bag's root directory
        :param str|iter namebasis:  a guide for naming the output files.  If 
                             a str, a default naming scheme is applied using 
                             namebasis as a base name.  Otherwise, it is 
                             a naming iterator; see SplitPlan.name_output_bags()
                             for its requirements.
        :return SplitPlan:   a plan that describes how the given bag should 
                             be split into multibags.  
        """
        if not namebasis:
            namebasis = os.path.splitext(os.path.basename(bagpath))[0]

        out = self._create_plan(bagpath)

        # update the output names
        ni = namebasis
        if isinstance(namebasis, (str, _unicode)):
            ni = SimpleNamer(namebasis)
        out.name_output_bags(ni)

        return out

    def split(self, bagpath, outdir, namebasis=None):
        """
        Split the given bag into multibags according to the strategy of this 
        splitter.  

        :param str bagpath:  the path to the the source bag's root directory
        :param str outdir:   the path to a directory where the output multibags
                             should be written
        :param str|iter namebasis:  a guide for naming the output files.  If 
                             a str, a default naming scheme is applied using 
                             namebasis as a base name.  Otherwise, it is 
                             a naming iterator; see SplitPlan.name_output_bags()
                             for its requirements.
        :return [str]:  a list of the names of the output bags.
        """
        plan = self.plan(bagpath, namebasis)

        out = []
        for mb in plan.apply_iter(outdir):
            ## offer option to serialize

            out.append(mb)

        return out
        

class WellPackedSplitter(Splitter):
    """
    a Splitter that sets a maximum size limit, tries to minimize the total 
    number of bags (each under the size limit).
    """

    def __init__(self, maxsize=60000, targetsize=None, forhead=None):
        """
        Create the splitter based on the "well-packed" algorithm

        :param int maxsize:  the maximum size of an output bag.  No bag will be 
                             bigger than this limit except when a single file
                             exceeds this limit (in this case,  this file will 
                             be placed in its on multibag by itself).
        :param int targetsize:  the preferred size of an output bag.  Bags will 
                             be packed until they just exceed this fize by one
                             file.  The total size will still be kept less than 
                             maxsize. 
        :param list[str] forhead:  a list of file paths that should be reserved 
                             for the head bag.  
        """
        self.maxsz = maxsize
        if not targetsize:
            targetsize = self.maxsz
        self.tsz = targetsize
        if forhead is None:
            forhead = []
        self.forhead = list(forhead)
        for i in range(len(forhead)):
            if not forhead[i].startswith('/'):
                forhead[i] = '/'+forhead[i]
                

    def _create_plan(self, bagpath):
        bag = ReadOnlyBag(bagpath)

        # these files are sorted by size, biggest one first
        finfos = self._sorted_files(bag)
        
        out = SplitPlan(bag)
        self._apply_algorithm(finfos, out)
        out.complete_plan()

        return out

    def _apply_algorithm(self, finfos, plan):
        manf = self._new_manifest()

        i = 0
        while len(finfos) > 0:
            if i >= len(finfos):
                # no more files can be found that will fit in this bag
                plan._manifests.append(manf)
                manf = self._new_manifest()
                i = 0

            newsz = manf['totalsize'] + finfos[i]['size']
            if newsz > self.maxsz:
                if manf['totalsize'] == 0:
                    # this file by itself exceeds our maxsize; put it in
                    # bag by itself.
                    self._add_to_manifest(finfos, i, manf)
                    i = 0
                    plan._manifests.append(manf)
                    manf = self._new_manifest()
                else:
                    # find a smaller one
                    i += 1
            else:
                self._add_to_manifest(finfos, i, manf)
                if newsz > self.tsz:
                    # exceeded our target size; start a new one
                    i = 0
                    plan._manifests.append(manf)
                    manf = self._new_manifest()

        if len(manf['contents']) > 0:
            plan._manifests.append(manf)
        
        return plan

    def _add_to_manifest(self, files, idx, manifest):
        fi = files.pop(idx)
        manifest['contents'].append(fi['path'][1:])
        manifest['totalsize'] += fi['size']

    def _new_manifest(self):
        # return an empty manifest
        return {
            'contents': [],
            'totalsize': 0
        }
        
    _special = [re.compile(r) for r in
                       r"^/bagit.txt$ ^/bag-info.txt$ ^/fetch.txt$".split() +
                       r"^/(tag)?manifest-(\w+).txt$".split()]
    def _is_special(self, filename):
        for r in self._special:
            if r.match(filename):
                return True
        return False

    @staticmethod
    def _cmp_by_size(infoa, infob):
        # we want sort to descend in size
        out = infob['size'] - infoa['size']
        if out != 0:
            return out
        return ((infoa['path'] < infob['path']) and -1) or \
               ((infoa['path'] > infob['path']) and +1) or 0

    def _sorted_files(self, bag):
        finfos = [{"path": p, "size": f.size, "name": p.split('/')[-1]}
                   for p,f in bag._root.fs.walk.info(namespaces=['details'])
                       if not f.is_dir and not self._is_special(p)
                                       and p not in self.forhead]
                          
        finfos.sort(key=cmp_to_key(self._cmp_by_size))
        return finfos

class NeighborlySplitter(WellPackedSplitter):
    """
    a Splitter that sets a maximum size limit, tries to minimize the total 
    number of bags (each under the size limit), and tries to keep files close to
    each other in the hierarchy in the same output bag.  
    """

    def _apply_algorithm(self, finfos, plan):
        manf = self._new_manifest()

        i = 0
        while len(finfos) > 0:
            for dirpath in self._dirpaths_for(finfos, i):

                # fill the open manifest with files in the dirpath directory
                i = self._select_from_dir(finfos, i, manf, dirpath)
            
                if i < 0:
                    # the manifest is full; open a new one
                    break
            
                if i >= len(finfos):
                    # no more files in the current dirpath directory will fit 
                    # in the manifest; start looking for files in the next nearby
                    # directory
                    i = 0
                    continue
                
            # the manifest is full or there are no more files that will fit in
            # this manifest.  
            plan._manifests.append(manf)
            manf = self._new_manifest()
            i = 0

                    
        return plan

    def _dirpath(self, path):
        if path == '/':
            return None
        return path.rsplit('/', 1)[0] + '/'

    def _dirpaths_for(self, finfos, ref):
        refdir = self._dirpath(finfos[ref]['path'])

        # sort directories
        dirs = [self._dirpath(f['path']) for f in finfos]
        dirs.sort()

        # uniquify
        for i in range(len(dirs)-1):
            d = dirs.pop(0)
            if d != dirs[0]:
                dirs.append(d)

        # put descendent directories before anscestors
        i = dirs.index(refdir)
        return dirs[i:] + dirs[:i]

    def _select_from_dir(self, finfos, i, manifest, dirpath):
        if not dirpath.endswith('/'):
            dirpath += '/'

        while i < len(finfos):
            if dirpath == self._dirpath(finfos[i]['path']):
                
                newsz = manifest['totalsize'] + finfos[i]['size']
                if newsz > self.maxsz:
                    if manifest['totalsize'] == 0:
                        # this file by itself exceeds our maxsize; put it in
                        # a bag by itself.
                        self._add_to_manifest(finfos, i, manifest)
                        return -1
                    else:
                        # find a smaller file to put in manifest
                        i += 1

                else:
                    self._add_to_manifest(finfos, i, manifest)
                    if newsz > self.tsz:
                        # exceeded our target size; start a new one
                        return -1
            else:
                i += 1
                    
        return i


class SimpleNamer(object):
    """
    A simple naming iterator that creates names for output multibags made up 
    of a given base name and a sequence number.
    """
    def __init__(self, base):
        self.base = base
        self.sn = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.sn += 1
        return "{0}_{1}.mbag".format(self.base, self.sn)

    def next(self):
        return self.__next__()

