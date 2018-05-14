"""
Tools for splitting a single bag (a ProgenitorBag) into a set of 
Multibag-compliant bags.
"""
import os, re
from abc import ABCMeta, abstractmethod

from .access.bagit import Bag, ReadOnlyBag

_bagsepre = re.compile(r'/')
_ossepre = re.compile(os.sep)

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

    @abstractmethod
    def exists(self, path):
        """
        return True if the given path exists in this bag.  
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplemented()

    @abstractmethod
    def isdir(self, path):
        """
        return True if the given path exists as a subdirectory in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplemented()

    @abstractmethod
    def isfile(self, path):
        """
        return True if the given path exists as a file in the bag
        :param path str:  the path to test, given relative to the bag's base
                          directory.  '/' must be used as the path delimiter.
        """
        raise NotImplemented()

    @abstractmethod
    def walk(self):
        """
        Walk the source bag contents returning the triplets returned by
        os.path.  The difference is that the first element in the triplet,
        the base directory, will be relative to the bag's base directory;
        for files and directories directly below the base, the field will 
        be an empty string.
        """
        raise NotImplemented()

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
        

class LocalDirProgenitor(ProgenitorMixin):
    """
    An implementation of the ProgenitorMixin for a Bag that is stored as 
    a directory on a local filesystem.  
    """
    is_readonly = False

    def __init__(self, bagdir):
        self._bagdir = bagdir.rstrip(os.sep)
        self._bagname = os.path.basename(bagdir)
        super(LocalDirProgenitor, self).__init__()

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

def asProgenitor(bag):
    """
    add the ProgenitorMixin interface to the given bag.  The input bag instance 
    is returned with its class updated.
    """
    if not isinstance(bag, Bag):
        raise ValueError("asProgenitor(): input not of type Bag: " + str(bag))

    if isinstance(bag, ProgenitorMixin):
        return bag

    if isinstance(bag, ReadOnlyBag):
        raise NotImplemented()

    if not os.path.exists(os.path.join(bag.path, "bagit.txt")):
        raise ValueError("Unsupported Bag implementation: "+str(type(bag)))

    bag.__class__ = _LocalProgenitorBag
    LocalDirProgenitor.__init__(bag, bag.path)
    return bag
    

class SplitPlan(object):
    """
    a description of how to distribute the payload and metadata files 
    from a progenitor bag across multiple output multibags.  
    """

    def __init__(self, source):
        """
        create an empty plan for splitting a given source bag
        """
        if not isinstance(source, Bag):
            raise ValueError("SplitPlan(): source is not a Bag instance")
        self.progenitor = source
        self._manifests = []

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

    def is_complete(self):
        """
        return True if the plan is set to produce output multibags that 
        will include all files from the source bag.  
        """
        try:
            self.missing().next()
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
                name = naming_iter.next()
                self._set_manifest_name(m, name)
        except StopIteration as ex:
            after = (name and (" (after %s)" % name)) or ""
            raise RuntimeError("Naming iterator exhausted prematurely"+after)

    def _set_manifest_name(self, manifest, name):
        manifest['name'] = name
        return manifest

    
                                        
            
        

    
