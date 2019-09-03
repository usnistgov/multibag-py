.. _multibag-using:

.. _Multibag Profile: https://github.com/usnistgov/multibag-py/blob/apidoc/docs/multibag-profile-spec.md
.. _Multibag Profile specification: https://github.com/usnistgov/multibag-py/blob/apidoc/docs/multibag-profile-spec.md
.. _BagIt specification: https://tools.ietf.org/html/rfc8493
.. _bagit python package: https://github.com/LibraryOfCongress/bagit-python

******************
Using ``multibag``
******************

.. toctree::
   :maxdepth: 2

Most of the main functionality of multibag package are available
at its top level; thus, you can access it by importing ``multibag``::

  import multibag

When you installed the multibag package, you also get the vanilla
`bagit python package, bagit`_, which you can use independently.  


The multibag data model
=======================

The multibag data model is a simple extension of the standard data
model for a BagIt-compliant bag.  That is, a *bag* is a directory
containing:

  * a ``data`` subdirectory which contains the main data files that
    make up a collection--called the *payload*.  This files can have
    arbitrary names and arranged in an arbitrary directory hierarchy.

  * a ``bagit.txt`` file that identifies the directory as a *bag*

  * a ``bag-info.txt`` that contains human readable metadata about the
    the collection.

  * a ``manifest-``:emphasis:`alg`:code:`.txt` file (where *alg* is a checksum
    algorithm label like ``sha256``) that lists checksum values for
    files in the payload.

The `BagIt specification`_ defines other files that can be included in
bag that provide additional functionality.  Further, the specification
allows of any other files to be included that are not meaningful to
the base specification; this allows for the definition of BagIt
profiles like Multibag.

The multibag model builds on the base BagIt model.  In it, we define a
multibag aggregation as a set of one or more bags that contain files
that, as a whole, represent a coherent collection.  Each bag in the
aggregation is compliant with the base BagIt standard; furthermore,
the bags can be combined to create a single, compliant bag.

One of the bags in the aggregation is designated the *head bag*.  This
bag contains extra metadata for interpreting the collection as a
multibag aggregation.  In particular,

  * the ``bag-info.txt`` contains extra tags that identifier the bag as
    a head bag, the version of the multibag profile specification it
    complies with, and pointers to previous versions of the
    aggregation.
  * a tag file called ``multibag/member-bags.tsv`` lists the names of
    the bags that belong to the aggregation.
  * a tag file called ``multibag/file-lookup.tsv`` lists which bag
    contains each file in the collection.

(See  `Multibag Profile Specification`_ for details on the syntax for
these and other profile tags.)

To create a new version of the collection, one creates a new head bag
and zero or more additional member bags.  These new bags contain new files
or file that have been updated with the new version.  The new head
bag, then, lists these new bags as members along with the previous
bags that contain files that have not changed.  Thus, creating a new
version is *non-destructive*: previous versions of the collection can
be reconstituted by examining the appropriate head bag.  

Thus, a multibag aggregation can represent multiple versions of the
collection simultanously where each version is represented by a
different head bag (and the version identifier--e.g., 2.1.3--is
recorded in the ``bag-info.txt`` file).  Usually, many of the bags in
the aggregation will be listed in multiple head bags.  


Creating test data collections
==============================

If you are just trying out the ``multibag`` package, it helps to have some data 
collections of various sizes to work with.  The package includes a module,
:mod:`multibag.testing.mkdata`, that can create "fake" data collections made
up of files of various sizes and organized into various subdirectories.

The simplest way to create a directory with a bunch of files in it is with the
:py:func:`~multibag.testing.mkdata.mkdataset`  function:

.. code-block:: python

   import multibag.testing.mkdata as mkdata
   mkdata.mkdataset("datadir", totalsize=10000000, filecount=20)

This will create in the current directory a subdirectory called ``datadir``.
It will contain 20 500-kB files (for a combined total size of 10 MB).  If you
look inside ``datadir``, you'll see that the files have names made up of a
number is that is its size in bytes plus a random string.  The contents of the
file are just lines of character data.

You can create more complicated arrangements of fake data.  For example, 
you may want your data directory to contain subdirectories.  Here's one way to
do that:

.. code-block:: python

   mkdata.mkdataset("datadir2", totalsize=10000000, filecount=20,
                    plan={ 'files': [
                              { "totalsize": 500000, "totalfiles": 2 }
                           ],
                           'dirs': [
                              { "totalsize": 500000, "totalfiles": 10 },
                              { "totalfiles": 90 }
                           ]} )

This time, ``datadir2`` will contain...

   * 2 files, 250 kB each
   * a subdirectory (with a random name) containing 10 files, 50 kB each
   * a second subdirectory containing 90 files, 100 kB each

The ``plan`` argument allows for a variety of ways to distribute data any
number of directories and files; see the
:py:mod:`~multibag.testing.mkdata`  module documentation for the full
explanation of the plan description syntax.

As you will see if you run the above examples,
:py:func:`~multibag.testing.mkdata.mkdataset` just creates a directory with
data files in it.  If you want a quick-and-dirty way to turn this directory
into a BagIt bag is with the ``make_bag()`` function from the
`bagit python package`_:

.. code-block:: python

   import bagit
   bagit.make_bag("datadir")

This will tranform ``datadir`` into a legal bag; in particular, all of the fake
data files you created with ``mkdataset()`` will now be in a ``data``
subdirectory.  

Creating multibag aggregations
==============================

In this section, we look at three scenarios multibag aggregations:

1. **Turning a single bag into a single-multibag aggregation.**  Here we are
   creating an aggregation that is compliant with the `Multibag Profile`_,
   but which contains only one member bag.  In this scenario, the multibag
   metadata are added to the existing bag to turn it into a head bag.  
   You may wish to do this when preparing the bag for future amendments
   (under scenario 3 below); this is not required in order to make
   amendments, but it provides consistency to your repository of bags, 
   allowing you to access bag content in a consistent way (using the
   multibag paradigm).

.. p

2. **Splitting a large bag into a multibag aggregation.**  This applies a
   customizable algorithm for splitting files across multiple output bags
   in which one will be set up as the head bag.  In the algorithms included
   in this package, you can set a maximum size for each member bag, such
   that if the source bag is less than that size, no splitting is done:  
   instead, a single-multibag aggregation is created (as in scenario 1, 
   above).
 
.. p

3. **Amending an aggreation with additional multibags.**  This scenario
   applies when creating a new version of the aggregation in which contents 
   (be it payload data or metadata) are updated or added to.  A new head
   bag is created plus zero or more additional bags (depending on the
   splitting algorithm).  


Creating a single-multibag aggregation
--------------------------------------

You can take any legal BagIt bag into a Multibag-compliant head bag for a 
single-bag aggregation (itself being the only member) using the
:py:func:`~multibag.amend.make_single_multibag` function:

.. code-block:: python

   multibag.amend.make_single_multibag("bagdir")

This will update the bag represented by the directory, `bagdir` "in
place", inserting the necessary metadata required by the Multibag profile.
Alternatively, while converting, you can specify a version string and a
persistent identifier that you wish to assign to the bag:

.. code-block:: python

   multibag.amend.make_single_multibag("bagdir", version="1.0.1", pid="doi:00000/XXXX")

When these extra parameters are not provided, the version is set to "1" and no
identifier is set.  If a resolvable identifier is being assigned, it should 
resolve to the bag itself (or a serialized form of it).

This function uses the :py:class:`~multibag.amend.SingleMultibagMaker` class to
do the work: after intantiation, it calls its ``convert()`` function.  You can
use the the class directly if you want to control the conversion via its other
public functions.


Splitting a large bag into a multibag aggregation
-------------------------------------------------

The simplest way to a large bag into an multibag aggregation of smaller bags is
with the :py:func:`~multibag.split.split_bag` function:

.. code-block:: python

   multibag.split_bag("mybag", "mymultibag", 100000000)

This splits the original source bag with the root directory name, ``mybag``
(also referred to as the *progenitor* bag) into a set of output
multibags where none exceed the maximum size of 100 MB.  The output files will
have root directory names, ``mymultibag_1``, ``mymultibag_2``, and so on.

You can take tighter control of the process and output bag naming by using a 
:py:class:`~multibag.split.Splitter` class to do the splitting.  A
:py:class:`~multibag.split.Splitter` figures out how to distribute the files
from the original source (or *progenitor*) bag into the output multibags and
captures that as a *split plan*.  It can then execute that plan.

:py:class:`~multibag.split.Splitter` is an abstract class as there can be
different ways to split up the files.  The multibag package comes with two
implementations, capturing two slightly different algortithms:

* :py:class:`~multibag.split.WellPackedSplitter` -- this algorithm sets a
  target size for each bag in the output aggregation and distributes the
  files so as to minimize the number of output bags by packing them as close
  as close to the target size as possible.

* :py:class:`~multibag.split.NeighborlySplitter` -- this algorithm is a 
  variation on :py:class:`~multibag.split.WellPackedSplitter`, except it 
  tries to keep files that are close together in the progenitor bag's hierarchy
  in the same output bag.

Of course, other algorithms that optimize for a particular bag producer's needs
can be enabled by implementing the :py:class:`~multibag.split.Splitter` class.

a :py:class:`~multibag.split.Splitter` encapsulates a particular strategy for
splitting bags: once instantiated, it can be applied any number of source
bags.  It does this by first creating a :py:class:`~multibag.split.SplitPlan`
for a given source bag; this is an object that creates a list of
manifests--that is, file paths from the source bag--where each manifest
represents the contents of each of the output multibags.  The plan is then
iteratively executed to create the output multibags.  The
:py:class:`~multibag.split.Splitter` interface gives you access to these 
steps in case, say, you wish to manipulate the plan before or as you execute it.
For example, as each output bag is produced, you can serialize it (e.g. into a
zip file) or move to other storage before creating the next one.  

Here's an example of using the :py:class:`~multibag.split.Splitter` interface
directly:

.. code-block:: python

   from __future__ import print_function
   import os, shutil, bagit, multibag

   # make a bag with some fake data, total size of 10 MB
   import multibag.testing.mkdata as mkdata
   mkdata.mkdataset("mybag", totalsize=10000000, filecount=20)
   bagit.make_bag("mybag")

   # create a splitter that limits bag sizes to 2 MB each
   splitter = multibag.WellPackedSplitter(2000000)

   # create the plan
   splitplan = splitter.plan("mybag")
                
   # print the expected output bag sizes: 
   print([m['totalsize'] for m in splitplan.manifests()])
                
   # create the output bags, zipping them up as they are created
   for outbag in splitplan.apply_iter('.'):
       # outbag was just created             
       if not os.system("zip -r {0}.zip {0}".format(outbag)):
           print("{0}: Failed to serialize with zip: {1}".format(outbag))
           continue
       try:
           shutil.rmtree(outbag)
       except OSError as ex:
           print("{0}: Failed to remove bag after serialization: {1}"
                 .format(outbag, str(ex)))
       
   
Amending an aggregation with additional multibags
-------------------------------------------------


Accessing the contents of multibag aggregations
===============================================

Accessing contents of serialized bags
-------------------------------------

Extracting files from an aggregation
------------------------------------


Reconstituting an aggregated bag
--------------------------------

*Not yet implemented; see `Multibag Profile Specification`_
