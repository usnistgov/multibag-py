.. _multibag-using:

.. _Multibag Profile Specification: https://github.com/usnistgov/multibag-py/blob/apidoc/docs/multibag-profile-spec.md

******************
Using ``multibag``
******************

.. toctree::
   :maxdepth: 2

The multibag data model
=======================

The multibag data model is a simple extension of the standard data
model for a BagIt-compliant bag.  In this model, we define a multibag
aggregation as a set of one or more bags that contain files that, as a
whole, represent a coherent collection.  Each bag in the aggregation
is compliant with the base BagIt standard; furthermore, the bags can
be combined to create a single, compliant bag.

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
these profile tags.)

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
:mod:`multibag.testing`, that can create "fake" data collections made up files
of various sizes and organized into various subdirectories.

Creating multibag aggregations
==============================


Creating a single-multibag aggregation
--------------------------------------


Splitting a large bag into a multibag aggregation
-------------------------------------------------


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
