.. Multibag-Py documentation master file, created by
   sphinx-quickstart on Tue Nov  6 17:09:48 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Multibag-Py: A reference implementation for the Multibag BagIt Profile
======================================================================

The ``multibag`` python package represents both a reference implementation and
a practical library supporting the Multibag profile for the BagIt Packaging
specification.  The `BagIt specification (V1.0, IETF RFC8493)
<https://tools.ietf.org/html/draft-kunze-bagit-17>`_ provides a means of
organizing a set of files that constitute a logical whole--such as a dataset or
data collection--such that they can be serialized and transfered together.  The
`Multibag profile <multibag-profile-spec.md>`_ specifies additional conventions
that address issues related to using BagIt as a data preservation format.
First, it provides conventions for splitting a very large collection into
multiple compliant bags; in particular, the profile describes how the bags can
be combined into a single compliant bag after transfer or restoration from
long-term storage.  Second, it provides an efficient means of creating updates
to a collection through the creation of additional "errata" bags which override
contents from the previously created bags, establishing a new version of the
collection.  Because the previously created bags themselves are not changed,
the update is *non-destructive*, allowing any previously defined version of the
collection to be reconstituted.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Motivation and Features
-----------------------


Using the ``multibag`` package
------------------------------


API Reference
-------------

.. autosummary::
   :toctree: generated

   multibag
   multibag.split
   multibag.amend
   multibag.validate
   multibag.testing
   multibag.access


Indices and tables
^^^^^^^^^^^^^^^^^^

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
