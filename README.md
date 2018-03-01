# pymultibag
This python library provides a reference implementation for the Multibag BagIt Profile

The Multibag BagIt Profile defines a means for splitting a data
aggregation across multiple bags which we refer to as the _Multibag_
profile.  One key motivation for splitting an aggregation over several
bags is to make it easier to handle very large aggregations in storage
or transmission environments that would otherwise place a limit on the
size of a bag.  The BagIt specification already supports this basic
functionality; this profile expands on this functionality to
accomplish the following goals: 
   * provide a standard recipe for combining all bags representing a
     single logical aggregation into a single compliant bag. 
   * support non-destructive updates to a bag aggregation by allowing
     one to create a new aggregation that combines an existing set of
     bag aggregations with an "errata" or "update" bag that contains
     only the files that have changed. 

The draft specification for this Profile can be found in the [docs
directory](docs).


