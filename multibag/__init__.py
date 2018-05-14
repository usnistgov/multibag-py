"""
a reference implementation of the Multibag BagIt Profile.

The main multibag module provides the ProgenitorBag class in which an instance
wraps around a standard bag that would be split in to Multibag-compliant 
member bags.  It also provides the Multibag class in which an instance wraps 
a Multibag member bag; it can test the bag's validity with Multibag 
specification. The HeadBag class is Multibag specialization specifically for 
head bags which can be used to reconstitute the progenitor bag.  
"""
