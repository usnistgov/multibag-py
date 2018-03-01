# The Multibag BagIt Profile

__Contents__
* [Overview](#Overview)
* [The Multibag Data Structure](#The_Multibag_Data_Structure)
* [Multibag Metadata Elements](#Multibag_Metadata_Elements)
* [The Multibag Tag Directory](#The_Multibag_Tag_Directory)
  * [The `group-members.txt` File](#The_group-members.txt_File)
  * [The `group-directory.txt` File](#The_group-directory.txt_File)
* [Multibag Aggregation Updates](#Multibag_Aggregation_Updates)
* [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag)

<a name="Overview"></a>
## Overview
_This section is non-normative._

This specification describes a general-purpose profile on the BagIt standard for splitting a data aggregation across multiple bags which we refer to as the _Multibag_ profile.  One key motivation for splitting an aggregation over several bags is to make it easier to handle very large aggregations in storage or transmission environments that would otherwise place a limit on the size of a bag.  The BagIt specification already supports this basic functionality; this profile expands on this functionality to accomplish the following goals:
   * provide a standard recipe for combining all bags representing a single logical aggregation into a single compliant bag.
   * support non-destructive updates to a bag aggregation by allowing one to create a new aggregation that combines an existing set of bag aggregations with an "errata" or "update" bag that contains only the files that have changed.

A bag that is compliant with the Multibag profile supports the following:
   * the standard BagIt structures
   * an additional set of Multibag fields in the `bag-info.txt` file
   * a multibag-specific tag directory containing tag files specified by this document

When the Multibag profile is used to handle large data aggregations, the individual files are distributed across two or more bags (referred to in this document as a _Multibag aggregation_).  This profile defines how the multiple bags can be recombined to create a single coherent bag containing the aggregation.  On the other hand, often an application will want to extract only one or a few files from the aggregation and, thus, would prefer to avoid retrieving, transmitting, and/or unpacking all of the bags in the aggregation.  To enable this, one of the bags is known as the _Head Bag_; it contains both a listing of all of the other bags in the Multibag aggregation as well as a file lookup list for locating individual files.  Thus, to retrieve a subset of the files within the aggregation, an application would first retrieve and unpack the Head Bag (which can be made to be quite small) to consult its lookup list, and then retrieve and open only the member bags that contain the desired files.  

> Note that this version of the Multibag Profile does not address the problem of how to split up and distribute a single large file among multiple bags.  This is planned to be addressed in a future version.

It is intended that the Multibag profile be used for dataset preservation: all the files that are part of the dataset are stored into a repository as a Multibag aggregation.  Often it is necessary to later make updates to the dataset, and for many reasons it would not be desirable to recreate the entire aggregation, particularly if it is important to retain the earlier versions and when the dataset is large:
   * A small change to the dataset would mean replicating a large amount of unchanged data.
   * It may be expensive (in computing resources) to retrieve and re-save the aggregation from and to the preservation storage.

To address this problem, this profile supports a mechanism for _non-destructive updates_.  It allows a repository to create "errata" or "update" bags which contain only those parts of the dataset that have changed.  These are combined with the previous bags to create a new Multibag aggregation and a new version of the dataset.  All previous versions of the dataset can be reconstituted by retrieving the proper Head Bag for that version.  

> Note that this version of the Multibag Profile does not address how to handle deletions of files in an update to an aggregation.  This is planned to be addressed in a future version.

<a name="The_Multibag_Data_Structure"></a>
## The Multibag Data Structure

_This section is normative._

A Multibag aggregation is a set of one or more bags that are each compliant with this specification.  The full set of files under the `data` directories in each of the bags represent a logical aggregation of data that can (in principle) be combined into a single bag, given the rules in the section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag).  One of the bags in the set is designated the _Head Bag_.  This contains metadata that identifies the other bags that make up the aggregation as well as which bags contain which files.  

When a Multibag aggregation is being created to capture a large file aggregation, each bag would normally contain a different subset of the files; in this case, typically, no two bags would contain file under the `data` directory with the same filepath.  How the `data` files are distributed amongst the bags in this use case is the choice of the creating application.  When a Multibag aggregation is supporting non-destructive updates (discussed in section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates)), files with the same path MAY appear in different bags.  These files are considered different versions of the same file.  When the bags are combined to create a single bag (according to the rules given in the section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag)), only the latest version of the files is retained.  Regardless of the reason for there being multiple files with the same path in different member bags, all the files in the aggregation MUST be distributed such that applying the rules in section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag), produces a compliant BagIt bag that properly represents the full file aggregation.  

The multibag profile does not place any requirements on the names given to the bags in the multibag aggregation, nor does it specify how one determines a bag's membership in an aggregation without opening up and examining the Head Bag's metadata.  Further, this profile does not define a means for identifying the Head Bag of an aggregation without opening and examining bags.  Applications may apply bag naming conventions to accomplish this; however, the convention should take into account the mechansim for non-destructive updates (see section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates)).  

Note that a bag can be part of multiple bag aggregations simultaneously.  Specifically, Multibag's mechanism for non-destructive updates leverages this feature.  A bag can only be the Head Bag for one Multibag aggregation.  

<a name="Multibag_Metadata_Elements"></a>
## Multibag Metadata Elements

_This section is normative._

A bag compliant with the Multibag profile MUST contain a `bag-info.txt` file as defined by the BagIt standard.  The Multibag profile defines several metadata elements that can appear in the `bag-info.txt` file using the standard BagIt tag syntax.  The names of the elements and the meanings of their values are as follows:

<dl>
   <dt> `Multibag-Version` </dt>
   <dd> The version of the Multibag profile specification that the bag conforms to. The version described by this document is 0.2. </dd>

   <dt> `Multibag-Reference` </dt>
   <dd> A URL pointing to Multibag specification referred to in the `Mulibag-Version` element. </dd>

   <dt> `Multibag-Tag-Directory` </dt>
   <dd> the path relative to the bag's base directory to the multibag-specific tag directory. </dd>

   <dt> `Multibag-Head-Version` </dt>
   <dd> the version of the bag aggregation that the current bag is the head bag for (see notes below). </dd>

   <dt> `Multibag-Head-Deprecates` </dt>
   <dd> one or two tokens, separated by a comma, where the first field is the version of another Multibag aggregation that the current aggregation deprecates, and the second field is the name of the head bag for the deprecated aggregation. </dd>
<dl>

The use of these metadata elements are subject to the following constraints:

   * A bag that conforms to the Multibag profile MUST include the `Multibag-Version` metadata element.

   * The bag that represents the Head Bag MUST include the `Multibag-Head-Version` metadata element; its value is a version identifier for the Multibag aggregation it is the Head Bag for. As described in the section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates), an update to the aggregation will be represented by a new Head Bag; the version provided in the `Multibag-Head-Version` metadata element in the new Head Bag MUST be different from that in Head Bags for all previous versions.

   * Only Head Bags SHOULD contain the `Multibag-Head-Deprecates` metadata element. It MAY be provided multiple times to identify multiple previous versions of the aggregation. In general, the element is optional, regardless of the existence of previous versions.

   * The `Multibag-Tag-Directory` is optional, and it should only appear if the bag represents the Head Bag. If the bag is a Head Bag but this element is not provided, the path to the multibag-specific tag directory will be assumed to be simply, 'multibag'.

   * Including the `Multibag-Reference` metadata element is optional.

In addition to including the Multibag-specific metadata, the `bag-info.txt` file SHOULD also include the `Bag-Group-Identifer` element, set to an identifier referring to the Multibag aggregation as a whole.  The elements, `Internal-Sender-Identifier` and `Internal-Sender-Description`, are also recommended for inclusion.  It is recommended that the `Internal-Sender-Description` represent a description of the aggregation as a whole.  

It is recommended that the `bag-info.txt` _not_ include the `Bag-Count` element.  (This element gives the total number of bags in its group and the position the bag in the group.)  Because a bag can be a member of multiple Multibag groups (and different versions of the group), there could be contexts where its value is not correct.  In particular, updating a Multibag aggregation via the mechanism given in section, [Multibag Aggregation Updates](#Multibag_Aggregation_Updates), will invalidate the `Bag-Count` value.  

### Example `bag-info.txt`

_This section is non-normative._

_in progress: fill in example, explanation_
<verbatim>

</verbatim>

<a name="The_Multibag_Tag_Directory"></a>
## The Multibag Tag Directory

_This section is normative._

A Multibag Head Bag MUST contain a special directory located outside of the `data` directory which is referred to as a _Multibag Tag Directory_.  The location of this directory is given as the value of the `Multibag-Tag-Directory` metadata element in the `bag-info.txt` file (see section, [Multibag Metadata Elements](#Multibag_Metadata_Elements)) as a file path relative to the bag's base directory; if the element is not given, the location of MUST be a directory directly within the bag's base directory called `multibag`.  

The directory MUST contain two files called `group-members.txt` and `group-directory.txt`, respectively.  The directory may contain other files, but applications that support this profile can ignore them to properly interact with bags in the bag aggregation.  

<a name="The_group-members.txt_File"></a>
### The `group-members.txt` File

_This section is normative._

The purpose of the `group-members.txt` file is to allow applications to know which other bags belong to a Multibag aggregation by examining the aggregation's Head Bag.  It also allows the application to retrieve the other member bags from remote locations if they are so available.  

The `group-members.txt` is a text file that lists the names of the bags that make up the Multibag aggregation.  Each line of the file has the format:

```
BAGNAME [URL]
```

where BAGNAME is the name of the bag.  This name should match the name of the base directory; it should not matched a serialized form of the bag.  URL is a URL from which a serialized copy of the bag can be retrieved.  The URL field is optional.  

The order that bags are listed in is significant to the mechanism for combining Multibag bags into a single bag, as described in section, [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag).  The last bag named in the list MUST be the Head Bag for the aggregation.  

#### Example

_This section is non-normative._

<a name="The_group-directory.txt_File"></a>
### The `group-directory.txt` File

_This section is normative._

The purpose of the `group-directory.txt` file is to allow applications to locate individual files across all of the aggregation's bags without having to open and examine all of the bags; rather, an application need only open the HeadBag to discover a file's location.

The `group-directory.txt` file is a text file that lists the names of files available in the union of all the bags that make up the Multibag aggregation with an indication of in which bag the file is stored.  Each line of the file has the format:

```
FILEPATH BAGNAME
```

where FILEPATH is the path to the file relative to the bag's base directory, and BAGNAME is the name of the bag in the aggregation that the file is located in.  

Creators of Multibag-compliant bags should include in the `group-directory.txt` lising all files that users might want easy access to--i.e. the ability to extract an individual file from its enclosing bag without having to potentially unserialize and search all of the bags in the aggregation.  All files under the `data` directories in all of the bags SHOULD be listed in the file.  Other metadata or tag files outside of the `data` directories MAY be listed as well.  

<a name="Multibag_Aggregation_Updates"></a>
## Multibag Aggregation Updates

_This section is normative._

This section describes how to update Multibag aggregation while retaining full access to the un-updated version without recreating the entire aggregation.  The updating application creates one or more new Multibag-compliant bags that contain new files to be added to the aggregation or new versions of files contained in the previous aggregation; these files can be located anywhere in the bag--i.e. either inside the `data` directory or out.  A new file is one whose path does not match any of the files within the previous aggregation; a new version of file is one that does match a file within the previous aggregation.  

One of the new bags is designated as the Head Bag for the updated version of the aggregation; it MUST meet all of the requirements of a Head Bag.  It SHOULD include in its `bag-info.txt` file the `Multibag-Head-Deprecates` metadata element, identifying the Head Bag of the previous aggregation that it replaces.  The new Head Bag MAY also replicate the `Multibag-Head-Deprecates` metadata occurrences in the deprecated Head Bag so as to reference even earlier versions of the aggregation.   

The `group-members.txt` file in the new Head Bag MUST list all of member bags of the previous aggregation that contain data that is to be part of the new aggregation.  The file is allowed to not include one, some, or all of the bags from the previous aggregation if none of their files should be included or that they would otherwise be replaced in the new aggregation.  The order of listing of the bags from the previous aggregation's `group-members.txt` file MUST be the preserved in the new one.  The new bags MUST be listed after the bags from the previous aggregation, and the new Head Bag MUST be listed last.  The order of the new bags (that are not Head Bags) must take into consideration the rules for [Combining Multibags Into a Single Bag](#Combining_Multibags_Into_a_Single_Bag).  

The `group-directory.txt` file in the new Head Bag SHOULD list all of the files from the previous Head Bag's `group-directory.txt` file.  Files that are not considered part of the new aggregation MAY be absent from this file; however, a file's absence should not be taken as an indication that the file has been deleted as part of the update.  


> Note that this version of the Multibag Profile does not address how to handle deletions of files in an update to an aggregation.  This is planned to be addressed in a future version.


<a name="Combining_Multibags_Into_a_Single_Bag"></a>
## Combining Multibags Into a Single Bag

_This section is normative._

It must always be possible, in principle, to combine all of the bags in a Multibag aggregation into a single BagIt-compliant bag (barring storage and compute resource limitations) by following the process detailed in this section.  The `group-members.txt` file lists the member bags in an aggregation in the order which they must be combined.  

An application MUST be able to combine a Multibag aggregation into a single bag by following the these steps:

   1 The application retrieves the aggregation's Head Bag and extracts the `group-members.txt` file.
   1 The application retrieves the first bag listed in the file, unserializes it (if necessary), and copies it to a location in storage where the final single bag is to be assembled. The directory structure of the bag is retained in the copy.
   1 The application retrieves and unserializes (if necessary) each subsequent bag in the list, in order, and unpacks or copies its contents into the same storage location, retaining the bag's directory structure. In this process, updated versions of files MAY overwrite deprecated versions.

Previous versions of a Multibag aggregation may be assembled into a single bag by consulting a Head Bag's `Multibag-Head-Deprecates` metadata (in its `bag-info.txt` file) and retrieving the Head Bag of the previous version that the element refers to; the application can then follow the above steps with the deprecated Head Bag.  
