"""
This module provide a means for validating multibags.
"""
from collections import Sequence, OrderedDict

from .split import MBAG_VERSION as CURRENT_VERSION

ERROR = 1
WARN  = 2
REC   = 4
ALL   = 7
PROB  = 3
issuetypes = [ ERROR, WARN, REC ]

type_labels = { ERROR: "error", WARN: "warning", REC: "recommendation" }
ERROR_LAB = type_labels[ERROR]
WARN_LAB  = type_labels[WARN]
REC_LAB   = type_labels[REC]

class ValidationIssue(object):
    """
    an object capturing issues detected by a validator.  It contains attributes 
    describing the type of error, identity of the recommendation that was 
    violated, and a prose description of the violation.
    """
    ERROR = issuetypes[0]
    WARN  = issuetypes[1]
    REC   = issuetypes[2]
    
    def __init__(self, idlabel='', issuetype=ERROR, spec='', passed=True, 
                 comments=None, profver=CURRENT_VERSION):
        if comments and not isinstance(comments, Sequence):
            comments = [ comments ]

        self._pver = profver
        self._lab = idlabel
        self._spec = spec
        self.type = issuetype
        self._passed = passed
        self._comm = []
        if comments:
            self._comm.extend([str(c) for c in comments])

    @property
    def profile_version(self):
        """
        The version of the named BagIt profile that this issue references
        """
        return self._pver
    @profile_version.setter
    def profile_version(self, version):
        self._pver = version

    @property
    def label(self):
        """
        A label that identifies the recommendation within the profile that 
        was tested.  
        """
        return self._lab
    @label.setter
    def label(self, value):
        self._lab = value

    @property
    def type(self):
        """
        return the issue type, one of ERROR, WARN, or REC
        """
        return self._type
    @type.setter
    def type(self, issuetype):
        if issuetype not in issuetypes:
            raise ValueError("ValidationIssue: not a recognized issue type: "+
                             issuetype)
        self._type = issuetype

    @property
    def specification(self):
        """
        the explanation of the requirement or recommendation that the test
        checks for
        """
        return self._spec
    @specification.setter
    def specification(self, text):
        self._spec = text

    def add_comment(self, text):
        """
        attach a comment to this issue.  The comment typically provides some 
        context-specific information about how a issue failed (e.g. by 
        specifying a line number)
        """
        self._comm.append(str(text))

    @property
    def comments(self):
        """
        return a tuple of strings giving comments about the issue that are
        context-specific to its application
        """
        return tuple(self._comm)

    def passed(self):
        """
        return True if this test is marked as having passed.
        """
        return self._passed

    def failed(self):
        """
        return True if this test is marked as having passed.
        """
        return not self.passed()

    @property
    def summary(self):
        """
        a one-line description of the issue that was tested.  
        """
        status = (self.passed() and "PASSED") or type_labels[self._type].upper()
        out = "{0}: multibag {1} {2}".format(status, self.profile_version, 
                                             self.label)
        if self.specification:
            out += ": {0}".format(self.specification)
        return out

    @property
    def description(self):
        """
        a potentially lengthier description of the issue that was tested.  
        It starts with the summary and follows with the attached comments 
        providing more details.  Each comment is delimited with a newline; 
        A newline is not added to the end of the last comment.
        """
        out = self.summary
        if self._comm:
            comms = self._comm
            if not isinstance(comms, (list, tuple)):
                comms = [comms]
            out += "\n  "
            out += "\n  ".join(comms)
        return out

    def __str__(self):
        out = self.summary
        if self._comm and self._comm[0]:
            out += " ({0})".format(self._comm[0])
        return out

    def to_tuple(self):
        """
        return a tuple containing the issue data
        """
        return (self.type, "multibag", self.profile_version, self.label, 
                self.specification, self._passed, self._comm)

    def to_json_obj(self):
        """
        return an OrderedDict that can be encoded into a JSON object node
        which contains the data in this ValidationIssue.
        """
        return OrderedDict([
            ("type", type_labels[self.type]),
            ("profile_name", self.profile),
            ("profile_version", self.profile_version),
            ("label", self.label),
            ("spec", self.message),
            ("comments", self.comments)
        ])

    @classmethod
    def from_tuple(cls, data):
        return ValidationIssue(data[1], data[2], data[3], data[0], 
                               data[4], data[5], data[6])

class ValidationResults(object):
    """
    a container for collecting results from validation tests
    """
    ERROR = ERROR
    WARN  = WARN
    REC   = REC
    ALL   = ALL
    PROB  = PROB
    
    def __init__(self, bagname, want=ALL):
        """
        initialize an empty set of results for a particular bag

        :param bagname str:   the name of the bag being validated
        :param want    int:   the desired types of tests to collect.  This 
                              controls the result of ok().
        """
        self.bagname = bagname
        self.want    = want

        self.results = {
            ERROR: [],
            WARN:  [],
            REC:   []
        }

    def applied(self, issuetype=ALL):
        """
        return a list of the validation tests that were applied of the
        requested types:
        :param issuetype int:  an bit-wise and-ing of the desired issue types
                               (default: ALL)
        """
        out = []
        if ERROR & issuetype:
            out += self.results[ERROR]
        if WARN & issuetype:
            out += self.results[WARN]
        if REC & issuetype:
            out += self.results[REC]
        return out

    def count_applied(self, issuetype=ALL):
        """
        return the number of validation tests of requested types that were 
        applied to the named bag.
        """
        return len(self.applied(issuetype))

    def failed(self, issuetype=ALL):
        """
        return the validation tests of the requested types which failed when
        applied to the named bag.
        """
        return [issue for issue in self.applied(issuetype) if issue.failed()]
    
    def count_failed(self, issuetype=ALL):
        """
        return the number of validation tests of requested types which failed
        when applied to the named bag.
        """
        return len(self.failed(issuetype))

    def passed(self, issuetype=ALL):
        """
        return the validation tests of the requested types which passed when
        applied to the named bag.
        """
        return [issue for issue in self.applied(issuetype) if issue.passed()]
    
    def count_passed(self, issuetype=ALL):
        """
        return the number of validation tests of requested types which passed
        when applied to the named bag.
        """
        return len(self.passed(issuetype))

    def ok(self):
        """
        return True if none of the validation tests of the types specified by 
        the constructor's want parameter failed.
        """
        return self.count_failed(self.want) == 0

    def _add_issue(self, issue, type, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to type and its status set to passed (True) or failed (False).

        :param issue   ValidationIssue:  the issue to add
        :param type    int:              the issue type code (ERROR, WARN, 
                                         or REC)
        :param passed  bool:             either True or False, indicating whether
                                         the issue test passed or failed
        :param comments str or list of str:  one or more comments to add to the 
                                         issue instance.
        """
        issue.type = type
        issue._passed = bool(passed)

        if comments:
            if isinstance(comments, (str, unicode)) or \
               not isinstance(comments, Sequence):
                comments = [ comments ]
            for comm in comments:
                issue.add_comment(comm)
        
        issue = ValidationIssue(issue._prof, issue._pver, issue._lab,
                                issue.type, issue._spec, issue._passed,
                               (issue.comments and list(issue.comments)) or None)
        self.results[type].append(issue)

    def _err(self, issue, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to ERROR and its status set to passed (True) or failed (False).

        :param issue   ValidationIssue:  the issue to add
        :param passed  bool:             either True or False, indicating whether
                                         the issue test passed or failed
        :param comments str or list of str:  one or more comments to add to the 
                                         issue instance.
        """
        self._add_issue(issue, ERROR, passed, comments)

    def _warn(self, issue, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to WARN and its status set to passed (True) or failed (False).
        """
        self._add_issue(issue, WARN, passed, comments)

    def _rec(self, issue, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to REC and its status set to passed (True) or failed (False).
        """
        self._add_issue(issue, REC, passed, comments)


