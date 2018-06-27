# encoding: utf-8

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os, pdb, json
import tempfile, shutil
import unittest as test

import multibag.validate as val
from multibag.access.bagit import Bag, ReadOnlyBag, Path, open_bag

datadir = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       "access", "data")

class TestValidationIssue(test.TestCase):

    def test_ctor(self):
        issue = val.ValidationIssue("A1.1")

        self.assertEqual(issue.profile_version, "0.3")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.ERROR)
        self.assertTrue(issue.passed())
        self.assertFalse(issue.failed())
        self.assertEqual(issue.specification, "")
        self.assertEqual(len(issue.comments), 0)

        issue = val.ValidationIssue("A1.1", val.REC, profver="3.1",
                                    spec="Life must self replicate.",
                                    passed=False)

        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertFalse(issue.passed())
        self.assertTrue(issue.failed())
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertEqual(len(issue.comments), 0)

        issue = val.ValidationIssue("A1.1", val.REC, profver="3.1",
                                    spec="Life must self replicate.",
                                    passed=False)

        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertFalse(issue.passed())
        self.assertTrue(issue.failed())
        self.assertEqual(len(issue.comments), 0)

        issue = val.ValidationIssue("A1.1", val.REC, profver="3.1", 
                                    spec="Life must self replicate.",
                                    comments=["little", "green"])

        self.assertEqual(issue.profile_version, "3.1")
        self.assertEqual(issue.label, "A1.1")
        self.assertEqual(issue.type, issue.REC)
        self.assertTrue(issue.passed())
        self.assertFalse(issue.failed())
        self.assertEqual(issue.specification, "Life must self replicate.")
        self.assertEqual(len(issue.comments), 2)
        self.assertEqual(issue.comments[0], "little")
        self.assertEqual(issue.comments[1], "green")

    def test_description(self):
        
        issue = val.ValidationIssue("A1.1")
        self.assertEqual(issue.summary, "PASSED: multibag 0.3 A1.1")
        self.assertEqual(str(issue), issue.summary)
        self.assertEqual(issue.description, issue.summary)

        issue = val.ValidationIssue("A1.1", profver="3.1",
                                    spec="Life must self-replicate")
        self.assertEqual(issue.summary,
                         "PASSED: multibag 3.1 A1.1: Life must self-replicate")
        self.assertEqual(str(issue), issue.summary)
        self.assertEqual(issue.description, issue.summary)

        issue = val.ValidationIssue("A1.1", profver="3.1",
                                    spec="Life must self-replicate", 
                                    passed=False, comments=["Little", "green"])
        self.assertEqual(issue.summary,
                         "ERROR: multibag 3.1 A1.1: Life must self-replicate")
        self.assertEqual(str(issue),
                    "ERROR: multibag 3.1 A1.1: Life must self-replicate (Little)")
        self.assertEqual(issue.description,
          "ERROR: multibag 3.1 A1.1: Life must self-replicate\n  Little\n  green")

class TestValidationResults(test.TestCase):

    pass


if __name__ == '__main__':
    test.main()
    

