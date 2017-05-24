# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from dse import Unavailable, Timeout, ConsistencyLevel
import re


class ConsistencyExceptionTest(unittest.TestCase):
    """
    Verify Cassandra Exception string representation
    """

    def extract_consistency(self, msg):
        """
        Given message that has 'consistency': 'value', extract consistency value as a string
        :param msg: message with consistency value
        :return: String representing consistency value
        """
        match = re.search("'consistency':\s+'([\w\s]+)'", msg)
        return match and match.group(1)

    def test_timeout_consistency(self):
        """
        Verify that Timeout exception object translates consistency from input value to correct output string
        """
        consistency_str = self.extract_consistency(repr(Timeout("Timeout Message", consistency=None)))
        self.assertEqual(consistency_str, 'Not Set')
        for c in ConsistencyLevel.value_to_name.keys():
            consistency_str = self.extract_consistency(repr(Timeout("Timeout Message", consistency=c)))
        self.assertEqual(consistency_str, ConsistencyLevel.value_to_name[c])

    def test_unavailable_consistency(self):
        """
        Verify that Unavailable exception object translates consistency from input value to correct output string
        """
        consistency_str = self.extract_consistency(repr(Unavailable("Unavailable Message", consistency=None)))
        self.assertEqual(consistency_str, 'Not Set')
        for c in ConsistencyLevel.value_to_name.keys():
            consistency_str = self.extract_consistency(repr(Unavailable("Timeout Message", consistency=c)))
        self.assertEqual(consistency_str, ConsistencyLevel.value_to_name[c])
