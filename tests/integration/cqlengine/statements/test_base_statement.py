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
    import unittest  # noqa

from dse.query import FETCH_SIZE_UNSET
from dse.cqlengine.statements import BaseCQLStatement


class BaseStatementTest(unittest.TestCase):

    def test_fetch_size(self):
        """ tests that fetch_size is correctly set """
        stmt = BaseCQLStatement('table', None, fetch_size=1000)
        self.assertEqual(stmt.fetch_size, 1000)

        stmt = BaseCQLStatement('table', None, fetch_size=None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)

        stmt = BaseCQLStatement('table', None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)
