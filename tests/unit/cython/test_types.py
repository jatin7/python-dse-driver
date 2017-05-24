# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from tests.unit.cython.utils import cyimport, cythontest
types_testhelper = cyimport('tests.unit.cython.types_testhelper')

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class TypesTest(unittest.TestCase):

    @cythontest
    def test_datetype(self):
        types_testhelper.test_datetype(self.assertEqual)

    @cythontest
    def test_date_side_by_side(self):
        types_testhelper.test_date_side_by_side(self.assertEqual)
