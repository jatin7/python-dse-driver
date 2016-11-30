# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from tests.unit.cython.utils import cyimport, cythontest
utils_testhelper = cyimport('tests.unit.cython.utils_testhelper')

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class UtilsTest(unittest.TestCase):
    """Test Cython Utils functions"""

    @cythontest
    def test_datetime_from_timestamp(self):
        utils_testhelper.test_datetime_from_timestamp(self.assertEqual)