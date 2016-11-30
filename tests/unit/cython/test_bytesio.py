# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from tests.unit.cython.utils import cyimport, cythontest
bytesio_testhelper = cyimport('tests.unit.cython.bytesio_testhelper')

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class BytesIOTest(unittest.TestCase):
    """Test Cython BytesIO proxy"""

    @cythontest
    def test_reading(self):
        bytesio_testhelper.test_read1(self.assertEqual, self.assertRaises)
        bytesio_testhelper.test_read2(self.assertEqual, self.assertRaises)
        bytesio_testhelper.test_read3(self.assertEqual, self.assertRaises)

    @cythontest
    def test_reading_error(self):
        bytesio_testhelper.test_read_eof(self.assertEqual, self.assertRaises)
