# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from dse.bytesio cimport BytesIOReader

def test_read1(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    assert_equal(reader.read(2)[:2], b'ab')
    assert_equal(reader.read(2)[:2], b'cd')
    assert_equal(reader.read(0)[:0], b'')
    assert_equal(reader.read(2)[:2], b'ef')

def test_read2(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(5)
    reader.read(1)

def test_read3(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(6)

def test_read_eof(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(5)
    # cannot convert reader.read to an object, do it manually
    # assert_raises(EOFError, reader.read, 2)
    try:
        reader.read(2)
    except EOFError:
        pass
    else:
        raise Exception("Expected an EOFError")
    reader.read(1) # see that we can still read this
