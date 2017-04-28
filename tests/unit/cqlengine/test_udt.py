# Copyright 2013-2017 DataStax, Inc.
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

from dse.cqlengine import columns
from dse.cqlengine.models import Model
from dse.cqlengine.usertype import UserType


class UDTTest(unittest.TestCase):

    def test_initialization_without_existing_connection(self):
        """
        Test that users can define models with UDTs without initializing
        connections.

        Written to reproduce PYTHON-649.
        """

        class Value(UserType):
            t = columns.Text()

        class DummyUDT(Model):
            __keyspace__ = 'ks'
            primary_key = columns.Integer(primary_key=True)
            value = columns.UserDefinedType(Value)
