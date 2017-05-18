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

import sys

from dse.cqlengine.connection import get_session
from dse.cqlengine.models import Model
from dse.cqlengine import columns

from uuid import uuid4

class TestQueryUpdateModel(Model):

    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)
    text_set = columns.Set(columns.Text, required=False)
    text_list = columns.List(columns.Text, required=False)
    text_map = columns.Map(columns.Text, columns.Text, required=False)

class BaseCassEngTestCase(unittest.TestCase):

    session = None

    def setUp(self):
        self.session = get_session()

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{0} doesn't have attribute: {1}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{0} shouldn't have the attribute: {1}".format(obj, attr))

    if sys.version_info > (3, 0):
        def assertItemsEqual(self, first, second, msg=None):
            return self.assertCountEqual(first, second, msg)
