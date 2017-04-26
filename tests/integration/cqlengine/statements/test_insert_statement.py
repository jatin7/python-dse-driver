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

import six

from dse.cqlengine.columns import Column
from dse.cqlengine.statements import InsertStatement


class InsertStatementTests(unittest.TestCase):

    def test_statement(self):
        ist = InsertStatement('table', None)
        ist.add_assignment(Column(db_field='a'), 'b')
        ist.add_assignment(Column(db_field='c'), 'd')

        self.assertEqual(
            six.text_type(ist),
            'INSERT INTO table ("a", "c") VALUES (%(0)s, %(1)s)'
        )

    def test_context_update(self):
        ist = InsertStatement('table', None)
        ist.add_assignment(Column(db_field='a'), 'b')
        ist.add_assignment(Column(db_field='c'), 'd')

        ist.update_context_id(4)
        self.assertEqual(
            six.text_type(ist),
            'INSERT INTO table ("a", "c") VALUES (%(4)s, %(5)s)'
        )
        ctx = ist.get_context()
        self.assertEqual(ctx, {'4': 'b', '5': 'd'})

    def test_additional_rendering(self):
        ist = InsertStatement('table', ttl=60)
        ist.add_assignment(Column(db_field='a'), 'b')
        ist.add_assignment(Column(db_field='c'), 'd')
        self.assertIn('USING TTL 60', six.text_type(ist))
