# Copyright 2016 DataStax, Inc.
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

from dse.query import BatchStatement, SimpleStatement


class BatchStatementTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a PR

    def test_clear(self):
        keyspace = 'keyspace'
        routing_key = 'routing_key'
        custom_payload = {'key': six.b('value')}

        ss = SimpleStatement('whatever', keyspace=keyspace, routing_key=routing_key, custom_payload=custom_payload)

        batch = BatchStatement()
        batch.add(ss)

        self.assertTrue(batch._statements_and_parameters)
        self.assertEqual(batch.keyspace, keyspace)
        self.assertEqual(batch.routing_key, routing_key)
        self.assertEqual(batch.custom_payload, custom_payload)

        batch.clear()
        self.assertFalse(batch._statements_and_parameters)
        self.assertIsNone(batch.keyspace)
        self.assertIsNone(batch.routing_key)
        self.assertFalse(batch.custom_payload)

        batch.add(ss)

    def test_clear_empty(self):
        batch = BatchStatement()
        batch.clear()
        self.assertFalse(batch._statements_and_parameters)
        self.assertIsNone(batch.keyspace)
        self.assertIsNone(batch.routing_key)
        self.assertFalse(batch.custom_payload)

        batch.add('something')

    def test_add_all(self):
        batch = BatchStatement()
        statements = ['%s'] * 10
        parameters = [(i,) for i in range(10)]
        batch.add_all(statements, parameters)
        bound_statements = [t[1] for t in batch._statements_and_parameters]
        str_parameters = [str(i) for i in range(10)]
        self.assertEqual(bound_statements, str_parameters)

    def test_len(self):
        for n in 0, 10, 100:
            batch = BatchStatement()
            batch.add_all(statements=['%s'] * n,
                          parameters=[(i,) for i in range(n)])
            self.assertEqual(len(batch), n)
