# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from tests.integration import PROTOCOL_VERSION

def assert_quiescent_pool_state(test_case, cluster):

    for session in cluster.sessions:
        pool_states = session.get_pool_state().values()
        test_case.assertTrue(pool_states)

        for state in pool_states:
            test_case.assertFalse(state['shutdown'])
            test_case.assertGreater(state['open_count'], 0)
            test_case.assertTrue(all((i == 0 for i in state['in_flights'])))

    for holder in cluster.get_connection_holders():
        for connection in holder.get_connections():
            # all ids are unique
            req_ids = connection.request_ids
            test_case.assertEqual(len(req_ids), len(set(req_ids)))
            test_case.assertEqual(connection.highest_request_id, len(req_ids) - 1)
            test_case.assertEqual(connection.highest_request_id, max(req_ids))

