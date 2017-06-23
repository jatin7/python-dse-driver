# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from tests.integration import use_singledc

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from dse import OperationTimedOut
from dse.cluster import ExecutionProfile
from dse.connection import Connection
from dse.query import SimpleStatement
from dse.policies import ConstantSpeculativeExecutionPolicy, RoundRobinPolicy
from tests.integration import BasicSharedKeyspaceUnitTestCase, greaterthancass21

from tests import notwindows

from mock import patch

def setup_module():
    use_singledc()


class BadRoundRobinPolicy(RoundRobinPolicy):

    def make_query_plan(self, working_keyspace=None, query=None):
        pos = self._position
        self._position += 1

        hosts = []
        for _ in range(10):
            hosts.extend(self._live_hosts)

        return hosts


# This doesn't work well with Windows clock granularity
@notwindows
class SpecExecTest(BasicSharedKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        cls.common_setup(1)

        spec_ep_brr = ExecutionProfile(load_balancing_policy=BadRoundRobinPolicy(), speculative_execution_policy=ConstantSpeculativeExecutionPolicy(.01, 20))
        spec_ep_rr = ExecutionProfile(speculative_execution_policy=ConstantSpeculativeExecutionPolicy(.01, 20))
        spec_ep_rr_lim = ExecutionProfile(load_balancing_policy=BadRoundRobinPolicy(), speculative_execution_policy=ConstantSpeculativeExecutionPolicy(.01, 1))
        spec_ep_brr_lim = ExecutionProfile(load_balancing_policy=BadRoundRobinPolicy(), speculative_execution_policy=ConstantSpeculativeExecutionPolicy(0.4, 10))

        cls.cluster.add_execution_profile("spec_ep_brr", spec_ep_brr)
        cls.cluster.add_execution_profile("spec_ep_rr", spec_ep_rr)
        cls.cluster.add_execution_profile("spec_ep_rr_lim", spec_ep_rr_lim)
        cls.cluster.add_execution_profile("spec_ep_brr_lim", spec_ep_brr_lim)

    @greaterthancass21
    def test_speculative_execution(self):
        """
        Test to ensure that speculative execution honors LBP, and that they retry appropriately.

        This test will use various LBP, and ConstantSpeculativeExecutionPolicy settings and ensure the proper number of hosts are queried
        @since 3.7.0
        @jira_ticket PYTHON-218
        @expected_result speculative retries should honor max retries, idempotent state of queries, and underlying lbp.

        @test_category metadata
        """
        self.session.execute("""USE {0}""".format(self.keyspace_name))
        self.session.execute("""create or replace function timeout (arg int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS $$ long start = System.currentTimeMillis(); while(System.currentTimeMillis() - start < arg){} return arg; $$;""")
        self.session.execute("""CREATE TABLE  d (k int PRIMARY KEY , i int);""")
        self.session.execute("""INSERT INTO d (k,i) VALUES (0, 1000);""")
        statement = SimpleStatement("""SELECT timeout(i) FROM d WHERE k =0""", is_idempotent=True)
        statement_non_idem = SimpleStatement("""SELECT timeout(i) FROM d WHERE k =0""", is_idempotent=False)

        # This LBP should repeat hosts up to around 30
        result = self.session.execute(statement, execution_profile='spec_ep_brr')
        self.assertEqual(21, len(result.response_future.attempted_hosts))

        # This LBP should keep host list to 3
        result = self.session.execute(statement, execution_profile='spec_ep_rr')
        self.assertEqual(3, len(result.response_future.attempted_hosts))
        # Spec_execution policy should limit retries to 1
        result = self.session.execute(statement, execution_profile='spec_ep_rr_lim')

        self.assertEqual(2, len(result.response_future.attempted_hosts))

        # Spec_execution policy should not be used if  the query is not idempotent
        result = self.session.execute(statement_non_idem, execution_profile='spec_ep_brr')
        self.assertEqual(1, len(result.response_future.attempted_hosts))

        # Default policy with non_idem query
        result = self.session.execute(statement_non_idem)
        self.assertEqual(1, len(result.response_future.attempted_hosts))

        # Should be able to run an idempotent query against default execution policy with no speculative_execution_policy
        result = self.session.execute(statement)
        self.assertEqual(1, len(result.response_future.attempted_hosts))

        # Test timeout with spec_ex
        with self.assertRaises(OperationTimedOut):
            result = self.session.execute(statement, execution_profile='spec_ep_rr', timeout=.5)

        # PYTHON-736 Test speculation policy works with a prepared statement
        statement = self.session.prepare("SELECT timeout(i) FROM d WHERE k = ?")
        # non-idempotent
        result = self.session.execute(statement, (0,), execution_profile='spec_ep_brr')
        self.assertEqual(1, len(result.response_future.attempted_hosts))
        # idempotent
        statement.is_idempotent = True
        result = self.session.execute(statement, (0,), execution_profile='spec_ep_brr')
        self.assertLess(1, len(result.response_future.attempted_hosts))

    #TODO redo this tests with Scassandra
    def test_speculative_and_timeout(self):
        """
        Test to ensure the timeout is honored when using speculative execution
        @since 3.10
        @jira_ticket PYTHON-750
        @expected_result speculative retries be schedule every fixed period, during the maximum
        period of the timeout.

        @test_category metadata
        """
        # We mock this so no messages are sent, otherwise a reponse might arrive
        # and we would not know how many hosts we queried
        with patch.object(Connection, "send_msg", return_value = 100) as mocked_send_msg:

            statement = SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (0, 1);", is_idempotent=True)

            # An OperationTimedOut is placed here in response_future,
            # that's why we can't call session.execute,which would raise it, but
            # we have to directly wait for the event
            response_future = self.session.execute_async(statement, execution_profile='spec_ep_brr_lim', timeout=2.2)
            response_future._event.wait(4)
            self.assertIsInstance(response_future._final_exception, OperationTimedOut)

            # This is because 2.2 / 0.4 + 1 = 6
            self.assertEqual(len(response_future.attempted_hosts), 6)
