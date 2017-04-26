# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from tests.integration import use_singledc, greaterthanorequaldse51, BasicSharedKeyspaceUnitTestCaseWTable, DSE_VERSION

import logging
log = logging.getLogger(__name__)

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from itertools import cycle, count
from six.moves import range

from dse import ProtocolVersion
from dse.cluster import Cluster, ExecutionProfile,ContinuousPagingOptions
from dse.concurrent import execute_concurrent
from dse.query import SimpleStatement


def setup_module():
    use_singledc()


@greaterthanorequaldse51
class ContPagingTests(BasicSharedKeyspaceUnitTestCaseWTable):

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION and DSE_VERSION < '5.1':
            return
        super(ContPagingTests, cls).setUpClass()
        cls.default_cont = object
        cls.one_page_cont = object()
        cls.many_pages_cont = object()

        cls.execution_profiles = {"CONTDEFAULT": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions()),
                              "ONEPAGE": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(max_pages=1)),
                              "MANYPAGES": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(max_pages=10)),
                              "BYTES": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(page_unit=ContinuousPagingOptions.PagingUnit.BYTES)),
                              "SLOW": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(max_pages_per_second=1)),}
        cls.cluster = Cluster(protocol_version=ProtocolVersion.DSE_V1, execution_profiles=cls.execution_profiles)

        cls.session = cls.cluster.connect(wait_for_all_pools=True)
        statements_and_params = zip(cycle(["INSERT INTO  "+cls.ks_name+"."+cls.ks_name+" (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(150)])
        execute_concurrent(cls.session, list(statements_and_params))

        cls.select_all_statement = "SELECT * FROM {0}.{0}".format(cls.ks_name)

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION and DSE_VERSION < '5.1':
            return
        super(ContPagingTests, cls).tearDownClass()


    def test_continous_paging(self):
        """
        Test to ensure that various continuous paging schemes return the full set of results.
        @since DSE 2.0
        @jira_ticket PYTHON-615
        @expected_result various continous paging options should fetch all the results

        @test_category queries
        """
        for ep in self.execution_profiles.keys():
            results = list(self.session.execute(self.select_all_statement, execution_profile= ep))
            self.assertEqual(len(results), 150)


    def test_page_fetch_size(self):
        """
        Test to ensure that continuous paging works appropriately with fetch size.
        @since DSE 2.0
        @jira_ticket PYTHON-615
        @expected_result continuous paging options should work sensibly with various fetch size

        @test_category queries
        """

        # Since we fetch one page at a time results should match fetch size
        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 150):
            self.session.default_fetch_size = fetch_size
            results = list(self.session.execute(self.select_all_statement, execution_profile= "ONEPAGE"))
            self.assertEqual(len(results), fetch_size)

        # Since we fetch ten pages at a time results should match fetch size * 10
        for fetch_size in (2, 3, 7, 10, 15):
            self.session.default_fetch_size = fetch_size
            results = list(self.session.execute(self.select_all_statement, execution_profile= "MANYPAGES"))
            self.assertEqual(len(results), fetch_size*10)

        # Default settings for continuous paging should be able to fetch all results regardless of fetch size
        for fetch_size in (2, 3, 7, 10, 15):
            self.session.default_fetch_size = fetch_size
            results = list(self.session.execute(self.select_all_statement, execution_profile= "CONTDEFAULT"))
            self.assertEqual(len(results), 150)

        # Changing the units should, not affect the number of results, if max_pages is not set
        for fetch_size in (2, 3, 7, 10, 15):
            self.session.default_fetch_size = fetch_size
            results = list(self.session.execute(self.select_all_statement, execution_profile= "BYTES"))
            self.assertEqual(len(results), 150)

        # This should take around 3 seconds to fetch but should still complete with all results
        self.session.default_fetch_size = 50
        results = list(self.session.execute(self.select_all_statement, execution_profile= "SLOW"))
        self.assertEqual(len(results), 150)

    def test_paging_cancel(self):
        """
        Test to ensure we can cancel a continuous paging session once it's started
        @since DSE 2.0
        @jira_ticket PYTHON-615
        @expected_result This query should be canceled before any sizable amount of results can be returned
        @test_category queries
        """

        self.session.default_fetch_size = 1
        # This combination should fetch one result a second. We should see a very few results
        results = self.session.execute_async(self.select_all_statement, execution_profile= "SLOW")
        result_set =results.result()
        result_set.cancel_continuous_paging()
        result_lst =list(result_set)
        self.assertLess(len(result_lst), 2, "Cancel should have aborted fetch immediately")

    def test_con_paging_verify_writes(self):
        """
        Test to validate results with a few continuous paging options
        @since DSE 2.0
        @jira_ticket PYTHON-615
        @expected_result all results should be returned correctly
        @test_category queries
        """
        sane_eps = ("CONTDEFAULT", "BYTES")
        prepared = self.session.prepare(self.select_all_statement)


        for ep in sane_eps:
            for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
                self.session.default_fetch_size = fetch_size
                results = self.session.execute(self.select_all_statement, execution_profile=ep)
                result_array = set()
                result_set = set()
                for result in results:
                    result_array.add(result.k)
                    result_set.add(result.v)

                self.assertEqual(set(range(150)), result_array)
                self.assertEqual(set([0]), result_set)

                statement = SimpleStatement(self.select_all_statement)
                results = self.session.execute(statement, execution_profile=ep)
                result_array = set()
                result_set = set()
                for result in results:
                    result_array.add(result.k)
                    result_set.add(result.v)

                self.assertEqual(set(range(150)), result_array)
                self.assertEqual(set([0]), result_set)

                results = self.session.execute(prepared, execution_profile=ep)
                result_array = set()
                result_set = set()
                for result in results:
                    result_array.add(result.k)
                    result_set.add(result.v)

                self.assertEqual(set(range(150)), result_array)
                self.assertEqual(set([0]), result_set)


