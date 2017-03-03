# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from copy import copy
from itertools import chain
import json
import six
import time
import sys

from dse import OperationTimedOut, ConsistencyLevel, InvalidRequest
from dse.protocol import ServerError, SyntaxException
from dse.query import QueryTrace
from dse.policies import WhiteListRoundRobinPolicy
from dse.cluster import NoHostAvailable

from dse.cluster import EXEC_PROFILE_GRAPH_DEFAULT, GraphExecutionProfile, Cluster
from dse.graph import (SimpleGraphStatement, graph_object_row_factory, single_object_row_factory,\
                       graph_result_row_factory, Result, Edge, Vertex, Path, GraphOptions, _graph_options)
from dse.util import SortedSet

from tests.integration.advanced import BasicGraphUnitTestCase, use_single_node_with_graph, use_singledc_wth_graph, generate_classic, generate_line_graph, generate_multi_field_graph, generate_large_complex_graph, ALLOW_SCANS, MAKE_NON_STRICT,\
    validate_classic_vertex, validate_classic_edge, validate_path_result_type, validate_line_edge, validate_generic_vertex_result_type, fetchCustomGeoType
from tests.integration import PROTOCOL_VERSION, greaterthanorequaldse51, DSE_VERSION


def setup_module():
    use_single_node_with_graph()


class BasicGraphTest(BasicGraphUnitTestCase):

    def test_basic_query(self):
        """
        Test to validate that basic graph query results can be executed with a sane result set.

        Creates a simple classic tinkerpot graph, and attempts to find all vertices
        related the vertex marco, that have a label of knows.
        See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should find two vertices related to marco via 'knows' edges.

        @test_category dse graph
        """
        generate_classic(self.session)
        rs = self.session.execute_graph('''g.V().has('name','marko').out('knows').values('name')''')
        self.assertFalse(rs.has_more_pages)
        results_list = [result.value for result in rs.current_rows]
        self.assertEqual(len(results_list), 2)
        self.assertIn('vadas', results_list)
        self.assertIn('josh', results_list)

    def test_classic_graph(self):
        """
        Test to validate that basic graph generation, and vertex and edges are surfaced correctly

        Creates a simple classic tinkerpot graph, and iterates over the the vertices and edges
        ensureing that each one is correct. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """
        generate_classic(self.session)
        rs = self.session.execute_graph('g.V()')
        for vertex in rs:
            validate_classic_vertex(self, vertex)
        rs = self.session.execute_graph('g.E()')
        for edge in rs:
            validate_classic_edge(self, edge)

    def test_graph_classic_path(self):
        """
        Test to validate that the path version of the result type is generated correctly. It also
        tests basic path results as that is not covered elsewhere

        @since 1.0.0
        @jira_ticket PYTHON-479
        @expected_result path object should be unpacked correctly including all nested edges and verticies
        @test_category dse graph
        """
        generate_classic(self.session)

        rs = self.session.execute_graph("g.V().hasLabel('person').has('name', 'marko').as('a').outE('knows').inV().as('c', 'd').outE('created').as('e', 'f', 'g').inV().path()")
        rs_list = list(rs)
        self.assertEqual(len(rs_list), 2)
        for result in rs_list:
            path = result.as_path()
            validate_path_result_type(self, path)

    def test_large_create_script(self):
        """
        Test to validate that server errors due to large groovy scripts are properly surfaced

        Creates a very large line graph script and executes it. Then proceeds to create a line graph script
        that is to large for the server to handle expects a server error to be returned

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """
        query_to_run = generate_line_graph(250)
        self.session.execute_graph(query_to_run)
        query_to_run = generate_line_graph(300)
        self.assertRaises(SyntaxException, self.session.execute_graph, query_to_run)

    def test_range_query(self):
        """
        Test to validate range queries are handled correctly.

        Creates a very large line graph script and executes it. Then proceeds to to a range
        limited query against it, and ensure that the results are formated correctly and that
        the result set is properly sized.

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result result set should be properly formated and properly sized

        @test_category dse graph
        """
        query_to_run = generate_line_graph(250)
        self.session.execute_graph(query_to_run)
        rs = self.session.execute_graph("g.E().range(0,10)")
        self.assertFalse(rs.has_more_pages)
        results = list(rs)
        self.assertEqual(len(results), 10)
        for result in results:
            validate_line_edge(self, result)

    def test_result_types(self):
        """
        Test to validate that the edge and vertex version of results are constructed correctly.

        @since 1.0.0
        @jira_ticket PYTHON-479
        @expected_result edge/vertex result types should be unpacked correctly.
        @test_category dse graph
        """
        generate_multi_field_graph(self.session)  # TODO: we could just make a single vertex with properties of all types, or even a simple query that just uses a sequence of groovy expressions

        prof = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, row_factory=graph_result_row_factory)  # requires simplified row factory to avoid shedding id/~type information used for validation below
        rs = self.session.execute_graph("g.V()", execution_profile=prof)

        for result in rs:
            self._validate_type(result)

    def test_large_result_set(self):
        """
        Test to validate that large result sets return correctly.

        Creates a very large graph. Ensures that large result sets are handled appropriately.

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result when limits of result sets are hit errors should be surfaced appropriately

        @test_category dse graph
        """
        generate_large_complex_graph(self.session, 5000)
        rs = self.session.execute_graph("g.V()")
        for result in rs:
            validate_generic_vertex_result_type(self, result)

    def test_parameter_passing(self):
        """
        Test to validate that parameter passing works as expected

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result parameters work as expected

        @test_category dse graph
        """

        s = self.session
        # unused parameters are passed, but ignored
        s.execute_graph("null", {"doesn't": "matter", "what's": "passed"})

        # multiple params
        results = s.execute_graph("[a, b]", {'a': 0, 'b': 1})
        self.assertEqual(results[0].value, 0)
        self.assertEqual(results[1].value, 1)

        # different value types
        for param in (None, "string", 1234, 5.678, True, False):
            result = s.execute_graph('x', {'x': param})[0]
            self.assertEqual(result.value, param)

    def test_consistency_passing(self):
        """
        Test to validated that graph consistency levels are properly surfaced to the base dirver

        @since 1.0.0
        @jira_ticket PYTHON-509
        @expected_result graph consistency levels are surfaced correctly
        @test_category dse graph
        """
        cl_attrs = ('graph_read_consistency_level', 'graph_write_consistency_level')

        # Iterates over the graph options and constructs an array containing
        # The graph_options that correlate to graoh read and write consistency levels
        graph_params = [a[2] for a in _graph_options if a[0] in cl_attrs]

        s = self.session
        default_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        default_graph_opts = default_profile.graph_options
        try:
            # Checks the default graph attributes and ensures that both  graph_read_consistency_level and graph_write_consistency_level
            # Are None by default
            for attr in cl_attrs:
                self.assertIsNone(getattr(default_graph_opts, attr))

            res = s.execute_graph("null")
            for param in graph_params:
                self.assertNotIn(param, res.response_future.message.custom_payload)

            # session defaults are passed
            opts = GraphOptions()
            opts.update(default_graph_opts)
            cl = {0: ConsistencyLevel.ONE, 1: ConsistencyLevel.LOCAL_QUORUM}
            for k, v in cl.items():
                setattr(opts, cl_attrs[k], v)
            default_profile.graph_options = opts

            res = s.execute_graph("null")

            for k, v in cl.items():
                self.assertEqual(res.response_future.message.custom_payload[graph_params[k]], six.b(ConsistencyLevel.value_to_name[v]))

            # passed profile values override session defaults
            cl = {0: ConsistencyLevel.ALL, 1: ConsistencyLevel.QUORUM}
            opts = GraphOptions()
            opts.update(default_graph_opts)
            for k, v in cl.items():
                attr_name = cl_attrs[k]
                setattr(opts, attr_name, v)
                self.assertNotEqual(getattr(default_profile.graph_options, attr_name), getattr(opts, attr_name))
            tmp_profile = s.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, graph_options=opts)
            res = s.execute_graph("null", execution_profile=tmp_profile)

            for k, v in cl.items():
                self.assertEqual(res.response_future.message.custom_payload[graph_params[k]], six.b(ConsistencyLevel.value_to_name[v]))
        finally:
            default_profile.graph_options = default_graph_opts

    def test_additional_custom_payload(self):
        s = self.session
        custom_payload = {'some': 'example'.encode('utf-8'), 'items': 'here'.encode('utf-8')}
        sgs = SimpleGraphStatement("null", custom_payload=custom_payload)
        future = s.execute_graph_async(sgs)

        default_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        default_graph_opts = default_profile.graph_options
        for k, v in chain(custom_payload.items(), default_graph_opts.get_options_map().items()):
            self.assertEqual(future.message.custom_payload[k], v)

    def test_geometric_graph_types(self):
        """
        Test to validate that geometric types function correctly

        Creates a very simple graph, and tries to insert a simple point type

        @since 1.0.0
        @jira_ticket DSP-8087
        @expected_result json types associated with insert is parsed correctly

        @test_category dse graph
        """
        self.session.execute_graph('''import org.apache.cassandra.db.marshal.geometry.Point;
                                      schema.propertyKey('pointP').{0}.ifNotExists().create();
                                      schema.vertexLabel('PointV').properties('pointP').ifNotExists().create();'''.format(fetchCustomGeoType("point")))

        rs = self.session.execute_graph('''g.addV(label, 'PointV', 'pointP', 'POINT(0 1)');''')

        # if result set is not parsed correctly this will throw an exception
        self.assertIsNotNone(rs)

    def test_result_forms(self):
        """
        Test to validate that geometric types function correctly

        Creates a very simple graph, and tries to insert a simple point type

        @since 1.0.0
        @jira_ticket DSP-8087
        @expected_result json types assoicated with insert is parsed correctly

        @test_category dse graph
        """
        generate_classic(self.session)
        rs = list(self.session.execute_graph('g.V()'))
        self.assertGreater(len(rs), 0, "Result set was empty this was not expected")
        for v in rs:
            validate_classic_vertex(self, v)

        rs = list(self.session.execute_graph('g.E()'))
        self.assertGreater(len(rs), 0, "Result set was empty this was not expected")
        for e in rs:
            validate_classic_edge(self, e)

    def test_vertex_multiple_properties(self):
        """
        Test verifying vertex property form for various Cardinality

        All key types are encoded as a list, regardless of cardinality

        Single cardinality properties have only one value -- the last one added

        Default is single (this is config dependent)

        @since 1.0.0
        @jira_ticket PYTHON-487

        @test_category dse graph
        """
        s = self.session
        s.execute_graph('''Schema schema = graph.schema();
                           schema.propertyKey('mult_key').Text().multiple().ifNotExists().create();
                           schema.propertyKey('single_key').Text().single().ifNotExists().create();
                           schema.vertexLabel('MPW1').properties('mult_key').ifNotExists().create();
                           schema.vertexLabel('SW1').properties('single_key').ifNotExists().create();''')

        v = s.execute_graph('''v = graph.addVertex('MPW1')
                                 v.property('mult_key', 'value')
                                 v''')[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['mult_key']), 1)
        self.assertEqual(v.properties['mult_key'][0].label, 'mult_key')
        self.assertEqual(v.properties['mult_key'][0].value, 'value')

        # multiple_with_two_values
        v = s.execute_graph('''g.addV(label, 'MPW1', 'mult_key', 'value0', 'mult_key', 'value1')''')[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['mult_key']), 2)
        self.assertEqual(v.properties['mult_key'][0].label, 'mult_key')
        self.assertEqual(v.properties['mult_key'][1].label, 'mult_key')
        self.assertEqual(v.properties['mult_key'][0].value, 'value0')
        self.assertEqual(v.properties['mult_key'][1].value, 'value1')

        # single_with_one_value
        v = s.execute_graph('''v = graph.addVertex('SW1')
                                 v.property('single_key', 'value')
                                 v''')[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['single_key']), 1)
        self.assertEqual(v.properties['single_key'][0].label, 'single_key')
        self.assertEqual(v.properties['single_key'][0].value, 'value')

        # single_with_two_values
        with self.assertRaises(InvalidRequest):
            v = s.execute_graph('''v = graph.addVertex('SW1')
                                 v.property('single_key', 'value0', 'single_key', 'value1')
                                 v''')[0]

    def test_vertex_property_properties(self):
        """
        Test verifying vertex property properties

        @since 1.0.0
        @jira_ticket PYTHON-487

        @test_category dse graph
        """
        s = self.session
        s.execute_graph("schema.propertyKey('k0').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('k1').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('key').Text().properties('k0', 'k1').ifNotExists().create();")
        s.execute_graph("schema.vertexLabel('MLP').properties('key').ifNotExists().create();")
        v = s.execute_graph('''v = graph.addVertex('MLP')
                                 v.property('key', 'value', 'k0', 'v0', 'k1', 'v1')
                                 v''')[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['key']), 1)
        p = v.properties['key'][0]
        self.assertEqual(p.label, 'key')
        self.assertEqual(p.value, 'value')
        self.assertEqual(p.properties, {'k0': 'v0', 'k1': 'v1'})

    def test_profile_graph_options(self):
        s = self.session
        statement = SimpleGraphStatement("true")
        ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT)
        self.assertTrue(s.execute_graph(statement, execution_profile=ep)[0].value)

        # bad graph name to verify it's passed
        ep.graph_options = ep.graph_options.copy()
        ep.graph_options.graph_name = "definitely_not_correct"
        self.assertRaises(InvalidRequest, s.execute_graph, statement, execution_profile=ep)


    def test_execute_graph_timeout(self):
        s = self.session

        value = [1, 2, 3]
        query = "[%r]" % (value,)

        # default is passed down
        default_graph_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        rs = self.session.execute_graph(query)
        self.assertEqual(rs[0].value, value)
        self.assertEqual(rs.response_future.timeout, default_graph_profile.request_timeout)

        # tiny timeout times out as expected
        tmp_profile = copy(default_graph_profile)
        tmp_profile.request_timeout = sys.float_info.min

        max_retry_count = 10
        for _ in range(max_retry_count):
            start = time.time()
            try:
                with self.assertRaises(OperationTimedOut):
                    s.execute_graph(query, execution_profile=tmp_profile)
                break
            except:
                end = time.time()
                self.assertAlmostEqual(start, end, 1)
        else:
            raise Exception("session.execute_graph didn't time out in {0} tries".format(max_retry_count))


    def test_execute_graph_trace(self):
        s = self.session

        value = [1, 2, 3]
        query = "[%r]" % (value,)

        # default is no trace
        rs = s.execute_graph(query)
        self.assertEqual(rs[0].value, value)
        self.assertIsNone(rs.get_query_trace())

        # request trace
        rs = s.execute_graph(query, trace=True)
        self.assertEqual(rs[0].value, value)
        qt = rs.get_query_trace(max_wait_sec=10)
        self.assertIsInstance(qt, QueryTrace)
        self.assertIsNotNone(qt.duration)

    def test_execute_graph_row_factory(self):
        s = self.session

        # default Results
        default_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        self.assertEqual(default_profile.row_factory, graph_object_row_factory)
        result = s.execute_graph("123")[0]
        self.assertIsInstance(result, Result)
        self.assertEqual(result.value, 123)

        # other via parameter
        prof = s.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, row_factory=single_object_row_factory)
        rs = s.execute_graph("123", execution_profile=prof)
        self.assertEqual(rs.response_future.row_factory, single_object_row_factory)
        self.assertEqual(json.loads(rs[0]), {'result': 123})

    def _validate_type(self, vertex):
        out_vertex = "out_vertex"
        if DSE_VERSION >= "5.1":
            out_vertex = "~out_vertex"

        for properties in vertex.properties.values():
            prop = properties[0]
            type_indicator = prop['id'][out_vertex]['~label']
            if any(type_indicator.startswith(t) for t in ('int', 'short', 'long')):
                typ = int
            elif any(type_indicator.startswith(t) for t in ('float', 'double')):
                typ = float
            elif any(type_indicator.startswith(t) for t in ('date', 'negdate')):
                typ = six.text_type
            else:
                pass
                self.fail("Received unexpected type: %s" % type_indicator)
            self.assertIsInstance(prop['value'], typ)


class GraphTimeoutTests(BasicGraphUnitTestCase):

        def test_should_wait_indefinitely_by_default(self):
            """
            Tests that by default the client should wait indefinitely for server timeouts

            @since 1.0.0
            @jira_ticket PYTHON-589

            @test_category dse graph
            """
            desired_timeout = 1000

            graph_source = "test_timeout_1"
            ep_name = graph_source
            ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT)
            ep.graph_options = ep.graph_options.copy()
            ep.graph_options.graph_source = graph_source
            self.cluster.add_execution_profile(ep_name, ep)

            to_run = '''graph.schema().config().option("graph.traversal_sources.{0}.evaluation_timeout").set('{1} ms')'''.format(graph_source, desired_timeout)
            self.session.execute_graph(to_run, execution_profile=ep_name)
            with self.assertRaises(InvalidRequest) as ir:
                self.session.execute_graph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1", execution_profile=ep_name)
            self.assertTrue("Script evaluation exceeded the configured threshold of 1000" in str(ir.exception) or
                            "Script evaluation exceeded the configured threshold of evaluation_timeout at 1000" in str(ir.exception))

        def test_request_timeout_less_then_server(self):
            """
            Tests that with explicit request_timeouts set, that a server timeout is honored if it's relieved prior to the
            client timeout

            @since 1.0.0
            @jira_ticket PYTHON-589

            @test_category dse graph
            """
            desired_timeout = 1000
            graph_source = "test_timeout_2"
            ep_name = graph_source
            ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, request_timeout=32)
            ep.graph_options = ep.graph_options.copy()
            ep.graph_options.graph_source = graph_source
            self.cluster.add_execution_profile(ep_name, ep)



            to_run = '''graph.schema().config().option("graph.traversal_sources.{0}.evaluation_timeout").set('{1} ms')'''.format(graph_source, desired_timeout)
            self.session.execute_graph(to_run, execution_profile=ep_name)
            with self.assertRaises(InvalidRequest) as ir:
                self.session.execute_graph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1", execution_profile=ep_name)
            self.assertTrue("Script evaluation exceeded the configured threshold of 1000" in str(ir.exception) or
                            "Script evaluation exceeded the configured threshold of evaluation_timeout at 1000" in str(ir.exception))

        def test_server_timeout_less_then_request(self):
            """
            Tests that with explicit request_timeouts set, that a client timeout is honored if it's triggered prior to the
            server sending a timeout.

            @since 1.0.0
            @jira_ticket PYTHON-589

            @test_category dse graph
            """
            graph_source = "test_timeout_3"
            ep_name = graph_source
            ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, request_timeout=1)
            ep.graph_options = ep.graph_options.copy()
            ep.graph_options.graph_source = graph_source
            self.cluster.add_execution_profile(ep_name, ep)
            server_timeout = 10000
            to_run = '''graph.schema().config().option("graph.traversal_sources.{0}.evaluation_timeout").set('{1} ms')'''.format(graph_source, server_timeout)
            self.session.execute_graph(to_run, execution_profile=ep_name)

            with self.assertRaises(Exception) as e:
                self.session.execute_graph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1", execution_profile=ep_name)
                self.assertTrue(isinstance(e, InvalidRequest) or isinstance(e, OperationTimedOut))


class GraphProfileTests(BasicGraphUnitTestCase):

        def test_graph_profile(self):
            """
            Test verifying various aspects of graph config properties.

            @since 1.0.0
            @jira_ticket PYTHON-570

            @test_category dse graph
            """
            generate_classic(self.session)
            # Create variou execution policies
            exec_dif_factory = GraphExecutionProfile(row_factory=single_object_row_factory)
            exec_dif_factory.graph_options.graph_name = self.graph_name
            exec_dif_lbp = GraphExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
            exec_dif_lbp.graph_options.graph_name = self.graph_name
            exec_bad_lbp = GraphExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.2']))
            exec_dif_lbp.graph_options.graph_name = self.graph_name
            exec_short_timeout = GraphExecutionProfile(request_timeout=1, load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
            exec_short_timeout.graph_options.graph_name = self.graph_name

            # Add a single exection policy on cluster creation
            local_cluster = Cluster(protocol_version=PROTOCOL_VERSION, execution_profiles={"exec_dif_factory": exec_dif_factory})
            local_session = local_cluster.connect()
            rs1 = self.session.execute_graph('g.V()')
            rs2 = local_session.execute_graph('g.V()', execution_profile='exec_dif_factory')

            # Verify default and non default policy works
            self.assertFalse(isinstance(rs2[0], Vertex))
            self.assertTrue(isinstance(rs1[0], Vertex))
            # Add other policies validate that lbp are honored
            local_cluster.add_execution_profile("exec_dif_ldp", exec_dif_lbp)
            local_session.execute_graph('g.V()', execution_profile="exec_dif_ldp")
            local_cluster.add_execution_profile("exec_bad_lbp", exec_bad_lbp)
            with self.assertRaises(NoHostAvailable):
                local_session.execute_graph('g.V()', execution_profile="exec_bad_lbp")

            # Try with missing EP
            with self.assertRaises(ValueError):
                local_session.execute_graph('g.V()', execution_profile='bad_exec_profile')

            # Validate that timeout is honored
            local_cluster.add_execution_profile("exec_short_timeout", exec_short_timeout)
            with self.assertRaises(Exception) as e:
                self.assertTrue(isinstance(e, InvalidRequest) or isinstance(e, OperationTimedOut))
                local_session.execute_graph('java.util.concurrent.TimeUnit.MILLISECONDS.sleep(2000L);', execution_profile='exec_short_timeout')

class GraphMetadataTest(BasicGraphUnitTestCase):

    @greaterthanorequaldse51
    def test_dse_workloads(self):
        """
        Test to ensure dse_workloads is populated appropriately.
        Field added in DSE 5.1

        @since DSE 2.0
        @jira_ticket PYTHON-667
        @expected_result dse_workloads set is set on host model

        @test_category metadata
        """
        for host in self.cluster.metadata.all_hosts():
            self.assertIsInstance(host.dse_workloads, SortedSet)
            self.assertIn("Cassandra", host.dse_workloads)
            self.assertIn("Graph", host.dse_workloads)
