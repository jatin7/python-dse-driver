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

import sys
import re
import os
import time
import requests
from os.path import expanduser
from uuid import UUID
from decimal import Decimal
from ccmlib import common
import datetime
from dse.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT

from tests.integration import PROTOCOL_VERSION, DSE_VERSION, get_server_versions, BasicKeyspaceUnitTestCase, drop_keyspace_shutdown_cluster, get_cluster, get_node, teardown_package as base_teardown
from tests.integration import use_singledc, use_single_node, wait_for_node_socket
from dse.protocol import ServerError
from dse.util import Point, LineString, Polygon
from dse.graph import Edge, Vertex, Path
home = expanduser('~')

greaterthanorequaldse51 = unittest.skipUnless(DSE_VERSION >= '5.1', 'DSE version 5.1 or greater required')

# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)
MAKE_STRICT = "schema.config().option('graph.schema_mode').set('production')"
MAKE_NON_STRICT = "schema.config().option('graph.schema_mode').set('development')"
ALLOW_SCANS = "schema.config().option('graph.allow_scan').set('true')"

# A map of common types and their corresponding groovy declaration for use in schema creation and insertion
MAX_LONG = 9223372036854775807
MIN_LONG = -9223372036854775808
ZERO_LONG = 0

if sys.version_info < (3, 0):
    MAX_LONG = long(MAX_LONG)
    MIN_LONG = long(MIN_LONG)
    ZERO_LONG = long(ZERO_LONG)


TYPE_MAP = {"point1": ["Point()", Point(.5, .13)],
            "point2": ["Point()", Point(-5, .0)],
            "linestring1": ["Linestring()", LineString(((1.0, 2.0), (3.0, 4.0), (-89.0, 90.0)))],
            "polygon1": ["Polygon()", Polygon([(10.0, 10.0), (80.0, 10.0), (80., 88.0), (10., 89.0), (10., 10.0)], [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)], [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])],
            "smallint1": ["Smallint()", 1],
            "varint1": ["Varint()", 2147483647],
            "bigint1": ["Bigint()", MAX_LONG],
            "bigint2": ["Bigint()", MIN_LONG],
            "bigint2": ["Bigint()", ZERO_LONG],
            "int1": ["Int()", 100],
            "float1": ["Float()", .5],
            "double1": ["Double()", .3415681],
            "uuid1": ["Uuid()", UUID('12345678123456781234567812345678')],
            "decimal1": ["Decimal()", Decimal(10)],
            "duration1": ["Duration()", datetime.timedelta(milliseconds=1)],
            "inet1": ["Inet()", "127.0.0.1"],
            "blob": ["Blob()",  bytearray(b"Hello World")],
            "timestamp": ["Timestamp()", datetime.datetime.now().replace(microsecond=0)]
            }


def find_spark_master(session):

    # Itterate over the nodes the one with port 7080 open is the spark master
    for host in session.hosts:
        ip = host.address
        port = 7077
        spark_master = (ip, port)
        if common.check_socket_listening(spark_master, timeout=3):
            return spark_master[0]
    return None


def wait_for_spark_workers(num_of_expected_workers, timeout):
    """
    This queries the spark master and checks for the expected number of workers
    """
    start_time = time.time()
    match = False
    while True:
        r = requests.get("http://localhost:7080")
        match = re.search('Alive Workers:.*(\d+)</li>', r.text)
        num_workers = int(match.group(1))
        if num_workers == num_of_expected_workers:
            match = True
            break
        elif time.time() - start_time > timeout:
            match = True
            break
        time.sleep(1)
    return match


def use_single_node_with_graph(start=True):
    use_single_node(start=start, workloads=['graph'])


def use_single_node_with_graph_and_spark(start=True):
    use_single_node(start=start, workloads=['graph', 'spark'])


def use_single_node_with_graph_and_solr(start=True):
    use_single_node(start=start, workloads=['graph', 'solr'])


def use_singledc_wth_graph(start=True):
    use_singledc(start=start, workloads=['graph'])


def use_singledc_wth_graph_and_spark(start=True):
    use_cluster_with_graph(3)


def use_cluster_with_graph(num_nodes):
    """
    This is a  work around to account for the fact that spark nodes will conflict over master assignment
    when started all at once.
    """

    # Create the cluster but don't start it.
    use_singledc(start=False, workloads=['graph', 'spark'])
    # Start first node.
    get_node(1).start(wait_for_binary_proto=True)
    # Wait binary protocol port to open
    wait_for_node_socket(get_node(1), 120)
    # Wait for spark master to start up
    spark_master_http = ("localhost", 7080)
    common.check_socket_listening(spark_master_http, timeout=60)
    tmp_cluster = Cluster(protocol_version=PROTOCOL_VERSION)

    # Start up remaining nodes.
    try:
        session = tmp_cluster.connect()
        statement = "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': '%d'}" % (num_nodes)
        session.execute(statement)
    finally:
        tmp_cluster.shutdown()

    for i in range(1, num_nodes+1):
        if i is not 1:
            node = get_node(i)
            node.start(wait_for_binary_proto=True)
            wait_for_node_socket(node, 120)

    # Wait for workers to show up as Alive on master
    wait_for_spark_workers(3, 120)


def reset_graph(session, graph_name):
        session.execute_graph('system.graph(name).ifNotExists().create()', {'name': graph_name})
        wait_for_graph_inserted(session, graph_name)


def wait_for_graph_inserted(session, graph_name):
        count = 0
        exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name})[0].value
        while not exists and count < 50:
            time.sleep(1)
            exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name})[0].value
        return exists


class BasicGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """
    @property
    def graph_name(self):
        return self._testMethodName.lower()

    def session_setup(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        self.session_setup()
        self.reset_graph()
        profiles = self.cluster.profile_manager.profiles
        profiles[EXEC_PROFILE_GRAPH_DEFAULT].graph_options.graph_name = self.graph_name
        profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].graph_options.graph_name = self.graph_name
        self.clear_schema()

    def tearDown(self):
        self.cluster.shutdown()

    def clear_schema(self):
        self.session.execute_graph('schema.clear()')

    def reset_graph(self):
        reset_graph(self.session, self.graph_name)

    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)


class BasicSharedGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """

    @classmethod
    def session_setup(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.graph_name = cls.__name__.lower()

    @classmethod
    def setUpClass(cls):
        cls.session_setup()
        cls.reset_graph()
        profiles = cls.cluster.profile_manager.profiles
        profiles[EXEC_PROFILE_GRAPH_DEFAULT].graph_options.graph_name = cls.graph_name
        profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].graph_options.graph_name = cls.graph_name
        cls.clear_schema()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()

    @classmethod
    def clear_schema(self):
        self.session.execute_graph('schema.clear()')

    @classmethod
    def reset_graph(self):
        reset_graph(self.session, self.graph_name)

    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)

def fetchCustomGeoType(type):
    if type.lower().startswith("point"):
        return getPointType()
    elif type.lower().startswith("line"):
        return getLineType()
    elif type.lower().startswith("poly"):
        return getPolygonType()
    else:
        return None

def getPointType():
    if DSE_VERSION.startswith("5.0"):
        return "Point()"

    return "Point().withGeoBounds()"

def getLineType():
    if DSE_VERSION.startswith("5.0"):
        return "Linestring()"

    return "Linestring().withGeoBounds()"

def getPolygonType():
    if DSE_VERSION.startswith("5.0"):
        return "Polygon()"

    return "Polygon().withGeoBounds()"



class BasicGeometricUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This base test class is used by all the geomteric tests. It contains class level teardown and setup
    methods. It also contains the test fixtures used by those tests
    """
    @classmethod
    def common_dse_setup(cls, rf, keyspace_creation=True):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        if keyspace_creation:
            cls.create_keyspace(rf)
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.session.set_keyspace(cls.ks_name)

    @classmethod
    def setUpClass(cls):
        cls.common_dse_setup(1)
        cls.initalizeTables()

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster(cls.ks_name, cls.session, cls.cluster)

    @classmethod
    def initalizeTables(cls):
        udt_type = "CREATE TYPE udt1 (g {0})".format(cls.cql_type_name)
        large_table = "CREATE TABLE tbl (k uuid PRIMARY KEY, g {0}, l list<{0}>, s set<{0}>, m0 map<{0},int>, m1 map<int,{0}>, t tuple<{0},{0},{0}>, u frozen<udt1>)".format(cls.cql_type_name)
        simple_table = "CREATE TABLE tblpk (k {0} primary key, v int)".format( cls.cql_type_name)
        cluster_table = "CREATE TABLE tblclustering (k0 int, k1 {0}, v int, primary key (k0, k1))".format(cls.cql_type_name)
        cls.session.execute(udt_type)
        cls.session.execute(large_table)
        cls.session.execute(simple_table)
        cls.session.execute(cluster_table)


def generate_line_graph(length):
        query_parts = []
        query_parts.append(ALLOW_SCANS+';')
        query_parts.append("schema.propertyKey('index').Int().ifNotExists().create();")
        query_parts.append("schema.propertyKey('distance').Int().ifNotExists().create();")
        query_parts.append("schema.vertexLabel('lp').properties('index').ifNotExists().create();")
        query_parts.append("schema.edgeLabel('goesTo').properties('distance').connection('lp', 'lp').ifNotExists().create();")
        for index in range(0, length):
            query_parts.append('''Vertex vertex{0} = graph.addVertex(label, 'lp', 'index', {0}); '''.format(index))
            if index is not 0:
                query_parts.append('''vertex{0}.addEdge('goesTo', vertex{1}, 'distance', 5); '''.format(index-1, index))
        final_graph_generation_statement = "".join(query_parts)
        return final_graph_generation_statement


def generate_classic(session):
    to_run = [MAKE_STRICT, ALLOW_SCANS, '''schema.propertyKey('name').Text().ifNotExists().create();
            schema.propertyKey('age').Int().ifNotExists().create();
            schema.propertyKey('lang').Text().ifNotExists().create();
            schema.propertyKey('weight').Float().ifNotExists().create();
            schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
            schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();
            schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();
            schema.edgeLabel('created').connection('software', 'software').add();
            schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();''',
            '''Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
            Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
            Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
            Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
            Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
            Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
            marko.addEdge('knows', vadas, 'weight', 0.5f);
            marko.addEdge('knows', josh, 'weight', 1.0f);
            marko.addEdge('created', lop, 'weight', 0.4f);
            josh.addEdge('created', ripple, 'weight', 1.0f);
            josh.addEdge('created', lop, 'weight', 0.4f);
            peter.addEdge('created', lop, 'weight', 0.2f);''']
    for run in to_run:
        succeed = False
        count = 0
        # Retry up to 10 times this is an issue for
        # Graph Mult-NodeClusters
        while count < 10 and not succeed:
            try:
                session.execute_graph(run)
                succeed = True
            except (ServerError):
                print("error creating classic graph retrying")
                time.sleep(.5)
            count += 1


def generate_multi_field_graph(session):
        to_run = [ALLOW_SCANS,
                  '''schema.propertyKey('shortvalue').Smallint().ifNotExists().create();
                     schema.vertexLabel('shortvertex').properties('shortvalue').ifNotExists().create();
                     short s1 = 5000; graph.addVertex(label, "shortvertex", "shortvalue", s1);''',
                  '''schema.propertyKey('intvalue').Int().ifNotExists().create();
                     schema.vertexLabel('intvertex').properties('intvalue').ifNotExists().create();
                     int i1 = 1000000000; graph.addVertex(label, "intvertex", "intvalue", i1);''',
                  '''schema.propertyKey('intvalue2').Int().ifNotExists().create();
                     schema.vertexLabel('intvertex2').properties('intvalue2').ifNotExists().create();
                     Integer i2 = 100000000; graph.addVertex(label, "intvertex2", "intvalue2", i2);''',
                  '''schema.propertyKey('longvalue').Bigint().ifNotExists().create();
                     schema.vertexLabel('longvertex').properties('longvalue').ifNotExists().create();
                     long l1 = 9223372036854775807; graph.addVertex(label, "longvertex", "longvalue", l1);''',
                  '''schema.propertyKey('longvalue2').Bigint().ifNotExists().create();
                     schema.vertexLabel('longvertex2').properties('longvalue2').ifNotExists().create();
                     Long l2 = 100000000000000000L; graph.addVertex(label, "longvertex2", "longvalue2", l2);''',
                  '''schema.propertyKey('floatvalue').Float().ifNotExists().create();
                     schema.vertexLabel('floatvertex').properties('floatvalue').ifNotExists().create();
                     float f1 = 3.5f; graph.addVertex(label, "floatvertex", "floatvalue", f1);''',
                  '''schema.propertyKey('doublevalue').Double().ifNotExists().create();
                     schema.vertexLabel('doublevertex').properties('doublevalue').ifNotExists().create();
                     double d1 = 3.5e40; graph.addVertex(label, "doublevertex", "doublevalue", d1);''',
                  '''schema.propertyKey('doublevalue2').Double().ifNotExists().create();
                     schema.vertexLabel('doublevertex2').properties('doublevalue2').ifNotExists().create();
                     Double d2 = 3.5e40d; graph.addVertex(label, "doublevertex2", "doublevalue2", d2);''']

        for run in to_run:
            session.execute_graph(run)


def generate_type_graph_schema(session, prime_schema=True):
    """
    This method will prime the schema for all types in the TYPE_MAP
    """

    session.execute_graph(ALLOW_SCANS)
    if(prime_schema):
        for key in TYPE_MAP.keys():
            prop_type = fetchCustomGeoType(key)
            if prop_type is None:
                prop_type=TYPE_MAP[key][0]
            vertex_label = key
            prop_name = key+"value"
            insert_string = ""
            insert_string += "schema.propertyKey('{0}').{1}.ifNotExists().create();".format(prop_name, prop_type)
            insert_string += "schema.vertexLabel('{0}').properties('{1}').ifNotExists().create();".format(vertex_label, prop_name)
            session.execute_graph(insert_string)
    else:
        session.execute_graph(MAKE_NON_STRICT)


def generate_address_book_graph(session, size):
    to_run = [ALLOW_SCANS,
              "schema.propertyKey('name').Text().create()\n" +
              "schema.propertyKey('coordinates')."+getPointType()+".create()\n" +
              "schema.propertyKey('city').Text().create()\n" +
              "schema.propertyKey('state').Text().create()\n" +
              "schema.propertyKey('description').Text().create()\n" +
              "schema.vertexLabel('person').properties('name', 'coordinates', 'city', 'state', 'description').create()\n" +
              "schema.vertexLabel('person').index('search').search().by('name').asString().by('coordinates').by('description').asText().add()",
              "g.addV('person').property('name', 'Paul Thomas Joe').property('city', 'Rochester').property('state', 'MN').property('coordinates', Geo.point(-92.46295, 44.0234)).property('description', 'Lives by the hospital')",
              "g.addV('person').property('name', 'George Bill Steve').property('city', 'Minneapolis').property('state', 'MN').property('coordinates', Geo.point(-93.266667, 44.093333)).property('description', 'A cold dude')",
              "g.addV('person').property('name', 'James Paul Smith').property('city', 'Chicago').property('state', 'IL').property('coordinates', Geo.point(-87.684722, 41.836944)).property('description', 'Likes to hang out')",
              "g.addV('person').property('name', 'Jill Alice').property('city', 'Atlanta').property('state', 'GA').property('coordinates', Geo.point(-84.39, 33.755)).property('description', 'Enjoys a nice cold coca cola')"]

    for run in to_run:
        session.execute_graph(run)


def generate_large_complex_graph(session, size):
        to_run = '''schema.config().option('graph.schema_mode').set('development');
            int size = 2000;
            List ids = new ArrayList();
            schema.propertyKey('ts').Int().single().ifNotExists().create();
            schema.propertyKey('sin').Int().single().ifNotExists().create();
            schema.propertyKey('cos').Int().single().ifNotExists().create();
            schema.propertyKey('ii').Int().single().ifNotExists().create();
            schema.vertexLabel('lcg').properties('ts', 'sin', 'cos', 'ii').ifNotExists().create();
            schema.edgeLabel('linked').connection('lcg', 'lcg').ifNotExists().create();
            Vertex v = graph.addVertex(label, 'lcg');
            v.property("ts", 100001);
            v.property("sin", 0);
            v.property("cos", 1);
            v.property("ii", 0);
            ids.add(v.id());
            Random rand = new Random();
            for (int ii = 1; ii < size; ii++) {
                v = graph.addVertex(label, 'lcg');
                v.property("ii", ii);
                v.property("ts", 100001 + ii);
                v.property("sin", Math.sin(ii/5.0));
                v.property("cos", Math.cos(ii/5.0));
                Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();
                v.addEdge("linked", u);
                ids.add(u.id());
                ids.add(v.id());
            }
            g.V().count();'''
        prof = session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, request_timeout=32)
        session.execute_graph(to_run, execution_profile=prof)


def validate_classic_vertex(test, vertex):
        vertex_props = vertex.properties.keys()
        test.assertEqual(len(vertex_props), 2)
        test.assertIn('name', vertex_props)
        test.assertTrue('lang' in vertex_props or 'age' in vertex_props)


def validate_classic_vertex_return_type(test, vertex):
    validate_generic_vertex_result_type(vertex)
    vertex_props = vertex.properties
    test.assertIn('name', vertex_props)
    test.assertTrue('lang' in vertex_props or 'age' in vertex_props)


def validate_generic_vertex_result_type(test, vertex):
    test.assertIsInstance(vertex, Vertex)
    for attr in ('id', 'type', 'label', 'properties'):
        test.assertIsNotNone(getattr(vertex, attr))


def validate_classic_edge_properties(test, edge_properties):
    test.assertEqual(len(edge_properties.keys()), 1)
    test.assertIn('weight', edge_properties)


def validate_classic_edge(test, edge):
    validate_generic_edge_result_type(test, edge)
    validate_classic_edge_properties(test, edge.properties)


def validate_line_edge(test, edge):
    validate_generic_edge_result_type(test, edge)
    edge_props = edge.properties
    test.assertEqual(len(edge_props.keys()), 1)
    test.assertIn('distance', edge_props)


def validate_generic_edge_result_type(test, edge):
    test.assertIsInstance(edge, Edge)
    for attr in ('properties', 'outV', 'outVLabel', 'inV', 'inVLabel', 'label', 'type', 'id'):
        test.assertIsNotNone(getattr(edge, attr))


def validate_path_result_type(test, path):
    test.assertIsInstance(path, Path)
    test.assertIsNotNone(path.labels)
    for obj in path.objects:
        if isinstance(obj, Edge):
            validate_classic_edge(test, obj)
        elif isinstance(obj, Vertex):
            validate_classic_vertex(test, obj)
        else:
            test.fail("Invalid object found in path " + str(object.type))
