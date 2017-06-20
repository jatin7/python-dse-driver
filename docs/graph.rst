DSE Graph Queries
=================
Use :meth:`.Session.execute_graph` or :meth:`.Session.execute_graph_async` for executing gremlin queries in DSE Graph.
The DSE driver defines three Execution Profiles suitable for graph execution:

* :data:`~.cluster.EXEC_PROFILE_GRAPH_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT`

See :doc:`getting_started` and `Execution Profile documentation <http://datastax.github.io/python-driver/execution_profiles.html>`_
for more detail on working with profiles.

The DSE driver executes graph queries over the Cassandra native protocol::

    from dse.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
    from dse.graph import GraphOptions

    # create the default execution profile pointing at a specific graph
    graph_name = 'test'
    ep = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph_name))
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()

    # use the system execution profile (or one with no graph_options.graph_name set) when accessing the system API
    session.execute_graph("system.graph(name).ifNotExists().create()", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)

    # ... set dev mode or configure graph schema ...

    result = session.execute_graph('g.addV("name", "John", "age", 35)')  # uses the default execution profile
    vertex = result[0]
    type(vertex)  # :class:`.Vertex`

    session.execute_graph("system.graph(name).drop()", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)

By default (with :class:`.GraphExecutionProfile.row_factory` set to :func:`.graph.graph_object_row_factory`), known graph result
types are unpacked and returned as specialized types (:class:`.Vertex`, :class:`.Edge`). If the result is not one of these
types, a :class:`.graph.Result` is returned, containing the graph result parsed from JSON and removed from its outer dict.
The class has some accessor convenience methods for accessing top-level properties by name (`type`, `properties` above),
or lists by index::

    # dicts with `__getattr__` or `__getitem__`
    result = session.execute_graph("[[key_str: 'value', key_int: 3]]", execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0]  # Using system exec just because there is no graph defined
    result  # dse.graph.Result({u'key_str': u'value', u'key_int': 3})
    result.value  # {u'key_int': 3, u'key_str': u'value'} (dict)
    result.key_str  # u'value'
    result.key_int  # 3
    result['key_str']  # u'value'
    result['key_int']  # 3

    # lists with `__getitem__`
    result = session.execute_graph('[[0, 1, 2]]', execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0]
    result  # dse.graph.Result([0, 1, 2])
    result.value  # [0, 1, 2] (list)
    result[1]  # 1 (list[1])

You can use a different row factory by setting :attr:`.Session.default_graph_row_factory` or passing it to
:meth:`.Session.execute_graph`. For example, :func:`.graph.single_object_row_factory` returns the JSON result string`,
unparsed. :func:`.graph.graph_result_row_factory` returns parsed, but unmodified results (such that all metadata is retained,
unlike :func:`.graph.graph_object_row_factory`, which sheds some as attributes and properties are unpacked). These results
also provide convenience methods for converting to known types (:meth:`~.Result.as_vertex`, :meth:`~.Result.as_edge`, :meth:`~.Result.as_path`).

Vertex and Edge properties are never unpacked since their types are unknown. If you know your graph schema and want to
deserialize properties, use the :class:`.GraphSON1TypeDeserializer`. It provides convenient methods to deserialize by types (e.g.
deserialize_date, deserialize_uuid, deserialize_polygon etc.) Example::

    # ...
    from dse.graph import GraphSON1TypeDeserializer

    row = session.execute_graph("g.V().toList()")[0]
    value = row.properties['my_property_key'][0].value  # accessing the VertexProperty value
    value = GraphSON1TypeDeserializer.deserialize_timestamp(value)

    print value  # 2017-06-26 08:27:05
    print type(value)  # <type 'datetime.datetime'>


Named parameters are passed in a dict to :meth:`.cluster.Session.execute_graph`::

    result_set = session.execute_graph('[a, b]', {'a': 1, 'b': 2}, execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
    [r.value for r in result_set]  # [1, 2]

The following python types can be passed as named parameters and will be serialized
automatically to their graph representation:

==========   ================
DSE Graph    Python
==========   ================
boolean      bool
bigint       long, int (PY3)
int          int
smallint     int
varint       int
float        float
double       double
uuid         uuid.UUID
Decimal      Decimal
inet         str
timestamp    datetime.datetime
date         datetime.date
time         datetime.time
point        Point
linestring   LineString
polygon      Polygon
blob         bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
==========   ================

Example::

    s.execute_graph("""
      g.addV('all_types').
      property('blob', blob_value).
      property('timestamp', timestamp_value).
      property('polygon', polygon_value).toList()
    """, {
      'timestamp_value': datetime.datetime.now(),
      'blob_value': bytearray('hello world'),
      'polygon_value': Polygon(((30, 10), (40, 40), (20, 40), (10, 20), (30, 10)))
    })


As with all Execution Profile parameters, graph options can be set in the cluster default (as shown in the first example)
or specified per execution::

    ep = session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT,
                                                graph_options=GraphOptions(graph_name='something-else'))
    session.execute_graph(statement, execution_profile=ep)
