# Copyright 2016 DataStax, Inc.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import json
import six

from dse.graph import (SimpleGraphStatement, GraphOptions, Result,
                       _graph_options, graph_result_row_factory, single_object_row_factory,
                       Vertex, Edge, Path)


class GraphResultTests(unittest.TestCase):

    _values = (None, 1, 1.2, True, False, [1, 2, 3], {'x': 1, 'y': 2})

    def test_result_value(self):
        for v in self._values:
            result = self._make_result(v)
            self.assertEqual(result.value, v)

    def test_result_attr(self):
        # value is not a dict
        result = self._make_result(123)
        with self.assertRaises(ValueError):
            result.something

        expected = {'a': 1, 'b': 2}
        result = self._make_result(expected)
        self.assertEqual(result.a, 1)
        self.assertEqual(result.b, 2)
        with self.assertRaises(AttributeError):
            result.not_present

    def test_result_item(self):
        # value is not a dict, list
        result = self._make_result(123)
        with self.assertRaises(ValueError):
            result['something']
        with self.assertRaises(ValueError):
            result[0]

        # dict key access
        expected = {'a': 1, 'b': 2}
        result = self._make_result(expected)
        self.assertEqual(result['a'], 1)
        self.assertEqual(result['b'], 2)
        with self.assertRaises(KeyError):
            result['not_present']
        with self.assertRaises(ValueError):
            result[0]

        # list index access
        expected = [0, 1]
        result = self._make_result(expected)
        self.assertEqual(result[0], 0)
        self.assertEqual(result[1], 1)
        with self.assertRaises(IndexError):
            result[2]
        with self.assertRaises(ValueError):
            result['something']

    def test_as_vertex(self):
        prop_name = 'name'
        prop_val = 'val'
        vertex_dict = {'id': object(),
                       'label': object(),
                       'type': 'vertex',
                       'properties': {prop_name: [{'value': prop_val, 'whatever': object()}]}
                      }
        required_attrs = [k for k in vertex_dict if k != 'properties']
        result = self._make_result(vertex_dict)
        vertex = result.as_vertex()
        for attr in required_attrs:
            self.assertEqual(getattr(vertex, attr), vertex_dict[attr])
        self.assertEqual(len(vertex.properties), 1)
        self.assertEqual(vertex.properties[prop_name], prop_val)

        # no props
        modified_vertex_dict = vertex_dict.copy()
        del modified_vertex_dict['properties']
        vertex = self._make_result(modified_vertex_dict).as_vertex()
        self.assertEqual(vertex.properties, {})

        # wrong 'type'
        modified_vertex_dict = vertex_dict.copy()
        modified_vertex_dict['type'] = 'notavertex'
        result = self._make_result(modified_vertex_dict)
        self.assertRaises(TypeError, result.as_vertex)

        # missing required properties
        for attr in required_attrs:
            modified_vertex_dict = vertex_dict.copy()
            del modified_vertex_dict[attr]
            result = self._make_result(modified_vertex_dict)
            self.assertRaises(TypeError, result.as_vertex)

    def test_as_edge(self):
        prop_name = 'name'
        prop_val = 'val'
        edge_dict = {'id': object(),
                     'label': object(),
                     'type': 'edge',
                     'inV': object(),
                     'inVLabel': object(),
                     'outV': object(),
                     'outVLabel': object(),
                     'properties': {prop_name: prop_val}
                    }
        required_attrs = [k for k in edge_dict if k != 'properties']
        result = self._make_result(edge_dict)
        edge = result.as_edge()
        for attr in required_attrs:
            self.assertEqual(getattr(edge, attr), edge_dict[attr])
        self.assertEqual(len(edge.properties), 1)
        self.assertEqual(edge.properties[prop_name], prop_val)

        # no props
        modified_edge_dict = edge_dict.copy()
        del modified_edge_dict['properties']
        edge = self._make_result(modified_edge_dict).as_edge()
        self.assertEqual(edge.properties, {})

        # wrong 'type'
        modified_edge_dict = edge_dict.copy()
        modified_edge_dict['type'] = 'notanedge'
        result = self._make_result(modified_edge_dict)
        self.assertRaises(TypeError, result.as_edge)

        # missing required properties
        for attr in required_attrs:
            modified_edge_dict = edge_dict.copy()
            del modified_edge_dict[attr]
            result = self._make_result(modified_edge_dict)
            self.assertRaises(TypeError, result.as_edge)

    def test_as_path(self):
        vertex_dict = {'id': object(),
                       'label': object(),
                       'type': 'vertex',
                       'properties': {'name': [{'value': 'val', 'whatever': object()}]}
                       }
        edge_dict = {'id': object(),
                     'label': object(),
                     'type': 'edge',
                     'inV': object(),
                     'inVLabel': object(),
                     'outV': object(),
                     'outVLabel': object(),
                     'properties': {'name': 'val'}
                     }
        path_dict = {'labels': [['a', 'b'], ['c']],
                     'objects': [vertex_dict, edge_dict]
                    }
        result = self._make_result(path_dict)
        path = result.as_path()
        self.assertEqual(path.labels, path_dict['labels'])

        # make sure inner objects are bound correctly
        for d, result in zip(path_dict['objects'], path.objects):
            self.assertEqual(d, result.value)
            if result.type == 'vertex':
                self.assertIsInstance(path.objects[0].as_vertex(), Vertex)
            else:
                self.assertIsInstance(path.objects[1].as_edge(), Edge)

        # missing required properties
        for attr in path_dict:
            modified_path_dict = path_dict.copy()
            del modified_path_dict[attr]
            result = self._make_result(modified_path_dict)
            self.assertRaises(TypeError, result.as_path)

    def test_str(self):
        for v in self._values:
            self.assertEqual(str(self._make_result(v)), str(v))

    def test_repr(self):
        for v in self._values:
            result = self._make_result(v)
            self.assertEqual(eval(repr(result)), result)

    def _make_result(self, value):
        # direct pass-through now
        return Result(value)


class GraphTypeTests(unittest.TestCase):
    # see also: GraphResultTests.test_as_*

    def test_vertex_str_repr(self ):
        prop_name = 'name'
        prop_val = 'val'
        kwargs = {'id': 'id_val', 'label': 'label_val', 'type': 'vertex', 'properties': {prop_name: [{'value': prop_val}]}}
        vertex = Vertex(**kwargs)
        transformed = kwargs.copy()
        transformed['properties'] = {prop_name: prop_val}
        self.assertEqual(eval(str(vertex)), transformed)
        self.assertEqual(eval(repr(vertex)), vertex)

    def test_edge_str_repr(self ):
        prop_name = 'name'
        prop_val = 'val'
        kwargs = {'id': 'id_val', 'label': 'label_val', 'type': 'edge',
                  'inV': 'inV_val', 'inVLabel': 'inVLabel_val',
                  'outV': 'outV_val', 'outVLabel': 'outVLabel_val',
                  'properties': {prop_name: prop_val}}
        edge = Edge(**kwargs)
        self.assertEqual(eval(str(edge)), kwargs)
        self.assertEqual(eval(repr(edge)), edge)

    def test_path_str_repr(self ):
        kwargs = {'labels': [['a', 'b'], ['c']], 'objects': range(10)}
        path = Path(**kwargs)
        transformed = kwargs.copy()
        transformed['objects'] = [Result(o) for o in kwargs['objects']]
        self.assertEqual(eval(str(path)), transformed)
        self.assertEqual(eval(repr(path)), path)


class GraphOptionTests(unittest.TestCase):

    opt_mapping = dict((t[0], t[2]) for t in _graph_options)

    api_params = dict((p, str(i)) for i, p in enumerate(opt_mapping))

    def test_init(self):
        opts = GraphOptions(**self.api_params)
        self._verify_api_params(opts, self.api_params)
        self._verify_api_params(GraphOptions(), {})

    def test_update(self):
        opts = GraphOptions(**self.api_params)
        new_params = dict((k, str(int(v) + 1)) for k, v in self.api_params.items())
        opts.update(GraphOptions(**new_params))
        self._verify_api_params(opts, new_params)

    def test_get_options(self):
        # nothing set --> base map
        base = GraphOptions(**self.api_params)
        self.assertIs(GraphOptions().get_options_map(base), base._graph_options)

        # something set overrides
        other = GraphOptions(graph_name='unit_test')
        options = other.get_options_map(base)
        updated = self.opt_mapping['graph_name']
        self.assertEqual(options[updated], six.b('unit_test'))
        for name in (n for n in self.opt_mapping.values() if n != updated):
            self.assertEqual(options[name], base._graph_options[name])

        # base unchanged
        self._verify_api_params(base, self.api_params)

    def test_set_attr(self):
        expected = 'test@@@@'
        opts = GraphOptions(graph_name=expected)
        self.assertEqual(opts.graph_name, six.b(expected))
        expected = 'somethingelse####'
        opts.graph_name = expected
        self.assertEqual(opts.graph_name, six.b(expected))

        # will update options with set value
        another = GraphOptions()
        self.assertIsNone(another.graph_name)
        another.update(opts)
        self.assertEqual(another.graph_name, six.b(expected))

        opts.graph_name = None
        self.assertIsNone(opts.graph_name)
        # will not update another with its set-->unset value
        another.update(opts)
        self.assertEqual(another.graph_name, six.b(expected))  # remains unset
        opt_map = opts.get_options_map(another)
        self.assertIs(opt_map, another._graph_options)

    def test_del_attr(self):
        opts = GraphOptions(**self.api_params)
        test_params = self.api_params.copy()
        del test_params['graph_alias']
        del opts.graph_alias
        self._verify_api_params(opts, test_params)

    def _verify_api_params(self, opts, api_params):
        self.assertEqual(len(opts._graph_options), len(api_params))
        for name, value in api_params.items():
            value = six.b(value)
            self.assertEqual(getattr(opts, name), value)
            self.assertEqual(opts._graph_options[self.opt_mapping[name]], value)


class GraphStatementTests(unittest.TestCase):

    def test_init(self):
        # just make sure Statement attributes are accepted
        kwargs = {'query_string': object(),
                  'retry_policy': object(),
                  'consistency_level': object(),
                  'fetch_size': object(),
                  'keyspace': object(),
                  'custom_payload': object()}
        statement = SimpleGraphStatement(**kwargs)
        for k, v in kwargs.items():
            self.assertIs(getattr(statement, k), v)

        # but not a bogus parameter
        kwargs['bogus'] = object()
        self.assertRaises(TypeError, SimpleGraphStatement, **kwargs)


class GraphRowFactoryTests(unittest.TestCase):

    def test_object_row_factory(self):
        col_names = []  # unused
        rows = [object() for _ in range(10)]
        self.assertEqual(single_object_row_factory(col_names, ((o,) for o in rows)), rows)

    def test_graph_result_row_factory(self):
        col_names = []  # unused
        rows = [json.dumps({'result': i}) for i in range(10)]
        results = graph_result_row_factory(col_names, ((o,) for o in rows))
        for i, res in enumerate(results):
            self.assertIsInstance(res, Result)
            self.assertEqual(res.value, i)
