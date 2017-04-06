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

from mock import patch

from dse import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException, ProtocolVersion
from dse.cluster import _Scheduler, Session, Cluster, _NOT_SET, default_lbp_factory, \
    ExecutionProfile, EXEC_PROFILE_DEFAULT
from dse.hosts import Host
from dse.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy
from dse.query import SimpleStatement, named_tuple_factory, tuple_factory
from tests.unit.utils import mock_session_pools

try:
    from dse.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None  # noqa


class ExceptionTypeTest(unittest.TestCase):

    def test_exception_types(self):
        """
        PYTHON-443
        Sanity check to ensure we don't unintentionally change class hierarchy of exception types
        """
        self.assertTrue(issubclass(Unavailable, DriverException))
        self.assertTrue(issubclass(Unavailable, RequestExecutionException))

        self.assertTrue(issubclass(ReadTimeout, DriverException))
        self.assertTrue(issubclass(ReadTimeout, RequestExecutionException))
        self.assertTrue(issubclass(ReadTimeout, Timeout))

        self.assertTrue(issubclass(WriteTimeout, DriverException))
        self.assertTrue(issubclass(WriteTimeout, RequestExecutionException))
        self.assertTrue(issubclass(WriteTimeout, Timeout))

        self.assertTrue(issubclass(CoordinationFailure, DriverException))
        self.assertTrue(issubclass(CoordinationFailure, RequestExecutionException))

        self.assertTrue(issubclass(ReadFailure, DriverException))
        self.assertTrue(issubclass(ReadFailure, RequestExecutionException))
        self.assertTrue(issubclass(ReadFailure, CoordinationFailure))

        self.assertTrue(issubclass(WriteFailure, DriverException))
        self.assertTrue(issubclass(WriteFailure, RequestExecutionException))
        self.assertTrue(issubclass(WriteFailure, CoordinationFailure))

        self.assertTrue(issubclass(FunctionFailure, DriverException))
        self.assertTrue(issubclass(FunctionFailure, RequestExecutionException))

        self.assertTrue(issubclass(RequestValidationException, DriverException))

        self.assertTrue(issubclass(ConfigurationException, DriverException))
        self.assertTrue(issubclass(ConfigurationException, RequestValidationException))

        self.assertTrue(issubclass(AlreadyExists, DriverException))
        self.assertTrue(issubclass(AlreadyExists, RequestValidationException))
        self.assertTrue(issubclass(AlreadyExists, ConfigurationException))

        self.assertTrue(issubclass(InvalidRequest, DriverException))
        self.assertTrue(issubclass(InvalidRequest, RequestValidationException))

        self.assertTrue(issubclass(Unauthorized, DriverException))
        self.assertTrue(issubclass(Unauthorized, RequestValidationException))

        self.assertTrue(issubclass(AuthenticationFailed, DriverException))

        self.assertTrue(issubclass(OperationTimedOut, DriverException))

        self.assertTrue(issubclass(UnsupportedOperation, DriverException))


class ClusterTest(unittest.TestCase):

    def test_invalid_contact_point_types(self):
        with self.assertRaises(ValueError):
            Cluster(contact_points=[None], protocol_version=4, connect_timeout=1)
        with self.assertRaises(TypeError):
            Cluster(contact_points="not a sequence", protocol_version=4, connect_timeout=1)


class SchedulerTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a ticket

    @patch('time.time', return_value=3)  # always queue at same time
    @patch('dse.cluster._Scheduler.run')  # don't actually run the thread
    def test_event_delay_timing(self, *_):
        """
        Schedule something with a time collision to make sure the heap comparison works

        PYTHON-473
        """
        sched = _Scheduler(None)
        sched.schedule(0, lambda: None)
        sched.schedule(0, lambda: None)  # pre-473: "TypeError: unorderable types: function() < function()"t


class SessionTest(unittest.TestCase):
    def setUp(self):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

    # TODO: this suite could be expanded; for now just adding a test covering a PR
    @mock_session_pools
    def test_default_serial_consistency_level(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy)])

        # default is None
        default_profile = c.profile_manager.default
        self.assertIsNone(default_profile.serial_consistency_level)

        sentinel = 1001
        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL, sentinel):
            default_profile.serial_consistency_level = cl

            # default is passed through
            f = s.execute_async(query='')
            self.assertEqual(f.message.serial_consistency_level, cl)

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                self.assertEqual(default_profile.serial_consistency_level, cl)
                self.assertEqual(f.message.serial_consistency_level, cl_override)


class ProtocolVersionTests(unittest.TestCase):

    def test_protocol_downgrade_test(self):

        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.DSE_V1)
        self.assertEqual(ProtocolVersion.V4,lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V4)
        self.assertEqual(ProtocolVersion.V3,lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V3)
        self.assertEqual(0,lower)

        self.assertTrue(ProtocolVersion.uses_error_code_map(ProtocolVersion.DSE_V1))
        self.assertTrue(ProtocolVersion.uses_int_query_flags(ProtocolVersion.DSE_V1))

        self.assertFalse(ProtocolVersion.uses_error_code_map(ProtocolVersion.V4))
        self.assertFalse(ProtocolVersion.uses_int_query_flags(ProtocolVersion.V4))



class ExecutionProfileTest(unittest.TestCase):
    def setUp(self):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

    def _verify_response_future_profile(self, rf, prof):
        self.assertEqual(rf._load_balancer, prof.load_balancing_policy)
        self.assertEqual(rf._retry_policy, prof.retry_policy)
        self.assertEqual(rf.message.consistency_level, prof.consistency_level)
        self.assertEqual(rf.message.serial_consistency_level, prof.serial_consistency_level)
        self.assertEqual(rf.timeout, prof.request_timeout)
        self.assertEqual(rf.row_factory, prof.row_factory)

    @mock_session_pools
    def test_default_exec_parameters(self):
        cluster = Cluster()
        self.assertEqual(cluster.profile_manager.default.load_balancing_policy.__class__, default_lbp_factory().__class__)
        self.assertEqual(cluster.profile_manager.default.retry_policy.__class__, RetryPolicy)
        self.assertEqual(cluster.profile_manager.default.request_timeout, 10.0)
        self.assertEqual(cluster.profile_manager.default.consistency_level, ConsistencyLevel.LOCAL_ONE)
        self.assertEqual(cluster.profile_manager.default.serial_consistency_level, None)
        self.assertEqual(cluster.profile_manager.default.row_factory, named_tuple_factory)

    @mock_session_pools
    def test_default_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        default_profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
        rf = session.execute_async("query")
        self._verify_response_future_profile(rf, default_profile)

        rf = session.execute_async("query", execution_profile='non-default')
        self._verify_response_future_profile(rf, non_default_profile)

    @mock_session_pools
    def test_statement_params_override_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        rf = session.execute_async("query", execution_profile='non-default')

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        self.assertNotEqual(ss.retry_policy.__class__, rf._load_balancer.__class__)
        self.assertNotEqual(ss.consistency_level, rf.message.consistency_level)
        self.assertNotEqual(ss._serial_consistency_level, rf.message.serial_consistency_level)
        self.assertNotEqual(my_timeout, rf.timeout)

        rf = session.execute_async(ss, timeout=my_timeout, execution_profile='non-default')
        expected_profile = ExecutionProfile(non_default_profile.load_balancing_policy, ss.retry_policy,
                                            ss.consistency_level, ss._serial_consistency_level, my_timeout, non_default_profile.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_profile_name_value(self):

        internalized_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        cluster = Cluster(execution_profiles={'by-name': internalized_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        rf = session.execute_async("query", execution_profile='by-name')
        self._verify_response_future_profile(rf, internalized_profile)

        by_value = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        rf = session.execute_async("query", execution_profile=by_value)
        self._verify_response_future_profile(rf, by_value)

    @mock_session_pools
    def test_exec_profile_clone(self):

        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        profile_attrs = {'request_timeout': 1,
                         'consistency_level': ConsistencyLevel.ANY,
                         'serial_consistency_level': ConsistencyLevel.SERIAL,
                         'row_factory': tuple_factory,
                         'retry_policy': RetryPolicy(),
                         'load_balancing_policy': default_lbp_factory()}
        reference_attributes = ('retry_policy', 'load_balancing_policy')

        # default and one named
        for profile in (EXEC_PROFILE_DEFAULT, 'one'):
            active = cluster.profile_manager.profiles[profile]
            clone = session.execution_profile_clone_update(profile)
            self.assertIsNot(clone, active)

            all_updated = session.execution_profile_clone_update(clone, **profile_attrs)
            self.assertIsNot(all_updated, clone)
            for attr, value in profile_attrs.items():
                self.assertEqual(getattr(clone, attr), getattr(active, attr))
                if attr in reference_attributes:
                    self.assertIs(getattr(clone, attr), getattr(active, attr))
                self.assertNotEqual(getattr(all_updated, attr), getattr(active, attr))

        # cannot clone nonexistent profile
        self.assertRaises(ValueError, session.execution_profile_clone_update, 'DOES NOT EXIST', **profile_attrs)

    def test_no_profiles_same_name(self):
        # can override default in init
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})

        # cannot update default
        self.assertRaises(ValueError, cluster.add_execution_profile, EXEC_PROFILE_DEFAULT, ExecutionProfile())

        # cannot update named init
        self.assertRaises(ValueError, cluster.add_execution_profile, 'one', ExecutionProfile())

        # can add new name
        cluster.add_execution_profile('two', ExecutionProfile())

        # cannot add a profile added dynamically
        self.assertRaises(ValueError, cluster.add_execution_profile, 'two', ExecutionProfile())
