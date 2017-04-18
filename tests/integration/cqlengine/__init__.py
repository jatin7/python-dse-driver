# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import os
import warnings
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa
from dse import ConsistencyLevel

from dse.cqlengine import connection
from dse.cqlengine.management import create_keyspace_simple, CQLENG_ALLOW_SCHEMA_MANAGEMENT
import dse

from tests.integration import get_server_versions, use_single_node, PROTOCOL_VERSION, DSE_IP, set_default_dse_ip
DEFAULT_KEYSPACE = 'cqlengine_test'


CQL_SKIP_EXECUTE = bool(os.getenv('CQL_SKIP_EXECUTE', False))


def setup_package():
    warnings.simplefilter('always')  # for testing warnings, make sure all are let through
    os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'

    set_default_dse_ip()
    use_single_node()

    setup_connection(DEFAULT_KEYSPACE)
    create_keyspace_simple(DEFAULT_KEYSPACE, 1)


def teardown_package():
    connection.unregister_connection("default")


def is_prepend_reversed():
    # do we have https://issues.apache.org/jira/browse/CASSANDRA-8733 ?
    ver, _ = get_server_versions()
    return not (ver >= (2, 0, 13) or ver >= (2, 1, 3))


def setup_connection(keyspace_name):
    connection.setup([DSE_IP],
                     consistency=ConsistencyLevel.ONE,
                     protocol_version=PROTOCOL_VERSION,
                     default_keyspace=keyspace_name)


class StatementCounter(object):
    """
    Simple python object used to hold a count of the number of times
    the wrapped method has been invoked
    """
    def __init__(self, patched_func):
        self.func = patched_func
        self.counter = 0

    def wrapped_execute(self, *args, **kwargs):
        self.counter += 1
        return self.func(*args, **kwargs)

    def get_counter(self):
        return self.counter


def execute_count(expected):
    """
    A decorator used wrap dse.cqlengine.connection.execute. It counts the number of times this method is invoked
    then compares it to the number expected. If they don't match it throws an assertion error.
    This function can be disabled by running the test harness with the env variable CQL_SKIP_EXECUTE=1 set
    """
    def innerCounter(fn):
        def wrapped_function(*args, **kwargs):
            # Create a counter monkey patch into dse.cqlengine.connection.execute
            count = StatementCounter(dse.cqlengine.connection.execute)
            original_function = dse.cqlengine.connection.execute
            # Monkey patch in our StatementCounter wrapper
            dse.cqlengine.connection.execute = count.wrapped_execute
            # Invoked the underlying unit test
            to_return = fn(*args, **kwargs)
            # Get the count from our monkey patched counter
            count.get_counter()
            # DeMonkey Patch our code
            dse.cqlengine.connection.execute = original_function
            # Check to see if we have a pre-existing test case to work from.
            if len(args) is 0:
                test_case = unittest.TestCase("__init__")
            else:
                test_case = args[0]
            # Check to see if the count is what you expect
            test_case.assertEqual(count.get_counter(), expected, msg="Expected number of dse.cqlengine.connection.execute calls ({0}) doesn't match actual number invoked ({1})".format(expected, count.get_counter()))
            return to_return
        # Name of the wrapped function must match the original or unittest will error out.
        wrapped_function.__name__ = fn.__name__
        wrapped_function.__doc__ = fn.__doc__
        # Escape hatch
        if(CQL_SKIP_EXECUTE):
            return fn
        else:
            return wrapped_function

    return innerCounter


