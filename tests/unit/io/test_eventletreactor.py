# Copyright 2016-2017 DataStax, Inc.
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
    import unittest # noqa

from tests.unit.io.utils import TimerConnectionTests
from tests import notpypy, MONKEY_PATCH_LOOP, notmonkeypatch

from eventlet import monkey_patch

try:
    from dse.io.eventletreactor import EventletConnection
except ImportError:
    EventletConnection = None  # noqa

# There are some issues with some versions of pypy and eventlet
@notpypy
@unittest.skipIf(EventletConnection is None, "Skpping the eventlet tests because it's not installed")
@notmonkeypatch
class EventletTimerTest(unittest.TestCase, TimerConnectionTests):
    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if not MONKEY_PATCH_LOOP:
            return
        monkey_patch()
        cls.connection_class = EventletConnection
        EventletConnection.initialize_reactor()

    # There is no unpatching because there is not a clear way
    # of doing it reliably
