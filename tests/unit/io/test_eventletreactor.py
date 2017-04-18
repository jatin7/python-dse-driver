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
    import unittest # noqa

from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback
from tests import is_eventlet_monkey_patched
import time

try:
    from dse.io.eventletreactor import EventletConnection
except ImportError:
    EventletConnection = None  # noqa


class EventletTimerTest(unittest.TestCase):

    def setUp(self):
        if EventletConnection is None:
            raise unittest.SkipTest("Eventlet libraries not available")
        if not is_eventlet_monkey_patched():
            raise unittest.SkipTest("Can't test eventlet without monkey patching")
        EventletConnection.initialize_reactor()

    def test_multi_timer_validation(self, *args):
        """
        Verify that timer timeouts are honored appropriately
        """
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, EventletConnection, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, EventletConnection, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, EventletConnection, 0, 100, 1, 100, True)

    def test_timer_cancellation(self):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = EventletConnection.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        timer_manager = EventletConnection._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())
