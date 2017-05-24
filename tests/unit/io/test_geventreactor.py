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
from tests import MONKEY_PATCH_LOOP, notmonkeypatch
try:
    from dse.io.geventreactor import GeventConnection
    import gevent.monkey
except ImportError:
    GeventConnection = None  # noqa


@unittest.skipIf(GeventConnection is None, "Skpping the gevent tests because it's not installed")
@notmonkeypatch
class GeventTimerTest(unittest.TestCase, TimerConnectionTests):
    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if not MONKEY_PATCH_LOOP:
            return
        gevent.monkey.patch_all()
        cls.connection_class = GeventConnection
        GeventConnection.initialize_reactor()

    # There is no unpatching because there is not a clear way
    # of doing it reliably
