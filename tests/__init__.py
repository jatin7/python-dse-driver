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
    import unittest  # noqa
import logging
import sys
import socket
import platform
import os

log = logging.getLogger()
log.setLevel('DEBUG')
# if nose didn't already attach a log handler, add one here
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(module)s:%(lineno)s]: %(message)s'))
    log.addHandler(handler)


def is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('socket')


def is_gevent_monkey_patched():
    if 'gevent.monkey' not in sys.modules:
        return False
    import gevent.socket
    return socket.socket is gevent.socket.socket


def is_monkey_patched():
    return is_gevent_monkey_patched() or is_eventlet_monkey_patched()

MONKEY_PATCH_LOOP = bool(os.getenv('MONKEY_PATCH_LOOP', False))
EVENT_LOOP_MANAGER = os.getenv('EVENT_LOOP_MANAGER', "libev")


# If set to to true this will force the Cython tests to run regardless of whether they are installed
cython_env = os.getenv('VERIFY_CYTHON', "False")


VERIFY_CYTHON = False

if(cython_env == 'True'):
    VERIFY_CYTHON = True

notwindows = unittest.skipUnless(not "Windows" in platform.system(), "This test is not adequate for windows")
notpypy = unittest.skipUnless(not platform.python_implementation() == 'PyPy', "This tests is not suitable for pypy")
notmonkeypatch = unittest.skipUnless(MONKEY_PATCH_LOOP, "Skipping this test because monkey patching is required")
