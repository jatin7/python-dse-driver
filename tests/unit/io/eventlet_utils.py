# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms


import os
import select
import socket
import thread
import Queue
import threading
import __builtin__
import ssl
import time


def eventlet_un_patch_all():
    """
    A method to unpatch eventlet monkey patching used for the reactor tests
    """

    # These are the modules that are loaded by eventlet we reload them all
    modules_to_unpatch = [os, select, socket, thread, time, Queue, threading, ssl, __builtin__]
    for to_unpatch in modules_to_unpatch:
        reload(to_unpatch)


