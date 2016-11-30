# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms


from gevent import monkey


def gevent_un_patch_all():
    """
    A method to unpatch gevent libraries. These are unloaded
    in the same order that gevent monkey patch loads theirs.
    Order cannot be arbitrary. This is used in the unit tests to
    un monkey patch gevent
    """
    restore_saved_module("os")
    restore_saved_module("time")
    restore_saved_module("thread")
    restore_saved_module("threading")
    restore_saved_module("_threading_local")
    restore_saved_module("stdin")
    restore_saved_module("stdout")
    restore_saved_module("socket")
    restore_saved_module("select")
    restore_saved_module("ssl")
    restore_saved_module("subprocess")


def restore_saved_module(module):
    """
    gevent monkey patch keeps a list of all patched modules.
    This will restore the original ones
    :param module: to unpatch
    :return:
    """

    # Check the saved attributes in geven monkey patch
    if not (module in monkey.saved):
        return
    _module = __import__(module)

    # If it exist unpatch it
    for attr in monkey.saved[module]:
        if hasattr(_module, attr):
            setattr(_module, attr, monkey.saved[module][attr])

