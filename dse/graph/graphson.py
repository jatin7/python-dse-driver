# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import datetime
import base64
import uuid
from decimal import Decimal
from collections import OrderedDict

import six

from dse.util import Polygon, Point, LineString


class TextSerializer(object):

    @staticmethod
    def serialize(value):
        return six.text_type(value)


class InstantSerializer(object):

    @staticmethod
    def serialize(value):
        if isinstance(value, datetime.datetime):
            value = datetime.datetime(*value.utctimetuple()[:7])
        else:
            value = datetime.datetime.combine(value, datetime.datetime.min.time())
        value = "{0}Z".format(value.isoformat())

        return value


class BlobSerializer(object):

    @staticmethod
    def serialize(value):
        value = base64.b64encode(value)
        if six.PY3:
            value = value.decode('utf-8')
        return value


class DateSerializer(object):
    FORMAT = '%Y-%m-%d'

    @classmethod
    def serialize(cls, value):
        return value.strftime(cls.FORMAT)


class TimeSerializer(object):
    FORMATS = [
        '%H:%M',
        '%H:%M:%S',
        '%H:%M:%S.%f'
    ]

    @classmethod
    def serialize(cls, value):
        return value.strftime(cls.FORMATS[2])


class GraphSONSerializer(object):
    # When we fall back to a superclass's serializer, we iterate over this map.
    # We want that iteration order to be consistent, so we use an OrderedDict,
    # not a dict.
    _serializers = OrderedDict([
        (bytearray, BlobSerializer),
        (Decimal, TextSerializer),
        # datetime comes before date because it's a date subclass; we want
        # datetime subclasses to serialze with datetime's serializer
        (datetime.datetime, InstantSerializer),
        (datetime.date, DateSerializer),
        (datetime.time, TimeSerializer),
        #datetime.timedelta: ...
        (uuid.UUID, TextSerializer),
        (Polygon, TextSerializer),
        (Point, TextSerializer),
        (LineString, TextSerializer)
    ])

    @classmethod
    def register(cls, type, serializer):
        cls._serializers[type] = serializer

    @classmethod
    def serialize(cls, value):
        """
        Serialize a Python value to graphson.
        """

        # The serializer matching logic is as follow:
        # 1. Try to find the python type by direct access.
        # 2. Try to find the first serializer by class inheritance.
        # 3. If no serializer found, return the raw value.

        # Note that when trying to find the serializer by class inheritance,
        # the order that serializers are registered is important. The use of
        # an OrderedDict is to avoid the difference between executions.
        try:
            return cls._serializers[type(value)].serialize(value)
        except KeyError:
            for key, serializer in cls._serializers.items():
                if isinstance(value, key):
                    return serializer.serialize(value)

        return value


if six.PY2:
    GraphSONSerializer.register(buffer, BlobSerializer)
else:
    GraphSONSerializer.register(memoryview, BlobSerializer)
    GraphSONSerializer.register(bytes, BlobSerializer)
