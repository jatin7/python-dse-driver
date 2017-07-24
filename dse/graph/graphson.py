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
import re
from decimal import Decimal
from collections import OrderedDict

import six

from dse.util import Polygon, Point, LineString


class _GraphSONTypeType(type):
    """GraphSONType metaclass, required to create a class property."""

    @property
    def graphson_type_id(cls):
        return "{0}:{1}".format(cls.prefix, cls.type_id)


@six.add_metaclass(_GraphSONTypeType)
class GraphSONType(object):
    """Represent a serializable GraphSON type"""

    prefix = 'g'
    type_id = None

    @classmethod
    def serialize(cls, value):
        return six.text_type(value)

    @classmethod
    def deserialize(cls, value):
        return value


class InstantType(GraphSONType):

    prefix = 'gx'
    type_id = 'Instant'

    @classmethod
    def serialize(cls, value):
        if isinstance(value, datetime.datetime):
            value = datetime.datetime(*value.utctimetuple()[:7])
        else:
            value = datetime.datetime.combine(value, datetime.datetime.min.time())
        value = "{0}Z".format(value.isoformat())

        return value

    @classmethod
    def deserialize(cls, value):
        try:
            d = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            d = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return d


class LocalDateType(GraphSONType):
    FORMAT = '%Y-%m-%d'

    prefix = 'gx'
    type_id = 'LocalDate'

    @classmethod
    def serialize(cls, value):
        return value.strftime(cls.FORMAT)

    @classmethod
    def deserialize(cls, value):
        try:
            return datetime.datetime.strptime(value, cls.FORMAT).date()
        except ValueError:
            # negative date
            return value


class LocalTimeType(GraphSONType):
    FORMATS = [
        '%H:%M',
        '%H:%M:%S',
        '%H:%M:%S.%f'
    ]

    prefix = 'gx'
    type_id = 'LocalTime'

    @classmethod
    def serialize(cls, value):
        return value.strftime(cls.FORMATS[2])

    @classmethod
    def deserialize(cls, value):
        dt = None
        for f in cls.FORMATS:
            try:
                dt = datetime.datetime.strptime(value, f)
                break
            except ValueError:
                continue

        if dt is None:
            raise ValueError('Unable to decode LocalTime: {0}'.format(value))

        return dt.time()


class BlobType(GraphSONType):

    prefix = 'dse'
    type_id = 'blob'

    @classmethod
    def serialize(cls, value):
        value = base64.b64encode(value)
        if six.PY3:
            value = value.decode('utf-8')
        return value

    @classmethod
    def deserialize(cls, value):
        return bytearray(base64.b64decode(value))


class UUIDType(GraphSONType):

    type_id = 'UUID'

    @classmethod
    def deserialize(cls, value):
        return uuid.UUID(value)


class BigDecimalType(GraphSONType):

    prefix = 'gx'
    type_id = 'BigDecimal'

    @classmethod
    def deserialize(cls, value):
        return Decimal(value)


class DurationType(GraphSONType):

    prefix = 'gx'
    type_id = 'Duration'

    _duration_regex = re.compile(r"""
        ^P((?P<days>\d+)D)?
        T((?P<hours>\d+)H)?
        ((?P<minutes>\d+)M)?
        ((?P<seconds>[0-9.]+)S)?$
    """, re.VERBOSE)
    _duration_format = "P{days}DT{hours}H{minutes}M{seconds}S"

    _seconds_in_minute = 60
    _seconds_in_hour = 60 * _seconds_in_minute
    _seconds_in_day = 24 * _seconds_in_hour

    @classmethod
    def serialize(cls, value):
        total_seconds = int(value.total_seconds())
        days, total_seconds =  divmod(total_seconds, cls._seconds_in_day)
        hours, total_seconds = divmod(total_seconds, cls._seconds_in_hour)
        minutes, total_seconds = divmod(total_seconds, cls._seconds_in_minute)
        total_seconds += value.microseconds / 1e6

        return cls._duration_format.format(
            days=int(days), hours=int(hours), minutes=int(minutes), seconds=total_seconds
        )

    @classmethod
    def deserialize(cls, value):
        duration = cls._duration_regex.match(value)
        if duration is None:
            raise ValueError('Invalid duration: {0}'.format(value))

        duration = {k: float(v) if v is not None else 0
                    for k, v in six.iteritems(duration.groupdict())}
        return datetime.timedelta(days=duration['days'], hours=duration['hours'],
                                  minutes=duration['minutes'], seconds=duration['seconds'])


class PointType(GraphSONType):

    prefix = 'dse'
    type_id = 'Point'

    @classmethod
    def deserialize(cls, value):
        return Point.from_wkt(value)


class LineStringType(GraphSONType):

    prefix = 'dse'
    type_id = 'LineString'

    @classmethod
    def deserialize(cls, value):
        return LineString.from_wkt(value)


class PolygonType(GraphSONType):

    prefix = 'dse'
    type_id = 'Polygon'

    @classmethod
    def deserialize(cls, value):
        return Polygon.from_wkt(value)


class GraphSON1TypeSerializer(object):
    # When we fall back to a superclass's serializer, we iterate over this map.
    # We want that iteration order to be consistent, so we use an OrderedDict,
    # not a dict.
    """
    Serialize python objects to graphson types.
    """

    _serializers = OrderedDict([
        (bytearray, BlobType),
        (Decimal, BigDecimalType),
        # datetime comes before date because it's a date subclass; we want
        # datetime subclasses to serialze with datetime's serializer
        (datetime.datetime, InstantType),
        (datetime.date, LocalDateType),
        (datetime.time, LocalTimeType),
        (datetime.timedelta, DurationType),
        (uuid.UUID, UUIDType),
        (Polygon, PolygonType),
        (Point, PointType),
        (LineString, LineStringType)
    ])

    @classmethod
    def register(cls, type, serializer):
        cls._serializers[type] = serializer

    @classmethod
    def serialize(cls, value):
        """
        Serialize a python object to graphson.

        :param value: The python object to serialize.
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
    GraphSON1TypeSerializer.register(buffer, BlobType)
else:
    GraphSON1TypeSerializer.register(memoryview, BlobType)
    GraphSON1TypeSerializer.register(bytes, BlobType)


class GraphSON1TypeDeserializer(object):
    """
    Deserialize graphson1 types to python objects.
    """

    _deserializers = {
        t.graphson_type_id: t
        for t in [UUIDType, BigDecimalType, InstantType, BlobType, PointType,
                  LineStringType, PolygonType, LocalDateType, LocalTimeType, DurationType]
    }

    @classmethod
    def deserialize(cls, type_id, value):
        """
        Deserialize a `type_id` value to a python object.

        :param type_id: The graphson type_id. e.g. 'gx:Instant'
        :param value: The graphson value to deserialize.
        """
        try:
            return cls._deserializers[type_id].deserialize(value)
        except KeyError:
            raise ValueError('Invalid `type_id` specified')

    @classmethod
    def deserialize_date(cls, value):
        return cls._deserializers[LocalDateType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_time(cls, value):
        return cls._deserializers[LocalTimeType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_timestamp(cls, value):
        return cls._deserializers[InstantType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_duration(cls, value):
        return cls._deserializers[DurationType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_int(cls, value):
        return int(value)

    deserialize_smallint = deserialize_int

    deserialize_varint = deserialize_int

    @classmethod
    def deserialize_bigint(cls, value):
        if six.PY3:
            return cls.deserialize_int(value)
        return long(value)

    @classmethod
    def deserialize_double(cls, value):
        return float(value)

    deserialize_float = deserialize_double

    @classmethod
    def deserialize_uuid(cls, value):
        return cls._deserializers[UUIDType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_decimal(cls, value):
        return cls._deserializers[BigDecimalType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_blob(cls, value):
        return cls._deserializers[BlobType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_point(cls, value):
        return cls._deserializers[PointType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_linestring(cls, value):
        return cls._deserializers[LineStringType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_polygon(cls, value):
        return cls._deserializers[PolygonType.graphson_type_id].deserialize(value)

    @classmethod
    def deserialize_inet(cls, value):
        return value
