# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import datetime

from dse.cython_utils cimport datetime_from_timestamp


def test_datetime_from_timestamp(assert_equal):
    assert_equal(datetime_from_timestamp(1454781157.123456), datetime.datetime(2016, 2, 6, 17, 52, 37, 123456))
    # PYTHON-452
    assert_equal(datetime_from_timestamp(2177403010.123456), datetime.datetime(2038, 12, 31, 10, 10, 10, 123456))
