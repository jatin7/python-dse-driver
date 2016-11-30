# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from datetime import datetime, timedelta
from uuid import uuid4
from dse.cqlengine.functions import get_total_seconds

from tests.integration.cqlengine.base import BaseCassEngTestCase

from dse.cqlengine.management import sync_table
from dse.cqlengine.management import drop_table
from dse.cqlengine.models import Model
from dse.cqlengine import columns
from tests.integration.cqlengine import execute_count


class DateTimeQueryTestModel(Model):

    user = columns.Integer(primary_key=True)
    day = columns.DateTime(primary_key=True)
    data = columns.Text()


class TestDateTimeQueries(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestDateTimeQueries, cls).setUpClass()
        sync_table(DateTimeQueryTestModel)

        cls.base_date = datetime.now() - timedelta(days=10)
        for x in range(7):
            for y in range(10):
                DateTimeQueryTestModel.create(
                    user=x,
                    day=(cls.base_date+timedelta(days=y)),
                    data=str(uuid4())
                )

    @classmethod
    def tearDownClass(cls):
        super(TestDateTimeQueries, cls).tearDownClass()
        drop_table(DateTimeQueryTestModel)

    @execute_count(1)
    def test_range_query(self):
        """ Tests that loading from a range of dates works properly """
        start = datetime(*self.base_date.timetuple()[:3])
        end = start + timedelta(days=3)

        results = DateTimeQueryTestModel.filter(user=0, day__gte=start, day__lt=end)
        assert len(results) == 3

    @execute_count(3)
    def test_datetime_precision(self):
        """ Tests that millisecond resolution is preserved when saving datetime objects """
        now = datetime.now()
        pk = 1000
        obj = DateTimeQueryTestModel.create(user=pk, day=now, data='energy cheese')
        load = DateTimeQueryTestModel.get(user=pk)

        self.assertAlmostEqual(get_total_seconds(now - load.day), 0, 2)
        obj.delete()

