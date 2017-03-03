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
    import unittest  # noqa

import datetime

from dse.util import Date, Time, Duration, DateRangeBound, DateRange, DateRangePrecision, OPEN_BOUND


class DateTests(unittest.TestCase):

    def test_from_datetime(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        self.assertEqual(str(d), str(expected_date))

    def test_from_string(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        sd = Date('1492-10-12')
        self.assertEqual(sd, d)
        sd = Date('+1492-10-12')
        self.assertEqual(sd, d)

    def test_from_date(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        self.assertEqual(d.date(), expected_date)

    def test_from_days(self):
        sd = Date(0)
        self.assertEqual(sd, Date(datetime.date(1970, 1, 1)))
        sd = Date(-1)
        self.assertEqual(sd, Date(datetime.date(1969, 12, 31)))
        sd = Date(1)
        self.assertEqual(sd, Date(datetime.date(1970, 1, 2)))

    def test_limits(self):
        min_builtin = Date(datetime.date(1, 1, 1))
        max_builtin = Date(datetime.date(9999, 12, 31))
        self.assertEqual(Date(min_builtin.days_from_epoch), min_builtin)
        self.assertEqual(Date(max_builtin.days_from_epoch), max_builtin)
        # just proving we can construct with on offset outside buildin range
        self.assertEqual(Date(min_builtin.days_from_epoch - 1).days_from_epoch,
                         min_builtin.days_from_epoch - 1)
        self.assertEqual(Date(max_builtin.days_from_epoch + 1).days_from_epoch,
                         max_builtin.days_from_epoch + 1)

    def test_invalid_init(self):
        self.assertRaises(ValueError, Date, '-1999-10-10')
        self.assertRaises(TypeError, Date, 1.234)

    def test_str(self):
        date_str = '2015-03-16'
        self.assertEqual(str(Date(date_str)), date_str)

    def test_out_of_range(self):
        self.assertEqual(str(Date(2932897)), '2932897')
        self.assertEqual(repr(Date(1)), 'Date(1)')

    def test_equals(self):
        self.assertEqual(Date(1234), 1234)
        self.assertEqual(Date(1), datetime.date(1970, 1, 2))
        self.assertFalse(Date(2932897) == datetime.date(9999, 12, 31))  # date can't represent year > 9999
        self.assertEqual(Date(2932897), 2932897)


class TimeTests(unittest.TestCase):

    def test_units_from_string(self):
        one_micro = 1000
        one_milli = 1000 * one_micro
        one_second = 1000 * one_milli
        one_minute = 60 * one_second
        one_hour = 60 * one_minute

        tt = Time('00:00:00.000000001')
        self.assertEqual(tt.nanosecond_time, 1)
        tt = Time('00:00:00.000001')
        self.assertEqual(tt.nanosecond_time, one_micro)
        tt = Time('00:00:00.001')
        self.assertEqual(tt.nanosecond_time, one_milli)
        tt = Time('00:00:01')
        self.assertEqual(tt.nanosecond_time, one_second)
        tt = Time('00:01:00')
        self.assertEqual(tt.nanosecond_time, one_minute)
        tt = Time('01:00:00')
        self.assertEqual(tt.nanosecond_time, one_hour)
        tt = Time('01:00:00.')
        self.assertEqual(tt.nanosecond_time, one_hour)

        tt = Time('23:59:59.123456')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro)

        tt = Time('23:59:59.1234567')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 700)

        tt = Time('23:59:59.12345678')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 780)

        tt = Time('23:59:59.123456789')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 789)

    def test_micro_precision(self):
        Time('23:59:59.1')
        Time('23:59:59.12')
        Time('23:59:59.123')
        Time('23:59:59.1234')
        Time('23:59:59.12345')

    def test_from_int(self):
        tt = Time(12345678)
        self.assertEqual(tt.nanosecond_time, 12345678)

    def test_from_time(self):
        expected_time = datetime.time(12, 1, 2, 3)
        tt = Time(expected_time)
        self.assertEqual(tt, expected_time)

    def test_as_time(self):
        expected_time = datetime.time(12, 1, 2, 3)
        tt = Time(expected_time)
        self.assertEqual(tt.time(), expected_time)

    def test_equals(self):
        # util.Time self equality
        self.assertEqual(Time(1234), Time(1234))

    def test_str_repr(self):
        time_str = '12:13:14.123456789'
        self.assertEqual(str(Time(time_str)), time_str)
        self.assertEqual(repr(Time(1)), 'Time(1)')

    def test_invalid_init(self):
        self.assertRaises(ValueError, Time, '1999-10-10 11:11:11.1234')
        self.assertRaises(TypeError, Time, 1.234)
        self.assertRaises(ValueError, Time, 123456789000000)
        self.assertRaises(TypeError, Time, datetime.datetime(2004, 12, 23, 11, 11, 1))


class DurationTests(unittest.TestCase):

    def test_valid_format(self):

        valid = Duration(1, 1, 1)
        self.assertEqual(valid.months, 1)
        self.assertEqual(valid.days, 1)
        self.assertEqual(valid.nanoseconds, 1)

        valid = Duration(nanoseconds=100000)
        self.assertEqual(valid.months, 0)
        self.assertEqual(valid.days, 0)
        self.assertEqual(valid.nanoseconds, 100000)

        valid = Duration()
        self.assertEqual(valid.months, 0)
        self.assertEqual(valid.days, 0)
        self.assertEqual(valid.nanoseconds, 0)

        valid = Duration(-10, -21, -1000)
        self.assertEqual(valid.months, -10)
        self.assertEqual(valid.days, -21)
        self.assertEqual(valid.nanoseconds, -1000)

    def test_equality(self):

        first = Duration(1, 1, 1)
        second = Duration(-1, 1, 1)
        self.assertNotEqual(first, second)

        first = Duration(1, 1, 1)
        second = Duration(1, 1, 1)
        self.assertEqual(first, second)

        first = Duration()
        second = Duration(0, 0, 0)
        self.assertEqual(first, second)

        first = Duration(1000, 10000, 2345345)
        second = Duration(1000, 10000, 2345345)
        self.assertEqual(first, second)

        first = Duration(12, 0, 100)
        second = Duration(nanoseconds=100, months=12)
        self.assertEqual(first, second)

    def test_str(self):

        self.assertEqual(str(Duration(1, 1, 1)), "1mo1d1ns")
        self.assertEqual(str(Duration(1, 1, -1)), "-1mo1d1ns")
        self.assertEqual(str(Duration(1, 1, 1000000000000000)), "1mo1d1000000000000000ns")
        self.assertEqual(str(Duration(52, 23, 564564)), "52mo23d564564ns")

def _daterangebound_to_dict(drb):
    return {'milliseconds': drb.milliseconds,
            'precision': drb.precision}

class DateRangeTypeTests(unittest.TestCase):
    dt = datetime.datetime(1990, 2, 3, 13, 58, 45, 777777)
    smallest_datetime_timestamp = -62135596800000

    def test_bound_requires_not_None_datetime(self):
        with self.assertRaises(TypeError):
            DateRangeBound(datetime=None, precision='YEAR')

    def test_bound_requires_not_None_precision(self):
        with self.assertRaises(TypeError):
            DateRangeBound(datetime=self.dt, precision=None)

    def test_invalid_constructor_arg_combinations(self):
        self.assertRaises(ValueError, DateRange, lower_bound=(None, None), value=(None, None))
        self.assertRaises(ValueError, DateRange, upper_bound=(None, None), value=(None, None))

    def test_bound_rounding_milli(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.MILLISECOND).round_down()
        )
        expected = {'milliseconds': 634053525778,
                    'precision': DateRangePrecision.MILLISECOND}
        self.assertEqual(actual, expected)

    def test_bound_rounding_second(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.SECOND).round_down()
        )
        expected = {'milliseconds': 634053525000,
                    'precision': DateRangePrecision.SECOND}
        self.assertEqual(actual, expected)

    def test_bound_rounding_minute(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.MINUTE).round_down()
        )
        expected = {'milliseconds': 634053480000,
                    'precision': DateRangePrecision.MINUTE}
        self.assertEqual(actual, expected)

    def test_bound_rounding_hour(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.HOUR).round_down()
        )
        expected = {'milliseconds': 634050000000,
                    'precision': DateRangePrecision.HOUR}
        self.assertEqual(actual, expected)

    def test_bound_rounding_day(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.DAY).round_down()
        )
        expected = {'milliseconds': 634003200000,
                    'precision': DateRangePrecision.DAY}
        self.assertEqual(actual, expected)

    def test_bound_rounding_month(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.MONTH).round_down()
        )
        expected = {'milliseconds': 633830400000,
                    'precision': DateRangePrecision.MONTH}
        self.assertEqual(actual, expected)

    def test_bound_rounding_year(self):
        actual = _daterangebound_to_dict(
            DateRangeBound(self.dt,
                           precision=DateRangePrecision.YEAR).round_down()
        )
        expected = {'milliseconds': 631152000000,
                    'precision': DateRangePrecision.YEAR}
        self.assertEqual(actual, expected)

    def test_from_daterange_value(self):
        drb = DateRangeBound(self.dt, 'HOUR')
        self.assertEqual(drb, DateRangeBound.from_value(drb))

    def test_from_tuple_value(self):
        self.assertEqual(
            DateRangeBound(self.dt, 'DAY'),
            DateRangeBound.from_value((self.dt, 'DAY'))
        )

    def test_from_dict_value(self):
        self.assertEqual(
            DateRangeBound(self.dt, 'DAY'),
            DateRangeBound.from_value({'milliseconds': self.dt,
                                       'precision': 'DAY'})
        )

    def test_date_range_bound_str_repr(self):
        daterange = DateRangeBound.from_value((None, None))
        self.assertEqual(
            str(daterange),
            '*'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'YEAR')
        self.assertEqual(
            str(daterange),
            '1990'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'MONTH')
        self.assertEqual(
            str(daterange),
            '1990-02'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'DAY')
        self.assertEqual(
            str(daterange),
            '1990-02-03'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'HOUR')
        self.assertEqual(
            str(daterange),
            '1990-02-03T13Z'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'MINUTE')
        self.assertEqual(
            str(daterange),
            '1990-02-03T13:58Z'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'SECOND')
        self.assertEqual(
            str(daterange),
            '1990-02-03T13:58:45Z'
        )
        self._check_rpr(daterange)

        daterange = DateRangeBound(self.dt, 'MILLISECOND')
        self.assertEqual(
            str(daterange),
            '1990-02-03T13:58:45.778Z'
        )
        self._check_rpr(daterange)

    def test_date_range_str_repr(self):
        daterange = DateRange(
                        value=DateRangeBound(self.dt, 'SECOND')
                    )
        self.assertEqual(
            str(daterange),
            '1990-02-03T13:58:45Z'
        )
        self._check_rpr(daterange)

        daterange = DateRange(
                        lower_bound=DateRangeBound(self.dt, 'SECOND'),
                        upper_bound=OPEN_BOUND
                    )
        self.assertEqual(
            str(daterange),
            '[1990-02-03T13:58:45Z TO *]'
        )
        self._check_rpr(daterange)

        daterange = DateRange(
                        lower_bound=OPEN_BOUND,
                        upper_bound=DateRangeBound(self.dt, 'SECOND')
                    )
        self.assertEqual(
            str(daterange),
            '[* TO 1990-02-03T13:58:45Z]'
        )
        self._check_rpr(daterange)

        daterange = DateRange(
                        lower_bound=OPEN_BOUND,
                        upper_bound=OPEN_BOUND
                    )
        self.assertEqual(
            str(daterange),
            '[* TO *]'
        )
        self._check_rpr(daterange)

        daterange = DateRange(
                        lower_bound=DateRangeBound(self.dt, 'SECOND'),
                        upper_bound=DateRangeBound(self.dt, 'YEAR')
                    )
        self.assertEqual(
            str(daterange),
            '[1990-02-03T13:58:45Z TO 1990]'
        )
        self._check_rpr(daterange)

        daterange = DateRange(value=OPEN_BOUND)
        self.assertEqual(
            str(daterange),
            '*'
        )
        self._check_rpr(daterange)

    def test_negative_daterangebound_str_repr(self):
        daterange = DateRangeBound(self.smallest_datetime_timestamp - 1, 'MILLISECOND')
        self.assertEqual(
            str(daterange),
            '-62135596800001ms'
        )
        self._check_rpr(daterange)

    def test_daterange_with_negative_bound_str_repr(self):
        daterange = DateRange(
                        lower_bound=DateRangeBound(
                            self.smallest_datetime_timestamp - 1,
                            'MILLISECOND'
                        ),
                        upper_bound=DateRangeBound(self.dt, 'SECOND')
                    )
        self.assertEqual(
            str(daterange),
            '[-62135596800001ms TO 1990-02-03T13:58:45Z]'
        )
        self._check_rpr(daterange)

    def test_comparison_operators(self):
        l = [
            DateRange(
                lower_bound=OPEN_BOUND,
                upper_bound=OPEN_BOUND
               ),
            DateRange(
                lower_bound=OPEN_BOUND,
                upper_bound=OPEN_BOUND
               )
        ]
        self._check_sequence_consistency(l, equal=True)

        l = [
            DateRange(
                value=DateRangeBound(
                    datetime.datetime(2014, 10, 1, 0),
                    DateRangePrecision.YEAR
                )
            ),
            DateRange(
                value=DateRangeBound(
                    datetime.datetime(2014, 10, 2, 0),
                    DateRangePrecision.YEAR
                )
            ),
            DateRange(
                value=DateRangeBound(
                    datetime.datetime(2014, 10, 3, 0),
                    DateRangePrecision.YEAR
                )
            )
        ]
        self._check_sequence_consistency(l, equal=True)

        l = [
            DateRange(
                value=DateRangeBound(
                    datetime.datetime(2014, 10, 1, 0),
                    DateRangePrecision.DAY
                )
            ),
            DateRange(
                value=DateRangeBound(
                    datetime.datetime(2014, 10, 2, 0),
                    DateRangePrecision.DAY
                )
            ),
            DateRange(
                value=DateRangeBound(
                    datetime.datetime(2014, 10, 3, 0),
                    DateRangePrecision.DAY
                )
            )
        ]
        self._check_sequence_consistency(l)

        l = [
            DateRange(
                lower_bound=OPEN_BOUND,
                upper_bound=DateRangeBound(
                    datetime.datetime(2016, 1, 1, 10, 15, 15, 999000),
                    DateRangePrecision.MILLISECOND
                ),
            ),
            DateRange(
                lower_bound=DateRangeBound(
                    datetime.datetime(2015, 3, 1, 10, 15, 15, 10000),
                    DateRangePrecision.MILLISECOND
                ),
                upper_bound=DateRangeBound(
                    datetime.datetime(2016, 1, 1, 10, 15, 30, 999000),
                    DateRangePrecision.MILLISECOND
                )
            ),
            DateRange(
                lower_bound=DateRangeBound(
                    datetime.datetime(2015, 3, 1, 10, 15, 16, 10000),
                    DateRangePrecision.MILLISECOND
                ),
                upper_bound=OPEN_BOUND
            )
            ,
            DateRange(
                lower_bound=DateRangeBound(
                    datetime.datetime(2015, 3, 1, 10, 15, 16, 10000),
                    DateRangePrecision.MILLISECOND
                ),
                upper_bound=DateRangeBound(
                    datetime.datetime(2016, 1, 1, 10, 15, 30, 999000),
                    DateRangePrecision.MILLISECOND
                )
            ),
            DateRange(
                lower_bound=DateRangeBound(
                    datetime.datetime(2015, 3, 1, 10, 15, 16, 10000),
                    DateRangePrecision.MILLISECOND
                ),
                upper_bound=DateRangeBound(
                    datetime.datetime(2016, 1, 1, 10, 15, 31, 999000),
                    DateRangePrecision.MILLISECOND
                )
            )
        ]

        self._check_sequence_consistency(l)

    def _check_sequence_consistency(self, ordered_sequence, equal=False):
        for i, el in enumerate(ordered_sequence):
            for previous in ordered_sequence[:i]:
                self._check_order_consistency(previous, el, equal)
            for posterior in ordered_sequence[i + 1:]:
                self._check_order_consistency(el, posterior, equal)

    def _check_order_consistency(self, smaller, bigger, equal=False):
        self.assertLessEqual(smaller, bigger)
        self.assertGreaterEqual(bigger, smaller)
        if equal:
            self.assertEqual(smaller, bigger)
        else:
            self.assertNotEqual(smaller, bigger)
            self.assertLess(smaller, bigger)
            self.assertGreater(bigger, smaller)

    def _check_rpr(self, daterange):
        self.assertEqual(daterange,
                         eval(repr(daterange).replace("milliseconds", "value")))
