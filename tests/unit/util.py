# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms


def check_sequence_consistency(unitTest, ordered_sequence, equal=False):
    for i, el in enumerate(ordered_sequence):
        for previous in ordered_sequence[:i]:
            _check_order_consistency(unitTest, previous, el, equal)
        for posterior in ordered_sequence[i + 1:]:
            _check_order_consistency(unitTest, el, posterior, equal)


def _check_order_consistency(unitTest, smaller, bigger, equal=False):
    unitTest.assertLessEqual(smaller, bigger)
    unitTest.assertGreaterEqual(bigger, smaller)
    if equal:
        unitTest.assertEqual(smaller, bigger)
    else:
        unitTest.assertNotEqual(smaller, bigger)
        unitTest.assertLess(smaller, bigger)
        unitTest.assertGreater(bigger, smaller)