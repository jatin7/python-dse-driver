# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from unittest import TestCase
from dse.cqlengine.operators import *

import six


class TestWhereOperators(TestCase):

    def test_symbol_lookup(self):
        """ tests where symbols are looked up properly """

        def check_lookup(symbol, expected):
            op = BaseWhereOperator.get_operator(symbol)
            self.assertEqual(op, expected)

        check_lookup('EQ', EqualsOperator)
        check_lookup('NE', NotEqualsOperator)
        check_lookup('IN', InOperator)
        check_lookup('GT', GreaterThanOperator)
        check_lookup('GTE', GreaterThanOrEqualOperator)
        check_lookup('LT', LessThanOperator)
        check_lookup('LTE', LessThanOrEqualOperator)
        check_lookup('CONTAINS', ContainsOperator)

    def test_operator_rendering(self):
        """ tests symbols are rendered properly """
        self.assertEqual("=", six.text_type(EqualsOperator()))
        self.assertEqual("!=", six.text_type(NotEqualsOperator()))
        self.assertEqual("IN", six.text_type(InOperator()))
        self.assertEqual(">", six.text_type(GreaterThanOperator()))
        self.assertEqual(">=", six.text_type(GreaterThanOrEqualOperator()))
        self.assertEqual("<", six.text_type(LessThanOperator()))
        self.assertEqual("<=", six.text_type(LessThanOrEqualOperator()))
        self.assertEqual("CONTAINS", six.text_type(ContainsOperator()))


