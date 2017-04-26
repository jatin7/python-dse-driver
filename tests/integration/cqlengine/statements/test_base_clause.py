# Copyright 2016-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from unittest import TestCase
from dse.cqlengine.statements import BaseClause


class BaseClauseTests(TestCase):

    def test_context_updating(self):
        ss = BaseClause('a', 'b')
        assert ss.get_context_size() == 1

        ctx = {}
        ss.set_context_id(10)
        ss.update_context(ctx)
        assert ctx == {'10': 'b'}


