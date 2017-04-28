# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from base import benchmark, BenchmarkThread
from six.moves import range


class Runner(BenchmarkThread):

    def run(self):
        self.start_profile()

        for _ in range(self.num_queries):
            self.session.execute(self.query, self.values)

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
