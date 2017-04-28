# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import logging

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)

class Runner(BenchmarkThread):

    def run(self):
        futures = []

        self.start_profile()

        for i in range(self.num_queries):
            key = "{0}-{1}".format(self.thread_num, i)
            future = self.run_query(key)
            futures.append(future)

        for future in futures:
            future.result()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
