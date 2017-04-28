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
from six.moves import queue

log = logging.getLogger(__name__)


class Runner(BenchmarkThread):

    def run(self):
        futures = queue.Queue(maxsize=121)

        self.start_profile()

        for i in range(self.num_queries):
            if i >= 120:
                old_future = futures.get_nowait()
                old_future.result()

            key = "{}-{}".format(self.thread_num, i)
            future = self.run_query(key)
            futures.put_nowait(future)

        while True:
            try:
                futures.get_nowait().result()
            except queue.Empty:
                break

        self.finish_profile


if __name__ == "__main__":
    benchmark(Runner)
