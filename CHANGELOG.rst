2.0.0rc1
========

Release merged in changes from cassandra-driver 3.8.1 (PYTHON-712)

Bug Fixes
--------
* Migrate DateRange and DateRangeBound off namedtuple implementations (PYTHON-701)
* Add missing rich comparisons for some custom types (PYTHON-714)
* Fix Comparisons for DateRange util types (PYTHON-718)

Other
-----
* Remove support for Cassandra 2.0 and below (PYTHON-681)

2.0.0b3
=======
February 13, 2017

Features
--------
* Support DSE DateRange type (PYTHON-668)
* RLAC CQL output for materialized views (PYTHON-682)

Bug Fixes
---------
* DSE_V1 protocol should not include all of protocol v5 (PYTHON-694)

1.1.0
=====
November 2, 2016

Features
--------
* Add Geom Types wkt deserializer

1.0.4
=====
September 13, 2016

Release upgrading to cassandra-driver 3.7.0

1.0.3
=====
August 5, 2016

Release upgrading to cassandra-driver 3.6.0

1.0.0
=====
June 28, 2016

Features
--------
* DSE Graph Client timeouts in custom payload (PYTHON-589)
* Make DSEGSSAPIAuthProvider accept principal name (PYTHON-574)
* Add config profiles to DSE graph execution (PYTHON-570)
* DSE Driver version checking (PYTHON-568)

Bug Fixes
---------
* Resolve FQDN from ip address and use that as host passed to SASLClient (PYTHON-566)
* Geospatial type implementations don't handle 'EMPTY' values. (PYTHON-481)

1.0.0a2
=======
March 30, 2016

Features
--------
* Distinct default timeout for graph queries (PYTHON-477)
* Graph result parsing for known types (PYTHON-479,487)
* Distinct read/write CL for graph execution (PYTHON-509)
* Target graph analytics query to spark master when available (PYTHON-510)

Bug Fixes
---------
* Correctly handle other types in geo type equality (PYTHON-508)

1.0.0a1
=======
February 4, 2016

Initial release
