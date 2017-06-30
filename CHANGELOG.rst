2.2.0
=====

Features
--------
* Implement serializers for the Graph String API (PYTHON-778)

Merged from OSS master (pre-3.11):

Features
--------
* Add idle_heartbeat_timeout cluster option to tune how long to wait for heartbeat responses. (PYTHON-762)

Bug Fixes
---------
* is_idempotent flag is not propagated from PreparedStatement to BoundStatement (PYTHON-736)
* Fix asyncore hang on exit (PYTHON-767)
* Driver takes several minutes to remove a bad host from session (PYTHON-762)
* Installation doesn't always fall back to no cython in Windows (PYTHON-763)
* Avoid to replace a connection that is supposed to shutdown (PYTHON-772)
* request_ids may not be returned to the pool (PYTHON-739)

Other
-----
* Bump Cython dependency version to 0.25.2 (PYTHON-754)
* Fix DeprecationWarning when using lz4 (PYTHON-769)

2.1.0
=====
May 24, 2017

Other
-----
* Correct licence in DSE Driver (PYTHON-748)

Merged from OSS 3.10.0:

Features
--------
* Add Duration type to cqlengine (PYTHON-750)
* Community PR review: Raise error on primary key update only if its value changed (PYTHON-705)
* get_query_trace() contract is ambiguous (PYTHON-196)

Bug Fixes
---------
* Queries using speculative execution policy timeout prematurely (PYTHON-755)
* Fix `map` where results are not consumed (PYTHON-749)
* Driver fails to encode Duration's with large values (PYTHON-747)
* UDT values are not updated correctly in CQLEngine (PYTHON-743)
* UDT types are not validated in CQLEngine (PYTHON-742)
* to_python is not implemented for types columns.Type and columns.Date in CQLEngine (PYTHON-741)
* Clients spin infinitely trying to connect to a host that is drained (PYTHON-734)
* Resulset.get_query_trace returns empty trace sometimes (PYTHON-730)
* Memory grows and doesn't get removed (PYTHON-720)
* Fix RuntimeError caused by change dict size during iteration (PYTHON-708)
* fix ExponentialReconnectionPolicy may throw OverflowError problem (PYTHON-707)
* Avoid using nonexistent prepared statement in ResponseFuture (PYTHON-706)

Other
-----
* Update README (PYTHON-746)
* Test python versions 3.5 and 3.6 (PYTHON-737)
* Docs Warning About Prepare "select *" (PYTHON-626)
* Increase Coverage in CqlEngine Test Suite (PYTHON-505)
* Example SSL connection code does not verify server certificates (PYTHON-469)

2.0.0
=====

Release merged in changes from cassandra-driver 3.9.0

Bug Fixes
---------
* DateRange Parse Error (PYTHON-729)
* MontonicTimestampGenerator.__init__ ignores class defaults (PYTHON-728)
* metadata.get_host returning None unexpectedly (PYTHON-709)
* Sockets associated with sessions not getting cleaned up on session.shutdown() (PYTHON-673)

Other
-----
* Write documentation examples for DSE 2.0 features (PYTHON-732)

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
