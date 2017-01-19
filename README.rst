DataStax Enterprise Python Driver
=================================

A modern, `feature-rich <https://github.com/datastax/python-driver#features>`_ and highly-tunable Python client library for Apache Cassandra (1.2+) and DataStax Enterprise (3.1+) using exclusively Cassandra's binary protocol and Cassandra Query Language v3.

The driver supports Python 2.7, 3.3, and 3.4.

Feedback Requested
------------------
**Help us focus our efforts!** Provide your input on the `Platform and Runtime Survey <https://docs.google.com/a/datastax.com/forms/d/10wkbKLqmqs91gvhFW5u43y60pg_geZDolVNrxfO5_48/viewform>`_ (we kept it short).

Features
--------
##### TBD: point to unified documentation
* `Synchronous <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session.execute>`_ and `Asynchronous <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session.execute_async>`_ APIs
* `Simple, Prepared, and Batch statements <http://datastax.github.io/python-driver/api/cassandra/query.html#cassandra.query.Statement>`_
* Asynchronous IO, parallel execution, request pipelining
* `Connection pooling <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.get_core_connections_per_host>`_
* Automatic node discovery
* `Automatic reconnection <http://datastax.github.io/python-driver/api/cassandra/policies.html#reconnecting-to-dead-hosts>`_
* Configurable `load balancing <http://datastax.github.io/python-driver/api/cassandra/policies.html#load-balancing>`_ and `retry policies <http://datastax.github.io/python-driver/api/cassandra/policies.html#retrying-failed-operations>`_
* `Concurrent execution utilities <http://datastax.github.io/python-driver/api/cassandra/concurrent.html>`_
* `Object mapper <http://datastax.github.io/python-driver/object_mapper.html>`_
* DSE Graph execution API
* DSE Geometric type serialization
* DSE PlainText and GSSAPI authentication

Installation
------------
Installation through pip is recommended::

    $ pip install cassandra-driver-dse

For more complete installation instructions, see the `installation guide <http://docs.datastax.com/en/developer/python-driver-dse/1.1/installation/>`_.

Documentation
-------------
The documentation can be found online `here <http://docs.datastax.com/en/latest-dse-python-driver/>`_.

A couple of links for getting up to speed:

* `Installation <http://docs.datastax.com/en/developer/python-driver-dse/1.1/installation/>`_
* `Getting started guide <http://docs.datastax.com/en/developer/python-driver-dse/1.1/getting_started/>`_
* `API docs <http://docs.datastax.com/en/developer/python-driver-dse/1.1/api/>`_

Reporting Problems
------------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.

Getting Help
------------
Your two best options for getting help with the driver are the
`mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_
and the IRC channel.

For IRC, use the #datastax-drivers channel on irc.freenode.net.  If you don't have an IRC client,
you can use `freenode's web-based client <http://webchat.freenode.net/?channels=#datastax-drivers>`_.

License
-------
Copyright 2016 DataStax

Licensed under the DataStax DSE Driver License;
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

http://www.datastax.com/terms/datastax-dse-driver-license-terms

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
