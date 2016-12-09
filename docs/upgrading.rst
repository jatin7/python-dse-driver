Upgrading
=========

.. toctree::
   :maxdepth: 1

Upgrading Existing Code from DSE Python Driver <2.0
---------------------------------------------------

There are some important changes in the 2.0 release of the DSE python driver:

- The new artifact name is: ``dse-driver``.
- Legacy cluster/session parameters have been removed in favor of Execution Profiles. Refer to the section below for more information: :ref:`changes-execution-profiles`
- ``dse-driver`` is a now a complete driver for DataStax Enterprise rather than an extension of the core cassandra driver.

  For this reason, there is no need to import anything from ``cassandra`` anymore. All imports should be done from the ``dse`` package. So, a code snippet like:

.. code-block:: python

    from cassandra.cluster import NoHostAvailable
    from dse.cluster import Cluster

    cluster = Cluster()
    try:
        session = cluster.connect()
    except NoHostAvailable:
        print 'Unable to connect to cluster'

...becomes:

.. code-block:: python

    from dse.cluster import NoHostAvailable
    from dse.cluster import Cluster

    cluster = Cluster()
    try:
        session = cluster.connect()
    except NoHostAvailable:
        print 'Unable to connect to cluster'

Upgrading Existing Code from Cassandra Driver
---------------------------------------------
Minimal Property Settings
~~~~~~~~~~~~~~~~~~~~~~~~~
Upgrading from ``cassandra-driver`` to ``dse-driver`` can be as simple as changing the Cluster import
to the ``dse`` package:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()
    print session.execute("SELECT release_version FROM system.local")[0]

...becomes:

.. code-block:: python

    from dse.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()
    print session.execute("SELECT release_version FROM system.local")[0]

.. _changes-execution-profiles:

Changes in Execution Property Defaults
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The DSE Driver takes advantage of *configuration profiles* to allow different execution configurations for the various
query handlers. Please see the :doc:`Execution Profile documentation <execution_profiles>` for a more generalized
discussion of the API. What follows here is an upgrade guide for DSE driver, which uses this API. Using this API
disallows use of the legacy config parameters, so changes must be made when setting non-default options
for any of these parameters:

- ``Cluster.load_balancing_policy``
- ``Cluster.retry_policy``
- ``Session.default_timeout``
- ``Session.default_consistency_level``
- ``Session.default_serial_consistency_level``
- ``Session.row_factory``

For example:

.. code-block:: python

    from dse.cluster import Cluster
    from dse.query import tuple_factory

    cluster = Cluster()
    session = cluster.connect()
    session.row_factory = tuple_factory

    print session.execute("SELECT release_version FROM system.local")[0]

Here we are only setting one of these attributes, so we use the default profile and change that one attribute:

.. code-block:: python

    from dse.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
    from dse.query import tuple_factory
    from dse.cluster import Cluster

    profile = ExecutionProfile(row_factory=tuple_factory)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect()

    print session.execute("SELECT release_version FROM system.local")[0]

Profiles are passed in by ``execution_profile`` dict.

Here we have more default execution parameters being set:

.. code-block:: python

    from dse import ConsistencyLevel
    from dse.cluster import Cluster
    from dse.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
    from dse.query import tuple_factory

    cluster = Cluster(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
                      default_retry_policy=DowngradingConsistencyRetryPolicy())
    session = cluster.connect()
    session.default_timeout = 15
    session.row_factory = tuple_factory
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    session.default_serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL

    print session.execute("SELECT release_version FROM system.local")[0]

In this case we can construct the base ``ExecutionProfile`` passing all attributes:

.. code-block:: python

    from dse import ConsistencyLevel
    from dse.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
    from dse.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
    from dse.query import tuple_factory
    from dse.cluster import Cluster

    profile = ExecutionProfile(WhiteListRoundRobinPolicy(['127.0.0.1']),
                               DowngradingConsistencyRetryPolicy(),
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_SERIAL,
                               15, tuple_factory)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect()

    print session.execute("SELECT release_version FROM system.local")[0]

It should be noted that this sets the default behavior for CQL requests. The DSE driver also defines a set of default
profiles for graph execution:

* :data:`~.cluster.EXEC_PROFILE_GRAPH_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT`

Users are free to setup additional profiles to be used by name:

.. code-block:: python

    profile_long = ExecutionProfile(request_timeout=30)
    cluster = Cluster(execution_profiles={'long': profile_long})
    session = cluster.connect()
    session.execute(statement, execution_profile='long')

Also, parameters passed to ``Session.execute`` or attached to ``Statement``\s are still honored as before.
