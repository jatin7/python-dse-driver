DataStax Enterprise Python Driver
=================================
This is the documentation for the DataStax Enterprise Python Driver for DSE.

The driver supports DSE 5.0+ and Python, 2.7, 3.3, and 3.4.

Contents
--------
:doc:`installation`
    How to install the driver.

:doc:`getting_started`
    A guide through the first steps of connecting to Cassandra and executing queries

:doc:`object_mapper`
    Introduction to the integrated object mapper, cqlengine

:doc:`api/index`
    The API documentation.

:doc:`upgrading`
    A guide to upgrading versions of the driver

:doc:`execution_profiles`
    An introduction to a more flexible way of configuring request execution

:doc:`performance`
    Tips for getting good performance.

:doc:`query_paging`
    Notes on paging large query results

:doc:`lwt`
    Working with results of conditional requests

:doc:`user_defined_types`
    Working with Cassandra 2.1's user-defined types

:doc:`security`
    An overview of the security features of the driver

:doc:`dates_and_times`
    Some discussion on the driver's approach to working with timestamp, date, time types

:doc:`faq`
    A collection of Frequently Asked Questions

:doc:`auth`
    Example configuring DSE authentication

:doc:`geo_types`
    Working with DSE geometry types

:doc:`graph`
    Graph queries with DSE Graph

:doc:`api/index`
    The API documentation.

.. toctree::
   :hidden:

   api/index
   installation
   getting_started
   upgrading
   execution_profiles
   performance
   query_paging
   lwt
   security
   user_defined_types
   object_mapper
   geo_types
   graph
   auth
   dates_and_times
   faq

Getting Help
------------
Visit the :doc:`FAQ section <faq>` in this documentation.

Please send questions to the `mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_.

Alternatively, you can use IRC.  Connect to the #datastax-drivers channel on irc.freenode.net.
If you don't have an IRC client, you can use `freenode's web-based client <http://webchat.freenode.net/?channels=#datastax-drivers>`_.

Reporting Issues
----------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.
