DSE Authentication
==================
The DSE extension provides two auth providers that work both with legacy kerberos and Cassandra authenticators,
as well as the new DSE Unified Authentication. This allows client to configure this auth provider independently,
and in advance of any server upgrade. These auth providers are configured in the same way as any previous implementation::

    from dse.auth import DSEGSSAPIAuthProvider
    auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect()

Implementations are :attr:`.DSEPlainTextAuthProvider`, :class:`.DSEGSSAPIAuthProvider` and :class:`.SaslAuthProvider`.

DSE Unified Authentication
--------------------------

With DSE (>=5.1), unified Authentication allows you to:

* Proxy Login: Authenticate using a fixed set of authentication credentials but allow authorization of resources based another user id.
* Proxy Execute: Authenticate using a fixed set of authentication credentials but execute requests based on another user id.

Limitations
~~~~~~~~~~~

* Proxy Login cannot be used with Kerberos due to a known issue. It is currently investigated and a fix will be available soon.

Proxy Login
~~~~~~~~~~~

Proxy login allows you to authenticate with a user but act as another one. You need to ensure the authenticated user has the permission to use the authorization of resources of the other user. ie. this example will allow the `server` user to authenticate as usual but use the authorization of `user1`:

.. code-block:: text

    GRANT PROXY.LOGIN on role user1 to server

then you can do the proxy authentication....

.. code-block:: python

    from dse.cluster import Cluster
    from dse.auth import SaslAuthProvider

    sasl_kwargs = {
      "service": 'dse',
      "mechanism":"PLAIN",
      "username": 'server',
      'password': 'server',
      'identity': 'user1'
    }

    auth_provider = SaslAuthProvider(**sasl_kwargs)
    c = Cluster(auth_provider=auth_provider)
    s = c.connect()
    s.execute(...)  # all requests will be executed as 'user1'

If you are using kerberos, you can use directly :class:`.DSEGSSAPIAuthProvider` and pass the principal for the authorization, like this:

.. code-block:: python

    from dse.cluster import Cluster
    from dse.auth import DSEGSSAPIAuthProvider

    # Ensure the kerberos ticket of the server user is set with the kinit utility.
    auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="user1@DATASTAX.COM")
    c = Cluster(auth_provider=auth_provider)
    s = c.connect()
    s.execute(...)  # all requests will be executed as 'user1'


Proxy Execute
~~~~~~~~~~~~~

Proxy execute allows you to execute requests as another user than the authenticated one. You need to ensure the authenticated user has the permission to use the authorization of resources of the specified user. ie. this example will allow the `server` user to execute requests as `user1`:

.. code-block:: text

    GRANT PROXY.EXECUTE on role user1 to server

then you can do a proxy execute...

.. code-block:: python

    from dse.cluster import Cluster
    from dse.auth import DSEPlainTextAuthProvider,

    auth_provider = DSEPlainTextAuthProvider('server', 'server')

    c = Cluster(auth_provider=auth_provider)
    s = c.connect()
    s.execute('select * from k.t;', execute_as='user1')  # the request will be executed as 'user1'

Please see the `official documentation <https://docs.datastax.com/en/latest-dse/datastax_enterprise/unifiedAuth/unifiedAuthTOC.html>`_ for more details on the feature and configuration process.
