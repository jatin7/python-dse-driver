Releasing
=========
* Run the tests and ensure they all pass
* Update CHANGELOG.rst
* Update the version in ``dse/__init__.py``

  * For beta releases, use a version like ``(2, 1, '0b1')``
  * For release candidates, use a version like ``(2, 1, '0rc1')``
  * When in doubt, follow PEP 440 versioning
* Add the new version in ``docs.yaml``

* Commit the changelog and version changes
* Tag the release.  For example: ``git tag -a 1.0.0 -m 'version 1.0.0'``
* Push the commit and tag: ``git push --tags origin master``
* For a GA release, upload the package to pypi::

    python setup.py register
    python setup.py sdist upload

* On pypi, make the latest GA the only visible version
* Update the docs (see below)
* Append a 'postN' string to the version tuple in ``dse/__init__.py``
  so that it looks like ``(x, y, z, 'postN')``

  * After a beta or rc release, this should look like ``(2, 1, '0b1', 'post0')``

* After the release has been tagged, add a section to docs.yaml with the new tag ref::

    versions:
      - name: <version name>
        ref: <release tag>

* Commit and push
* Update 'dse-test' branch to reflect new release

    * this is typically a matter of merging or rebasing onto master
    * test and push updated branch to origin

* Update the JIRA versions: https://datastax-oss.atlassian.net/plugins/servlet/project-config/PYTHON/versions
* Make an announcement on the mailing list

Building the Docs
=================
Sphinx is required to build the docs. You probably want to install through apt,
if possible::

    sudo apt-get install python-sphinx

pip may also work::

    sudo pip install -U Sphinx

To build the docs, run::

    python setup.py doc

To upload the docs, checkout the ``gh-pages`` branch (it's usually easier to
clone a second copy of this repo and leave it on that branch) and copy the entire
contents all of ``docs/_build/X.Y.Z/*`` into the root of the ``gh-pages`` branch
and then push that branch to github.

For example::

    python setup.py doc
    cp -R docs/_build/1.0.0-beta1/* ~/python-driver-docs/
    cd ~/python-driver-docs
    git add --all
    git commit -m 'Update docs'
    git push origin gh-pages

If docs build includes errors, those errors may not show up in the next build unless
you have changed the files with errors.  It's good to occassionally clear the build
directory and build from scratch::

    rm -rf docs/_build/*

Documentor
==========
We now also use another tool called Documentor with Sphinx source to build docs.
This gives us versioned docs with nice integrated search.

Dependencies
------------
Sphinx
~~~~~~
Installed as described above

Documentor
~~~~~~~~~~
Clone and setup Documentor as specified in `the project <https://github.com/riptano/documentor#installation-and-quick-start>`_.
This tool assumes Ruby, bundler, and npm are present.

Building
--------
The setup script expects documentor to be in the system path. You can either add it permanently or run with something
like this::

    PATH=$PATH:<documentor repo>/bin python setup.py doc

The docs will not display properly just browsing the filesystem in a browser. To view the docs as they would be in most
web servers, use the SimpleHTTPServer module::

    cd docs/_build/
    python -m SimpleHTTPServer

Then, browse to `localhost:8000 <http://localhost:8000>`_.

Running the Tests
=================
In order for the extensions to be built and used in the test, run::

    DSE_VERSION=5.0.4 python setup.py nosetests

You can run a specific test module or package like so::

    DSE_VERSION=5.0.4 python setup.py nosetests -w tests/unit/

You can run a specific test method like so::

    DSE_VERSION=5.0.4 python setup.py nosetests -w tests/unit/test_connection.py:ConnectionTest.test_bad_protocol_version

Note that the version has to be specified, otherwise by default the Open Source version of Cassandra will run. You can also specify the version using a Cassandra directory (to test unreleased versions)::

    CASSANDRA_DIR=/home/user/bdp python setup.py nosetests -w tests/integration/standard

For this to work DSE has to be built, so once the appropriate commit is checked out, inside the ``bdp`` folder:

	./gradlew clean dist

Running the advanced authentication tests
-----------------------------
This tests are in the file ``tests/integration/advanced/test_auth.py``. These tests are run the same way as the rest but first the we have to set the variable ADS_HOME:

	git clone https://github.com/riptano/testeng-devtools.git
	cd testeng-devtools/EmbeddedAds
	mvn clean install
	cp target/embedded-ads-1.0.1-SNAPSHOT-*.jar embedded-ads.jar
	export ADS_HOME=`pwd`

After this we can run the tests normally from the appropriate folder:

	DSE_VERSION=5.0.4 python setup.py nosetests -w tests/integration/advanced/test_auth.py

Seeing Test Logs in Real Time
-----------------------------
Sometimes it's useful to output logs for the tests as they run::

    DSE_VERSION=5.0.4 python setup.py nosetests -w tests/unit/ --nocapture --nologcapture

Use tee to capture logs and see them on your terminal::

    DSE_VERSION=5.0.4 python setup.py nosetests -w tests/unit/ --nocapture --nologcapture 2>&1 | tee test.log

Specifying the usage of an already running on the DSE cluster
----------------------------------------------------
The test will start the appropriate DSE clusters when necessary  but if you don't want this to happen because a DSE cluster is already running the flag ``USE_CASS_EXTERNAL`` can be used, for example:

	USE_CASS_EXTERNAL=1 python setup.py nosetests -w tests/integration/standard

Specifying the usage of an already running Cassandra cluster
----------------------------------------------------
The test will start the appropriate Cassandra clusters when necessary  but if you don't want this to happen because a Cassandra cluster is already running the flag ``USE_CASS_EXTERNAL`` can be used, for example:

	USE_CASS_EXTERNAL=1 python setup.py nosetests -w tests/integration/standard

Specify a Protocol Version for Tests
------------------------------------
You can explicitly set it with the ``PROTOCOL_VERSION`` environment variable::

    DSE_VERSION=5.0.4 PROTOCOL_VERSION=3 python setup.py nosetests -w tests/integration/standard

Testing Multiple Python Versions
--------------------------------
If you want to test all of python 2.7, 3.3, 3.4 and pypy, use tox (this is what
TravisCI runs)::

    tox

By default, tox only runs the unit tests because I haven't put in the effort
to get the integration tests to run on TravicCI.  However, the integration
tests should work locally.  To run them, edit the following line in tox.ini::

    commands = {envpython} setup.py build_ext --inplace nosetests --verbosity=2 tests/unit/

and change ``tests/unit/`` to ``tests/``.

Running the Benchmarks
======================
There needs to be a version of DSE running locally so before running the benchmarks, if ccm is installed:

	ccm create benchmark_cluster -v 3.0.1 -n 1 -s

There needs to be a version of DSE running locally so before running the benchmarks, if ccm is installed:

	ccm create 5.0.4 --dse --dse-username=your_username@datastax.com --dse-password=your_password -v 5.0.4 -n 1 -s


To run the benchmarks, pick one of the files under the ``benchmarks/`` dir and run it::

    python benchmarks/future_batches.py

There are a few options.  Use ``--help`` to see them all::

    python benchmarks/future_batches.py --help

Packaging for dse-driver
========================
A source distribution is included in dse-driver, which uses the driver internally for ``cqlsh``.

To package a released version, checkout the tag and build a source zip archive::

    python setup.py sdist --formats=zip

If packaging a pre-release (untagged) version, it is useful to include a commit hash in the archive
name to specify the built version::

    python setup.py egg_info -b-`git rev-parse --short HEAD` sdist --formats=zip

The file ``dist/dse_driver-<version spec>.zip``) will be created.

Most notes on releasing and testing are the same as those in the core driver `README-dev <https://github.com/riptano/python-dse-driver/blob/master/README-dev.rst>`_.

Here we discuss any differences.

Releasing an EAP
================

An EAP release is only uploaded on a private server and it is not published on pypi.

* Clean the environment
python setup.py clean

* Package the source distribution::

    python setup.py sdist

* Test the source distribution::

    pip install dist/dse-driver-<version>.tar.gz

* Upload the package on the EAP download server::

    scp dist/dse-driver-<version>.tar.gz username@jenkins2.datastax.lan:/datastax/www/eap.datastax.com/drivers/python

* Build the documentation::

    python setup.py doc

* Upload the docs on the EAP download server::

    scp -r docs/_build/<version>/  username@jenkins2.datastax.lan:/datastax/www/eap.datastax.com/drivers/python/docs

* Sync the Jenkins EAP dir to the public server with [the `Admin_Sync_Jenkins_To_Master` Jenkins job](http://jenkins2.datastax.lan:8080/view/All/job/Admin_Sync_Jenkins_To_Master/).
* _After that job finishes_, sync the public server to the S3 nodes with [the `Admin_Sync_Master_To_Nodes` Jenkins job](http://jenkins2.datastax.lan:8080/view/All/job/Admin_Sync_Master_To_Nodes/).
* Update links and version numbers on [the DS Academy EAP page](https://academy.datastax.com/content/dse-drivers-eap-download-and-install).
* Mention the release in the `#release-team` and `#eap-warroom` Slack rooms.
