teiid-wildfly
=====

## To build Teiid Wildfly

$mvn clean install

## To start Teiid Wildfly

Once built you can start up the Wildfly server with Teiid by extracting "wildfly-build/target/teiid-wildfly-<version>-server.zip" and running:

    {server-dir}/bin/standalone.sh -c=standalone-teiid.xml

In order to be able to access the Wildfly console you need to setup a user by running "./bin/add-user.sh" or "bin/add-user.bat" from the server directory.

You should be able to access the Wildfly console at http://localhost:9990 and the Teiid console at http://localhost:9990/console/App.html#teiid.

For more info see [Installation Guide](http://teiid.github.io/teiid-documents/master/content/admin/Installation_Guide.html).

Licenses
-------

The default license for all submodules is the [Apache Software License (ASL) v2.0][1]

Where applicable individual submodules will provide additional copyright and license information.

[1]: view-source:https://www.apache.org/licenses/LICENSE-2.0
