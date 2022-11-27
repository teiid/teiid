teiid
=====

[![Build Status](https://api.travis-ci.com/teiid/teiid.svg?branch=master)](https://travis-ci.org/teiid/teiid)
[![Join the chat at https://gitter.im/teiid](https://badges.gitter.im/teiid/teiid.svg)](https://gitter.im/teiid?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Teiid is a data virtualization system that allows applications to use data from multiple, heterogeneous data stores.

## Useful Links
- Website - http://teiid.org
- Latest Documentation - http://teiid.github.io/teiid-documents/master/content/
- Documentation Project - https://github.com/teiid/teiid-documents
- JIRA Issues -  https://issues.jboss.org/browse/TEIID
- User Forum - https://community.jboss.org/en/teiid?view=discussions
- Wiki - https://community.jboss.org/wiki/TheTeiidProject

## To build Teiid
- install JDK 1.9 or higher
- install maven 3.2+ - http://maven.apache.org/download.html
- Create a github account and fork Teiid

Enter the following:

	$ git clone https://github.com/<yourname>/teiid.git
	$ cd teiid
	$ mvn clean install -P dev -s settings.xml
	
you can find the deployment artifacts in the "teiid/build/target" directory once the build is completed.

## Travis Builds

Teiid includes a travis build config.  By default it performs only an "install" on every commit.  It allows for a
cron based build to be configured as well for deploying snapshots.  The snapshot build requires add the environment
variables SONATYPE_USERNAME and SONATYPE_PASSWORD that should set to the user access token values of an
authorized sonatype account.

## Teiid for WildFly

Teiid for WildFly may be built by including the wildfly profile with the relevant build command.  For example:

    $ mvn clean install -P dev,wildfly -s settings.xml
    
The Teiid WildFly kits will then be located under wildfly/wildfly-build/target

Licenses
-------

The default license for all submodules is the [Apache Software License (ASL) v2.0][1]

Where applicable individual submodules will provide additional copyright and license information.

[1]: view-source:https://www.apache.org/licenses/LICENSE-2.0
