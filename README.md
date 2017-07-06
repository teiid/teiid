teiid
=====

Teiid is a data virtualization system that allows applications to use data from multiple, heterogeneous data stores.

## Useful Links
- Website - http://teiid.org
- Documentation - https://teiid.gitbooks.io/documents/content/
- Documentation Project - https://teiid.gitbooks.io
- JIRA Issues -  https://issues.jboss.org/browse/TEIID
- User Forum - https://community.jboss.org/en/teiid?view=discussions
- Wiki - https://community.jboss.org/wiki/TheTeiidProject

## To build Teiid
- install JDK 1.8 or higher
- install maven 3.2+ - http://maven.apache.org/download.html
- Create a github account and fork Teiid
- Use the -Ddocker=true argument to enable image building, which requires a running Docker daemon.

Enter the following:

	$ git clone https://github.com/<yourname>/teiid.git
	$ cd teiid
	$ mvn clean install -P dev -s settings.xml
	
you can find the deployment artifacts in the "teiid/build/target" directory once the build is completed.

Licenses
-------

The default license for all submodules is the [Apache Software License (ASL) v2.0][1]

Where applicable individual submodules will provide additional copyright and license information.

[1]: view-source:https://www.apache.org/licenses/LICENSE-2.0


