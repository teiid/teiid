There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

Note that all instances of [version] should be replaced by the appropriate ucanaccess and it's dependency version.

Step 1: Installing the Driver Module

	1) Download ucanaccess 2.0.9.4 or later(https://sourceforge.net/projects/ucanaccess/files/)

	2) Overlay the "modules" directory on the "<jboss-as>/modules" directory

	3) Unzip 1) will get 5 jars(ucanaccess-[version].jar,hsqldb.jar,jackcess-[version].jar,commons-lang-[version].jar,commons-logging-[version].jar), copy all these jars into the "<jboss-as>/modules/net/ucanaccess/main" directory.

	4) Start server

Step 2: Creating the datasource 

	Option 1: Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "ucanaccess.xml" 
	file under the "datasources" subsystem. You may have to edit contents according to where your Access File 
	is located and credentials you need to use to access it.
	
	Option 2: Take a look at create-ucanaccess-ds.cli script, and modify and execute using JBoss CLI tool as below 
	
	./bin/jboss-cli.sh --connect --file=create-ucanaccess-ds.cli

	Option 3: Deploy ucanaccess-ds.xml file. You may have to edit contents according to where your Access File
        is located and credentials you need to use to access it.
	
