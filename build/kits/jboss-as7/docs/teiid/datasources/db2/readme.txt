There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

Step 1: Deploying the JDBC Driver
 
	Option 1: use the JBoss CLI tool, and deploy the "db2jcc4.jar" or later jar by issuing the command
		deploy db2jcc4.jar
		
	Option 2:(recommended)
		1) Stop the server if it is running.

		2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

		3) Then copy the db2 database JDBC driver jar file "db2jcc4.jar" into
			"<jboss-as>/modules/com/ibm/db2/main" directory.
		4) Add following to standalone[-teiid].xml file under "datasources" subsystem under <drivers> element 
		4) start server

Step 2: Creating the datasource 

	Option 1: Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "db2.xml" 
	or "db2-xa.xml" file under the "datasources" subsystem. You may have to edit contents according 
	to where your db2 server is located and credentials you need to use to access it.
	
	Option 2: Take a look at create-db2-ds.cli script, and *modify* and execute using JBoss CLI tool as below 
	
	./Jboss-admin.sh --file create-db2-ds.cli
	