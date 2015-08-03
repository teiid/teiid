There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

Note that all instances of [version] should be replaced by the appropriate vertica version.

Step 1: Deploying the JDBC Driver
 
	Option 1: use the JBoss CLI tool, and add the vertica driver by issuing the command

		./bin/jboss-cli.sh --connect --file=create-vertica-driver.cli
		
	Option 2: (recommended)
		1) Stop the server if it is running.

		2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

		3) Then copy the vertica database JDBC driver jar file into the
			"<jboss-as>/modules/com/vertica/jdbc/main" directory.
		4) start server

Step 2: Creating the datasource 

	Option 1: Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "vertica.xml" 
	file under the "datasources" subsystem. You may have to edit contents according to where your Vertica server 
	is located and credentials you need to use to access it.
	
	Option 2: Take a look at create-vertica-ds.cli script, and modify and execute using JBoss CLI tool as below 
	
	./bin/jboss-cli.sh --connect --file=create-vertica-ds.cli

	Option 3: Deploy vertica-ds.xml file. You may have to edit contents according to where your Vertica server
        is located and credentials you need to use to access it.
	
