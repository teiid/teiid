There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

Step 1: Deploying the JDBC Driver
 
	Option 1: use the JBoss CLI tool, and deploy the "CacheDB.jar" or later jar by issuing the command
		deploy CacheDB.jar
		
		Note: when you use CLI based deployment, the <driver> element in the <datasources> xml fragment, must
		be the name of the jar file deployed, and no need for adding <drivers> element in standalone-teiid.xml 
		or domain-teiid.xml file
	 
	Option 2:
		1) Stop the server if it is running.

		2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

		3) Then copy the mysql database JDBC driver jar file "CacheDB.jar" into
			"<jboss-as>/modules/com/intersys/main" directory.
		4) start server

Step 2: Creating the datasource 

	Option 1: Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "intersystems-cache.xml" 
	file under the "datasources" subsystem. You may have to edit contents according 
	to where your intersystems server is located and credentials you need to use to access it.
	
	Option 2: Take a look at create-ds.cli script, and modify and execute using JBoss CLI tool as below 
	
	./Jboss-admin.sh --file create-ds.cli
