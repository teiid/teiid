There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

Step 1: Deploying the JDBC Driver
 
	Option 1: if only a single jar represents the client driver and it declares the driver class as a 
	  java.sql.Driver service, you may use the JBoss CLI tool to deploy the client .jar.  Issue the command:
		deploy <name>.jar
				
	Option 2: (recommended)
		1) Stop the server if it is running.

		2) Create a module structure for the driver jar and any dependencies it has.  Note that WildFly contains 
		   many existing libraries as modules.  It is recommended that you locate all dependencies under a single
		   module.  However you may choose to manage dependencies in their own modules.   

		3) Then copy the jars into the appropriate directory locations, for example the derby jar would be 
		   located under:
			"<jboss-as>/modules/org/apache/derby/main" directory.
			
		4) start server
		
		You should refer to other jdbc sources in the parent directory for more examples of using modules, 
		including db2, derby, oracle, ucanaccess, etc.

Step 2: Creating the datasource 

	Option 1: Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "jdbc-ds.xml" 
	or "jdbc-xa.xml" file under the "datasources" subsystem. You may have to edit contents according 
	to the details of your source, such as where it is located and credentials you need to use to access it.
	
	Option 2: Modify the create-jdbc-ds.cli script, and execute using JBoss CLI tool as below 
	
	./Jboss-admin.sh --file create-jdbc-ds.cli
	