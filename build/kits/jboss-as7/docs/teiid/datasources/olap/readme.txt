There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

Step 1: Deploying the JDBC Driver
 
		1) Stop the server if it is running.

		2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

		3) Then copy the olap4j database JDBC driver jar file "olap4j.jar" into
			"<jboss-as>/modules/org/olap4j/main" directory.
		
		4) if you are working with mondrian directly then copy these jars too into "<jboss-as>/modules/org/olap4j/main" directory.
		
	        commons-math-1.0.jar
	        commons-vfs-1.0.jar
	        eigenbase-properties.jar
	        eigenbase-resgen.jar
	        eigenbase-xom.jar
	        javacup.jar
	        mondrian.jar		
		
		5) start server

Step 2: Creating the datasource 

	Option 1: Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "mondrian.xml" 
	or "olap-xmla.xml" file under the "datasources" subsystem. You may have to edit contents according 
	to where your olap server is located and credentials you need to use to access it.
	