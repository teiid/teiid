There are two parts to creating a datasource, depending upon whether this is the first time you are doing this, 
you can skip the deploying JDBC driver for the database, if you have previously already done this.

 
Option 1: use the JBoss CLI tool, and add the vector driver by issuing the command

	./bin/jboss-cli.sh --connect --file=create-actian-vector-ds.cli
	
Option 2: (recommended)
	1) Stop the server if it is running.

	2) Overlay the "modules" directory on the "<jboss-as>/modules/system/layers/dv/" directory 

	3) Then copy the actian vector database JDBC driver jar file into the
		"<jboss-as>/modules/system/layers/dv/com/actian/vector/main" directory.
    4) edit the contents of standalone-teiid.xml file, add the contents of "actian-vector.xml" file and provide
    {host} and {db-name}, {user} and {password} properties correctly
    
	5) start server


	
