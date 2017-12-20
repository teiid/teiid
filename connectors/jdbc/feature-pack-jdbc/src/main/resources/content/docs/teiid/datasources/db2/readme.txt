There are two parts to configuring a datasource, the driver and the datasource.   And you can skip the
deploying the JDBC driver if its already installed.  


Step 1: Deploying the JDBC Driver

	Option 1: (recommended and requires server restart)

		1) Overlay the "modules" directory to the "<jboss-as>/modules" directory 

		3) Then copy the db2 database JDBC driver jar file (i.e., "db2jcc4.jar") into
			"<jboss-as>/modules/com/ibm/db2/main" directory.

 
	Option 2: use the JBoss CLI tool, and deploy the "db2jcc4.jar" or later jar by issuing the command:  deploy db2jcc4.jar
		

Step 2:  Configuring the Driver and Datasource (CLI or Edit configuration)


	Option 1 - Run CLI Script (recommended and server must be started and driver deployed)

	-	edit the create-db2-ds.properties or create-db2-xa-ds.properties to set accordingly
	-	run the CLI script:   
		a.   non-XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-db2-ds.cli   --properties={path}/create-db2-ds.properties
		b.   XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-db2-xa-ds.cli   --properties={path}/create-db2-xa-ds.properties


	Option 2 - Edit configuration file standalone-teiid.xml or domain-teiid.xml

	-	Configure datasource and add contents from one of the following files, based on use, under the "datasources" subsystem:

		a.  non-XA - db2-ds.xml
		b.  XA - db2-xa-ds.xml


	-	Configure driver (if the driver has not already been configured) by copying one of the following under "datasources" subsystem under <drivers> element:


		a.  non-XA

                     <driver name="db2" module="com.ibm.db2">
                        <driver-class>com.ibm.db2.jcc.DB2Driver</driver-class>
                    </driver>   


        b.  XA

                    <driver name="db2" module="com.ibm.db2">
                        <xa-datasource-class>com.ibm.db2.jcc.DB2XADataSource</xa-datasource-class>
                    </driver>
       
		