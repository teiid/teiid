There are two parts to configuring a datasource, the driver and the datasource.   And you can skip the
deploying the JDBC driver if its already installed.  


Step 1: Deploying the JDBC Driver

	Option 1: (recommended and requires server restart)

		1) Overlay the "modules" directory to the "<jboss-as>/modules" directory 

		3) Then copy the db2 database JDBC driver jar file (i.e., "sqljdbc4.jar") into
			"<jboss-as>/modules/com/microsoft/sqlserver/main" directory.

 
	Option 2: use the JBoss CLI tool, and deploy the "sqljdbc4.jar" or later jar by issuing the command:  deploy sqljdbc4.jar
		

Step 2:  Configuring the Driver and Datasource (CLI or Edit configuration)


	Option 1 - Run CLI Script (recommended and server must be started and driver deployed)

	-	edit the create-ms-native-ds.properties or create-ms-native-xa-ds.properties to set accordingly
	-	run the CLI script:   
		a.   non-XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-ms-native-ds.cli   --properties={path}/create-ms-native-ds.properties
		b.   XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-ms-native-xa-ds.cli   --properties={path}/create-ms-native-xa-ds.properties


	Option 2 - Edit configuration file standalone-teiid.xml or domain-teiid.xml

	-	Configure datasource and add contents from one of the following files, based on use, under the "datasources" subsystem:

		a.  non-XA - sqlserver-ds.xml
		b.  XA - sqlserver-xa-ds.xml


	-	Configure driver (if the driver has not already been configured) by copying one of the following under "datasources" subsystem under <drivers> element:


		a.  non-XA

                    <driver name="sqlserver" module="com.microsoft.sqlserver">
                        <driver-class>com.microsoft.sqlserver.jdbc.SQLServerDriver</driver-class>
                    </driver>

		b.  XA

                    <driver name="sqlserverXA" module="com.microsoft.sqlserver">
                        <xa-datasource-class>com.microsoft.sqlserver.jdbc.SQLServerXADataSource</xa-datasource-class>
                    </driver>

		
		c.  or configure both, non-XA and XA within the same <driver> element, but if used, may need to map the correct driver_name in the datasource:

		   <driver name="sqlserver" module="com.microsoft.sqlserver">
                        <driver-class>com.microsoft.sqlserver.jdbc.SQLServerDriver</driver-class>
 			<xa-datasource-class>com.microsoft.sqlserver.jdbc.SQLServerXADataSource</xa-datasource-class>
                    </driver>
