There are two parts to configuring a datasource, the driver and the datasource.   And you can skip the
deploying the JDBC driver if its already installed.  


Step 1: Deploying the JDBC Driver

	Option 1: (recommended and requires server restart)

		1) Overlay the "modules" directory to the "<jboss-as>/modules" directory 

		3) Then copy the db2 database JDBC driver jar file (i.e., "postgresql-8.3-603.jdbc3.jar") into
			"<jboss-as>/modules/org/postgresql/main" directory.

 
	Option 2: use the JBoss CLI tool, and deploy the "postgresql-8.3-603.jdbc3.jar" or later jar by issuing the command:  deploy postgresql-8.3-603.jdbc3.jar
		

Step 2:  Configuring the Driver and Datasource (CLI or Edit configuration)


	Option 1 - Run CLI Script (recommended and server must be started and driver deployed)

	-	edit the create-postgresql-ds.properties or create-postgresql-xa-ds.properties to set accordingly
	-	run the CLI script:   
		a.   non-XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-postgresql-ds.cli   --properties={path}/create-postgresql-ds.properties
		b.   XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-postgresql-xa-ds.cli   --properties={path}/create-postgresql-xa-ds.properties


	Option 2 - Edit configuration file standalone-teiid.xml or domain-teiid.xml

	-	Configure datasource and add contents from one of the following files, based on use, under the "datasources" subsystem:

		a.  non-XA - postgresql-ds.xml
		b.  XA - postgresql-xa-ds.xml


	-	Configure driver (if the driver has not already been configured) by copying one of the following under "datasources" subsystem under <drivers> element:


		a.  non-XA

                    <driver name="postgres" module="org.postgresql">
                        <driver-class>org.postgresql.Driver</driver-class>
                    </driver>	


		b.  XA

                    <driver name="postgresXA" module="org.postgresql">
                        <xa-datasource-class>org.postgresql.xa.PGXADataSource</xa-datasource-class>
                    </driver>

		
		c.  or configure both, non-XA and XA within the same <driver> element, but if used, may need to map the correct driver_name in the datasource:

		    <driver name="postgres" module="org.postgresql">
                        <driver-class>org.postgresql.Driver</driver-class>
 			<xa-datasource-class>org.postgresql.xa.PGXADataSource</xa-datasource-class>
                    </driver>
