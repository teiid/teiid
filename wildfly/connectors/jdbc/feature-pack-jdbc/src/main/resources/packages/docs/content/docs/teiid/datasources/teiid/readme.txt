There are two parts to configuring a datasource, the driver and the datasource.   And you can skip the
deploying the JDBC driver if its already installed.


NOTE:  If you installed Teiid into JBoss AS then the driver is already installed.

Step 1: Deploying the JDBC Driver

	Option 1: (recommended and requires server restart)

		1) Overlay the "modules" directory to the "<jboss-as>/modules" directory 

		3) Then copy the db2 database JDBC driver jar file (i.e., "db2jcc4.jar") into
			"<jboss-as>/modules/com/ibm/db2/main" directory.

 
	Option 2: use the JBoss CLI tool, and deploy the "db2jcc4.jar" or later jar by issuing the command:  deploy db2jcc4.jar
		

Step 2:  Configuring the Driver and Datasource (CLI or Edit configuration)


	Option 1 - Run CLI Script (recommended and server must be started and driver deployed)

	-	edit the appropriate .properties and set the values accordingly
	-	run the CLI script:   
                a.  Teiid local connection - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-teiid-local-ds.cli   --properties={path}/create-teiid-local-ds.properties
		a.  Teiid (remote) connection non-XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-teiid-ds.cli   --properties={path}/create-teiid-ds.properties
		b.  Teiid (remote) connection XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-teiid-xa-ds.cli   --properties={path}/create-teiid-xa-ds.properties


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

                    <driver name="db2XA" module="com.ibm.db2">
                        <xa-datasource-class>com.ibm.db2.jcc.DB2XADataSource</xa-datasource-class>
                    </driver>

		
		c.  or configure both, non-XA and XA within the same <driver> element, but if used, may need to map the correct driver_name in the datasource:

                    <driver name="db2" module="com.ibm.db2">
                        <driver-class>com.ibm.db2.jcc.DB2Driver</driver-class>
 			<xa-datasource-class>com.ibm.db2.jcc.DB2XADataSource</xa-datasource-class>
                    </driver>




Creating the datasource 

  Option 1: 
	
	Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "teiid.xml" 
	or "teiid-xa.xml" or "teiid-local.xml" file under the "datasources" subsystem. You may have to edit contents according 
	to where your Teiid server is located and credentials you need to use to access it.
	
  Option 2: 
	
	Take a look at create-teiid-ds.cli script, and modify and execute using JBoss CLI tool as below 
	
	./Jboss-admin.sh --file create-teiid-ds.cli
	
