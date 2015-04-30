There are two parts to configuring a datasource, the driver and the datasource.   And you can skip the
deploying the JDBC driver if its already installed.  


Step 1: Deploying the JDBC Driver

	Option 1: (recommended and requires server restart)

		1) Overlay the "modules" directory to the "<jboss-as>/modules" directory 

		3) Then copy the mysql database JDBC driver jar file (i.e., "mysql-connector-java-5.1.5.jar") into
			"<jboss-as>/modules/com/mysql/main" directory.

 
	Option 2: use the JBoss CLI tool, and deploy the "mysql-connector-java-5.1.5.jar" or later jar by issuing the command:  deploy mysql-connector-java-5.1.5.jar
		

Step 2:  Configuring the Driver and Datasource (CLI or Edit configuration)


	Option 1 - Run CLI Script (recommended and server must be started and driver deployed)

	-	edit the create-mysql-ds.properties or create-mysql-xa-ds.properties to set accordingly
	-	run the CLI script:   
		a.   non-XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-mysql-ds.cli   --properties={path}/create-mysql-ds.properties
		b.   XA - JBOSS_HOME/bin/jboss-cli.sh -c --file={path}/create-mysql-xa-ds.cli   --properties={path}/create-mysql-xa-ds.properties


	Option 2 - Edit configuration file standalone-teiid.xml or domain-teiid.xml

	-	Configure datasource and add contents from one of the following files, based on use, under the "datasources" subsystem:

		a.  non-XA - mysql-ds.xml
		b.  XA - mysql-xa-ds.xml


	-	Configure driver (if the driver has not already been configured) by copying one of the following under "datasources" subsystem under <drivers> element:


		a.  non-XA

                     <driver name="mysql" module="com.mysql">
                        <driver-class>com.mysql.jdbc.Driver</driver-class>
                    </driver>	


		b.  XA

                    <driver name="mysqlXA" module="com.mysql">
                        <xa-datasource-class>com.mysql.jdbc.jdbc2.optional.MysqlXADataSource</xa-datasource-class>
                    </driver>

		
		c.  or configure both, non-XA and XA within the same <driver> element, but if used, may need to map the correct driver_name in the datasource:

                    <driver name="mysql" module="com.mysql">
                        <driver-class>com.mysql.jdbc.Driver</driver-class>
 			<xa-datasource-class>com.mysql.jdbc.jdbc2.optional.MysqlXADataSource</xa-datasource-class>
                    </driver>



	
**************************************************************************************************************	
NOTE: MySQL JDBC driver does not include the Driver file name as the service loader mechanism as of 
version 5.1.5, so as is Step 1, Option 2 will not work. However you if you can manually fix jar to include 
service definitions as described here http://community.jboss.org/docs/DOC-16657 then you can use it.
*************************************************************************************************************** 
