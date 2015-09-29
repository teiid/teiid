Check the installation of JDBC and PI SQL DAS by locating the PIPC directory
and verifying there is a folder JDBC containing the file PIJDBCDriver.jar (platform
independent). Also verify the file RDSAWrapper.dll (native code library) is installed.
Note1: On a Linux System this shared library would be called libRDSAWrapper.so
Note2: If used with a 64 bit Java VM the names were RDSAWrapper64.dll
or libRDSAWrapper64.so
Verify the environment variable PI_RDSA_LIB is defined and refers to the shared library.
Verify PI SQL Data Access Server service is installed and running 

See See http://videostar.osisoft.com/trainingwebinars/learninglabs/PI%20JDBC%20Basics%20Learn%20How%20to%20Query%20PI.pdf



There are two parts to configuring a datasource, the driver and the datasource in JBoss EAP.

Step 1: Deploying the JDBC Driver. 

	Option 1: (recommended and requires server restart)

		1) Overlay the "modules" directory to the "<jboss-as>/modules" directory 

		2) Then copy the PI database JDBC driver jar file (i.e., "PIJDBCDriver.jar") into
			"<jboss-as>/modules/syatem/layers/dv/com/osisoft/main" directory.

Step 2:  Configuring the Driver and Datasource 

Edit standalone-teiid.xml file in <jboss-eap>/stanalone/configuration, and find "datasources" subsystem and 
add contents of "pi-ds.xml" file to that section.

Restart server.