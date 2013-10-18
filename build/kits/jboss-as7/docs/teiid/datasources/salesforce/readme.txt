SalesForce connector requires CXF and SpringFramework to function correctly. The CXF is already installed, however the
SpringFramework is not. So, you must download the SpringFramework jar files from http://www.springsource.org/download, 
and overlay the "modules" directory in this directory on to modules directory in the JBoss AS, then copy 
the SpringFramework files into "<jboss-as>/modules/org/springframework/spring/main" directory. 

Option 1:

In Teiid, for Salesforce datasource a JCA connector is provided and deployed at the install time. To create
salesforce datasource connection edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the salesforce.xml under "resource-adapters" subsystem section.

Option 2:

edit the create-salesforce-ds.properties to set accordingly, and then run the following CLI script
 in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-salesforce-ds.cli   --properties=create-salesforce-ds.properties