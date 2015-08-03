In Teiid, for MongoDB datasource a JCA connector is provided and deployed at the install time. To create mongodb datasource connection, choose 1 of the 2 following options:

1.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the mongodb.xml under "resource-adapters" subsystem section.

2.  edit the create-mongodb-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-mongodb-ds.cli   --properties=create-mongodb-ds.properties
