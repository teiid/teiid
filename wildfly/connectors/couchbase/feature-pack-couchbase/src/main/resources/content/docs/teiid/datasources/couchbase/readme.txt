In Teiid, for Couchbase datasource a JCA connector is provided and deployed at the install time. To create couchbase datasource connection, choose 1 of the 2 following options:

1.  edit the "standalone-teiid.xml" file or "domain-teiid.xml" file and add 
the contents of the couchbase.xml under "resource-adapters" subsystem section.

2.  edit the create-couchbase-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-couchbase-ds.cli   --properties=create-couchbase-ds.properties
