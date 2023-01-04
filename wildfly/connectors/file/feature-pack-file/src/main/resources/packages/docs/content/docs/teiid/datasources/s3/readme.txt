Either edit the configuration or use the cli to create a s3 datasource.  See the following examples:

* if you are in standalone mode edit the "standalone-teiid.xml" file and add 
the contents of the s3-ds.xml under "resource-adapters" subsystem section.

* edit the create-s3-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-s3-ds.cli   --properties=create-s3-ds.properties