Either edit the configuration or use the cli to create a file datasource.  See the following examples:

* if you are in standalone mode edit the "standalone-teiid.xml" file and add 
the contents of the file-ds.xml under "resource-adapters" subsystem section.

* edit the create-file-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-file-ds.cli   --properties=create-file-ds.properties