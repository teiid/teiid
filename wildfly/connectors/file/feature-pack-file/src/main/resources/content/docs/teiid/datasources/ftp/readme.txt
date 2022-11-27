Either edit the configuration or use the cli to create a ftp datasource.  See the following examples:

* if you are in standalone mode edit the "standalone-teiid.xml" ftp and add 
the contents of the ftp-ds.xml under "resource-adapters" subsystem section.

* edit the create-ftp-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --ftp=create-ftp-ds.cli   --properties=create-ftp-ds.properties