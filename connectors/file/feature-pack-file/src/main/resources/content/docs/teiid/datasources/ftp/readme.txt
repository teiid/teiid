In Teiid, for Ftp datasource a JCA connector is provided and deployed at the install time. 1 of the 2
following ways can be used to create file datasource connection:

1.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the file.xml under "resource-adapters" subsystem section.

2.  edit the create-file-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-ftp-ds.cli   --properties=create-ftp-ds.properties
