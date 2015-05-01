To install the google JCA connector do one of the following:

a.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the google.xml under "resource-adapters" subsystem section.

b.  edit the create-google-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-google-ds.cli   --properties=create-google-ds.properties
