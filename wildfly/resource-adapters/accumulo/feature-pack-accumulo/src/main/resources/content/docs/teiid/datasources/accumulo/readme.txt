In Teiid, for Accumulo datasource is a JCA connector is provided and deployed at the install time. To create accumulo datasource
connection, choose 1 of the 2 following options:

1.  edit the "standalone-teiid.xml" file and add 
the contents of the accumulo.xml under "resource-adapters" subsystem section.

2.  edit the create-accumulo-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-accumulo-ds.cli   --properties=create-accumulo-ds.properties
