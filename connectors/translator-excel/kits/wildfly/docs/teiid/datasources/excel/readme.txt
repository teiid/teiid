In Teiid, for Excel datasource a JCA connector is provided and deployed at the install time. 1 of the 3
following ways can be used to create file datasource connection:

1.  edit the "standalone" or "domain" related configuration file(s) and add 
the contents of the excel-ds.xml under "resource-adapters" subsystem section.

2.  edit the create-excel-ds.cli script and replace the ${..} properties with the appropriate values.
See the create-excel-ds.properties for examples.

3.  edit the create-excel-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-excel-ds.cli   --properties=create-excel-ds.properties

NOTE: #3 requires, before running, the editing of JBOSS_HOME/bin/jboss-cli.xml file and changing the following property to "true":

 <resolve-parameter-values>false</resolve-parameter-values>
