Option 1:

In Teiid, for Salesforce datasource a JCA connector is provided and deployed at the install time. To create
salesforce datasource connection edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the salesforce-xx.xml under "resource-adapters" subsystem section.

NOTE: salesforce-xx.xml uses the newer salesforce API version xx

Option 2:

edit the create-salesforce-ds.properties to set accordingly, and then run the following CLI script
 in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-salesforce-ds.cli   --properties=create-salesforce-ds.properties