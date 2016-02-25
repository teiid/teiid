Option 1:

In Teiid, for Salesforce datasource a JCA connector is provided and deployed at the install time. To create
salesforce datasource connection edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the salesforce.xml or salesforce-34.xml under "resource-adapters" subsystem section.

NOTE: salesforce-34.xml uses the newer salesforce API version 34, where as salesforce.xml uses version 22

Option 2:

edit the create-salesforce-ds.properties to set accordingly, and then run the following CLI script
 in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-salesforce-ds.cli   --properties=create-salesforce-ds.properties