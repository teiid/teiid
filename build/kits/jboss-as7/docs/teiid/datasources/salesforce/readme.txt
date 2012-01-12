Option 1:

In Teiid, for Salesforce datasource a JCA connector is provided and deployed at the install time. To create
salesforce datasource connection edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the salesforce.xml under "resource-adapters" subsystem section.

Option 2:

Take a look at create-salesforce-ds.cli script, and modify and execute using JBoss CLI tool as below  
./Jboss-admin.sh --file create-salesforce-ds.cli