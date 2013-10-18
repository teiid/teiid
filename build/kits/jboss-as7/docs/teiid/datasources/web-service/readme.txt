In Teiid, for web-service(SOAP, REST/HTTP) datasource a JCA connector is provided and deployed at the install time. To create
web service datasource connection, choose 1 of the 2 following options:

1.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the ws.xml under "resource-adapters" subsystem section.

2.  edit the create-ws-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-ws-ds.cli   --properties=create-ws-ds.properties
