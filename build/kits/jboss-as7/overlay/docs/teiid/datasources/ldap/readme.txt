In Teiid, for LDAP datasource a JCA connector is provided and deployed at the install time. To create
ldap datasource connection, choose 1 of the 2 following options:

1.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the ldap.xml under "resource-adapters" subsystem section.

2.  edit the create-ldap-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-ldap-ds.cli   --properties=create-ldap-ds.properties

