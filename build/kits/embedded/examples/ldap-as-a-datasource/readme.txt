This example demonstrates using the ldap Translator to access data in OpenLDAP Server., this example also tries to replicate the functionality of the "teiid's quick start example" from 

https://github.com/teiid/teiid-quickstarts/tree/master/ldap-as-a-datasource

Take look at the 'Setup LDAP Group and User's section of above link, we assume Group 'HR' be created, and 3 users('hr1', 'hr2', 'hr3') under it.

Note that:
	1. the directions from above link may be very specific to using a JBoss Server, however with this example the details about the JBoss Server will be omitted. 
	2. edit ldap.properties make sure ldap.url, ldap.adminUserDN and ldap.adminUserPassword point to a correct LDAP Server

