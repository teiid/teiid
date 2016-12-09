PrestoDB driver is already pre-intalled in the JBoss AS, so deploy a data soruce

1) edit standalone-teiid.xml or domain-teiid.xml file, under "datasources" subsystem copy the contents of prestodb.xml file
and modify it according to your prestodb server details.