H2 driver is already pre-intalled in the JBoss AS, so deploy a data source

1) edit standalone-teiid.xml or domain-teiid.xml file, under "datasources" subsystem copy the contents of h2.xml file
and modify it according to your h2 server details.