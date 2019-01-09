To configure the PrestoDB as datasource:

1) Stop the server if it is running.

2) Overlay the "modules" directory on the "$JBOSS_HOOME/modules" directory.

3) Then copy needed presto driver jar file defined in the "$JBOSS_HOOME/modules/com/facebook/presto/main/module.xml" file into "$JBOSS_HOOME/modules/com/facebook/presto/main" directory. You download the driver jar from https://prestodb.io/

4) Edit standalone-teiid.xml or domain-teiid.xml file, under "datasources" subsystem copy the contents of prestodb.xml file and modify it according to your prestodb server details.
