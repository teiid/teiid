To configure the hive as datasource, 

1) Stop the server if it is running.

2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

3) Then copy need hive jar files defined in the "<jboss-as>/modules/org/apache/hadoop/hive/main/modules.xml" file into  
"<jboss-as>/modules/org/apache/hadoop/hive/main" directory.

4) Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "hive.xml" file under the "datasources" subsystem.
you may have edit these contents according to where your hadoop/hive server is located.

5) restart  