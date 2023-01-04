To configure the hive as datasource, note that the default version of Hive driver is 0.11 

1) Stop the server if it is running.

2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

3) Then copy need hive jar files defined in the "<jboss-as>/modules/org/apache/hadoop/hive/main/modules.xml" file into  
"<jboss-as>/modules/org/apache/hadoop/hive/main" directory.

4) Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "hive.xml" or hive2.xml file under the "datasources" subsystem.
you may have edit these contents according to where your hadoop/hive server is located.

5) restart  

*****************************************
Note: 0.12 version of Hive Driver 
*****************************************
- In step (2) above, replace the "<jboss-as>/modules/system/layers/base/org/apache/hadoop/hive/main/modules.xml" file with "0.12/modules.xml" file
- then copy the 0.12 specific driver files in that directory