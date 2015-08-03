To configure the Cloudera's Impala as datasource 

1) Stop the server if it is running.

2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 

3) Then copy needed impala jar files defined in the "<jboss-as>/modules//org/apache/hadoop/impala/main/modules.xml" file into  
"<jboss-as>/modules/org/apache/hadoop/impala/main" directory. You download the jar files 
from https://downloads.cloudera.com/impala-jdbc/impala-jdbc-0.5-2.zip

note that incase you are using updated drivers, modify the versions of the files to correct versions in the module.xml file

4) Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "impala.xml" file under the "datasources" subsystem.
you may have edit these contents according to where your impala server is located.

5) start the server