To install the google JCA connector do the following:

1) Stop the server if it is running.
2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 
3) Copy the Gdata jars (gdata-core-1.0.jar, gdata-spreadsheet-3.0.jar) to <jboss-as>/modules/com/google/gdata/main

4) Then to complete the installation, choose 1 of the 2 following ways to create the datasource:

a.  edit the "standalone-teiid.xml" file or "doamin-teiid.xml" file and add 
the contents of the google.xml under "resource-adapters" subsystem section.

b.  edit the create-google-ds.properties to set accordingly, and then  
run the following CLI script in the JBOSS_HOME/bin/jboss-cli.sh -c --file=create-google-ds.cli   --properties=create-google-ds.properties


5) restart  
