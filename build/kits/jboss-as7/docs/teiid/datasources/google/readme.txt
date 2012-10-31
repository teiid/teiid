1) Stop the server if it is running.
2) Overlay the "modules" directory on the "<jboss-as>/modules" directory 
3) Copy the Gdata jars (gdata-core-1.0.jar, gdata-spreadsheet-3.0.jar) to <jboss-as>/modules/com/google/gdata/main
4) Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "google.xml" file under the "resource-adapters" subsystem. Configure it according to the documentation.
5) restart  
