If you installed Teiid into JBoss AS then the driver is already installed, you only need to create a datasource

Creating the datasource 

  Option 1: 
	
	Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "teiid.xml" 
	or "teiid-xa.xml" or "teiid-local.xml" file under the "datasources" subsystem. You may have to edit contents according 
	to where your Teiid server is located and credentials you need to use to access it.
	
  Option 2: 
	
	Take a look at create-teiid-ds.cli script, and modify and execute using JBoss CLI tool as below 
	
	./Jboss-admin.sh --file create-teiid-ds.cli
	
