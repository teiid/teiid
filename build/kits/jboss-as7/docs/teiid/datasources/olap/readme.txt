There are two types of connections described here. First is Mondrian, next is for XMLA. Typically you use XMLA 
with SAP BW, MS-SQL Anlaytics Cube etc. 

Mondrian: In this setup, Mondrian is deployed in the local VM. You can use Teiid as backing database, or any other database
depending upon which database you are using you may need to correct the "connection-url" and provide the correct dependency
on the JDBC driver module in Mondrian's "module.xml" file.
 
		1) Stop the server if it is running.

		2) Download the Mondrian from http://mondrian.pentaho.com/
		
		3) Unzip the mondiran.zip to a temp location.
		
		4) Follow Mondrian's installation instructions to install star schema in as defined in http://mondrian.pentaho.com/documentation/installation.php
		
		5) If you are using Teiid for defining the star schema, you will substitute above operation with equivalent for Teiid, 
		which is creating the VDB to be used with Mondrian
				 
		6) Overlay the "modules" directory from this directory on the "<jboss-as>/modules" directory, 
		
		7) Look at "<jboss-as>/modules/org/mondrian/main/module.xml", and copy all the dependent jars
		from "<mondrian-install>/lib/mondrian.war" file. You need to unzip this file and look in the "WEB-INF/lib" directory.
		Below is list of files you need to copy 
				
			commons-dbcp-1.2.1.jar
			commons-math-1.0.jar
			commons-pool-1.2.jar
			commons-vfs-1.0.jar
			eigenbase-properties.jar
			eigenbase-resgen.jar
			eigenbase-xom.jar
			javacup.jar
			mondrian.jar
					
		8) Now edit the "standalone-teiid.xml" file in the "datasources" section add datasource as defined in "mondrian.xml" file
		
		9) Now create VDB that uses this mondrian datasource you created above (sample is attached)
		
	   10) start the server
		
	   11) Deploy the VDB
	   
	   12) Access your VDB using JDBC/ODBC/ODATA etc. 



XMLA: This is most typical set-up, OLAP4j generic XML/A driver compatible with Pentaho Analysis (Mondrian), 
Microsoft SQL Server Analysis Services, Palo and SAP BW.

    1) Stop the server, if running
    
    2) Obtain the connection URL for your XMLA source.
    
    3) Edit "standalone-teiid.xml" and edit datasources subsystem, and create datasource as defined in "olap-xmla.xml" file.
    
    4) Create a VDB, that uses this datasource
     
    5) Start the JBoss AS server
    
    6) Deploy VDB
    
    7) Access your VDB using JDBC/ODBC/ODATA etc.