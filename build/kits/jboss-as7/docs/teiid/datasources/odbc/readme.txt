For ODBC datasource, we use JDBC <--> ODBC bridge driver provided by Oracle that comes with Sun JDK. So, the driver 
is already installed and available.

1) First create DSN to the data source you are trying to connect, using ODBC Database Manager on Windows. 
On Linux systems edit your odbc.ini and add a DSN. It is assumed that you have already installed the odbc
driver for source you are trying to connect and connection verified.

2) edit "${jboss-as}/modules/sun/jdk/main/module.xml" add the following
<path name="sun/jdbc/odbc"/>

3) if you want to create DSN-less connection then use the following connection URL format in the odbc.xml file
<connection-url>jdbc:odbc:Driver={Microsoft Excel Driver (*.xls)};Dbq=c:\ODBC\ExcelData.xls</connection-url> 

4) Edit the standalone-teiid.xml or domain-teiid.xml file and add contents of the "odbc.xml" 
file under the "datasources" subsystem. You may have to edit contents according 
to where your odbc server is located and credentials you need to use to access it.