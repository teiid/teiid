This example demonstrates using the HBase Translator with Phoenix Data Source to access data in HBase, this example also tries to replicate the functionality of the "teiid's quick start example" from 

	https://github.com/teiid/teiid-quickstarts/tree/master/hbase-as-a-datasource

Take look at the 'Setup HBase' section of above link, we assume Phoenix be installed on all HBase region server and sample date be insert into HBase 'Customer' table.

Copy phoenix-[version]-client.jar(can be download from http://phoenix.apache.org/download.html) to .../optional/hbase folder.

Run 'run.sh' in Linux and 'run.bat' in Windows to start embedded example.

Note that the directions from above link may be very specific to using a JBoss Server, however with this example the details about the JBoss Server will be omitted. 

