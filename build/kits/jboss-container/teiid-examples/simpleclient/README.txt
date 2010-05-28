JDBCClient.java shows making connections to Teiid in embedded mode through both a Driver
and a DataSource.

The program expects four arguments <host> <port> <vdb> <sql-command>.  There are helper run scripts 
that can be run as follows:

$./run.sh localhost 31000 portfolio "select * from CustomerAccount"

Note that the query is in quotes so that it is understood as a single argument.

See the other examples for deployable .vdb and .xml files to create vdbs.