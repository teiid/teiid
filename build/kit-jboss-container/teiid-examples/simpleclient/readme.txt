JDBCClient.java shows making connections to Teiid in embedded mode through both a Driver
and a DataSource.

The program expects two arguments <vdb name> and <sql query>.  There are helper run scripts 
that can be run as follows:

$run.sh admin "select * from groups"

Note that the query is in quotes so that it is understood as a single argument.

See the other examples for deployable .vdb and .def files to create vdbs.