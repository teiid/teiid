JDBCClient.java shows making connections to Teiid in through both a Driver
and a DataSource.

The program expects four arguments <host> <port> <vdb> <sql-command>.  There
are helper run scripts that can be run as follows:

$./run.sh host port vdb "query"

Note that the query is in quotes so that it is understood as a single argument.

e.g. With the portfolio vdb deployed:

$./run.sh localhost 31000 dynamicportfolio "select * from tables"

See the other examples for deployable .vdb and .xml files to create vdbs.

NOTE: To run more advanced queries, it would be better a fully featured Java client, 
such as SQuirreL [http://www.squirrelsql.org/].