Dynamicvdb-portfolio demonstrates how to federate data from a relational data source with a
text file-based data source.  This example uses the HSQL database which is referenced as 
the PortfolioDS data source in the server, but the creation SQL could be adapted to another 
database if you choose.


Data Source(s) setup:

-	Start the server (if not already started)
-	go to the JMX console (http://localhost:8080/jmx-console/) and select:   database=localDB,service=Hypersonic 
to present bean options.  Now invoke "startDatabaseManager" to bring up HSQL Database Manager.
-	Use the File/Open Script menu option to load the teiid-examples/dynamicvdb-portfolio/customer-schema.sql script and and then click Execute SQL to create the required tables and insert the example data.


Teiid Deployment:


Copy the following files to the <jboss.home>/server/default/deploy directory.

	(1) portfolio-vdb.xml
	(2) marketdata-file-ds.xml
	
	
Query Demonstrations:

==== Using the simpleclient example ====

1) Change your working directory to teiid-examples/simpleclient

2) Use the simpleclient example run script, using the following format

$./run.sh localhost 31000 dynamicportfolio "example query" 


example queries:

1	select * from product
2	select stock.* from (call MarketData.getTextFiles('*.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock
3.	select product.symbol, stock.price, company_name from product, (call MarketData.getTextFiles('*.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock where product.symbol=stock.symbol

Example 1 queries the relational source

Example 2 queries the text file-based
 source

Example 3 queries both the relational and the text file-based sources.  The files returned from the getTextFiles procedure are 
passed to the TEXTTABLE table function (via the nested table correlated reference f.file).  The TEXTTABLE function expects a 
text file with a HEADER containing entries for at least symbol and price columns. 


