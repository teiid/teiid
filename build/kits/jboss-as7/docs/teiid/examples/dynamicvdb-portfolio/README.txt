Dynamicvdb-portfolio demonstrates how to federate data from a relational data source with a
text file-based data source.  This example uses the H2 database which is referenced as 
the "accouts-ds" data source in the server, but the creation SQL could be adapted to another 
database if you choose.


Data Source(s) setup:

- Edit the contents of "standalone-teiid.xml" file, and add contents of following files to create H2, CSV data sources

	(1) portfolio-ds.xml.xml - under "datasources" subsystem element
	(2) marketdata-file-ds.xml - under "resource-adapter" subsystem
	
Teiid Deployment:

Copy the following files to the "<jboss.home>/standalone/deployments" directory

     (1) portfolio-vdb.xml
     (2) portfolio-vdb.xml.dodeploy


make sure the VDB is deloyed in the AS7 console.	
	
Query Demonstrations:

==== Using the simpleclient example ====

1) Change your working directory to "<jboss-install>/docs/teiid/examples/simpleclient"

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


