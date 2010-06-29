Demonstrates how to use the File Translator to integrate text resources with
a database.  This example assumes the use of Derby, but the creation SQL could be 
adapted to another database if you choose.

Install a recent version of Derby - see http://db.apache.org/derby/derby_downloads.html

Create the example dataset in Derby by running <derby home>/bin/ij customer-schema.sql

Put the <derby home>/lib/derbyclient.jar to the "<jboss home>/server/default/lib" directory.

Copy the following files to the <jboss.home>/server/default/deploy directory.
	- portfolio-vdb.xml
	- marketdata-file-ds.xml
	- portfolio-ds.xml 

Start the JBoss Container

Use the simple client example run script, or another client, to execute a query, e.g. 

$./run.sh localhost 31000 dynamicportfolio "select * from product, (call MarketData.getTextFiles('*.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) price where product.symbol=price.symbol"

This example will execute the query against both Derby and the text files.  The 
files returned from the getTextFiles procedure are passed to the TEXTTABLE table
function (via the nested table correlated reference f.file).  The TEXTTABLE 
function expects a text file with a HEADER containing entries for at least symbol
and price columns. 

