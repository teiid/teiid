Follow the same derby setup instructions as the portfolio example.

Copy the following files to the <jboss.home>/server/default/deploy directory.
	- portfolio-vdb.xml
	- ../portfolio/marketdata-file-ds.xml
	- ../portfolio/portfolio-ds.xml 

Start the JBoss Container

Use the simple client example run script e.g. 

$./run.sh localhost 31000 dynamicportfolio "select * from product, (call MarketData.getTextFiles('*.txt')) f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) price where product.symbol=price.symbol"

That will execute the query against both Derby and the text file using the 
connector supplied metadata. 

