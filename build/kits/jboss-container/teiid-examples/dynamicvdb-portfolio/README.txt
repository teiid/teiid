Follow the same derby setup instructions as the portfolio example.

Copy the following files to the <jboss.home>/server/default/deploy directory.
	- portfolio-dynamic-vdb.xml
	- ../portfolio/marketdata-file-ds.xml
	- ../portfolio/portfolio-ds.xml 
	- ../portfolio/marketdata-text-translator.xml

Start the JBoss Container

Use the simple client example run script i.e. 

$run.sh localhost 31000 dynamicportfolio "select * from product, price where product.symbol=price.symbol"

That will execute the query against both Derby and the text file using the 
connector supplied metadata. 

