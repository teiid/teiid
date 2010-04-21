Follow the same derby setup instructions as the portfolio example.

Copy the followng files to the <jboss.home>/server/default/deploy directory.
	- portfolio-dynamic-vdb.xml
	- ../portfolio/derby-connector-ds.xml
	- ../portfolio/text-connector-ds.xml
	- ../portfolio/portfolio-ds.xml 

Start the JBoss Container

Use the simple client example run script i.e. 

$run.sh dynamicportfolio "select * from product, price where product.symbol=price.symbol"

That will execute the query against both Derby and the text file using the 
connector supplied metadata running in Teiid embedded mode. 

