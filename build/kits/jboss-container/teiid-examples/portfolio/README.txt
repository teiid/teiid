Install a recent version of Derby - see http://db.apache.org/derby/derby_downloads.html

Create the example dataset in Derby by running <derby home>/bin/ij customer-schema.sql

Put the <derby home>/lib/derbyclient.jar to the "<jboss home>/server/default/lib" directory.

Copy the PortfolioModel/Portolio.vdb file to the "<jboss home>/server/default/deploy" directory.

Copy the Translators/ConnectionFactories for the example to the "<jboss home>/server/default/deploy" directory.
	- marketdata-file-ds.xml
	- portfolio-ds.xml 
 
Start the JBoss Container

Use the simple client example run script i.e. 

$run.sh localhost 31000 portfolio "select * from CustomerAccount"

That will execute the query against the CustomerAccount view in the Portfolio VDB. 

