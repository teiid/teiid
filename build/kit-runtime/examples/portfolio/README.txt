Install a recent version of Derby - see http://db.apache.org/derby/derby_downloads.html

Create the example dataset in Derby by running <derby home>/bin/ij customer-schema.sql

Put the <derby home>/lib/derbyclient.jar to the <teiid home>/extensions directory.

Copy the PortfolioModel/Portolio.vdb file to the <teiid home>/deploy directory.

Use the simple client example run script i.e. 

$run.sh portfolio "select * from CustomerAccount"

That will execute the query against the CustomerAccount view in the Portfolio VDB running in Teiid embedded mode. 

