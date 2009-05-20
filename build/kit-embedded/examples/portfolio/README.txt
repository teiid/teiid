Install a recent version of Derby - see http://db.apache.org/derby/derby_downloads.html

Create the example dataset in Derby by running <derby home>/bin/ij customer-schema.sql

Put the <derby home>/lib/derbyclient.jar to the <teiid embedded home>/extensions directory.

Run the demo with run.bat or run.sh.

This will execute the query contained in the run script against a vdb that integrates the
Derby datasource with file based data contained in the marketdata-price.txt file. 

