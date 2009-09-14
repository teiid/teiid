Follow the same derby setup instructions as the portfolio example.

Copy the dynamic.def file to the <teiid home>/deploy directory. 

Use the simple client example run script i.e. 

$run.sh dynamicportfolio "select * from product, price where product.symbol=price.symbol"

That will execute the query against both Derby and the text file using the 
connector supplied metadata running in Teiid embedded mode. 

