Follow the same derby setup instructions as the portfolio example.

Copy the vdbless.def file to the <teiid home>/deploy directory. 

Use the simple client example run script i.e. 

$run.sh vdblessportfolio "select * from product, price where product.symbol=price.symbol"

That will execute the query against both Derby and the text file using the 
vdbless connector supplied metadata running in Teiid embedded mode. 

