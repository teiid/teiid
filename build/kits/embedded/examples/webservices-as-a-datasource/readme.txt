This example demonstrates using the WS Translator to call a REST web services and transform the web service results into relational results, this example also tries to replicate the functionality of the "teiid's quick start example" from 

 	https://github.com/teiid/teiid-quickstarts/tree/master/webservices-as-a-datasource
 	
Take look at the above link, deploy 'CustomerRESTWebSvc.war' to JBoss Server(EAP 6.x),  This war contains customer information, so that no external web service is required in order to demonstrate the WS feature.

'CustomerRESTWebSvc.war'
========================
1. Build
   Go into the customer folder, execute Maven commands like below
	cd customer
	mvn clean install
this will generate deployment war 'CustomerRESTWebSvc.war'.

2. Deploy
   Deploy 'CustomerRESTWebSvc.war' to a running JBoss server(Assume JBoss EAP 6 run on localhost).
   
3. Consume
   The below url will list all customers
   		http://localhost:8080/CustomerRESTWebSvc/MyRESTApplication/customerList 
