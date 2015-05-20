
This example demonstrates using the MongoDB Translator to access data in mongodb.

This example also tries to replicate the functionality of the "teiid's quick start example" from 

https://github.com/teiid/teiid-quickstarts/tree/master/mongodb-as-a-datasource

Take look at the Setup mongoDB section of above link, we assume the employee document be insert under Employee connection as below:
	db.Employee.insert({employee_id: '1', FirstName: 'Teiid', LastName: 'JBoss'});
	db.Employee.insert({employee_id: '2', FirstName: 'Teiid', LastName: 'JBoss'});

Note that:
	1. the directions from above link may be very specific to using a JBoss Server, however with this example the details about the JBoss Server will be omitted. 
	2. edit mongodb.properties make sure server.list and db.name point to a correct mongo server

