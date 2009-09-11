the datasources directory is the location to define each database schema that can be used during the running of the tests
a datasource here is defined as a unique connection to a specific schema

To setup a datasource, do the following:

1.	open datasource_mapping.xml and find the datasource you would like to setup.  
	Use the name of the directory ("dir") as the name of the folder to create for the datasource.
1.	create the directory (if it doesn't exist)
2.	create (or place) a connection.properties file in the newly created directory.  See the 
	example_connection.properties in the derby directory as a starting point.
	