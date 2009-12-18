The datasources is the location the testing framework will look, by default, to find the datasources to use for testing.

Each datasource in use will have its own directory.   Within, that directory, will reside a connection.properties file that 
the process will look for.   The current directories contain an example properties file that can be used.   Rename the file
to connection.properties and update the properties.  

NOTE:  2 Datasource directories, each containing a connection.properties file, are required in order to run the integration tests.
		Each data source can be pointing to the same database instance, but the schemas must be different.
