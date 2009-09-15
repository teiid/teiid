the datasources directory is the location to define each database schema that can be used during the running of the tests
a datasource here is defined as a unique connection to a specific schema

To setup a datasource, do the following:

1.	open datasource_mapping.xml and find the datasource you would like to setup.  
	Use the name of the directory ("dir") as the name of the folder to create for the datasource.
1.	create the directory (if it doesn't exist)
2.	create (or place) a connection.properties file in the newly created directory.  See the 
	example_connection.properties in the derby directory as a starting point.


NOTE:  The datasource_mapping.xml has groupings by datasource type.   This is also a mechinism for creating groupings 
for special usecases.   An example would be to do the following:

-  add 2 different groups to datasource_mapping.xml 

	<datasourcetype name="TestIT">
		<datasource  name="mysqlmyA">
			<property name="dir">mysql</property>
			<property name="connectortype">MySQL JDBC XA Connector</property>		
		</datasource>

		<datasource  name="oraclemyB">
			<property name="dir">oracle</property>
			<property name="connectortype">Oracle JDBC XA Connector</property>		
		</datasource>
	</datasourcetype>
		

-  then, in the config properties file, map the models to each datasourcetype

pm1=mysqlmyA
pm2=oraclemyB

This force the association between the model and datasource.  