This directory contains examples of data source configuration files for the following types of sources:
    flat files
    LDAP
    SalesForce
    Web Services (for ws-security see Admin Guide)
    ODBC
    OLAP
    Mondrian
    Ingres
    Intersystems Cache
    Teiid JDBC template

JDBC Users: Please see the examples in the "<jboss-as>/docs/examples/jca" directory for creating data source configuration files for any type of relational database. These examples demonstrate creating both "local" and "xa" data sources.


---------

When server applications wants to obtain their Teiid jdbc connection from the server connection pool, then Teiid needs to be deployed as a datasource.   

We have provided a datasource -ds.xml template file to assist in the Teiid datasource deployment.   

To deploy, using the template, do the following:

1.	make a copy of the teiid-jdbc-template-ds.xml file
2.	edit the file and replace "(vdb)" with the name of your vdb that you will be accessing, updating the user and password accordingly.
3.	then copy the new -ds.xml file to the deploy directory of the profile you will be using.

Your application should be able to obtain a jdbc connection from the connection pool using the JNDI name defined in the -ds.xml file.  
By default, it called "TeiidDS".     If you have changed it, then your application will need to refer to it by the new name.

