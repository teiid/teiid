This example is continuation from the previous portfolio example. Make sure that you have the working example before using this example.

In this example, the vdb is defined with two different data access rules. 

1) read-only - this restricts access of vdb to only read i.e selects. This role is given to everybody who has a login 
credetials (use the user called "user" to login with password "user")

2) read-write access - this role gives read access, and also adds write access. i.e. inserts. This access is given only
to users with "superuser" JAAS role. (use user called "portfolio" to login with password "portfolio")

See the portfolio-vdb.xml for extra xml elements defined for define the above roles. For more information check out
Reference Guide's Data Roles chapter.

To deploy the VDB, follow same steps as before in the previous example.

To define the new users and their roles to be used with this example,copy both the teiid-security-user.properties, 
teiid-security-roles.properties into "<jboss-as>/server/<profile>/conf/props" directory. Server restart is required after this 
operation.


Query Demonstrations:

==== Using the simpleclient example ====

1) Change your working directory to teiid-examples/simpleclient

2) Use the simpleclient example run script, using the following format

$./run.sh localhost 31000 dynamicportfolio "example query" 


example queries:

1)	"select * from product" - this should execute correctly

2)	"insert into product (id, symbol,company_name) values (2000,'RHT','Red Hat')" - this will fail with data access error saying 
    that the user named "user" is not allowed write access.
    
Since this simpleclient example hard coded the default user and password, modify the included JDBCClient.java class 
to take the user name and password from command line and re-execute the query (2) with user name "portfolio" 
and password "portfolio" and see it executes to success! 

 