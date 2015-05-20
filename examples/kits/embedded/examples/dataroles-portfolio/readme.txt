

This example demonstrates how to use data roles to control access to data. This includes read-only and read-write access as well as the use of row-based filters and column masking.

In this example, the vdb is defined with three different data access rules. 

1) ReadOnly - this restricts access of vdb to only read i.e selects. This role is given to everybody who has a login credetial (use the user called "user" to login with password "user").
 Furthermore there are restrictions as to what customers and columns can be read.
 
 2) ReadWrite access - this role gives read access, and also adds write access. This access is given only to users with the "superuser" JAAS role. 
 (use user called "portfolio" to login with password "portfolio")
 
 3) Prices access - this role is used to give access to price listings. Its purpose is to demonstrate the use of a generic role (empty) for controlling access to information.

See the portfolio-vdb.xml for extra xml elements defined for define the above roles. For more information check out Reference Guide's Data Roles chapter.