This example demonstrates using the Cassandra Translator to access table in Cassandra. 

Set up Cassandra
================
1. Download and Install
   Download one of stable version from http://archive.apache.org/dist/cassandra/, Installing via unzip, for example
   		tar -xvf apache-cassandra-2.0.10-bin.tar.gz
2. create a keyspace and table
   Create 'demo' keyspace and 'users' table as Cassandra wiki page
   		http://wiki.apache.org/cassandra/GettingStarted
   		
Run Embedded Example
====================
Using 'run.sh' in Linux or 'run.bat' in Windows, the Cassandra table 'users' data will be queried via JDBC against a Teiid Virtual Database.


Note that, edit cassendra.properties make sure cassandra.address and cassandra.keybase point to a correct cassandra server

