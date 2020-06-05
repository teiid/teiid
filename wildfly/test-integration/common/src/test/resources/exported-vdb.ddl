
/*
###########################################
# START DATABASE foo
###########################################
*/
CREATE DATABASE foo VERSION '1' OPTIONS ("deployment-name" 'foo-vdb.ddl');
USE DATABASE foo VERSION '1';

--############ Translators ############

--############ Servers ############
CREATE SERVER NONE FOREIGN DATA WRAPPER loopback OPTIONS ("resource-name" 'NONE');


--############ Schemas ############
CREATE SCHEMA test SERVER NONE;


--############ Schema:test ############
SET SCHEMA test;

CREATE FOREIGN TABLE G1 (
	e1 integer,
	e2 string(25),
	e3 double,
	PRIMARY KEY(e1)
);

CREATE FOREIGN TABLE G2 (
	e1 integer,
	e2 string(25),
	e3 double,
	PRIMARY KEY(e1)
);
/*
###########################################
# END DATABASE foo
###########################################
*/

