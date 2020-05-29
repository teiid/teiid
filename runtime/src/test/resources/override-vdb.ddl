
/*
###########################################
# START DATABASE override
###########################################
*/
CREATE DATABASE override VERSION '1';
USE DATABASE override VERSION '1';

--############ Translators ############
CREATE FOREIGN DATA WRAPPER "mysql-override" TYPE mysql OPTIONS (RequiresCriteria 'true');


--############ Servers ############
CREATE SERVER s1 FOREIGN DATA WRAPPER "mysql-override" OPTIONS ("resource-name" 'java:/mysqlDS');


--############ Schemas ############
CREATE SCHEMA test SERVER s1;


--############ Schema:test ############
SET SCHEMA test;

IMPORT FROM SERVER s1 INTO test;


/*
###########################################
# END DATABASE override
###########################################
*/

