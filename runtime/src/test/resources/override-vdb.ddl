
/*
###########################################
# START DATABASE override
###########################################
*/
CREATE DATABASE override VERSION '1' OPTIONS ("connection-type" 'BY_VERSION');
USE DATABASE override VERSION '1';

--############ Translators ############
CREATE FOREIGN DATA WRAPPER mysql;

CREATE FOREIGN DATA WRAPPER "mysql-override" OPTIONS (RequiresCriteria 'true');


--############ Servers ############
CREATE SERVER s1 FOREIGN DATA WRAPPER "mysql-override" OPTIONS ("jndi-name" 'java:/mysqlDS');


--############ Schema:test ############
CREATE SCHEMA test SERVER s1;
SET SCHEMA test;

IMPORT FOREIGN SCHEMA "%" FROM SERVER s1 INTO test OPTIONS (
);

/*
###########################################
# END DATABASE override
###########################################
*/

