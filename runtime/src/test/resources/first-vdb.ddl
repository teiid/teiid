
/*
###########################################
# START DATABASE empty
###########################################
*/
CREATE DATABASE empty VERSION '2';
USE DATABASE empty VERSION '2';

--############ Translators ############

--############ Servers ############
CREATE SERVER z FOREIGN DATA WRAPPER y OPTIONS ("resource-name" 'z');


--############ Schemas ############
CREATE SCHEMA PM1 SERVER z;


--############ Roles ############
CREATE ROLE admin WITH FOREIGN ROLE superuser;


--############ Schema:PM1 ############
SET SCHEMA PM1;

CREATE FOREIGN TABLE "my-table" (
	"my-column" string
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE mytable (
	"my-column" string
) OPTIONS (UPDATABLE TRUE);
--############ Grants ############
GRANT SELECT,UPDATE ON TABLE "PM1.mytable" TO admin;


/*
###########################################
# END DATABASE empty
###########################################
*/

