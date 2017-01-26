
/*
###########################################
# START DATABASE Portfolio
###########################################
*/
CREATE DATABASE Portfolio VERSION '1' OPTIONS (ANNOTATION 'A Dynamic VDB', "connection-type" 'BY_VERSION', UseConnectorMetadata 'true');
USE DATABASE Portfolio VERSION '1';

--############ Translators ############
CREATE FOREIGN DATA WRAPPER file;

CREATE FOREIGN DATA WRAPPER h2;


--############ Servers ############
CREATE SERVER "h2-connector" TYPE 'NONE' FOREIGN DATA WRAPPER h2 OPTIONS ("jndi-name" 'java:/accounts-ds');

CREATE SERVER "text-connector" TYPE 'NONE' FOREIGN DATA WRAPPER file OPTIONS ("jndi-name" 'java:/marketdata-file');


--############ Schema:MarketData ############
CREATE  SCHEMA MarketData SERVER "text-connector";
SET SCHEMA MarketData;

CREATE FOREIGN PROCEDURE deleteFile(IN filePath string)
OPTIONS (ANNOTATION 'Delete the given file path. ');

CREATE FOREIGN PROCEDURE getFiles(IN pathAndPattern string OPTIONS (ANNOTATION 'The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path.')) RETURNS TABLE (file blob, filePath string)
OPTIONS (ANNOTATION 'Returns files that match the given path and pattern as BLOBs');

CREATE FOREIGN PROCEDURE getTextFiles(IN pathAndPattern string OPTIONS (ANNOTATION 'The path and pattern of what files to return.  Currently the only pattern supported is *.<ext>, which returns only the files matching the given extension at the given path.')) RETURNS TABLE (file clob, filePath string)
OPTIONS (ANNOTATION 'Returns text files that match the given path and pattern as CLOBs');

CREATE FOREIGN PROCEDURE saveFile(IN filePath string, IN file object OPTIONS (ANNOTATION 'The contents to save.  Can be one of CLOB, BLOB, or XML'))
OPTIONS (ANNOTATION 'Saves the given value to the given path.  Any existing file will be overriden.');
--############ Schema:Accounts ############
CREATE  SCHEMA Accounts SERVER "h2-connector" none OPTIONS ("importer.useFullSchemaName" 'false');
SET SCHEMA Accounts;

CREATE FOREIGN TABLE ACCOUNT (
	ACCOUNT_ID integer,
	SSN char(10),
	STATUS char(10),
	TYPE char(10),
	DATEOPENED timestamp,
	DATECLOSED timestamp,
	CONSTRAINT ACCOUNT_PK PRIMARY KEY(ACCOUNT_ID)
);

CREATE FOREIGN TABLE CUSTOMER (
	SSN char(10),
	FIRSTNAME string(64),
	LASTNAME string(64),
	ST_ADDRESS string(256),
	APT_NUMBER string(32),
	CITY string(64),
	STATE string(32),
	ZIPCODE string(10),
	PHONE string(15),
	CONSTRAINT CUSTOMER_PK PRIMARY KEY(SSN)
);

CREATE FOREIGN TABLE PRODUCT (
	ID integer,
	SYMBOL string(16),
	COMPANY_NAME string(256),
	CONSTRAINT PRODUCT_PK PRIMARY KEY(ID)
);
--############ Schema:Stocks ############
CREATE VIRTUAL SCHEMA Stocks;
SET SCHEMA Stocks;

CREATE VIEW Stock (
	product_id integer,
	symbol string,
	price bigdecimal,
	company_name string(256)
)
AS
SELECT A.ID, S.symbol, S.price, A.COMPANY_NAME FROM StockPrices AS S, Accounts.PRODUCT AS A WHERE S.symbol = A.SYMBOL;

CREATE VIEW StockPrices (
	symbol string,
	price bigdecimal
)
AS
SELECT SP.symbol, SP.price FROM (EXEC MarketData.getTextFiles('*.txt')) AS f, TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) AS SP;
--############ Roles & Grants ############
CREATE ROLE ReadOnly WITH ANY AUTHENTICATED;
CREATE ROLE Prices WITH JAAS ROLE prices;
CREATE ROLE ReadWrite WITH JAAS ROLE superuser;
GRANT SELECT ON SCHEMA Accounts TO ReadOnly;
GRANT ON COLUMN "Accounts.Account.SSN" MASK '"null"' TO ReadOnly;
GRANT ON TABLE "Accounts.Customer" TO ReadOnly;
GRANT ON COLUMN "Accounts.Customer.SSN" MASK '"null"' TO ReadOnly;
GRANT SELECT ON SCHEMA MarketData TO ReadOnly;
GRANT SELECT ON SCHEMA Stocks TO ReadOnly;
GRANT ON COLUMN "Stocks.StockPrices.Price" MASK ORDER 1 '"CASE WHEN hasRole('Prices') = true THEN Price END"' TO ReadOnly;

GRANT SELECT,INSERT,UPDATE,DELETE ON SCHEMA Accounts TO ReadWrite;
GRANT ON COLUMN "Accounts.Account.SSN" MASK ORDER 1 'SSN' TO ReadWrite;
GRANT ON TABLE "Accounts.Customer" TO ReadWrite;
GRANT ON COLUMN "Accounts.Customer.SSN" MASK ORDER 1 'SSN' TO ReadWrite;
GRANT SELECT,INSERT,UPDATE,DELETE ON SCHEMA MarketData TO ReadWrite;


/*
###########################################
# END DATABASE Portfolio
###########################################
*/

