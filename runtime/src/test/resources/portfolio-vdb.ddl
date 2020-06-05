
/*
###########################################
# START DATABASE Portfolio
###########################################
*/
CREATE DATABASE Portfolio VERSION '1' OPTIONS (ANNOTATION 'A Dynamic VDB', UseConnectorMetadata 'true');
USE DATABASE Portfolio VERSION '1';

--############ Translators ############

--############ Servers ############
CREATE SERVER "h2-connector" FOREIGN DATA WRAPPER h2 OPTIONS ("resource-name" 'java:/accounts-ds');

CREATE SERVER "text-connector" FOREIGN DATA WRAPPER file OPTIONS ("resource-name" 'java:/marketdata-file');


--############ Schemas ############
CREATE SCHEMA MarketData SERVER "text-connector";

CREATE SCHEMA Accounts SERVER "h2-connector" OPTIONS ("importer.useFullSchemaName" 'false');

CREATE VIRTUAL SCHEMA Stocks;


--############ Roles ############
CREATE ROLE ReadOnly WITH ANY AUTHENTICATED;

CREATE ROLE Prices WITH FOREIGN ROLE prices;

CREATE ROLE ReadWrite WITH FOREIGN ROLE superuser;


--############ Schema:MarketData ############
SET SCHEMA MarketData;

IMPORT FROM SERVER "text-connector" INTO MarketData;


--############ Schema:Accounts ############
SET SCHEMA Accounts;


        CREATE FOREIGN TABLE CUSTOMER
            (
               SSN varchar(10),
               FIRSTNAME varchar(64),
               LASTNAME varchar(64),
               ST_ADDRESS varchar(256),
               APT_NUMBER varchar(32),
               CITY varchar(64),
               STATE varchar(32),
               ZIPCODE varchar(10),
               PHONE varchar(15),
               CONSTRAINT CUSTOMER_PK PRIMARY KEY(SSN)
            );     
            CREATE FOREIGN TABLE ACCOUNT
            (
               ACCOUNT_ID integer,
               SSN varchar(10),
               STATUS varchar(10),
               "TYPE" char(1),
               DATEOPENED timestamp,
               DATECLOSED timestamp,
               CONSTRAINT ACCOUNT_PK PRIMARY KEY(ACCOUNT_ID)
            );
            CREATE FOREIGN TABLE  PRODUCT 
            (
               ID integer,
               SYMBOL varchar(16),
               COMPANY_NAME varchar(256),
               CONSTRAINT PRODUCT_PK PRIMARY KEY(ID)
            );
          

--############ Schema:Stocks ############
SET SCHEMA Stocks;


                
        CREATE VIEW StockPrices (
            symbol string,
            price bigdecimal
            )
            AS  
               SELECT SP.symbol, SP.price
                FROM (EXEC MarketData.getTextFiles('*.txt')) AS f, 
                    TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) AS SP;

        CREATE VIEW Stock (
            product_id integer,
            symbol string,
            price bigdecimal,
            company_name   varchar(256)
            )
            AS
                SELECT  A.ID, S.symbol, S.price, A.COMPANY_NAME
                    FROM StockPrices AS S, Accounts.PRODUCT AS A
                    WHERE S.symbol = A.SYMBOL;                 
         

--############ Grants ############
GRANT SELECT ON SCHEMA Accounts TO ReadOnly;
GRANT ON COLUMN "Accounts.Account.SSN" MASK 'null' TO ReadOnly;
CREATE POLICY "grant_policy_Accounts.Customer" ON Accounts.Customer FOR ALL TO ReadOnly USING (state <> 'New York');
GRANT ON COLUMN "Accounts.Customer.SSN" MASK 'null' TO ReadOnly;
GRANT SELECT ON SCHEMA MarketData TO ReadOnly;
GRANT SELECT ON SCHEMA Stocks TO ReadOnly;
GRANT ON COLUMN "Stocks.StockPrices.Price" MASK ORDER 1 'CASE WHEN hasRole(''Prices'') = true THEN Price END' TO ReadOnly;

REVOKE SELECT ON SCHEMA Accounts FROM Prices;

GRANT SELECT,INSERT,UPDATE,DELETE ON SCHEMA Accounts TO ReadWrite;
GRANT ON COLUMN "Accounts.Account.SSN" MASK ORDER 1 'SSN' TO ReadWrite;
CREATE POLICY "grant_policy_Accounts.Customer" ON Accounts.Customer FOR ALL TO ReadWrite USING (true);
GRANT ON COLUMN "Accounts.Customer.SSN" MASK ORDER 1 'SSN' TO ReadWrite;
GRANT SELECT,INSERT,UPDATE,DELETE ON SCHEMA MarketData TO ReadWrite;


/*
###########################################
# END DATABASE Portfolio
###########################################
*/

