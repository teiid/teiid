CREATE FOREIGN TABLE account ("type" string, ACCOUNT_ID integer) OPTIONS (UPDATABLE true);

CREATE VIEW hello1 (
SchemaName varchar(255) PRIMARY KEY ,
Name varchar(255)
) OPTIONS(UPDATABLE TRUE) AS
SELECT SchemaName,Name from sys.tables;
    
CREATE TRIGGER ON hello1 INSTEAD OF UPDATE
   AS FOR EACH ROW
   	BEGIN ATOMIC
    DECLARE string setclause = null;
    DECLARE string whereClause = 'ACCOUNT_ID=19980002';
    IF (CHANGING.SchemaName)
    	BEGIN
     setclause = 'accounts.account.TYPE'||'='||'new.SchemaName';
    	END
    EXECUTE IMMEDIATE 'UPDATE accounts.account SET ' || setclause || ' WHERE '||whereClause;
   	END;
