SET NAMESPACE 'http://www.teiid.org/translator/hbase/2014' AS teiid_hbase;
            
CREATE FOREIGN TABLE Customer (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    city string OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'customer'),
    name string OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'customer'),
    amount string OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'sales'),
    product string OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'sales'),
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE TypesTest (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    column1 varchar OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column2 varbinary OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column3 char OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column4 boolean OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column5 byte OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column6 tinyint OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column7 short OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column8 smallint OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column9 integer OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column10 serial OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column11 long OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column12 bigint OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column13 float OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column14 real OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column15 double OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column16 bigdecimal OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column17 decimal OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column18 date OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column19 time OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column20 timestamp OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE TimesTest (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    column1 date OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column2 time OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    column3 timestamp OPTIONS ("teiid_hbase:COLUMN_FAMILY" 'f'),
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE VIRTUAL PROCEDURE extractData(IN param String)
AS
BEGIN
	SELECT * FROM Customer WHERE PK > param;
END