SET NAMESPACE 'http://www.teiid.org/translator/hbase/2014' AS teiid_hbase;
            
CREATE FOREIGN TABLE Customer (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    city string,
    name string,
    amount string,
    product string,
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE TypesTest (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    column1 varchar,
    column2 varbinary,
    column3 char,
    column4 boolean,
    column5 byte,
    column6 tinyint,
    column7 short,
    column8 smallint,
    column9 integer,
    column10 serial,
    column11 long,
    column12 bigint,
    column13 float,
    column14 real,
    column15 double,
    column16 bigdecimal,
    column17 decimal,
    column18 date,
    column19 time,
    column20 timestamp,
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE TimesTest (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    column1 date,
    column2 time,
    column3 timestamp,
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');
