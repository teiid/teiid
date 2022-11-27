CREATE FOREIGN TABLE Customer (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    city string OPTIONS (nameinsource '"city"'),
    name string OPTIONS (nameinsource '"name"'),
    amount string OPTIONS (nameinsource '"amount"'),
    product string OPTIONS (nameinsource '"product"'),
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS(nameinsource '"Customer"', "UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE TypesTest (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    q1 varchar,
    q2 varbinary,
    q3 char,
    q4 boolean,
    q5 byte,
    q6 tinyint,
    q7 short,
    q8 smallint,
    q9 integer,
    q10 serial,
    q11 long,
    q12 bigint,
    q13 float,
    q14 real,
    q15 double,
    q16 bigdecimal,
    q17 decimal,
    q18 date,
    q19 time,
    q20 timestamp,
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE TimesTest (
    PK string OPTIONS (nameinsource 'ROW_ID'),
    column1 date,
    column2 time,
    column3 timestamp,
    CONSTRAINT PK0 PRIMARY KEY(PK)
) OPTIONS("UPDATABLE" 'TRUE');

CREATE FOREIGN TABLE smalla (
	IntKey integer OPTIONS (nameinsource 'intkey'), 
	StringKey string OPTIONS (nameinsource 'stringkey')
) OPTIONS (CARDINALITY 50, UPDATABLE 'TRUE', nameinsource 'smalla');
