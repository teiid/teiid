CREATE FOREIGN TABLE G1 (
	e1 integer NOT NULL,
	e2 string NOT NULL INDEX,
	e3 float ,
	e4 string[],
	e5 string[],
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE G2 (
	e1 integer not null,
	e2 string,
	e5 varbinary,
	e6 long,
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE G4 (
	e1 integer NOT NULL,
	e2 string NOT NULL,
	e3 double,
	e4 float,
	e5 short,
	e6 byte,
	e7 char(1),
	e8 long,
	e9 bigdecimal,
	e10 biginteger,
	e11 time,
	e12 timestamp,
	e13 date,
	e14 object,
	e15 blob,
	e16 clob,
	e17 xml,
	e18 geometry,
	CONSTRAINT FK_G2 FOREIGN KEY(e1) REFERENCES G2 (e1)
);