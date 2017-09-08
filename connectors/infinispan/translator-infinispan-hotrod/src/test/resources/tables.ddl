SET NAMESPACE 'http://www.teiid.org/translator/infinispan/2017' AS teiid_ispn;

CREATE FOREIGN TABLE G1 (
	e1 integer NOT NULL OPTIONS (ANNOTATION '@Id\u000A@IndexedField(index=true, store=false)', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:TAG" '1'),
	e2 string NOT NULL OPTIONS (ANNOTATION '@IndexedField', SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '2'),
	e3 float OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'float', "teiid_ispn:TAG" '3'),
	e4 string[] OPTIONS (ANNOTATION '@IndexedField(index=true, store=false)', SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '4'),
	e5 string[] OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '5'),
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G1', UPDATABLE TRUE, "teiid_ispn:CACHE" 'default');

CREATE FOREIGN TABLE G2 (
	e1 integer NOT NULL OPTIONS (ANNOTATION '@Id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:TAG" '1'),
	e2 string NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '2'),
	g3_e1 integer NOT NULL OPTIONS (NAMEINSOURCE 'e1', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:MESSAGE_NAME" 'pm1.G3', "teiid_ispn:PARENT_COLUMN_NAME" 'g3', "teiid_ispn:PARENT_TAG" '5', "teiid_ispn:TAG" '1'),
	g3_e2 string NOT NULL OPTIONS (NAMEINSOURCE 'e2', SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:MESSAGE_NAME" 'pm1.G3', "teiid_ispn:PARENT_COLUMN_NAME" 'g3', "teiid_ispn:PARENT_TAG" '5', "teiid_ispn:TAG" '2'),
	e5 varbinary OPTIONS (ANNOTATION '@IndexedField(index=false)', NATIVE_TYPE 'bytes', "teiid_ispn:TAG" '7'),
	e6 long OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'fixed64', "teiid_ispn:TAG" '8'),
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G2', UPDATABLE TRUE, "teiid_ispn:CACHE" 'default');

CREATE FOREIGN TABLE G4 (
	e1 integer NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:TAG" '1'),
	e2 string NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '2'),
	G2_e1 integer OPTIONS (NAMEINSOURCE 'e1', SEARCHABLE 'Searchable', "teiid_ispn:PSEUDO" 'g4'),
	CONSTRAINT FK_G2 FOREIGN KEY(G2_e1) REFERENCES G2 (e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G4', UPDATABLE TRUE, "teiid_ispn:CACHE" 'default', "teiid_ispn:MERGE" 'model.G2', "teiid_ispn:PARENT_COLUMN_NAME" 'g4', "teiid_ispn:PARENT_TAG" '6');

CREATE FOREIGN TABLE G5 (
	e1 integer NOT NULL OPTIONS (ANNOTATION '@Id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:TAG" '1'),
	e2 string NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '2'),
	e3 double OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'double', "teiid_ispn:TAG" '3'),
	e4 float OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'float', "teiid_ispn:TAG" '4'),
	e5 short OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:TAG" '5'),
	e6 byte OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int32', "teiid_ispn:TAG" '6'),
	e7 char OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '7'),
	e8 long OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int64', "teiid_ispn:TAG" '8'),
	e9 bigdecimal OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '9'),
	e10 biginteger OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'string', "teiid_ispn:TAG" '10'),
	e11 time OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int64', "teiid_ispn:TAG" '11'),
	e12 timestamp OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int64', "teiid_ispn:TAG" '12'),
	e13 date OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int64', "teiid_ispn:TAG" '13'),
	e14 object OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'bytes', "teiid_ispn:TAG" '14'),
	e15 blob OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'bytes', "teiid_ispn:TAG" '15'),
	e16 clob OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'bytes', "teiid_ispn:TAG" '16'),
	e17 xml OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'bytes', "teiid_ispn:TAG" '17'),
	e18 geometry OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'bytes', "teiid_ispn:TAG" '18'),
	CONSTRAINT PK_E1 PRIMARY KEY(e1)
) OPTIONS (ANNOTATION '@Indexed', NAMEINSOURCE 'pm1.G5', UPDATABLE TRUE, "teiid_ispn:CACHE" 'default');