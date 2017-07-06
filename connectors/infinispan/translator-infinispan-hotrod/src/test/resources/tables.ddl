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