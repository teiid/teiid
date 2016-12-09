SET NAMESPACE 'http://www.teiid.org/translator/object/2016' AS n0;

CREATE FOREIGN TABLE Person (
	name string NOT NULL OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	email string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	CONSTRAINT PK_ID PRIMARY KEY(id)
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE ST_Person (
	name string NOT NULL OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	email string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	CONSTRAINT PK_ID PRIMARY KEY(id)
) OPTIONS (UPDATABLE TRUE, "n0:primary_table" 'objectvdb.Person');
