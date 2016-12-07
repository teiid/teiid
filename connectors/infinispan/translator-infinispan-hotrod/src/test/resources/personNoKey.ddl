CREATE FOREIGN TABLE Person (
	name string NOT NULL OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	email string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String')
);
	