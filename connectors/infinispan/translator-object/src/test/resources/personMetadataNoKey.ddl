 CREATE FOREIGN TABLE Person (
	email string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	id integer OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),
	name string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String')
);
