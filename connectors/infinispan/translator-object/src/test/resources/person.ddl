 CREATE FOREIGN TABLE Person (
		id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
		email string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
		name string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
		CONSTRAINT PK_ID PRIMARY KEY(id)
 ) OPTIONS (NAMEINSOURCE '"PersonPojo"', UPDATABLE TRUE);	
 
 CREATE FOREIGN TABLE PhoneNumber (
	number string OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	type string OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Enum'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	CONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'phones')
) OPTIONS (NAMEINSOURCE '"PhoneNumberPojo"', UPDATABLE TRUE);