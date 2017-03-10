CREATE FOREIGN TABLE Person (
	name string NOT NULL OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	email string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	CONSTRAINT PK_ID PRIMARY KEY(id)
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE Address (
	Address string NOT NULL OPTIONS (NAMEINSOURCE 'address.Address', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	City string NOT NULL OPTIONS (NAMEINSOURCE 'address.City', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	State string NOT NULL OPTIONS (NAMEINSOURCE 'address.State', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SELECTABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	CONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'address')
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE PhoneNumber (
	number string NOT NULL OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	type string OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Enum'),
	id integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SELECTABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	CONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'phones')
) OPTIONS (UPDATABLE TRUE);

