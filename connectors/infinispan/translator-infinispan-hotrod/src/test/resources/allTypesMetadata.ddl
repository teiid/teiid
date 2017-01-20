CREATE FOREIGN TABLE AllTypes (
	intKey integer NOT NULL OPTIONS (NAMEINSOURCE 'intKey', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	stringNum string OPTIONS (NAMEINSOURCE 'stringNum', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	stringKey string NOT NULL OPTIONS (NAMEINSOURCE 'stringKey', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	floatNum float OPTIONS (NAMEINSOURCE 'floatNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'float'),
	bigIntegerValue biginteger OPTIONS (NAMEINSOURCE 'bigIntegerValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),
	shortValue short OPTIONS (NAMEINSOURCE 'shortValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),
	doubleNum double OPTIONS (NAMEINSOURCE 'doubleNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'double'),
	byteArrayValue varbinary OPTIONS (NAMEINSOURCE 'byteArrayValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'byte[]'),
	intNum integer OPTIONS (NAMEINSOURCE 'intNum', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),
	bigDecimalValue bigdecimal OPTIONS (NAMEINSOURCE 'bigDecimalValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	longNum long OPTIONS (NAMEINSOURCE 'longNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),
	booleanValue boolean NOT NULL DEFAULT 'false' OPTIONS (NAMEINSOURCE 'booleanValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'boolean'),
	timeStampValue timestamp OPTIONS (NAMEINSOURCE 'timeStampValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),
	timeValue time OPTIONS (NAMEINSOURCE 'timeValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),
	dateValue date OPTIONS (NAMEINSOURCE 'dateValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),
	charValue integer OPTIONS (NAMEINSOURCE 'charValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),
	CONSTRAINT PK_INTKEY PRIMARY KEY(intKey)
) OPTIONS (UPDATABLE TRUE);