CREATE FOREIGN TABLE Trade (
	tradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),
	name string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	settled boolean OPTIONS (NAMEINSOURCE 'settled', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'boolean'),
	tradeDate date OPTIONS (NAMEINSOURCE 'tradeDate', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.util.Date'),
	CONSTRAINT PK_TRADEID PRIMARY KEY(tradeId)
) OPTIONS (UPDATABLE TRUE);