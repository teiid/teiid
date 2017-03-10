CREATE FOREIGN TABLE Trade (
	TradeObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.trades.Trade'),
	tradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),
	metaData object OPTIONS (NAMEINSOURCE 'metaData', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.trades.MetaData'),
	name string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	settled boolean OPTIONS (NAMEINSOURCE 'settled', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'boolean'),
	tradeDate date OPTIONS (NAMEINSOURCE 'tradeDate', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.util.Date'),
	CONSTRAINT PK_TRADEID PRIMARY KEY(tradeId)
) OPTIONS (UPDATABLE TRUE);

CREATE FOREIGN TABLE MetaData (
	content string OPTIONS (NAMEINSOURCE 'content', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),
	id integer OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),
	tradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SELECTABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),
	CONSTRAINT PK_TRADEID PRIMARY KEY(tradeId),
	CONSTRAINT FK_TRADE FOREIGN KEY(tradeId) REFERENCES Trade (tradeId) OPTIONS (NAMEINSOURCE 'MetaData')
) OPTIONS (UPDATABLE TRUE);