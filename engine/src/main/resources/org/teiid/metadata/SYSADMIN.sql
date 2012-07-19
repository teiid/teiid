CREATE FOREIGN TABLE MatViews (
	VDBName string(255) NOT NULL,
	SchemaName string(255) NOT NULL,
	Name string(255) NOT NULL,
	TargetSchemaName string(255),
	TargetName string,
	Valid boolean,
	LoadState string(255),
	Updated timestamp,
	Cardinality integer
);

CREATE FOREIGN TABLE VDBResources (
	resourcePath string(255),
	contents blob
);

CREATE FOREIGN PROCEDURE isLoggable(OUT loggable boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR')
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE logMsg(OUT logged boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR', IN msg object NOT NULL)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE refreshMatView(IN ViewName string NOT NULL, IN Invalidate boolean NOT NULL DEFAULT 'false', OUT RowsUpdated integer NOT NULL RESULT)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE refreshMatViewRow(IN ViewName string NOT NULL, IN Key object NOT NULL, OUT RowsUpdated integer NOT NULL RESULT)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE setColumnStats(IN tableName string NOT NULL, IN columnName string NOT NULL, IN distinctCount integer, IN nullCount integer, IN max string, IN min string)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE setProperty(OUT OldValue clob(2097152) NOT NULL RESULT, IN UID string(50) NOT NULL, IN Name string NOT NULL, IN "Value" clob(2097152))
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE setTableStats(IN tableName string NOT NULL, IN cardinality integer NOT NULL)
OPTIONS (UPDATECOUNT 0)
