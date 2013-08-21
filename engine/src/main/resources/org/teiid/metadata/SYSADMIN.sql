CREATE FOREIGN TABLE MatViews (
	VDBName string(255) NOT NULL,
	SchemaName string(255) NOT NULL,
	Name string(255) NOT NULL,
	TargetSchemaName string(255),
	TargetName string,
	Valid boolean,
	LoadState string(255),
	Updated timestamp,
	Cardinality integer,
	PRIMARY KEY (VDBName, SchemaName, Name)
);

CREATE FOREIGN TABLE VDBResources (
	resourcePath string(255),
	contents blob,
	PRIMARY KEY (resourcePath)
);

CREATE FOREIGN PROCEDURE isLoggable(OUT loggable boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR')
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE logMsg(OUT logged boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR', IN msg object NOT NULL)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE refreshMatView(OUT RowsUpdated integer NOT NULL RESULT, IN ViewName string NOT NULL, IN Invalidate boolean NOT NULL DEFAULT 'false')
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE refreshMatViewRow(OUT RowsUpdated integer NOT NULL RESULT, IN ViewName string NOT NULL, IN Key object NOT NULL)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE setColumnStats(IN tableName string NOT NULL, IN columnName string NOT NULL, IN distinctCount long, IN nullCount long, IN max string, IN min string)
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE setProperty(OUT OldValue clob(2097152) NOT NULL RESULT, IN UID string(50) NOT NULL, IN Name string NOT NULL, IN "Value" clob(2097152))
OPTIONS (UPDATECOUNT 0)

CREATE FOREIGN PROCEDURE setTableStats(IN tableName string NOT NULL, IN cardinality long NOT NULL)
OPTIONS (UPDATECOUNT 0)

CREATE VIRTUAL PROCEDURE matViewStatus(IN schemaName string NOT NULL, IN viewName string NOT NULL) RETURNS TABLE (TargetSchemaName varchar(50), TargetName varchar(50), Valid boolean, LoadState varchar(25), Updated timestamp, Cardinality integer) AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE integer vdbVersion = (SELECT convert(Version, integer) FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND Name = viewName);
	
	IF (uid IS NULL)
	BEGIN
		RAISE SQLEXCEPTION 'The view not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view is not declared as Materialized View in Metadata';
	END		  

	DECLARE string statusTable = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string crit = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';

	EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality FROM ' || VARIABLES.statusTable || crit AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality integer USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName;
END


CREATE VIRTUAL PROCEDURE loadMatView(IN schemaName string NOT NULL, IN viewName string NOT NULL, IN invalidate boolean NOT NULL DEFAULT 'false') RETURNS integer
AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE integer vdbVersion = (SELECT convert(Version, integer) FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND Name = viewName);
	DECLARE string status = 'CHECK';
	DECLARE integer rowsUpdated = 0;
	
	IF (uid IS NULL)
	BEGIN
		RAISE SQLEXCEPTION 'The view not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view is not declared as Materialized View in Metadata';
	END		  

	DECLARE string statusTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string beforeLoadScript = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_BEFORE_LOAD_SCRIPT');
	DECLARE string loadScript = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_LOAD_SCRIPT');
	DECLARE string afterLoadScript = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_AFTER_LOAD_SCRIPT');
	DECLARE integer ttl = (SELECT convert("value", integer) from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_TTL');
	DECLARE string matViewTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = 'MATERIALIZED_TABLE');	

	DECLARE string crit = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	DECLARE string updateStmt = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality' ||  crit;

	EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality FROM ' || VARIABLES.statusTable || crit AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality integer INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName;
	
	DECLARE boolean valid = (SELECT valid FROM #load);
	IF (valid is null)
    BEGIN ATOMIC
        EXECUTE IMMEDIATE 'INSERT INTO '|| VARIABLES.statusTable ||' (VDBName, VDBVersion, SchemaName, Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality) values (DVARS.vdbName, DVARS.vdbVersion, DVARS.schemaName, DVARS.viewName, null, DVARS.matViewTable, DVARS.valid, DVARS.loadStatus, DVARS.updated, -1)' USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, valid=false, loadStatus='LOADING', matViewTable=matViewTable, updated = now();
        VARIABLES.status = 'LOAD';
    END
	
	IF (VARIABLES.status = 'CHECK')
	BEGIN 
	    LOOP ON (SELECT valid, updated, loadstate, cardinality FROM #load) AS matcursor
	    BEGIN
		    IF (not matcursor.valid OR (matcursor.valid AND TIMESTAMPDIFF(SQL_TSI_SECOND, matcursor.updated, now()) > (ttl/1000)) OR invalidate OR loadstate = 'NEEDS_LOADING')
	        BEGIN ATOMIC
	            EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'LOADING', valid = convert('false', boolean), cardinality = matcursor.cardinality;
	            VARIABLES.status = 'LOAD';				
	        END
	        ELSE
	        BEGIN
	        	VARIABLES.status = 'DONE';
	        END
	    END
    END
	
    IF(VARIABLES.status = 'LOAD')
    BEGIN ATOMIC
        EXECUTE IMMEDIATE beforeLoadScript;
        EXECUTE IMMEDIATE loadScript;        
        EXECUTE IMMEDIATE afterLoadScript;
        EXECUTE IMMEDIATE 'SELECT count(*) as rowCount FROM ' || matViewTable AS rowCount integer INTO #load_count;        
        rowsUpdated = (SELECT rowCount FROM #load_count);        
        EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'LOADED', valid = convert('true', boolean), cardinality = VARIABLES.rowsUpdated;        			
        VARIABLES.status = 'DONE';
    EXCEPTION e 
        EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'FAILED_LOAD', valid = convert('false', boolean), cardinality = -1;
        VARIABLES.status = 'FAILED';
    END

	RETURN  rowsUpdated;
END
