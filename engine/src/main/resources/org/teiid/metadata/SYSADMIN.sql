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

CREATE FOREIGN TABLE Triggers (
	VDBName string(255) NOT NULL,
	SchemaName string(255) NOT NULL,
	TableName string(255) NOT NULL,
	Name string(255) NOT NULL,
	TriggerType string(50) NOT NULL,
	TriggerEvent string(50) NOT NULL,
	Status string(50) NOT NULL,
	FiringTime string(255),
	body string(4000) NOT NULL,
	ClobBody clob(2097152),
	UID string(50) NOT NULL,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name)
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

CREATE VIRTUAL PROCEDURE matViewStatus(IN schemaName string NOT NULL, IN viewName string NOT NULL) RETURNS TABLE (TargetSchemaName varchar(50), TargetName varchar(50), Valid boolean, LoadState varchar(25), Updated timestamp, Cardinality integer, OnErrorAction varchar(25)) AS
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
	DECLARE string action = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_ONERROR_ACTION');
	DECLARE string crit = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';

	EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, OnErrorAction FROM ' || VARIABLES.statusTable || crit AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality integer, OnErrorAction string USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, OnErrorAction = VARIABLES.action;
END


CREATE VIRTUAL PROCEDURE loadMatView(IN schemaName string NOT NULL, IN viewName string NOT NULL, IN invalidate boolean NOT NULL DEFAULT 'false') RETURNS integer
AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE integer vdbVersion = (SELECT convert(Version, integer) FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND Name = viewName);
	DECLARE string status = 'CHECK';
	DECLARE integer rowsUpdated = 0;
	DECLARE string crit;
	
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
	DECLARE string matViewStageTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATERIALIZED_STAGE_TABLE');	
	DECLARE string scope = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE');
	DECLARE string matViewTable = (SELECT TargetName from SYSADMIN.MatViews WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND Name = viewName);
	DECLARE string transformation = (SELECT body from SYSADMIN.Triggers WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND TableName = viewName AND TriggerEvent='EXECUTION');
	DECLARE string action = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_ONERROR_ACTION');
	
	IF ((scope IS null) OR (scope = 'NONE'))
	BEGIN 
		VARIABLES.crit = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	END
	ELSE IF (scope = 'VDB')
	BEGIN
		VARIABLES.crit = ' WHERE VDBName = DVARS.vdbName AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	END
	ELSE IF (scope = 'SCHEMA')
	BEGIN
		VARIABLES.crit = ' WHERE schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	END
	
	DECLARE string updateStmt = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality' ||  crit;

	EXECUTE IMMEDIATE 'SELECT Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality FROM ' || VARIABLES.statusTable || crit AS Name string, TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality integer INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName;
	
	DECLARE string previousRow = (SELECT Name FROM #load);
	IF (previousRow is null)
    BEGIN ATOMIC
        EXECUTE IMMEDIATE 'INSERT INTO '|| VARIABLES.statusTable ||' (VDBName, VDBVersion, SchemaName, Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, OnErrorAction) values (DVARS.vdbName, DVARS.vdbVersion, DVARS.schemaName, DVARS.viewName, null, DVARS.matViewTable, DVARS.valid, DVARS.loadStatus, DVARS.updated, -1, DVARS.action)' USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, valid=false, loadStatus='LOADING', matViewTable=matViewTable, updated = now(), action = VARIABLES.action;
        VARIABLES.status = 'LOAD';
    EXCEPTION e
        VARIABLES.rowsUpdated = -1;
        VARIABLES.status = 'DONE';
    END
	
	IF (VARIABLES.status = 'CHECK')
	BEGIN 
	    LOOP ON (SELECT valid, updated, loadstate, cardinality FROM #load) AS matcursor
	    BEGIN
		    IF (not matcursor.valid OR (matcursor.valid AND TIMESTAMPDIFF(SQL_TSI_SECOND, matcursor.updated, now()) > (ttl/1000)) OR invalidate OR loadstate = 'NEEDS_LOADING'  OR loadstate = 'FAILED_LOAD')
		        BEGIN ATOMIC
		            EXECUTE IMMEDIATE updateStmt AS "rows" integer into #updated USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'LOADING', valid = convert('false', boolean), cardinality = matcursor.cardinality;
					DECLARE integer updated = (SELECT "rows" FROM #updated);
					IF (updated = 0)
						BEGIN
							VARIABLES.status = 'DONE';
							VARIABLES.rowsUpdated = -1;
						END
					ELSE
						BEGIN					            
				            VARIABLES.status = 'LOAD';
			            END				
		        END
	        ELSE
		        BEGIN
		        	VARIABLES.rowsUpdated = -2;
		        	VARIABLES.status = 'DONE';
		        END
	    END
    END
	
    IF(VARIABLES.status = 'LOAD')
    BEGIN ATOMIC
    	IF (VARIABLES.loadScript IS null)
    	BEGIN
    		VARIABLES.loadScript = 'SELECT X.* INTO ' || matViewStageTable || ' FROM ('|| transformation || ') AS X';
    	END
    	
    	IF (VARIABLES.beforeLoadScript IS NOT null)
    	BEGIN
        	EXECUTE IMMEDIATE beforeLoadScript;
        END
        
        EXECUTE IMMEDIATE loadScript;
    	
    	IF (VARIABLES.afterLoadScript IS NOT null)
    	BEGIN                
        	EXECUTE IMMEDIATE afterLoadScript;
        END
        
        EXECUTE IMMEDIATE 'SELECT count(*) as rowCount FROM ' || matViewTable AS rowCount integer INTO #load_count;        
        rowsUpdated = (SELECT rowCount FROM #load_count);        
        EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'LOADED', valid = convert('true', boolean), cardinality = VARIABLES.rowsUpdated;        			
        VARIABLES.status = 'DONE';
    EXCEPTION e 
        EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'FAILED_LOAD', valid = convert('false', boolean), cardinality = -1;
        VARIABLES.status = 'FAILED';
        VARIABLES.rowsUpdated = -3;
    END

	RETURN  rowsUpdated;
END


CREATE VIRTUAL PROCEDURE updateMatView(IN schemaName string NOT NULL, IN viewName string NOT NULL, IN refreshCriteria string) RETURNS integer
AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE integer vdbVersion = (SELECT convert(Version, integer) FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND Name = viewName);
	DECLARE string status = 'CHECK';
	DECLARE integer rowsUpdated = 0;
	DECLARE string crit;
	DECLARE boolean invalidate = false;
	
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
	DECLARE string matViewTable = (SELECT TargetName from SYSADMIN.MatViews WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND Name = viewName);
	DECLARE string transformation = (SELECT body from SYSADMIN.Triggers WHERE VDBName = VARIABLES.vdbName AND SchemaName = schemaName AND TableName = viewName AND TriggerEvent='EXECUTION');
	DECLARE string scope = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE');

	IF ((scope IS null) OR (scope = 'NONE'))
	BEGIN 
		VARIABLES.crit = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	END
	ELSE IF (scope = 'VDB')
	BEGIN
		VARIABLES.crit = ' WHERE VDBName = DVARS.vdbName AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	END
	ELSE IF (scope = 'SCHEMA')
	BEGIN
		VARIABLES.crit = ' WHERE schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	END
	
	DECLARE string updateStmt = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated' ||  crit;
	DECLARE string updateStmtWithCardinality = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality' ||  crit;

	IF (VARIABLES.status = 'CHECK')
    BEGIN ATOMIC
        EXECUTE IMMEDIATE updateStmt AS "rows" integer into #updated USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'LOADING', valid = convert('false', boolean);
		DECLARE integer updated = (SELECT "rows" FROM #updated);
		IF (updated = 0)
			BEGIN
				VARIABLES.status = 'DONE';
				VARIABLES.rowsUpdated = -1;
			END
		ELSE
			BEGIN					            
	            VARIABLES.status = 'LOAD';
            END				
    END
	
    IF(VARIABLES.status = 'LOAD')
    BEGIN ATOMIC
        EXECUTE IMMEDIATE 'DELETE FROM '|| matViewTable || ' WHERE ' ||  refreshCriteria AS rowCount integer;
        EXECUTE IMMEDIATE 'SELECT X.* INTO ' || matViewTable || ' FROM ('|| transformation || ') AS X WHERE ' || refreshCriteria;
    	
        EXECUTE IMMEDIATE 'SELECT count(*) as rowCount FROM ' || matViewTable AS rowCount integer INTO #load_count;        
        rowsUpdated = (SELECT rowCount FROM #load_count);        
        EXECUTE IMMEDIATE updateStmtWithCardinality USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'LOADED', valid = convert('true', boolean), cardinality = VARIABLES.rowsUpdated;        			
        VARIABLES.status = 'DONE';
    EXCEPTION e 
        EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = viewName, updated = now(), LoadState = 'FAILED_LOAD', valid = convert('false', boolean), cardinality = -1;
        VARIABLES.status = 'FAILED';
        VARIABLES.rowsUpdated = -3;
    END

	RETURN  rowsUpdated;
END