CREATE FOREIGN TABLE Usage (
    VDBName string(255) NOT NULL,
    UID string(50) NOT NULL,
    object_type string(50) NOT NULL,
    SchemaName string(255) NOT NULL,
    Name string(255) NOT NULL,
    ElementName string(255),
    Uses_UID string(50) NOT NULL,
    Uses_object_type string(50) NOT NULL,
    Uses_SchemaName string(255) NOT NULL,
    Uses_Name string(255) NOT NULL,
    Uses_ElementName string(255),
    PRIMARY KEY (UID, Uses_UID)
);

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
	Body clob(2097152),
	TableUID string(50) NOT NULL,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name)
);

CREATE FOREIGN TABLE Views (
    VDBName string(255) NOT NULL,
    SchemaName string(255) NOT NULL,
    Name string(255) NOT NULL,
    Body clob(2097152) NOT NULL,
    UID string(50) NOT NULL,
    PRIMARY KEY (VDBName, SchemaName, Name),
    UNIQUE(UID)
);

CREATE FOREIGN TABLE StoredProcedures (
    VDBName string(255) NOT NULL,
    SchemaName string(255) NOT NULL,
    Name string(255) NOT NULL,
    Body clob(2097152) NOT NULL,
    UID string(50) NOT NULL,
    PRIMARY KEY (VDBName, SchemaName, Name),
    UNIQUE(UID)
);

CREATE FOREIGN TABLE SESSIONS (
    VDBName string(255) NOT NULL,
    SessionId string(255) NOT NULL,
    UserName string(255) NOT NULL,
    CreatedTime timestamp NOT NULL,
    ApplicationName string(255),
    IPAddress string(255),
    PRIMARY KEY (SessionId)
);

CREATE FOREIGN TABLE REQUESTS (
    VDBName string(255) NOT NULL,
    SessionId string(255) NOT NULL,
    ExecutionId long NOT NULL,
    Command clob NOT NULL,
    StartTimestamp timestamp NOT NULL,
    TransactionId string(255),
    ProcessingState string(255),
    ThreadState string(255),
    IsSource boolean NOT NULL,
    FOREIGN KEY (SessionId) REFERENCES SESSIONS (SessionId)
);

CREATE FOREIGN TABLE TRANSACTIONS (
    TransactionId string(255) NOT NULL,
    SessionId string(255),
    StartTimestamp timestamp NOT NULL,
    Scope string(255),
    FOREIGN KEY (SessionId) REFERENCES SESSIONS (SessionId)
);

CREATE FOREIGN PROCEDURE isLoggable(OUT loggable boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR')
OPTIONS (UPDATECOUNT 0);

CREATE FOREIGN PROCEDURE logMsg(OUT logged boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR', IN msg object)
OPTIONS (UPDATECOUNT 0);

CREATE FOREIGN PROCEDURE refreshMatView(OUT RowsUpdated integer NOT NULL RESULT, IN ViewName string NOT NULL, IN Invalidate boolean NOT NULL DEFAULT 'false')
OPTIONS (UPDATECOUNT 0);

CREATE FOREIGN PROCEDURE refreshMatViewRow(OUT RowsUpdated integer NOT NULL RESULT, IN ViewName string NOT NULL, IN Key object NOT NULL, VARIADIC KeyOther object)
OPTIONS (UPDATECOUNT 1);

CREATE FOREIGN PROCEDURE refreshMatViewRows(OUT RowsUpdated integer NOT NULL RESULT, IN ViewName string NOT NULL, VARIADIC Key object[] NOT NULL)
OPTIONS (UPDATECOUNT 1);

CREATE FOREIGN PROCEDURE setColumnStats(IN tableName string NOT NULL, IN columnName string NOT NULL, IN distinctCount long, IN nullCount long, IN max string, IN min string)
OPTIONS (UPDATECOUNT 0);

CREATE FOREIGN PROCEDURE setProperty(OUT OldValue clob(2097152) NOT NULL RESULT, IN UID string(50) NOT NULL, IN Name string NOT NULL, IN "Value" clob(2097152))
OPTIONS (UPDATECOUNT 0);

CREATE FOREIGN PROCEDURE setTableStats(IN tableName string NOT NULL, IN cardinality long NOT NULL)
OPTIONS (UPDATECOUNT 0);

CREATE VIRTUAL PROCEDURE matViewStatus(IN schemaName string NOT NULL, IN viewName string NOT NULL) RETURNS TABLE (TargetSchemaName varchar(50), TargetName varchar(50), Valid boolean, LoadState varchar(25), Updated timestamp, Cardinality long, LoadNumber long, OnErrorAction varchar(25), NodeName varchar(25)) AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE string vdbVersion = (SELECT Version FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = matViewStatus.schemaName AND Name = matViewStatus.viewName);
	
	IF (uid IS NULL)
	BEGIN
		RAISE SQLEXCEPTION 'The view was not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view is not declared as Materialized View in Metadata';
	END		  

    DECLARE string ownerVdbName = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_NAME'); 
    IF (ownerVdbName IS NOT NULL)
    BEGIN
        vdbName = ownerVdbName; 
    END
    
    DECLARE string ownerVdbVersion = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_VERSION');    
    IF (ownerVdbVersion IS NOT NULL)
    BEGIN
        vdbVersion = ownerVdbVersion; 
    END
    
    DECLARE string scope = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE');
    IF (scope = 'FULL')
    BEGIN
        vdbVersion = '0';
    END      

	DECLARE string statusTable = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string action = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_ONERROR_ACTION');
	DECLARE string crit = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
    DECLARE string statusTableInter = 'SYSADMIN.MatViews';
    DECLARE string critInter = ' WHERE VDBName = DVARS.vdbName AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
    DECLARE string defaultAction = 'THROW_EXCEPTION';
    DECLARE long defaultLoadNumber = -1;

	IF (statusTable IS NULL)
    BEGIN
		EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, VARIABLES.defaultLoadNumber, VARIABLES.defaultAction, NODE_ID() FROM ' || VARIABLES.statusTableInter || critInter AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long, OnErrorAction varchar(25), NodeName varchar(25) USING vdbName = VARIABLES.vdbName, schemaName = matViewStatus.schemaName, viewName = matViewStatus.viewName;
    END ELSE
    BEGIN
	    EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber, VARIABLES.action, NodeName FROM ' || VARIABLES.statusTable || crit AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long, OnErrorAction varchar(25), NodeName varchar(25) USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = matViewStatus.schemaName, viewName = matViewStatus.viewName;
    END
END;


CREATE VIRTUAL PROCEDURE loadMatView(IN schemaName string NOT NULL, IN viewName string NOT NULL, IN invalidate boolean NOT NULL DEFAULT 'false', IN only_if_needed boolean NOT NULL DEFAULT false) RETURNS integer
AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE string vdbVersion = (SELECT Version FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = loadMatView.schemaName AND Name = loadMatView.viewName);
	DECLARE string status = 'CHECK';
	DECLARE integer rowsUpdated = 0;
	DECLARE integer lineCount = 0;
	DECLARE integer index = 0;
	DECLARE string fullViewName = loadMatView.schemaName || '.' || loadMatView.viewName;
	
	IF (uid IS NULL)
	BEGIN
		RAISE SQLEXCEPTION 'The view '|| VARIABLES.fullViewName || ' was not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view ' || VARIABLES.fullViewName || ' is not declared as Materialized View in Metadata';
	END		  

    DECLARE string ownerVdbName = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_NAME');
    IF (ownerVDBName IS NOT NULL)
    BEGIN
        vdbName = ownerVdbName;
    END
         
    DECLARE string ownerVdbVersion = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_VERSION');
    IF (ownerVdbVersion IS NOT NULL)
    BEGIN
        vdbVersion = ownerVdbVersion;
    END     

    DECLARE string scope = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE');
    IF (scope = 'FULL')
    BEGIN
        vdbVersion = '0';
    END
    
	DECLARE string statusTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string[] targets = (SELECT (TargetName, TargetSchemaName) from SYSADMIN.MatViews WHERE VDBName = VARIABLES.vdbName AND SchemaName = loadMatView.schemaName AND Name = loadMatView.viewName);
	DECLARE string matViewTable = array_get(targets, 1);
	DECLARE string targetSchemaName = array_get(targets, 2);

    EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'INFO', msg=>'Materialization of view ' || VARIABLES.fullViewName || ' started' || case when only_if_needed then ' if needed.' else '.' end);        
    
    IF (targetSchemaName IS NULL)
    BEGIN     
        DECLARE string partColumn = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = 'teiid_rel:MATVIEW_PART_LOAD_COLUMN');
        
        IF (partColumn IS NULL)
        BEGIN
            rowsUpdated = (EXECUTE SYSADMIN.refreshMatView(VARIABLES.fullViewName, loadMatView.invalidate));
            EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'INFO', msg=>'Materialization of view ' || VARIABLES.fullViewName || ' completed. Rows updated = ' || VARIABLES.rowsUpdated);        
        END
        ELSE
        BEGIN ATOMIC
            partColumn = '"' || replace(partColumn, '"', '""') || '"';
            DECLARE string partValues = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = 'teiid_rel:MATVIEW_PART_LOAD_VALUES');
            IF (partValues IS NULL)
                partValues = 'SELECT DISTINCT(' || viewName || '.' || partColumn || ') FROM ' || schemaName || '.' || viewName || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
                         
            VARIABLES.rowsUpdated = 0;
            EXECUTE IMMEDIATE partValues as source string INTO #partValues; 
            LOOP ON (SELECT source from #partValues) AS sources
            BEGIN
                IF (source IS NULL)
                    CONTINUE;
                DECLARE integer result_rows = (EXECUTE sysadmin.updateMatView(schemaName, viewName, partColumn || ' = ''' || replace(cast(sources.source as string), '''', '''''') || ''''));
                IF (result_rows > 0)
                    VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + result_rows;
            EXCEPTION e
                EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
            END
        END
        RETURN rowsUpdated;
    EXCEPTION e
        rowsUpdated = -2;
        RETURN rowsUpdated;
    END

	DECLARE string beforeLoadScript = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_BEFORE_LOAD_SCRIPT');
	DECLARE string loadScript = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_LOAD_SCRIPT');
	DECLARE string afterLoadScript = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_AFTER_LOAD_SCRIPT');
	DECLARE integer ttl = (SELECT convert("value", integer) from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_TTL');
	DECLARE string matViewStageTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATERIALIZED_STAGE_TABLE');		
	DECLARE string action = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_ONERROR_ACTION');
	DECLARE string loadNumColumn = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_LOADNUMBER_COLUMN');
	DECLARE boolean implicitLoadScript = false;

    /* if load number based update scheme is in use, override the staging table based method */
    IF(loadNumColumn IS NOT null)
    BEGIN
        matViewStageTable = targetSchemaName ||'.'||matViewTable;
        DECLARE string KeyUID = (SELECT UID FROM SYS.Keys WHERE SchemaName = loadMatView.schemaName  AND TableName = loadMatView.viewName AND (Type = 'Primary' OR Type = 'Unique'));
        IF (KeyUID IS NULL)
        BEGIN            
            RAISE SQLEXCEPTION 'Primary key is required on view ' || VARIABLES.fullViewName || ' to perform materialization load';
        END                                    
    END
    
    DECLARE string updateCriteria = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
	DECLARE string updateStmt = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadNumber = DVARS.LoadNumber, LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality, NodeName=DVARS.NodeName, StaleCount=DVARS.StaleCount ' ||  VARIABLES.updateCriteria;

	EXECUTE IMMEDIATE 'SELECT Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber, StaleCount FROM ' || VARIABLES.statusTable || VARIABLES.updateCriteria AS Name string, TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long, StaleCount long INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = loadMatView.schemaName, viewName = loadMatView.viewName;
	
	DECLARE string previousRow = (SELECT Name FROM #load);
	IF (previousRow is null)
    BEGIN 
        EXECUTE IMMEDIATE 'INSERT INTO '|| VARIABLES.statusTable ||' (VDBName, VDBVersion, SchemaName, Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber, NodeName, StaleCount) values (DVARS.vdbName, DVARS.vdbVersion, DVARS.schemaName, DVARS.viewName, DVARS.TargetSchemaName, DVARS.matViewTable, DVARS.valid, DVARS.loadStatus, DVARS.updated, -1, 1, DVARS.NodeName, 0)' USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, targetSchemaName = VARIABLES.targetSchemaName, viewName = loadMatView.viewName, valid=false, loadStatus='LOADING', matViewTable=matViewTable, updated = now(), NodeName = NODE_ID();
        VARIABLES.status = 'LOAD';
    EXCEPTION e
        DELETE FROM #load;
        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
        EXECUTE IMMEDIATE 'SELECT Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber, StaleCount FROM ' || VARIABLES.statusTable || VARIABLES.updateCriteria AS Name string, TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long, StaleCount long INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName;		       
    END
    
    DECLARE long VARIABLES.loadNumber = 1;
    DECLARE boolean VARIABLES.valid = false;
	DECLARE long staleCount = (SELECT StaleCount FROM #load);
	
	IF (VARIABLES.status = 'CHECK')
	BEGIN 
	    LOOP ON (SELECT valid, updated, loadstate, cardinality, loadnumber FROM #load) AS matcursor
	    BEGIN
		    DECLARE boolean VARIABLES.load = false;
	        IF ((loadstate <> 'LOADING' AND NOT only_if_needed) OR (only_if_needed AND loadstate IN ('NEEDS_LOADING', 'FAILED_LOAD')) OR TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, matcursor.updated, now())/1000000 > ttl)
		        load = true;
		    ELSE
		        BEGIN
		            DECLARE string pollingQuery = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_POLLING_QUERY');
		            IF (pollingQuery IS NOT NULL)
		                BEGIN
		                    EXECUTE IMMEDIATE pollingQuery AS updateTimestamp timestamp INTO #poll;
		                    IF (matcursor.updated < (SELECT updateTimestamp from #poll))
		                        load = true;
		                END
		        END
		    IF (load)
		        BEGIN 
		            EXECUTE IMMEDIATE updateStmt || ' AND loadNumber = ' || matcursor.loadNumber USING loadNumber = matcursor.loadNumber + 1, vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName, updated = now(), LoadState = 'LOADING', valid = matcursor.valid AND NOT invalidate, cardinality = matcursor.cardinality, NodeName = NODE_ID(), StaleCount = VARIABLES.staleCount;
					DECLARE integer updated = VARIABLES.ROWCOUNT;
					IF (updated = 0)
						BEGIN
							VARIABLES.status = 'DONE';
							VARIABLES.rowsUpdated = -1;
						END
					ELSE
						BEGIN					            
				            VARIABLES.status = 'LOAD';
				            VARIABLES.loadNumber = matcursor.loadNumber + 1;
				            VARIABLES.valid = matcursor.valid;
			            END				
		        END
	        ELSE
		        BEGIN
	                IF (invalidate AND matcursor.valid)
                        EXECUTE IMMEDIATE 'UPDATE ' || VARIABLES.statusTable || ' SET valid = false ' || VARIABLES.updateCriteria || ' AND loadNumber = ' || matcursor.loadNumber USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName;
		        	VARIABLES.rowsUpdated = -1;
		        	VARIABLES.status = 'DONE';
		        END
	    END
    END
	
    IF(VARIABLES.status = 'LOAD')
    BEGIN ATOMIC
    	IF (VARIABLES.beforeLoadScript IS NOT null)
    	BEGIN
            EXECUTE IMMEDIATE VARIABLES.beforeLoadScript;
        END
        
        IF (VARIABLES.loadScript IS null)
        BEGIN
            DECLARE clob columns = (SELECT string_agg('"' || replace(Name, '"', '""') || '"', ',') FROM SYS.Columns WHERE SchemaName = loadMatView.schemaName  AND TableName = loadMatView.viewName);
            
            IF (VARIABLES.loadNumColumn IS null)
            BEGIN
                EXECUTE IMMEDIATE 'INSERT INTO ' || matViewStageTable || '(' || columns ||') SELECT '|| columns ||' FROM ' || schemaName || '.' || viewName || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
                VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT;
            END
            ELSE
            BEGIN
                DECLARE clob columnNames = '(' || columns || ', ' || VARIABLES.loadNumColumn || ')';
                DECLARE clob columnValues = columns || ', ' || cast(VARIABLES.loadNumber as string);
                
                DECLARE string partColumn = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = 'teiid_rel:MATVIEW_PART_LOAD_COLUMN');
                
                IF (partColumn IS NULL)
                BEGIN
                    EXECUTE IMMEDIATE 'UPSERT INTO ' || matViewStageTable || VARIABLES.columnNames || ' SELECT '|| VARIABLES.columnValues || ' FROM ' || schemaName || '.' || viewName || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
                    VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT; 
                    EXECUTE IMMEDIATE 'DELETE FROM ' || matViewStageTable || ' WHERE ' || VARIABLES.loadNumColumn || ' < ' || VARIABLES.loadNumber;
                    VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
                END
                ELSE
                BEGIN
                    partColumn = '"' || replace(partColumn, '"', '""') || '"';
                    DECLARE string partValues = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = 'teiid_rel:MATVIEW_PART_LOAD_VALUES');
                    IF (partValues IS NULL)
                        partValues = 'SELECT DISTINCT(' || viewName || '.' || partColumn || ') FROM ' || schemaName || '.' || viewName || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
                         
                    VARIABLES.rowsUpdated = 0;
                    EXECUTE IMMEDIATE partValues as source string INTO #partValues; 
                    LOOP ON (SELECT source from #partValues) AS sources
                        BEGIN
                        EXECUTE IMMEDIATE 'UPSERT INTO ' || matViewStageTable || VARIABLES.columnNames || ' SELECT '|| VARIABLES.columnValues || ' FROM ' || schemaName || '.' || viewName 
                            || ' WHERE ' || VARIABLES.partColumn || '= DVARS.source ' || ' OPTION NOCACHE ' || schemaName || '.' || viewName USING source = sources.source;
                        VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT; 
                        EXECUTE IMMEDIATE 'DELETE FROM ' || matViewStageTable || ' WHERE ' || VARIABLES.loadNumColumn || ' < ' || VARIABLES.loadNumber || ' AND ' || VARIABLES.partColumn || '= DVARS.source' USING source = sources.source;
                        VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
                    EXCEPTION e
                        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
                    END
                END
            END
            VARIABLES.implicitLoadScript = true;
        END

        IF (NOT VARIABLES.implicitLoadScript)
        BEGIN
            EXECUTE IMMEDIATE VARIABLES.loadScript;
            EXECUTE IMMEDIATE 'SELECT count(*) as rowCount FROM ' || targetSchemaName || '.' || matViewTable AS rowCount integer INTO #load_count;        
            rowsUpdated = (SELECT rowCount FROM #load_count);                    
        END
            	
    	IF (VARIABLES.afterLoadScript IS NOT null)
    	BEGIN
	    	IF (VARIABLES.loadScript IS null AND VARIABLES.valid AND VARIABLES.loadNumColumn IS null)
		    	--assume that the after state will be invalid, will be updated again below
		    	EXECUTE IMMEDIATE 'UPDATE ' || VARIABLES.statusTable || ' SET valid = false ' || VARIABLES.updateCriteria USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName;
	    	EXECUTE IMMEDIATE VARIABLES.afterLoadScript;
        END
        
        EXECUTE IMMEDIATE updateStmt || ' AND loadNumber = DVARS.loadNumber' USING  loadNumber = VARIABLES.loadNumber, vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName, updated = now(), LoadState = 'LOADED', valid = true, cardinality = VARIABLES.rowsUpdated, NodeName = NODE_ID(), StaleCount = 0;	
        VARIABLES.status = 'DONE';
        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'INFO', msg=>'Materialization of view ' || VARIABLES.fullViewName || ' completed. Rows updated = ' || VARIABLES.rowsUpdated || ' Load Number = ' || VARIABLES.loadNumber);
    EXCEPTION e 
        EXECUTE IMMEDIATE updateStmt || ' AND loadNumber = DVARS.loadNumber' USING  loadNumber = VARIABLES.loadNumber, vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName, updated = now(), LoadState = 'FAILED_LOAD', valid = VARIABLES.valid AND NOT invalidate, cardinality = -1, NodeName = NODE_ID(), StaleCount = VARIABLES.staleCount;
        VARIABLES.status = 'FAILED';
        VARIABLES.rowsUpdated = -3;
        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
    END

	RETURN  rowsUpdated;
END;


CREATE VIRTUAL PROCEDURE updateMatView(IN schemaName string NOT NULL, IN viewName string NOT NULL, IN refreshCriteria string NOT NULL) RETURNS integer
AS
BEGIN
	DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
	DECLARE string vdbVersion = (SELECT Version  FROM VirtualDatabases);
	DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = updateMatView.schemaName AND Name = updateMatView.viewName);
	DECLARE integer rowsUpdated = 0;
	DECLARE boolean invalidate = false;
	DECLARE string fullViewName = updateMatView.schemaName || '.' || updateMatView.viewName;
	
	IF (uid IS NULL)
	BEGIN
		RAISE SQLEXCEPTION 'The view '|| VARIABLES.fullViewName || ' not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view ' || VARIABLES.fullViewName || ' is not declared as Materialized View in Metadata';
	END		

    DECLARE string ownerVdbName = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_NAME');
    IF (ownerVDBName IS NOT NULL)
    BEGIN
        vdbName = ownerVdbName;
    END
         
    DECLARE string ownerVdbVersion = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_VERSION');
    IF (ownerVdbVersion IS NOT NULL)
    BEGIN
        vdbVersion = ownerVdbVersion;
    END     

    DECLARE string scope = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE');
    IF (scope = 'FULL')
    BEGIN
        vdbVersion = '0';
    END
    	
    EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'INFO', msg=>'Criteria based Update of Materialization of view ' || VARIABLES.fullViewName || ' started.');
	
	DECLARE string statusTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string[] targets = (SELECT (TargetName, TargetSchemaName) from SYSADMIN.MatViews WHERE VDBName = VARIABLES.vdbName AND SchemaName = updateMatView.schemaName AND Name = updateMatView.viewName);
    DECLARE string matViewTable = array_get(targets, 1);
    DECLARE string targetSchemaName = array_get(targets, 2);

    IF (targetSchemaName IS NULL)
    BEGIN
	    DECLARE string KeyUID = (SELECT UID FROM SYS.Keys WHERE SchemaName = updateMatView.schemaName  AND TableName = updateMatView.viewName AND (Type = 'Primary' OR Type = 'Unique'));
	    IF (KeyUID IS NULL)
	    BEGIN
	        RAISE SQLEXCEPTION 'Primary key is required on view ' || VARIABLES.fullViewName || ' to perform materialization update';
	    END

	    DECLARE string pkcolums = '(';
	    LOOP ON (SELECT Name FROM SYS.KeyColumns WHERE SchemaName = updateMatView.schemaName  AND TableName = updateMatView.viewName AND UID = VARIABLES.KeyUID) AS colname
	    BEGIN
       	    pkcolums = pkcolums || updateMatView.viewName || '.' || colname.Name || ', ';
	    END
	    pkcolums = pkcolums || ')';

        BEGIN ATOMIC
            /* to find all new added, updated */ 
            EXECUTE IMMEDIATE 'SELECT ' || VARIABLES.pkcolums || ' FROM ' || VARIABLES.fullViewName || ' WHERE ' || updateMatView.refreshCriteria || ' OPTION NOCACHE ' || VARIABLES.fullViewName || ' ' AS PrimaryKey object[] INTO #pklist;
            /* to find all deleted, updated */
            EXECUTE IMMEDIATE 'SELECT ' || VARIABLES.pkcolums || ' FROM ' || VARIABLES.fullViewName || ' WHERE ' || updateMatView.refreshCriteria || ' ' AS PrimaryKey object[] INTO #pklist2;
            DECLARE integer interrowUpdated = 0;

            LOOP ON (SELECT PrimaryKey FROM #pklist UNION SELECT PrimaryKey FROM #pklist2) AS pkrow
            BEGIN
                interrowUpdated = (EXECUTE SYSADMIN.refreshMatViewRows(VARIABLES.fullViewName, pkrow.PrimaryKey));
                IF (interrowUpdated > 0)
                BEGIN
                    rowsUpdated = rowsUpdated + interrowUpdated;
                END
            END
            EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'INFO', msg=>'Criteria based Update of Materialization of view ' || updateMatView.schemaName || '.' || updateMatView.viewName || ' is completed.'); 	       
        EXCEPTION e
           VARIABLES.rowsUpdated = -3;
           EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
        END  
        RETURN rowsUpdated;
    END
        
	DECLARE string loadNumColumn = (SELECT "Value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_LOADNUMBER_COLUMN');
    IF(loadNumColumn IS NOT null)
    BEGIN
        DECLARE string KeyUID = (SELECT UID FROM SYS.Keys WHERE SchemaName = updateMatView.schemaName  AND TableName = updateMatView.viewName AND Type = 'Primary');
        IF (KeyUID IS NULL)
        BEGIN
            RAISE SQLEXCEPTION 'Primary key is required on view ' || VARIABLES.fullViewName || ' to perform materialization update';
        END                                    
    END	

    DECLARE string updateCriteria = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';

    /* make sure table in valid state before updating */    
    EXECUTE IMMEDIATE 'SELECT Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber FROM ' || VARIABLES.statusTable || VARIABLES.updateCriteria AS Name string, TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = updateMatView.schemaName, viewName = updateMatView.viewName;
    DECLARE long loadNumber = nvl((SELECT LoadNumber FROM #load), 0);
    DECLARE long updatedCardinality = nvl((SELECT Cardinality FROM #load), 0);
    DECLARE boolean valid = nvl((SELECT Valid FROM #load), false);
    IF (NOT VARIABLES.valid)
    BEGIN
        RAISE SQLEXCEPTION 'View ' || VARIABLES.fullViewName || ' contents are not in valid status to perform materialization update. Run loadMatview to reload.';
    END
	
	DECLARE string updateStmtWithCardinality = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality, LoadNumber = DVARS.loadNumber, NodeName = DVARS.nodeName ' ||  VARIABLES.updateCriteria;

    BEGIN ATOMIC
        DECLARE clob columns = (SELECT string_agg('"' || replace(Name, '"', '""') || '"', ',') FROM SYS.Columns WHERE SchemaName = updateMatView.schemaName  AND TableName = updateMatView.viewName);
        IF(loadNumColumn IS null)
        BEGIN
            EXECUTE IMMEDIATE 'DELETE FROM ' || targetSchemaName || '.' || matViewTable || ' as ' || replace(viewName, '.', '_') || '  WHERE ' ||  refreshCriteria;
            VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality - VARIABLES.ROWCOUNT;
            EXECUTE IMMEDIATE 'INSERT INTO ' || targetSchemaName || '.' || matViewTable || ' (' || columns || ') SELECT '|| columns ||' FROM '|| schemaName || '.' || viewName || ' WHERE ' || refreshCriteria || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
            VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality + VARIABLES.ROWCOUNT;
        END
        ELSE
        BEGIN 
            DECLARE clob columnNames = '(' || columns || ', ' || VARIABLES.loadNumColumn || ')';
            DECLARE clob columnValues = columns || ', ' || cast(VARIABLES.loadNumber + 1 as string);
            EXECUTE IMMEDIATE 'UPSERT INTO ' || targetSchemaName || '.' || matViewTable || columnNames || ' SELECT ' || VARIABLES.columnValues || ' FROM '|| schemaName || '.' || viewName || ' WHERE ' || refreshCriteria || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
            VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality + VARIABLES.ROWCOUNT;
            EXECUTE IMMEDIATE 'DELETE FROM ' || targetSchemaName || '.' || matViewTable || ' as ' || replace(viewName, '.', '_') || ' WHERE ' || VARIABLES.loadNumColumn || ' <= ' || VARIABLES.loadNumber || ' AND ' || refreshCriteria;
            VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality - VARIABLES.ROWCOUNT;
        END
        
        VARIABLES.loadNumber = VARIABLES.loadNumber +1;        
        EXECUTE IMMEDIATE updateStmtWithCardinality USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = updateMatView.schemaName, viewName = updateMatView.viewName, updated = now(), LoadState = 'LOADED', valid = true, cardinality = VARIABLES.updatedCardinality, loadNumber = VARIABLES.loadNumber, NodeName = NODE_ID();
        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'INFO', msg=>'Criteria based Update of Materialization of view ' || VARIABLES.fullViewName || ' is completed. Rows updated = ' || VARIABLES.rowsUpdated || ' Load Number = ' || VARIABLES.loadNumber);                			
    EXCEPTION e 
        VARIABLES.rowsUpdated = -3;
        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
    END    
	RETURN  rowsUpdated;
END

CREATE VIRTUAL PROCEDURE updateStaleCount(IN schemaName string NOT NULL, IN viewName string NOT NULL) RETURNS integer
AS
BEGIN
    DECLARE string vdbName = (SELECT Name FROM VirtualDatabases);
    DECLARE string vdbVersion = (SELECT Version FROM VirtualDatabases);
    DECLARE string uid = (SELECT UID FROM Sys.Tables WHERE VDBName = VARIABLES.vdbName AND SchemaName = updateStaleCount.schemaName AND Name = updateStaleCount.viewName);
    DECLARE integer rowsUpdated = 0;
    DECLARE string fullViewName = updateStaleCount.schemaName || '.' || updateStaleCount.viewName;
        
    IF (uid IS NULL)
    BEGIN
        RAISE SQLEXCEPTION 'The view '|| VARIABLES.fullViewName || ' was not found';
    END
    
    DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
    IF (NOT isMaterialized)
    BEGIN
        RAISE SQLEXCEPTION 'The view ' || VARIABLES.fullViewName || ' is not declared as Materialized View in Metadata';
    END       

    DECLARE string ownerVdbName = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_NAME');
    IF (ownerVDBName IS NOT NULL)
    BEGIN
        vdbName = ownerVdbName;
    END
         
    DECLARE string ownerVdbVersion = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_OWNER_VDB_VERSION');
    IF (ownerVdbVersion IS NOT NULL)
    BEGIN
        vdbVersion = ownerVdbVersion;
    END     

    DECLARE string scope = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE');
    IF (scope = 'FULL')
    BEGIN
        vdbVersion = '0';
    END
    
    DECLARE string pct = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_MAX_STALENESS_PCT');

    DECLARE string statusTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
    DECLARE string selectCriteria = ' WHERE VDBName = DVARS.vdbName AND VDBVersion = DVARS.vdbVersion AND schemaName = DVARS.schemaName AND Name = DVARS.viewName';
    DECLARE string updateStmt = 'UPDATE ' || VARIABLES.statusTable || ' SET StaleCount=StaleCount+1, LoadState = DVARS.LoadState ' || VARIABLES.selectCriteria;

    EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'DEBUG', msg=>'Materialization of view ' || VARIABLES.fullViewName || ', updating the stale count.');        

    EXECUTE IMMEDIATE 'SELECT Name, LoadState, StaleCount, Cardinality FROM ' || VARIABLES.statusTable || VARIABLES.selectCriteria AS Name string, LoadState string, StaleCount long, Cardinality long  INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = updateStaleCount.schemaName, viewName = updateStaleCount.viewName;
    
    DECLARE string previousRow = (SELECT Name FROM #load);
    DECLARE string loadState = (SELECT LoadState FROM #load);
    IF (previousRow IS NOT null AND loadState = 'LOADED')
    BEGIN
        DECLARE long staleCount = (SELECT StaleCount FROM #load) + 1;
        DECLARE long cardinality = (SELECT Cardinality FROM #load);
        IF (cardinality = 0 OR ((100*staleCount/sqrt(cardinality)) >= convert(pct, integer)))
            VARIABLES.loadState = 'NEEDS_LOADING';
        EXECUTE IMMEDIATE updateStmt USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = updateStaleCount.schemaName, viewName = updateStaleCount.viewName, LoadState = VARIABLES.loadState;
    EXCEPTION e
        EXECUTE sysadmin.logMsg(context=>'org.teiid.PROCESSOR.MATVIEWS', level=>'WARN', msg=>e.exception);
    END
END

CREATE FOREIGN PROCEDURE terminateSession(OUT terminated boolean NOT NULL RESULT, IN SessionId string NOT NULL);

CREATE FOREIGN PROCEDURE cancelRequest(OUT cancelled boolean NOT NULL RESULT, IN SessionId string NOT NULL, IN executionId long NOT NULL);

CREATE FOREIGN PROCEDURE terminateTransaction(IN sessionid string NOT NULL);

CREATE FOREIGN PROCEDURE schemaSources(IN schemaName string NOT NULL) RETURNS TABLE (name string, resource string);
