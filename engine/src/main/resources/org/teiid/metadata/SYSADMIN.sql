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

CREATE FOREIGN PROCEDURE isLoggable(OUT loggable boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR')
OPTIONS (UPDATECOUNT 0);

CREATE FOREIGN PROCEDURE logMsg(OUT logged boolean NOT NULL RESULT, IN level string NOT NULL DEFAULT 'DEBUG', IN context string NOT NULL DEFAULT 'org.teiid.PROCESSOR', IN msg object NOT NULL)
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

CREATE VIRTUAL PROCEDURE matViewStatus(IN schemaName string NOT NULL, IN viewName string NOT NULL) RETURNS TABLE (TargetSchemaName varchar(50), TargetName varchar(50), Valid boolean, LoadState varchar(25), Updated timestamp, Cardinality long, LoadNumber long, OnErrorAction varchar(25)) AS
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
    IF ((scope IS NOT null) AND (scope = 'FULL'))
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
		EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, VARIABLES.defaultLoadNumber, VARIABLES.defaultAction FROM ' || VARIABLES.statusTableInter || critInter AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long, OnErrorAction varchar(25) USING vdbName = VARIABLES.vdbName, schemaName = matViewStatus.schemaName, viewName = matViewStatus.viewName;
    END ELSE
    BEGIN
	    EXECUTE IMMEDIATE 'SELECT TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber, VARIABLES.action FROM ' || VARIABLES.statusTable || crit AS TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long, OnErrorAction varchar(25) USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = matViewStatus.schemaName, viewName = matViewStatus.viewName;
    END
END;


CREATE VIRTUAL PROCEDURE loadMatView(IN schemaName string NOT NULL, IN viewName string NOT NULL, IN invalidate boolean NOT NULL DEFAULT 'false') RETURNS integer
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
		RAISE SQLEXCEPTION 'The view '|| VARIABLES.fullViewName || 'not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view ' || VARIABLES.fullViewName || 'is not declared as Materialized View in Metadata';
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
    IF ((scope IS NOT null) AND (scope = 'FULL'))
    BEGIN
        vdbVersion = '0';
    END
    
	DECLARE string statusTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string[] targets = (SELECT (TargetName, TargetSchemaName) from SYSADMIN.MatViews WHERE VDBName = VARIABLES.vdbName AND SchemaName = loadMatView.schemaName AND Name = loadMatView.viewName);
	DECLARE string matViewTable = array_get(targets, 1);
	DECLARE string targetSchemaName = array_get(targets, 2);

    EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'INFO', msg=>'Materialization of view ' || VARIABLES.fullViewName || ' started.');        
    
    /* matViewTable is null hints View is Internal Mat View*/
    DECLARE string tempMatViewTable = '#MAT_' || UCASE(VARIABLES.fullViewName);
    IF (matViewTable IS NULL OR matViewTable = tempMatViewTable)
    BEGIN        
        rowsUpdated = (EXECUTE SYSADMIN.refreshMatView(VARIABLES.fullViewName, loadMatView.invalidate));
        EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'INFO', msg=>'Materialization of view ' || VARIABLES.fullViewName || ' completed. Rows updated = ' || VARIABLES.rowsUpdated);        
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
	DECLARE string updateStmt = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadNumber = DVARS.LoadNumber, LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality' ||  VARIABLES.updateCriteria;

	EXECUTE IMMEDIATE 'SELECT Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber FROM ' || VARIABLES.statusTable || VARIABLES.updateCriteria AS Name string, TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = loadMatView.schemaName, viewName = loadMatView.viewName;
	
	DECLARE string previousRow = (SELECT Name FROM #load);
	IF (previousRow is null)
    BEGIN 
        EXECUTE IMMEDIATE 'INSERT INTO '|| VARIABLES.statusTable ||' (VDBName, VDBVersion, SchemaName, Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber) values (DVARS.vdbName, DVARS.vdbVersion, DVARS.schemaName, DVARS.viewName, DVARS.TargetSchemaName, DVARS.matViewTable, DVARS.valid, DVARS.loadStatus, DVARS.updated, -1, 1)' USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, targetSchemaName = VARIABLES.targetSchemaName, viewName = loadMatView.viewName, valid=false, loadStatus='LOADING', matViewTable=matViewTable, updated = now();
        VARIABLES.status = 'LOAD';
    EXCEPTION e
        DELETE FROM #load;
        EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'WARN', msg=>e.exception);
        EXECUTE IMMEDIATE 'SELECT Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber FROM ' || VARIABLES.statusTable || VARIABLES.updateCriteria AS Name string, TargetSchemaName string, TargetName string, Valid boolean, LoadState string, Updated timestamp, Cardinality long, LoadNumber long INTO #load USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName;		       
    END
    
    DECLARE long VARIABLES.loadNumber = 1;
    DECLARE boolean VARIABLES.valid = false;
	
	IF (VARIABLES.status = 'CHECK')
	BEGIN 
	    LOOP ON (SELECT valid, updated, loadstate, cardinality, loadnumber FROM #load) AS matcursor
	    BEGIN
		    IF (loadstate <> 'LOADING' OR TIMESTAMPDIFF(SQL_TSI_SECOND, matcursor.updated, now()) > (ttl/1000))
		        BEGIN 
		            EXECUTE IMMEDIATE updateStmt || ' AND loadNumber = ' || matcursor.loadNumber USING loadNumber = matcursor.loadNumber + 1, vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName, updated = now(), LoadState = 'LOADING', valid = matcursor.valid AND NOT invalidate, cardinality = matcursor.cardinality;
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
            DECLARE string columns = (SELECT cast(string_agg('"' || replace(Name, '"', '""') || '"', ',') as string) FROM SYS.Columns WHERE SchemaName = loadMatView.schemaName  AND TableName = loadMatView.viewName);
            
            IF (VARIABLES.loadNumColumn IS null)
            BEGIN
                EXECUTE IMMEDIATE 'INSERT INTO ' || matViewStageTable || '(' || columns ||') SELECT '|| columns ||' FROM ' || schemaName || '.' || viewName || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
                VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT;
            END
            ELSE
            BEGIN
                DECLARE string columnNames = '(' || columns || ', ' || VARIABLES.loadNumColumn || ')';
                DECLARE string columnValues = columns || ', ' || VARIABLES.loadNumber;
                EXECUTE IMMEDIATE 'UPSERT INTO ' || matViewStageTable || VARIABLES.columnNames || ' SELECT '|| VARIABLES.columnValues || ' FROM ' || schemaName || '.' || viewName || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
                VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT; 
                EXECUTE IMMEDIATE 'DELETE FROM ' || matViewStageTable || ' WHERE ' || VARIABLES.loadNumColumn || ' < ' || VARIABLES.loadNumber;
                VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
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
        
        EXECUTE IMMEDIATE updateStmt || ' AND loadNumber = DVARS.loadNumber' USING  loadNumber = VARIABLES.loadNumber, vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName, updated = now(), LoadState = 'LOADED', valid = true, cardinality = VARIABLES.rowsUpdated;        			
        VARIABLES.status = 'DONE';
        EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'INFO', msg=>'Materialization of view ' || VARIABLES.fullViewName || ' completed. Rows updated = ' || VARIABLES.rowsUpdated || ' Load Number = ' || VARIABLES.loadNumber);
    EXCEPTION e 
        EXECUTE IMMEDIATE updateStmt || ' AND loadNumber = DVARS.loadNumber' USING  loadNumber = VARIABLES.loadNumber, vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = schemaName, viewName = loadMatView.viewName, updated = now(), LoadState = 'FAILED_LOAD', valid = VARIABLES.valid AND NOT invalidate, cardinality = -1;
        VARIABLES.status = 'FAILED';
        VARIABLES.rowsUpdated = -3;
        EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'WARN', msg=>e.exception);
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
		RAISE SQLEXCEPTION 'The view '|| VARIABLES.fullViewName || 'not found';
	END
	
	DECLARE boolean isMaterialized = (SELECT IsMaterialized FROM SYS.Tables WHERE UID = VARIABLES.uid);
	IF (NOT isMaterialized)
	BEGIN
		RAISE SQLEXCEPTION 'The view ' || VARIABLES.fullViewName || 'is not declared as Materialized View in Metadata';
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
    IF ((scope IS NOT null) AND (scope = 'FULL'))
    BEGIN
        vdbVersion = '0';
    END
    	
    EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'INFO', msg=>'Criteria based Update of Materialization of view ' || VARIABLES.fullViewName || ' started.');
	
	DECLARE string statusTable = (SELECT "value" from SYS.Properties WHERE UID = VARIABLES.uid AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE');
	DECLARE string[] targets = (SELECT (TargetName, TargetSchemaName) from SYSADMIN.MatViews WHERE VDBName = VARIABLES.vdbName AND SchemaName = updateMatView.schemaName AND Name = updateMatView.viewName);
    DECLARE string matViewTable = array_get(targets, 1);
    DECLARE string targetSchemaName = array_get(targets, 2);

    /* matViewTable is null hints View is Internal Mat View */
    DECLARE string tempMatViewTable = '#MAT_' || UCASE(VARIABLES.fullViewName);
    IF (matViewTable IS NULL OR matViewTable = tempMatViewTable)
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
                END ELSE
                BEGIN
                    rowsUpdated = interrowUpdated;
                END
            END
            EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'INFO', msg=>'Criteria based Update of Materialization of view ' || updateMatView.schemaName || '.' || updateMatView.viewName || ' is completed.'); 	       
        EXCEPTION e
           VARIABLES.rowsUpdated = -3;
           EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'WARN', msg=>e.exception);
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
    DECLARE long loadNumber = (SELECT LoadNumber FROM #load);
    DECLARE long updatedCardinality = (SELECT Cardinality FROM #load);
    DECLARE boolean valid = (SELECT Valid FROM #load);
    IF (NOT VARIABLES.valid)
    BEGIN
        RAISE SQLEXCEPTION 'View ' || VARIABLES.fullViewName || ' contents are not in valid status to perform materialization update. Run loadMatview to reload.';
    END
	
	DECLARE string updateStmtWithCardinality = 'UPDATE ' || VARIABLES.statusTable || ' SET LoadState = DVARS.LoadState, valid = DVARS.valid, Updated = DVARS.updated, Cardinality = DVARS.cardinality, LoadNumber = DVARS.loadNumber ' ||  VARIABLES.updateCriteria;

    BEGIN ATOMIC
        DECLARE string columns = (SELECT cast(string_agg('"' || replace(Name, '"', '""') || '"', ',') as string) FROM SYS.Columns WHERE SchemaName = updateMatView.schemaName  AND TableName = updateMatView.viewName);
        IF(loadNumColumn IS null)
        BEGIN
            EXECUTE IMMEDIATE 'DELETE FROM ' || targetSchemaName || '.' || matViewTable || ' WHERE ' ||  replace(refreshCriteria, viewName, matViewTable);
            VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality - VARIABLES.ROWCOUNT;
            EXECUTE IMMEDIATE 'INSERT INTO ' || targetSchemaName || '.' || matViewTable || ' (' || columns || ') SELECT '|| columns ||' FROM '|| schemaName || '.' || viewName || ' WHERE ' || refreshCriteria || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
            VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality + VARIABLES.ROWCOUNT;
        END
        ELSE
        BEGIN 
            DECLARE string columnNames = '(' || columns || ', ' || VARIABLES.loadNumColumn || ')';
            DECLARE string columnValues = columns || ', ' || (VARIABLES.loadNumber + 1);
            EXECUTE IMMEDIATE 'UPSERT INTO ' || targetSchemaName || '.' || matViewTable || columnNames || ' SELECT ' || VARIABLES.columnValues || ' FROM '|| schemaName || '.' || viewName || ' WHERE ' || refreshCriteria || ' OPTION NOCACHE ' || schemaName || '.' || viewName;
            VARIABLES.rowsUpdated = VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality + VARIABLES.ROWCOUNT;
            EXECUTE IMMEDIATE 'DELETE FROM ' || targetSchemaName || '.' || matViewTable || ' WHERE ' || VARIABLES.loadNumColumn || ' <= ' || VARIABLES.loadNumber || ' AND ' || replace(refreshCriteria, viewName, matViewTable);
            VARIABLES.rowsUpdated = VARIABLES.rowsUpdated + VARIABLES.ROWCOUNT;
            VARIABLES.updatedCardinality = VARIABLES.updatedCardinality - VARIABLES.ROWCOUNT;
        END
        
        VARIABLES.loadNumber = VARIABLES.loadNumber +1;        
        EXECUTE IMMEDIATE updateStmtWithCardinality USING vdbName = VARIABLES.vdbName, vdbVersion = VARIABLES.vdbVersion, schemaName = updateMatView.schemaName, viewName = updateMatView.viewName, updated = now(), LoadState = 'LOADED', valid = true, cardinality = VARIABLES.updatedCardinality, loadNumber = VARIABLES.loadNumber;
        EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'INFO', msg=>'Criteria based Update of Materialization of view ' || VARIABLES.fullViewName || ' is completed. Rows updated = ' || VARIABLES.rowsUpdated || ' Load Number = ' || VARIABLES.loadNumber);                			
    EXCEPTION e 
        VARIABLES.rowsUpdated = -3;
        EXECUTE logMsg(context=>'org.teiid.MATVIEWS', level=>'WARN', msg=>e.exception);
    END    
	RETURN  rowsUpdated;
END
