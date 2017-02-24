/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.query.metadata;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.*;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.Grant.Permission.Privilege;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.parser.OptionsUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.vdb.runtime.VDBKey;

/**
 * This holds the local state of all Database instances.
 */
public abstract class DatabaseStore implements DDLProcessor {
    private ConcurrentHashMap<VDBKey, Database> databases = new ConcurrentHashMap<VDBKey, Database>();
    private Database currentDatabase;
    protected Schema currentSchema;
    private ReentrantLock lock = new ReentrantLock();
    protected int count;
    private boolean persist;
    private CommandContext commandContext;
   
    public abstract Map<String, Datatype> getRuntimeTypes();
    public abstract SystemFunctionManager getSystemFunctionManager();
    
    public void startEditing(boolean persist) {
        if (this.persist) {
            throw new AssertionError();
        }
        this.lock.lock();
        this.persist = persist;
    }

    public void stopEditing() throws MetadataException {
        try {
            // no-op
        } finally {
            this.currentDatabase = null;
            this.currentSchema = null;
            this.persist = false;
            lock.unlock();
        }
    }
    
    public Schema getSchema(String schemaName) {
        verifyDatabaseExists();
        return this.currentDatabase.getSchema(schemaName);
    }

    public Server getServer(String serverName) {
        verifyDatabaseExists();
        return this.currentDatabase.getServer(serverName);
    }
    
    public void databaseCreated(Database db) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.DATABASE, db);
        
        Database database = this.databases.get(vdbKey(db)); 
        if ( database != null) {
            throw new DuplicateRecordException(QueryPlugin.Event.TEIID31232,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31232, db.getName()));
        } 
        
        if (this.currentDatabase != null) {
            throw new MetadataException(QueryPlugin.Event.TEIID31242,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31242));
        }
        db.getMetadataStore().addDataTypes(getRuntimeTypes());
        this.databases.put(vdbKey(db), db);
    }

    private VDBKey vdbKey(Database db) {
        return new VDBKey(db.getName(), db.getVersion());
    }

    private void assertInEditMode() {
        if (!this.lock.isHeldByCurrentThread()) {
            throw new MetadataException(QueryPlugin.Event.TEIID31219,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31219));                                    
        }
    }
    
    public void databaseDropped(String dbName, String version) {
        assertInEditMode();
        Database db = this.databases.get(new VDBKey(dbName,version));
        if (db == null) {
            throw new MetadataException(QueryPlugin.Event.TEIID31231,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, dbName, version));                        
        }

        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.DATABASE, db);
        
        // remove locally
        this.databases.remove(new VDBKey(dbName, version));
        if (this.currentDatabase != null && this.currentDatabase.getName().equalsIgnoreCase(dbName)) {
            this.currentDatabase = null;
        }
    }
    
    public void databaseSwitched(String dbName, String version) {
        assertInEditMode();

        Database db = this.databases.get(new VDBKey(dbName, version));
        if (db == null) {
            throw new MetadataException(QueryPlugin.Event.TEIID31231,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, dbName, version));                        
        }
        
        if (currentDatabase != null && !this.currentDatabase.equals(db)) {
            throw new MetadataException(QueryPlugin.Event.TEIID31242,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31242));
        }
        
        this.currentDatabase = db;
    }
    
    protected boolean shouldValidateDatabaseBeforeDeploy() {
    	return true;
    }
    
    public void schemaCreated(Schema schema, List<String> serverNames) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.SCHEMA, schema);
        
        verifyDatabaseExists();
        setUUID(this.currentDatabase.getName(), this.currentDatabase.getVersion(), schema);
        this.currentDatabase.addSchema(schema);
        if (schema.isPhysical()) {
            for (String serverName:serverNames) {
                Server server = verifyServerExists(serverName);
                schema.addServer(server);
            }        
        } else {
            if (!serverNames.isEmpty()) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31236,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31236, schema.getName()));                                    
            }
        }
    }

    
    public void schemaDropped(String schemaName) {
        assertInEditMode();
        verifySchemaExists(schemaName);
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.SCHEMA, this.currentDatabase.getSchema(schemaName));
        
        this.currentDatabase.removeSchema(schemaName);

        if (this.currentSchema != null && this.currentSchema.getName().equalsIgnoreCase(schemaName)) {
            this.currentSchema = null;
        }
    }

    protected Table verifyTableExists(String tableName) {        
        verifyDatabaseExists();
        Schema schema = this.getCurrentSchema();
        
        Table previous = schema.getTable(tableName);
        
        if (previous == null){
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31237,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31237, tableName, this.currentDatabase.getName()));        	
        }
        return previous;
    }
    
    
    protected Procedure verifyProcedureExists(String procedureName) {        
        verifyDatabaseExists();
        Schema schema = this.getCurrentSchema();
        
        Procedure previous = schema.getProcedure(procedureName);
        
        if (previous == null){
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31239,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31239, procedureName, this.currentDatabase.getName()));        	
        }
        return previous;
    }    

    protected FunctionMethod verifyFunctionExists(String functionName) {        
        verifyDatabaseExists();
        Schema schema = this.getCurrentSchema();
        
        for (FunctionMethod fm:schema.getFunctions().values()){
        	if (fm.getName().equalsIgnoreCase(functionName)){
        		return fm;
        	}
        }
        throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31240,
                QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31240, functionName, this.currentDatabase.getName()));        	
    }
    
    protected Schema verifySchemaExists(String schemaName) {        
        verifyDatabaseExists();
        Schema schema = this.currentDatabase.getSchema(schemaName);
        if (schema == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31234,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31234, schemaName, this.currentDatabase.getName()));                                    
        }
        return schema;
    }

    protected Server verifyServerExists(String serverName) {        
        verifyDatabaseExists();
        Server server = this.currentDatabase.getServer(serverName);
        if (server == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31220,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31220, serverName, this.currentDatabase.getName()));                                    
        }
        return server;
    }
    
    protected void verifyDatabaseExists() {
        if (this.currentDatabase == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31233,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31233));                                    
        }
    }

    protected void verifyCurrentDatabaseIsNotSame(String dbName, String version) {
        if (this.currentDatabase == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31233,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31233));                                    
        }
        if (this.currentDatabase.getName().equalsIgnoreCase(dbName)
                && this.currentDatabase.getVersion().equalsIgnoreCase(version)) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31227,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31227, dbName, version));                                                
        }
    }
    
    public void schemaSwitched(String schemaName) {
        assertInEditMode();
        this.currentSchema = verifySchemaExists(schemaName);
    }

    
    public void dataWrapperCreated(DataWrapper wrapper) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.DATAWRAPPER, wrapper);
        
        verifyDatabaseExists();
        this.currentDatabase.addDataWrapper(wrapper);
    }

    
    public void dataWrapperDropped(String wrapperName) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyDataWrapperExists(wrapperName);
        
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.DATAWRAPPER, this.currentDatabase.getDataWrapper(wrapperName));
        
        for (Server s:this.currentDatabase.getServers()) {
            if (s.getDataWrapper().equalsIgnoreCase(wrapperName)) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31225,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31225, wrapperName, s.getName()));                                                
            }
        }

        this.currentDatabase.removeDataWrapper(wrapperName);
    }
    
    public void serverCreated(Server server) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyDataWrapperExists(server.getDataWrapper());
        
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.SERVER, server);

        this.currentDatabase.addServer(server);
    }
    
    private boolean verifyDataWrapperExists(String dataWrapperName) {
        if (this.currentDatabase.getDataWrapper(dataWrapperName) == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31247,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31247, dataWrapperName));                                
        }
        return true;
    }

    
    public void serverDropped(String serverName) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyServerExists(serverName);
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.SERVER, this.currentDatabase.getServer(serverName));
        
        for (Schema s : this.currentDatabase.getSchemas()) {
            for (Server server : s.getServers()) {
                if (server.getName().equalsIgnoreCase(serverName)) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31224,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31224, serverName, s.getName()));                                    
                }
            }
        }
        this.currentDatabase.removeServer(serverName);
    }

    protected Schema getCurrentSchema() {
        if (this.currentSchema == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31235,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31235));                                    
        }
        return this.currentSchema;
    }
    
    protected Database getCurrentDatabase() {
        if (this.currentDatabase == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31246,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31246));                                    
        }
        return this.currentDatabase;
    }    
    
    public Database getDatabase(String dbName, String version) {
        VDBKey key  =  new VDBKey(dbName, version);
        return this.databases.get(key);
    }

    public List<Database> getDatabases() {
        return new ArrayList<Database>(this.databases.values());
    }
    
    public void tableCreated(Table table) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.TABLE, table);
        
        Schema s = getCurrentSchema();
        setUUID(s.getUUID(), table);
        
        s.addTable(table);
    }
    
    public void setViewDefinition(final String tableName, final String definition, boolean updateFunctional) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.ALTER, Database.ResourceType.TABLE, getCurrentSchema().getTable(tableName));
        verifyTableExists(tableName).setSelectTransformation(definition);
    }
    
    public void tableModified(Table table) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
        
        verifyTableExists(table.getName());
    }    
    
    public void tableDropped(String tableName) {
        assertInEditMode();
        verifyTableExists(tableName);
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.TABLE, getCurrentSchema().getTable(tableName));
        
        Schema s = getCurrentSchema();
        s.removeTable(tableName);
    }

    public void procedureCreated(Procedure procedure) {
        assertInEditMode();
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.PROCEDURE, procedure);
        
        Schema s = getCurrentSchema();
        setUUID(s.getUUID(), procedure);
        for (ProcedureParameter param : procedure.getParameters()) {
            setUUID(s.getUUID(), param);
        }
        if (procedure.getResultSet() != null) {
            setUUID(s.getUUID(), procedure.getResultSet());
        }
        
        s.addProcedure(procedure);
    }
    
    public void setProcedureDefinition(final String procedureName, final String definition, boolean updateFunctional) {
        assertInEditMode();
        Procedure procedure = verifyProcedureExists(procedureName);
        
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.PROCEDURE, procedure);
        
        procedure.setQueryPlan(definition);
    }
    
    public void procedureModified(Procedure procedure) {
        assertInEditMode();
        verifyProcedureExists(procedure.getName());
        assertGrant(Grant.Permission.Privilege.ALTER, Database.ResourceType.PROCEDURE, procedure);
    }     

    public void procedureDropped(String procedureName) {
        assertInEditMode();
        verifyProcedureExists(procedureName);
		assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.PROCEDURE,
				getCurrentSchema().getProcedure(procedureName));

        Schema s = getCurrentSchema();
        s.removeProcedure(procedureName);
    }
    
    public void functionCreated(FunctionMethod function) {
        assertInEditMode();
		assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.FUNCTION, function);
        
        Schema s = getCurrentSchema();
        
      	setUUID(s.getUUID(), function);
        for (FunctionParameter param : function.getInputParameters()) {
            setUUID(s.getUUID(), param);
        }
        setUUID(s.getUUID(), function.getOutputParameter());
        
        s.addFunction(function);
    }

    public void functionDropped(String functionName) {
        assertInEditMode();
        FunctionMethod fm = verifyFunctionExists(functionName);
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.FUNCTION, fm);

        Schema s = getCurrentSchema();
        s.removeFunctions(functionName);
    }
    
    public void setTableTriggerPlan(final String triggerName, final String tableName, final Table.TriggerEvent event,
            final String triggerDefinition, boolean isAfter) {
        assertInEditMode();
        Table table = getCurrentSchema().getTable(tableName);
        if (table == null) {
            throw new MetadataException(QueryPlugin.Util.getString("SQLParser.group_doesnot_exist", tableName)); //$NON-NLS-1$
        }
        assertGrant(Grant.Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
        if (!table.isVirtual()) {
            if (!isAfter) {
                throw new MetadataException(QueryPlugin.Util.getString("SQLParser.not_view", tableName)); //$NON-NLS-1$
            }
            if (triggerName == null) {
                throw new MetadataException(QueryPlugin.Event.TEIID31213, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31213));
            } else if (table.getTriggers().containsKey(triggerName)) {
                throw new DuplicateRecordException(QueryPlugin.Event.TEIID31212, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31212, triggerName, table.getFullName()));
            }
            //add a named trigger
            Trigger t = new Trigger();
            t.setName(triggerName);
            t.setEvent(event);
            t.setPlan(triggerDefinition);
            //TODO: uid
            table.getTriggers().put(triggerName, t);
            return;
        } 
        if (isAfter) {
            throw new MetadataException(QueryPlugin.Util.getString("SQLParser.view_not_allowed", tableName)); //$NON-NLS-1$
        }
        if (event.equals(Table.TriggerEvent.INSERT)) {
            table.setInsertPlan(triggerDefinition);
        }
        else if (event.equals(Table.TriggerEvent.UPDATE)) {
            table.setUpdatePlan(triggerDefinition);
        }
        else if (event.equals(Table.TriggerEvent.DELETE)) {
            table.setDeletePlan(triggerDefinition);
        }        
    }
    
	public void enableTableTriggerPlan(final String tableName, final Table.TriggerEvent event, final boolean enable,
			boolean updateFunctional) {
        assertInEditMode();
        Table table = getCurrentSchema().getTable(tableName);
        if (table == null || !table.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31244,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31244, tableName));                                                    
        }
        assertGrant(Grant.Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
        
        switch(event) {
        case DELETE:
            table.setDeletePlanEnabled(enable);
            break;
        case INSERT:
            table.setInsertPlanEnabled(enable);
            break;
        case UPDATE:
            table.setUpdatePlanEnabled(enable);
            break;
        default:
            break;
        }
    }    

    public void createNameSpace(String prefix, String uri) {
        assertInEditMode();
        getCurrentDatabase().addNamespace(prefix, uri);
    }
    
    public void createDomain(String name, String baseType, Integer length, Integer scale, boolean notNull) {
        assertInEditMode();
        Datatype dt = getCurrentDatabase().addDomain(name, baseType, length, scale, notNull);
        setUUID(getCurrentDatabase().getUUID(), dt);
    }
    
    public Map<String, String> getNameSpaces() {
        return getCurrentDatabase().getNamespaces();
    }
    
    public void addOrSetOption(String recordName, Database.ResourceType type, String key, String value, boolean reload) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        record.setProperty(key, value);
        OptionsUtil.setOptions(record);
    }

    public AbstractMetadataRecord getSchemaRecord(String name, Database.ResourceType type) {
        Database database = getCurrentDatabase();
        
        CompositeMetadataStore store = new CompositeMetadataStore(database.getMetadataStore());
        //grants are already stored on the VDBMetaData
        store.getGrants().clear();
        TransformationMetadata qmi = new TransformationMetadata(DatabaseUtil.convert(database), store, null,
                getSystemFunctionManager().getSystemFunctions(), null);

        try {
            switch (type) {
            case TABLE:
                GroupSymbol gs = new GroupSymbol(name);
                ResolverUtil.resolveGroup(gs, qmi);
                Table t = (Table)gs.getMetadataID();
                if (t == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31245,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31245, name, getCurrentDatabase().getName(),
                                    getCurrentSchema().getName()));
                }
                return t;
            case PROCEDURE:
                StoredProcedureInfo sp = qmi.getStoredProcedureInfoForProcedure(name);
                if (sp == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31213,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31213, name, getCurrentSchema().getName(),
                                    getCurrentDatabase().getName()));                
                }
                if (sp.getProcedureID() instanceof Procedure) {
                    return (Procedure)sp.getProcedureID();
                }                
                return null;
            case COLUMN:
                Column c = qmi.getElementID(name);
                if (c == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31223,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31223, name));                                    
                }
                return c;
            case DATABASE:
                return getCurrentDatabase();
            case SCHEMA:
                Schema schema = qmi.getModelID(name);
                if (schema == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31234,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31234, name, getCurrentDatabase().getName()));                
                }
                return schema;
            case SERVER:
                Server server = getCurrentDatabase().getServer(name);
                if (server == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31220,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31220, name, getCurrentDatabase().getName()));                
                }
                return server;
            case DATAWRAPPER:
                DataWrapper dw = getCurrentDatabase().getDataWrapper(name);
                if (dw == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31247,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31247, name, getCurrentDatabase().getName()));                
                }
                return dw;
            default:
                break;
            }
        } catch (TeiidComponentException e) {
            throw new MetadataException(e);
        } catch (QueryResolverException e) {
            throw new MetadataException(e);
        }
        return null;
    }

    
    public void removeOption(String recordName, Database.ResourceType type, String key) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        OptionsUtil.removeOption(record, key);
    }
    
    
	public void addOrSetOption(String recordName, Database.ResourceType type, String childName,
			Database.ResourceType childType, String key, String value, boolean reload) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        if (record instanceof Table) {
            Column c = ((Table)record).getColumnByName(childName);
            if (c == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31248,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31248, childName, recordName));                                
            }
            c.setProperty(key, value);
            OptionsUtil.setOptions(c);
        } else if (record instanceof Procedure) {
            ProcedureParameter p = ((Procedure)record).getParameterByName(childName);
            if (p == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31249,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31249, childName, recordName));                
            }
            p.setProperty(key, value);
            OptionsUtil.setOptions(p);            
        }
    }
    
    
	public void removeOption(String recordName, Database.ResourceType type, String childName,
			Database.ResourceType childType, String key) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        if (record instanceof Table) {
            Column c = ((Table)record).getColumnByName(childName);
            if (c == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31248,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31248, childName, recordName));                                
            }
            OptionsUtil.removeOption(c,key);
        } else if (record instanceof Procedure) {
            ProcedureParameter p = ((Procedure)record).getParameterByName(childName);
            if (p == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31249,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31249, childName, recordName));                
            }
            OptionsUtil.removeOption(p, key);            
        }
    }
    
    
    public void importSchema(String schemaName, String serverType, String serverName, String foreignSchemaName, List<String> includeTables,
            List<String> excludeTables, Map<String,String> properties) {
        throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31221));
    }
    
    public void importDatabase(String dbName, String version, boolean importPolicies) {
        throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31226));
    }
    
    protected Role verifyRoleExists(String roleName) {        
        verifyDatabaseExists();
        Role role = this.currentDatabase.getRole(roleName);
        if (role == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31222,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31222, roleName, this.currentDatabase.getName()));                                    
        }
        return role;
    }
    
    public void roleCreated(Role role) {
        assertInEditMode();
        verifyDatabaseExists();
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.ROLE, role);
        
        this.currentDatabase.addRole(role);
    }
    
    public void roleDropped(String roleName) {
        assertInEditMode();
        Role role = verifyRoleExists(roleName);
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.ROLE, role);
        
        this.currentDatabase.removeRole(roleName);
    }
    
	public void grantCreated(Grant grant) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyRoleExists(grant.getRole());
        assertGrant(Grant.Permission.Privilege.CREATE, Database.ResourceType.GRANT, grant);
        
        for (Grant.Permission p:grant.getPermissions()) {
            if (p.getResourceType() == ResourceType.LANGUAGE) {
                continue;
            }
            AbstractMetadataRecord record = getSchemaRecord(p.getResourceName(), p.getResourceType());
            p.setResourceName(record.getFullName());
        }
        
        this.currentDatabase.addGrant(grant);
    }
    
    public void grantRevoked(Grant grant) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyRoleExists(grant.getRole());
        assertGrant(Grant.Permission.Privilege.DROP, Database.ResourceType.GRANT, grant);
        
        for (Grant.Permission p:grant.getPermissions()) {
            if (p.getResourceType() == ResourceType.LANGUAGE) {
                continue;
            }
            AbstractMetadataRecord record = getSchemaRecord(p.getResourceName(), p.getResourceType());
            p.setResourceName(record.getFullName());
        }
        
        this.currentDatabase.revokeGrant(grant);
    }
    
    public static MetadataFactory createMF(DatabaseStore events) {
        MetadataFactory mf = new MetadataFactory(events.getCurrentDatabase().getName(), events.getCurrentDatabase().getVersion(),
                events.getCurrentSchema().getName(), events.getCurrentDatabase().getMetadataStore().getDatatypes(), new Properties(), null);
        Map<String, String> nss = events.getNameSpaces();
        for (String key:nss.keySet()) {
            mf.addNamespace(key, nss.get(key));    
        }
        return mf;
    }
    
    private void setUUID(String prefix, AbstractMetadataRecord record) {
        // it is possible that metadata about UUID is already set.
        if (record.getUUID() != null && !record.getUUID().startsWith("tid:")) {
            return;
        }
        
        int lsb = 0;
        if (record.getParent() != null) {
            lsb  = record.getParent().getUUID().hashCode();
        }
        lsb = 31*lsb + record.getName().hashCode();
        String uuid = prefix+"-"+MetadataFactory.hex(lsb, 8) + "-" + MetadataFactory.hex(this.count++, 8); //$NON-NLS-1$ //$NON-NLS-2$
        record.setUUID(uuid);
    }    
    
    private long longHash(String s, long h) {
        if (s == null) {
            return h;
        }
        for (int i = 0; i < s.length(); i++) {
            h = 31*h + s.charAt(i);
        }
        return h;
    }
    
    private void setUUID(String vdbName, String vdbVersion, Schema schema) {
        long msb = longHash(vdbName, 0);
        try {
            //if this is just an int, we'll use the old style hash
            int val = Integer.parseInt(vdbVersion);
            msb = 31*msb + val;
        } catch (NumberFormatException e) {
            msb = 31*msb + this.currentDatabase.getVersion().hashCode();
        }
        msb = longHash(schema.getName(), msb);
        schema.setUUID("tid:" + MetadataFactory.hex(msb, 12)); //$NON-NLS-1$        
    }
    
    @Override
    public boolean vdbExists(String dbName, String version) {
        return getDatabase(dbName, version) != null;
    }
    
    @Override
	public String processDDL(String dbName, String version, String schema, String ddl, boolean persist,
			CommandContext commandContext) throws MetadataException {
        
        StringBuilder sb = new StringBuilder();
        startEditing(persist);
        try {
        	this.commandContext = commandContext;
            QueryParser parser = QueryParser.getQueryParser();
            if (dbName != null) {
                String str = "USE DATABASE "+SQLStringVisitor.escapeSinglePart(dbName)+" VERSION "+new Constant(version); //$NON-NLS-1$ //$NON-NLS-2$
                sb.append(str);
                logNParse(parser, this, str);
                if (schema != null) {
                    str = "SET SCHEMA "+SQLStringVisitor.escapeSinglePart(schema);  //$NON-NLS-1$
                    sb.append(";"); //$NON-NLS-1$
                    sb.append(str);
                    logNParse(parser, this, str);
                }
            }
            if (sb.length() > 0) {
                sb.append(";"); //$NON-NLS-1$
            }
            sb.append(ddl);
            logNParse(parser, this, ddl);
            return sb.toString();
        } finally {
        	this.commandContext = null;
            stopEditing();
        }        
    }
    private static void logNParse(QueryParser parser, DatabaseStore store, String sql) {
        LogManager.logDetail(LogConstants.CTX_METASTORE, "DDL: ", sql); //$NON-NLS-1$
        parser.parseDDL(store, new StringReader(sql));
    }
    
	private void assertGrant(Privilege allowence, ResourceType type, AbstractMetadataRecord record) {
		/*if (this.commandContext != null) {
			AuthorizationValidator validator = this.commandContext.getAuthorizationValidator();
			String[] resources = new String[] { record.getName() };

			if (validator.allowDDLEvent(this.commandContext, allowence, type, record)) {
				AuditMessage msg = new AuditMessage(allowence.name(), "ddl execution-granted", resources, //$NON-NLS-1$
						commandContext);
				LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);
			} else {
				AuditMessage msg = new AuditMessage(allowence.name(), "ddl execution-denied", resources, //$NON-NLS-1$
						commandContext);
				LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, msg);

				throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31243,
						QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31243, allowence.name(), type.name(),
								record.getName(), this.commandContext.getUserName()));
			}
		}*/
	}    
}
