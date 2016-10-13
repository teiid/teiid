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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.events.EventDistributor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.DuplicateRecordException;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.Grant;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Server;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.DatabaseStorage.PersistenceProxy;
import org.teiid.query.parser.OptionsUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.vdb.runtime.VDBKey;

public abstract class DatabaseStore {
    private LinkedHashMap<VDBKey, Database> databases = new LinkedHashMap<VDBKey, Database>();
    private Database currentDatabase;
    private Schema currentSchema;
    private ReentrantLock lock = new ReentrantLock();
    protected EventDistributor functionalStore; 
    private boolean metadataReloadRequired = false;
    protected int count;
    private PersistenceProxy persistenceProxy;
    private boolean save = false;
   
    public abstract Map<String, Datatype> getRuntimeTypes();
    public abstract Map<String, Datatype> getBuiltinDataTypes();
    
    public void startEditing() {
        this.lock.lock();
    }

    public void stopEditing() {
        deployCurrentVDB();
        this.currentDatabase = null;
        this.currentSchema = null;
        this.metadataReloadRequired = false;
        lock.unlock();
    }
    
    public void setFunctionalStore(EventDistributor store) {
        this.functionalStore = store;
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
                
        Database database = this.databases.get(vdbKey(db)); 
        if ( database != null) {
            throw new DuplicateRecordException(QueryPlugin.Event.TEIID31232,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31232, db.getName()));
        } else {
            this.metadataReloadRequired = true;
            this.databases.put(vdbKey(db), db);
            this.save = true;
        }

        // save the previous VDB if there was any.
        if (this.currentDatabase != null) {
            deployCurrentVDB();
        }
        
        this.currentDatabase = db;
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
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, dbName));                        
        }
        
        if (this.functionalStore != null) {
            try {
                functionalStore.dropDatabase(db);
            } catch (MetadataException e) {
                this.databases.put(vdbKey(db), db);
                throw e;
            }
        }
        
        // remove locally
        this.databases.remove(new VDBKey(dbName, version));
        this.save = true;
        if (this.currentDatabase != null && this.currentDatabase.getName().equalsIgnoreCase(dbName)) {
            this.currentDatabase = null;
        }

        // remove from persistence store
        if (this.persistenceProxy != null) {
            this.persistenceProxy.drop(db);
        }        
    }

    
    public void databaseSwitched(String dbName, String version) {
        assertInEditMode();

        Database db = this.databases.get(new VDBKey(dbName, version));
        if (db == null) {
            throw new MetadataException(QueryPlugin.Event.TEIID31231,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, dbName));                        
        }
        
        if (currentDatabase != null && !this.currentDatabase.equals(db)) {
            // before switching make sure the db is valid
            deployCurrentVDB();            
        }
        this.currentDatabase = db;
    }
    
    protected void deployCurrentVDB() {
        if (this.currentDatabase != null) {
        	
            TransformationMetadata metadata = new TransformationMetadata(this.currentDatabase);
            VDBMetaData vdb = metadata.getVdbMetaData();
            vdb.addAttchment(QueryMetadataInterface.class, metadata);
        	
            checkValidity(metadata, this.currentDatabase.getMetadataStore());
            
            if (this.metadataReloadRequired) {
                if (this.functionalStore != null) {
                    this.functionalStore.reloadDatabase(this.currentDatabase);
                }
                this.metadataReloadRequired = false;
            } 
            
            if (this.persistenceProxy != null && this.save) {
                this.persistenceProxy.save(this.currentDatabase);
                this.save = false;
            }
        }
    }
    
    public void schemaCreated(Schema schema, List<String> serverNames) {
        assertInEditMode();
        verifyDatabaseExists();
        setUUID(this.currentDatabase.getName(), this.currentDatabase.getVersion(), schema);
        this.currentDatabase.addSchema(schema);
        this.currentSchema = schema;
        
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
        this.save = true;
    }

    
    public void schemaDropped(String schemaName) {
        assertInEditMode();
        verifySchemaExists(schemaName);
        this.currentDatabase.removeSchema(schemaName);

        if (this.currentSchema != null && this.currentSchema.getName().equalsIgnoreCase(schemaName)) {
            this.currentSchema = null;
        }
        this.metadataReloadRequired = true;
        this.save = true;
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

    protected void verifyFunctionExists(String functionName) {        
        verifyDatabaseExists();
        Schema schema = this.getCurrentSchema();
        
        boolean found = false;
        for (FunctionMethod fm:schema.getFunctions().values()){
        	if (fm.getName().equalsIgnoreCase(functionName)){
        		found = true;
        	}
        }
        if (!found){
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31240,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31240, functionName, this.currentDatabase.getName()));        	
        }
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
        verifyDatabaseExists();
        this.currentDatabase.addDataWrapper(wrapper);

        if (this.functionalStore != null) {
            try {
                functionalStore.createDataWrapper(this.currentDatabase.getName(), this.currentDatabase.getVersion(), wrapper);
            } catch (MetadataException e) {
                this.currentDatabase.removeDataWrapper(wrapper.getName());
                throw e;
            }
        }
        this.save = true;
    }

    
    public void dataWrapperDropped(String wrapperName) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyDataWrapperExists(wrapperName);
        
        for (Server s:this.currentDatabase.getServers()) {
            if (s.getDataWrapper().equalsIgnoreCase(wrapperName)) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31225,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31225, wrapperName, s.getName()));                                                
            }
        }
        DataWrapper dw = this.currentDatabase.getDataWrapper(wrapperName);
        if (this.functionalStore != null) {
            try {
                this.functionalStore.dropDataWrapper(this.currentDatabase.getName(), this.currentDatabase.getVersion(),
                        wrapperName, dw.getType() != null);
            } catch (MetadataException e) {
                throw e;
            }
        }
        this.currentDatabase.removeDataWrapper(wrapperName);
        this.save = true;
    }
    
    public void serverCreated(Server server) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyDataWrapperExists(server.getDataWrapper());
        
        if (this.functionalStore != null) {
            try {
                functionalStore.createServer(this.currentDatabase.getName(), this.currentDatabase.getVersion(), server);
            } catch (MetadataException e) {
                throw e;
            }
        }
        this.currentDatabase.addServer(server);
        this.save = true;
    }
    
    private boolean verifyDataWrapperExists(String dataWrapperName) {
        if (this.currentDatabase.getDataWrapper(dataWrapperName) == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31214,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31214, dataWrapperName));                                
        }
        return true;
    }

    
    public void serverDropped(String serverName) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyServerExists(serverName);
        
        for (Schema s : this.currentDatabase.getSchemas()) {
            for (Server server : s.getServers()) {
                if (server.getName().equalsIgnoreCase(serverName)) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31224,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31224, serverName, s.getName()));                                    
                }
            }
        }

        Server server = this.currentDatabase.getServer(serverName);
        if (this.functionalStore != null) {
            try {
                this.functionalStore.dropServer(this.currentDatabase.getName(), this.currentDatabase.getVersion(),
                        server);
            } catch (MetadataException e) {
                throw e;
            }
        }
        this.currentDatabase.removeServer(serverName);
        this.save = true;
    }

    private void validateRecord(AbstractMetadataRecord record) throws MetadataException {
        if (this.functionalStore != null) {
            this.functionalStore.validateRecord(this.currentDatabase.getName(), this.currentDatabase.getVersion(),
                    record);
        }
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
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31212,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31212));                                    
        }
        return this.currentDatabase;
    }    
    
    public Database getDatabase(String dbName, String version) {
        VDBKey key  =  new VDBKey(dbName, version);
        Database db = this.databases.get(key);
        if (db == null) {
            throw new MetadataException(QueryPlugin.Event.TEIID31231,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, dbName, version));                        
        }
        return db;
    }

    public List<Database> getDatabases() {
        return new ArrayList<Database>(this.databases.values());
    }
    
    public void tableCreated(Table table) {
        assertInEditMode();
        Schema s = getCurrentSchema();
        setUUID(s.getUUID(), table);
        
        validateRecord(table);
        
        s.addTable(table);
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    public void tableModified(Table table) {
        assertInEditMode();
        verifyTableExists(table.getName());
        
        validateRecord(table);
        
        this.metadataReloadRequired = true;
        this.save = true;
    }    
    
    public void tableDropped(String tableName) {
        assertInEditMode();
        verifyTableExists(tableName);

        Schema s = getCurrentSchema();
        s.removeTable(tableName);
        
        this.metadataReloadRequired = true;
        this.save = true;        
    }

    public void procedureCreated(Procedure procedure) {
        assertInEditMode();
        Schema s = getCurrentSchema();
        setUUID(s.getUUID(), procedure);
        for (ProcedureParameter param : procedure.getParameters()) {
            setUUID(s.getUUID(), param);
        }
        if (procedure.getResultSet() != null) {
            setUUID(s.getUUID(), procedure.getResultSet());
        }
        
        validateRecord(procedure);
        
        s.addProcedure(procedure);
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    
    public void procedureModified(Procedure procedure) {
        assertInEditMode();
        verifyProcedureExists(procedure.getName());
        validateRecord(procedure);
        
        this.metadataReloadRequired = true;
        this.save = true;
    }     

    public void procedureDropped(String procedureName) {
        assertInEditMode();
        verifyProcedureExists(procedureName);

        Schema s = getCurrentSchema();
        s.removeProcedure(procedureName);
        
        this.metadataReloadRequired = true;
        this.save = true;        
    }
    
    public void functionCreated(FunctionMethod function) {
        assertInEditMode();
        Schema s = getCurrentSchema();
        
      	setUUID(s.getUUID(), function);
        for (FunctionParameter param : function.getInputParameters()) {
            setUUID(s.getUUID(), param);
        }
        setUUID(s.getUUID(), function.getOutputParameter());
        
        validateRecord(function);
        
        s.addFunction(function);
        this.metadataReloadRequired = true;
        this.save = true;
    }

    public void functionDropped(String functionName) {
        assertInEditMode();
        verifyFunctionExists(functionName);

        Schema s = getCurrentSchema();
        s.removeFunctions(functionName);
                
        this.metadataReloadRequired = true;
        this.save = true;        
    }
    
    public void setTableTriggerPlan(String tableName, Table.TriggerEvent event, String triggerDefinition) {
        assertInEditMode();
        Table table = getCurrentSchema().getTable(tableName);
        if (table == null || !table.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31210,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31210, tableName));                                                    
        }
        
        String previousPlan = null;
        switch(event) {
        case DELETE:
            previousPlan = table.getDeletePlan();
            table.setDeletePlan(triggerDefinition);
            break;
        case INSERT:
            previousPlan = table.getInsertPlan();
            table.setInsertPlan(triggerDefinition);
            break;
        case UPDATE:
            previousPlan = table.getUpdatePlan();
            table.setUpdatePlan(triggerDefinition);
            break;
        default:
            break;
        }
        
        if (this.functionalStore != null) {
            try {
                setTriggerEvent(event, table, triggerDefinition, functionalStore, true);
            } catch (MetadataException e) {
                setTriggerEvent(event, table, previousPlan, functionalStore, true);
                throw e;
            }
        }
        this.metadataReloadRequired = true;
        this.save = true;        
    }
    
    public void enableTableTriggerPlan(String tableName, Table.TriggerEvent event, boolean enable) {
        assertInEditMode();
        Table table = getCurrentSchema().getTable(tableName);
        if (table == null || !table.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31210,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31210, tableName));                                                    
        }
        
        String previousPlan = null;
        switch(event) {
        case DELETE:
            previousPlan = table.getDeletePlan();
            table.setDeletePlanEnabled(enable);
            break;
        case INSERT:
            previousPlan = table.getInsertPlan();
            table.setInsertPlanEnabled(enable);
            break;
        case UPDATE:
            previousPlan = table.getUpdatePlan();
            table.setUpdatePlanEnabled(enable);
            break;
        default:
            break;
        }
        
        if (this.functionalStore != null) {
            try {
                setTriggerEvent(event, table, previousPlan, functionalStore, enable);
            } catch (MetadataException e) {
                setTriggerEvent(event, table, previousPlan, functionalStore, enable);
                throw e;
            }
        }
        this.metadataReloadRequired = true;
        this.save = true;        
    }    

    private void setTriggerEvent(Table.TriggerEvent event, Table table, String triggerPlan,
            EventDistributor functionalStore, boolean enable) {
        switch(event) {
        case DELETE:
            functionalStore.setInsteadOfTriggerDefinition(this.currentDatabase.getName(),
                    this.currentDatabase.getVersion(), this.currentSchema.getName(), table.getName(),
                    Table.TriggerEvent.DELETE, triggerPlan, enable);
            break;
        case INSERT:
            functionalStore.setInsteadOfTriggerDefinition(this.currentDatabase.getName(),
                    this.currentDatabase.getVersion(), this.currentSchema.getName(), table.getName(),
                    Table.TriggerEvent.INSERT, triggerPlan, enable);
            break;
        case UPDATE:
            functionalStore.setInsteadOfTriggerDefinition(this.currentDatabase.getName(),
                    this.currentDatabase.getVersion(), this.currentSchema.getName(), table.getName(),
                    Table.TriggerEvent.UPDATE, triggerPlan, enable);
            break;
        default:
            break;
        }
    }
    
    public void createNameSpace(String prefix, String uri) {
        assertInEditMode();
        getCurrentDatabase().addNamespace(prefix, uri);
    }
    
    public Map<String, String> getNameSpaces() {
        return getCurrentDatabase().getNamespaces();
    }
    
    public void addOrSetOption(String recordName, Database.ResourceType type, String key, String value) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        record.setProperty(key, value);
        OptionsUtil.setOptions(record);
        this.metadataReloadRequired = true;
        this.save = true;
    }

    public AbstractMetadataRecord getSchemaRecord(String name, Database.ResourceType type) {
        TransformationMetadata qmi = new TransformationMetadata(getCurrentDatabase());
        try {
            switch (type) {
            case TABLE:
                GroupSymbol gs = new GroupSymbol(name);
                ResolverUtil.resolveGroup(gs, qmi);
                Table t = (Table)gs.getMetadataID();
                if (t == null) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31211,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31211, name, getCurrentDatabase().getName(),
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
                break;
            case DATABASE:
                Database db = getCurrentDatabase();
                if (db == null || !db.getName().equals(name)) {
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31231,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, name));                                
                }
                return db;
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
                    throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31214,
                            QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31214, name, getCurrentDatabase().getName()));                
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
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    
	public void addOrSetOption(String recordName, Database.ResourceType type, String childName,
			Database.ResourceType childType, String key, String value) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        if (record instanceof Table) {
            Column c = ((Table)record).getColumnByName(childName);
            if (c == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31215,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31215, childName, recordName));                                
            }
            c.setProperty(key, value);
            OptionsUtil.setOptions(c);
        } else if (record instanceof Procedure) {
            ProcedureParameter p = ((Procedure)record).getParameterByName(childName);
            if (p == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31216,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31216, childName, recordName));                
            }
            p.setProperty(key, value);
            OptionsUtil.setOptions(p);            
        }
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    
	public void removeOption(String recordName, Database.ResourceType type, String childName,
			Database.ResourceType childType, String key) {
        assertInEditMode();
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        if (record instanceof Table) {
            Column c = ((Table)record).getColumnByName(childName);
            if (c == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31215,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31215, childName, recordName));                                
            }
            OptionsUtil.removeOption(c,key);
        } else if (record instanceof Procedure) {
            ProcedureParameter p = ((Procedure)record).getParameterByName(childName);
            if (p == null) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31216,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31216, childName, recordName));                
            }
            OptionsUtil.removeOption(p, key);            
        }
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    
    public void importSchema(String schemaName, String serverName, String foreignSchemaName, List<String> includeTables,
            List<String> excludeTables, Map<String,String> properties) {
        throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31221));
    }
    
    public void importDatabase(String dbName, String version, boolean importPolicies) {
        throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31226));
    }
    
    private void checkValidity(TransformationMetadata metadata, MetadataStore metadataStore) {
    	VDBMetaData vdb = metadata.getVdbMetaData();
        LogManager.logDetail(LogConstants.CTX_METASTORE, "Database ", vdb.getName(), ":", vdb.getVersion(), " is being validated"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        ValidatorReport report = new MetadataValidator().validate(vdb, metadataStore);
        if (report.hasItems()) {
            throw new MetadataException(report.getFailureMessage());
        }       
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
        this.currentDatabase.addRole(role);
        this.save = true;
    }
    
    public void roleDropped(String roleName) {
        assertInEditMode();
        verifyRoleExists(roleName);
        
        this.currentDatabase.removeRole(roleName);
        this.save = true;
    }
    
    public void grantCreated(Grant grant) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyRoleExists(grant.getRole());
        
        for (Grant.Permission p:grant.getPermissions()) {
            AbstractMetadataRecord record = getSchemaRecord(p.getResourceName(), p.getResourceType());
            p.setResourceName(record.getFullName());
        }
        
        this.currentDatabase.addGrant(grant);
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    public void grantRevoked(Grant grant) {
        assertInEditMode();
        verifyDatabaseExists();
        verifyRoleExists(grant.getRole());
        
        for (Grant.Permission p:grant.getPermissions()) {
            AbstractMetadataRecord record = getSchemaRecord(p.getResourceName(), p.getResourceType());
            p.setResourceName(record.getFullName());
        }
        
        this.currentDatabase.revokeGrant(grant);
        this.metadataReloadRequired = true;
        this.save = true;
    }
    
    public static MetadataFactory createMF(DatabaseStore events) {
        MetadataFactory mf = new MetadataFactory(events.getCurrentDatabase().getName(), events.getCurrentDatabase().getVersion(),
                events.getCurrentSchema().getName(), events.getRuntimeTypes(), new Properties(), null);
        Map<String, String> nss = ((DatabaseStore)events).getNameSpaces();
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
    
    public void register(DatabaseStorage.PersistenceProxy proxy) {
        this.persistenceProxy = proxy;
    }
    
    public static String processDDL(DatabaseStorage storage, String dbName, String version, String schema, String ddl,
            boolean persist) throws MetadataException {
        
        StringBuilder sb = new StringBuilder();
        DatabaseStore store = storage.getStore();
        storage.startRecording(persist);
        store.startEditing();
        try {
            QueryParser parser = new QueryParser();
            if (dbName != null) {
                String str = "USE DATABASE "+SQLStringVisitor.escapeSinglePart(dbName)+" VERSION '"+version+"'";
                sb.append(str);
                logNParse(parser, store, str);
                if (schema != null) {
                    str = "USE SCHEMA "+SQLStringVisitor.escapeSinglePart(schema);
                    sb.append(";");
                    sb.append(str);
                    logNParse(parser, store, str);
                }
            }
            if (sb.length() > 0) {
                sb.append(";");
            }
            sb.append(ddl);
            logNParse(parser, store, ddl);
            return sb.toString();
        } finally {
            store.stopEditing();
            storage.stopRecording();
        }        
    }
    private static void logNParse(QueryParser parser, DatabaseStore store, String sql) {
        LogManager.logDetail(LogConstants.CTX_METASTORE, "DDL: ", sql); //$NON-NLS-1$
        parser.parseDDL(store, new StringReader(sql));
    }
}
