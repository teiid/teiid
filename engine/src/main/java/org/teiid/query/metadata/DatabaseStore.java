/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.query.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.connector.DataPlugin;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.*;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.Permission.Privilege;
import org.teiid.metadata.Table.Type;
import org.teiid.query.QueryPlugin;
import org.teiid.query.parser.OptionsUtil;
import org.teiid.query.parser.SQLParserUtil;
import org.teiid.query.parser.SQLParserUtil.ParsedDataType;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.vdb.runtime.VDBKey;

/**
 * This holds the local state of all Database instances.
 */
public abstract class DatabaseStore {
    public static enum Mode {ANY, DOMAIN, SCHEMA, DATABASE_STRUCTURE}

    private ConcurrentHashMap<VDBKey, Database> databases = new ConcurrentHashMap<VDBKey, Database>();
    private Database currentDatabase;
    protected Schema currentSchema;
    private ReentrantLock lock = new ReentrantLock();
    protected int count;
    private boolean persist;
    private Mode mode = Mode.ANY;
    private boolean seenOther;
    private boolean strict;

    public abstract Map<String, Datatype> getRuntimeTypes();

    public void startEditing(boolean persist) {
        if (this.persist) {
            throw new IllegalStateException();
        }
        this.lock.lock();
        this.persist = persist;
        this.mode = Mode.ANY;
        this.strict = false;
        this.seenOther = false;
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
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.DATABASE, db);

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

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    protected boolean assertInEditMode(Mode current) {
        if (!this.lock.isHeldByCurrentThread()) {
            throw new MetadataException(QueryPlugin.Event.TEIID31219,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31219));
        }
        if (mode == current || mode == Mode.ANY || current == Mode.ANY) {
            if (seenOther) {
                throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31257, current));
            }
            return true;
        }
        switch (mode) {
        case DATABASE_STRUCTURE:
            if (current == Mode.DOMAIN) {
                if (seenOther) {
                    throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31257, current));
                }
                return true;
            }
            break;
        case SCHEMA:
            if (!strict) {
                return false;
            }
            break;
        default:
            break;
        }
        if (strict) {
            throw new MetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31258, mode));
        }
        seenOther = true;
        //don't process
        return false;
    }

    /*public void databaseDropped(String dbName, String version) {
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
    }*/

    public void databaseSwitched(String dbName, String version) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }

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
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.SCHEMA, schema);

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

    public void schemaDropped(String schemaName, Boolean virtual) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        Schema s = verifySchemaExists(schemaName);
        if (virtual != null && !virtual ^ s.isPhysical()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31273,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31273, s.getName()));
        }
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.SCHEMA, s);

        this.currentDatabase.removeSchema(schemaName);

        if (this.currentSchema != null && this.currentSchema.getName().equalsIgnoreCase(schemaName)) {
            this.currentSchema = null;
        }
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
        Schema schema = verifySchemaExists(schemaName);
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        this.currentSchema = schema;
    }

    public void dataWrapperCreated(DataWrapper wrapper) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.DATAWRAPPER, wrapper);

        verifyDatabaseExists();
        this.currentDatabase.addDataWrapper(wrapper);
    }

    public void dataWrapperDropped(String wrapperName) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        verifyDatabaseExists();
        verifyDataWrapperExists(wrapperName);

        assertGrant(Permission.Privilege.DROP, Database.ResourceType.DATAWRAPPER, this.currentDatabase.getDataWrapper(wrapperName));

        for (Server s:this.currentDatabase.getServers()) {
            if (s.getDataWrapper().equalsIgnoreCase(wrapperName)) {
                throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31225,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31225, wrapperName, s.getName()));
            }
        }

        this.currentDatabase.removeDataWrapper(wrapperName);
    }

    public void serverCreated(Server server) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        verifyDatabaseExists();
        verifyDataWrapperExists(server.getDataWrapper());

        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.SERVER, server);

        this.currentDatabase.addServer(server);
    }

    private boolean verifyDataWrapperExists(String dataWrapperName) {
        //we'll let this be a deploy time validation
        /*if (this.currentDatabase.getDataWrapper(dataWrapperName) == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31247,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31247, dataWrapperName));
        }*/
        return true;
    }


    public void serverDropped(String serverName) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        verifyDatabaseExists();
        verifyServerExists(serverName);
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.SERVER, this.currentDatabase.getServer(serverName));

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
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.TABLE, table);

        Schema s = getCurrentSchema();
        setUUID(s.getUUID(), table);

        if (table.isVirtual() && table.getSelectTransformation() == null && table.getTableType() != Type.TemporaryTable) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31272,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31272, table.getFullName()));
        }

        s.addTable(table);
    }

    public void setViewDefinition(final String tableName, final String definition) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Table table = (Table)getSchemaRecord(tableName, ResourceType.TABLE);
        if (!table.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31238,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31238, table.getFullName()));
        }
        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
        table.setSelectTransformation(definition);
    }

    public void modifyTableName(String name, Database.ResourceType type, String newName) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Table table = (Table)getSchemaRecord(name, type);
        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
        Schema s = table.getParent();
        if (s.getTable(newName) != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60013, DataPlugin.Util.gs(DataPlugin.Event.TEIID60013, newName));
        }
        s.getTables().remove(table.getName());
        table.setName(newName);
        s.getTables().put(newName, table);
    }

    public void removeColumn(String objectName, Database.ResourceType type, String childName) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Table table = (Table)getSchemaRecord(objectName, type);
        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);

        Column column = table.getColumnByName(childName);
        if (column == null) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31223,
                QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31223, childName));
        }
        table.removeColumn(column);
    }

    public void tableDropped(String tableName, boolean globalTemp, boolean view) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Table table = (Table)getSchemaRecord(tableName, ResourceType.TABLE);
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.TABLE, table);

        if (!(globalTemp ^ (table.getTableType() != Type.TemporaryTable))) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31273,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31273, table.getFullName()));
        }
        if (view ^ table.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31273,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31273, table.getFullName()));
        }

        Schema s = table.getParent();
        s.removeTable(tableName);
    }

    public void procedureCreated(Procedure procedure) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.PROCEDURE, procedure);

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

    public void setProcedureDefinition(final String procedureName, final String definition) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Procedure procedure = (Procedure)getSchemaRecord(procedureName, ResourceType.PROCEDURE);

        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.PROCEDURE, procedure);

        if (!procedure.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31238,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31238, procedure.getFullName()));
        }

        procedure.setQueryPlan(definition);
    }

    public void procedureDropped(String procedureName, Boolean virtual) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Procedure procedure = (Procedure)getSchemaRecord(procedureName, ResourceType.PROCEDURE);

        if (virtual != null && virtual^procedure.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31273,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31273, procedure.getFullName()));
        }

        assertGrant(Permission.Privilege.DROP, Database.ResourceType.PROCEDURE,
                procedure);

        Schema s = procedure.getParent();
        s.removeProcedure(procedureName);
    }

    public void functionCreated(FunctionMethod function) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.FUNCTION, function);

        Schema s = getCurrentSchema();

          setUUID(s.getUUID(), function);
        for (FunctionParameter param : function.getInputParameters()) {
            setUUID(s.getUUID(), param);
        }
        setUUID(s.getUUID(), function.getOutputParameter());

        s.addFunction(function);
    }

    public void functionDropped(String functionName, Boolean virtual) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        FunctionMethod fm = verifyFunctionExists(functionName);
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.FUNCTION, fm);

        Schema s = getCurrentSchema();
        s.removeFunctions(functionName);
    }

    public void setTableTriggerPlan(final String triggerName, final String tableName, final Table.TriggerEvent event,
            final String triggerDefinition, boolean isAfter) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Table table = (Table)getSchemaRecord(tableName, ResourceType.TABLE);
        if (table == null) {
            throw new MetadataException(QueryPlugin.Util.getString("SQLParser.group_doesnot_exist", tableName)); //$NON-NLS-1$
        }
        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
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

    public void enableTableTriggerPlan(final String tableName, final Table.TriggerEvent event, final boolean enable) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        Table table = (Table) getSchemaRecord(tableName, ResourceType.TABLE);
        if (table == null || !table.isVirtual()) {
            throw new org.teiid.metadata.MetadataException(QueryPlugin.Event.TEIID31244,
                    QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31244, tableName));
        }
        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);

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
        if (!assertInEditMode(Mode.ANY)) {
            return;
        }
        getCurrentDatabase().addNamespace(prefix, uri);
    }

    public void createDomain(String name, String baseType, Integer length, Integer scale, boolean notNull) {
        if (!assertInEditMode(Mode.DOMAIN)) {
            return;
        }
        Datatype dt = getCurrentDatabase().addDomain(name, baseType, length, scale, notNull);
        setUUID(getCurrentDatabase().getUUID(), dt);
    }

    public void addOrSetOption(String recordName, Database.ResourceType type, String key, String value, boolean reload) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        record.setProperty(key, value);
        OptionsUtil.setOptions(record);
    }

    public AbstractMetadataRecord getSchemaRecord(String name, Database.ResourceType type) {
        TransformationMetadata qmi = getTransformationMetadata();
        try {
            switch (type) {
            case TABLE:
                GroupSymbol gs = new GroupSymbol(name);
                ResolverUtil.resolveGroup(gs, qmi);
                return (Table)gs.getMetadataID();
            case PROCEDURE:
                StoredProcedureInfo sp = qmi.getStoredProcedureInfoForProcedure(name);
                return (Procedure)sp.getProcedureID();
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
                throw new AssertionError();
            }
        } catch (TeiidComponentException e) {
            throw new MetadataException(e);
        } catch (QueryResolverException e) {
            throw new MetadataException(e);
        }
    }

    protected TransformationMetadata getTransformationMetadata() {
        throw new IllegalStateException("TransformationMetadata not yet available"); //$NON-NLS-1$
    }

    public void removeOption(String recordName, Database.ResourceType type, String key) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        key = getCurrentDatabase().resolveNamespaceInPropertyKey(key);
        AbstractMetadataRecord record = getSchemaRecord(recordName, type);
        OptionsUtil.removeOption(record, key);
    }

    public void addOrSetOption(String recordName, Database.ResourceType type, String childName,
            Database.ResourceType childType, String key, String value, boolean reload) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
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
        } else {
            //exception
        }
    }


    public void removeOption(String recordName, Database.ResourceType type, String childName,
            Database.ResourceType childType, String key) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
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
        } else {
            //exception
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
        return this.currentDatabase.findRole(roleName);
    }

    public void roleCreated(Role role) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        verifyDatabaseExists();
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.ROLE, role);

        this.currentDatabase.addRole(role);
    }

    public void roleDropped(String roleName) {
        if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
            return;
        }
        Role role = verifyRoleExists(roleName);
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.ROLE, role);

        this.currentDatabase.removeRole(roleName);
    }

    public void grantCreated(Grant grant) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        verifyDatabaseExists();
        verifyRoleExists(grant.getRole());
        assertGrant(Permission.Privilege.CREATE, Database.ResourceType.GRANT, grant);

        this.currentDatabase.addGrant(grant);
    }

    public void grantRevoked(Grant grant) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        verifyDatabaseExists();
        verifyRoleExists(grant.getRole());
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.GRANT, grant);

        this.currentDatabase.revokeGrant(grant);
    }

    public void policyCreated(String roleName, Policy policy) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        verifyDatabaseExists();
        verifyRoleExists(roleName);
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.POLICY, policy);

        this.currentDatabase.addPolicy(roleName, policy);
    }

    public void policyDropped(String roleName, Policy policy) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
        verifyDatabaseExists();
        verifyRoleExists(roleName);
        assertGrant(Permission.Privilege.DROP, Database.ResourceType.POLICY, policy);

        this.currentDatabase.removePolicy(roleName, policy);
    }

    public void renameBaseColumn(String objectName, Database.ResourceType type, String oldName, String newName) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }

        MetadataFactory factory = DatabaseStore.createMF(this);
        switch (type) {
        case TABLE:
            Table table = (Table)getSchemaRecord(objectName, type);
            assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
            factory.renameColumn(oldName, newName, table);
            break;
        case PROCEDURE:
            Procedure proc = (Procedure)getSchemaRecord(objectName, type);
            assertGrant(Permission.Privilege.ALTER, Database.ResourceType.PROCEDURE, proc);
            factory.renameParameter(oldName, newName, proc);
            break;
        default:
            throw new IllegalArgumentException("invalid type"); //$NON-NLS-1$
        }
    }

    public void alterBaseColumn(String objectName, Database.ResourceType type, String childName, ParsedDataType datatype, boolean autoIncrement, boolean notNull) {
        MetadataFactory factory = DatabaseStore.createMF(this);
        BaseColumn column = null;
        if (type == Database.ResourceType.TABLE){
            Table table = (Table)getSchemaRecord(objectName, type);
            assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
            column = table.getColumnByName(childName);
            if (column == null){
                throw new ParseException(QueryPlugin.Util.getString("SQLParser.no_table_column_found", childName, table.getName())); //$NON-NLS-1$
            }
        } else {
            Procedure proc = (Procedure)getSchemaRecord(objectName, type);
            assertGrant(Permission.Privilege.ALTER, Database.ResourceType.PROCEDURE, proc);
            column = proc.getParameterByName(childName);
            if (column == null){
                throw new ParseException(QueryPlugin.Util.getString("SQLParser.no_proc_column_found", childName, proc.getName())); //$NON-NLS-1$
            }
        }
        MetadataFactory.setDataType(datatype.getType(), column, factory.getDataTypes(), notNull);
        SQLParserUtil.setTypeInfo(datatype, column);
        if (notNull) {
           column.setNullType(Column.NullType.No_Nulls);
        }
        if (type == Database.ResourceType.TABLE){
            //must be called after setDataType as that will pull the defaults
            ((Column)column).setAutoIncremented(autoIncrement);
        }
    }

    public static MetadataFactory createMF(DatabaseStore events, Schema schema, boolean useSchema, Properties modelProperties) {
        MetadataFactory mf = new MetadataFactory(events.getCurrentDatabase().getName(), events.getCurrentDatabase().getVersion(),
                schema==null?"undefined":schema.getName(), events.getCurrentDatabase().getMetadataStore().getDatatypes(), modelProperties, null); //$NON-NLS-1$
        if (useSchema && schema != null) {
            mf.setSchema(schema);
        }
        return mf;
    }

    public static MetadataFactory createMF(DatabaseStore events) {
        //it's possible that the schema won't be set as we call this from the parsing context - statement will effectively be later ignored
        return createMF(events, events.currentSchema, false, new Properties());
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

    public Table getTableForCreateColumn(String objectName, ResourceType type) {
        if (!assertInEditMode(Mode.SCHEMA)) {
            return new Table(); //return a dummy table;
        }

        Table table = (Table)getSchemaRecord(objectName, type);
        assertGrant(Permission.Privilege.ALTER, Database.ResourceType.TABLE, table);
        return table;
    }

    /**
     * Get the NamespaceContainer associated with the current database
     * - this instance should not be modified unless in an edit context.
     */
    public NamespaceContainer getCurrentNamespaceContainer() {
        return getCurrentDatabase().getNamespaceContainer();
    }
}
