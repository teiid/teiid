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
package org.teiid.metadatastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Grant;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Server;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.NativeMetadataRepository;
import org.teiid.query.metadata.TransformationMetadata;

public class DefaultDatabaseStore extends DatabaseStore {
    private VDBRepository vdbRepo;
    private ExecutionFactoryProvider efp;
    private ConnectorManagerRepository cmr;
    
    @Override
    public Map<String, Datatype> getRuntimeTypes() {
        return vdbRepo.getRuntimeTypeMap();
    }
    @Override
    public Map<String, Datatype> getBuiltinDataTypes() {
        return vdbRepo.getSystemStore().getDatatypes();
    } 

    public void setVDBRepository(VDBRepository repo) {
        this.vdbRepo = repo;
    }
    
    public void setExecutionFactoryProvider(ExecutionFactoryProvider efp) {
        this.efp = efp;
    }
    
    public void setConnectorManagerRepository(ConnectorManagerRepository cmr) {
        this.cmr = cmr;
    }

    @Override
    public void importSchema(String schemaName, String serverName, String foreignSchemaName, List<String> includeTables,
            List<String> excludeTables, Map<String,String> properties) {
        
        verifySchemaExists(schemaName);
        verifyServerExists(serverName);
        schemaSwitched(schemaName);
        
        Schema schema = getSchema(schemaName);
        Server server = getServer(serverName);
        
        MetadataFactory mf = DatabaseStore.createMF(this);
        NativeMetadataRepository nmr = new NativeMetadataRepository();
        
        mf.getModelProperties().put("importer.schemaPattern", foreignSchemaName);
        
        if (excludeTables != null && !excludeTables.isEmpty()) {
            mf.getModelProperties().put("importer.excludeTables", getCSV(excludeTables));
        }

        // TODO: need to add this to jdbc translator
        if (includeTables != null && !includeTables.isEmpty()) {
            mf.getModelProperties().put("importer.includeTables", getCSV(includeTables));    
        }
        
        if (schema.getProperties() != null) {
        	mf.getModelProperties().putAll(schema.getProperties());
        }
        if (properties != null) {
        	mf.getModelProperties().putAll(properties);
        }
        
        // can not retry, as this needs to be inline, the user needs to retry if fails. This is considered 
        // to be conversational, not happening at deployment time
        ConnectorManager cm = this.cmr.getConnectorManager(serverName);
        try {
            nmr.loadMetadata(mf, this.efp.getExecutionFactory(server.getDataWrapper()), cm.getConnectionFactory());
        } catch (Exception e) {

            throw new MetadataException(e);
        }
        
        importSchema(mf.getSchema());
    }
    
    private void importSchema(Schema schema) {
        for (Table t:schema.getTables().values()) {
            tableCreated(t);
        }
        
        for(Procedure p:schema.getProcedures().values()) {
            procedureCreated(p);
        }

        for (FunctionMethod fm:schema.getFunctions().values()) {
            functionCreated(fm);
        }
    }
    
    private String getCSV(List<String> strings) {        
        StringBuilder sb = new StringBuilder();
        if (strings != null && !strings.isEmpty()) {
            for (String str:strings) {
                if (sb.length() > 0) {
                    sb.append(",");                    
                }
                sb.append(str);
            }
        }
        return sb.toString();
    }
    @Override
    public void importDatabase(String dbName, String version, boolean importPolicies) {
        verifyCurrentDatabaseIsNotSame(dbName, version);
        
        Database from = getDatabase(dbName, version);
        if (from == null) {
            VDBMetaData fromVDB = this.vdbRepo.getVDB(dbName, version);
            MetadataStore fromMetadataStore = null;
            if (fromVDB != null) {
                fromMetadataStore = fromVDB.getAttachment(TransformationMetadata.class).getMetadataStore();    
            }
            if (fromVDB == null || fromMetadataStore == null) {
                throw new MetadataException(QueryPlugin.Event.TEIID31231,
                        QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31231, dbName, version));
            }
            from = DatabaseUtil.convert(fromVDB, fromMetadataStore);
        }
        
        Database to = getCurrentDatabase();
        assertOverlap(from, to, importPolicies);
        merge(from, to, importPolicies);
    }
    
    private void assertOverlap(Database from, Database to, boolean importRoles) {

        for (Server fromServer : from.getServers()) {
            Server toServer = to.getServer(fromServer.getName());
            if (toServer != null && !toServer.equals(fromServer)) {
                throw new MetadataException(QueryPlugin.Event.TEIID31229, QueryPlugin.Util
                        .gs(QueryPlugin.Event.TEIID31229, fromServer.getName(), to.getName(), to.getVersion()));
            }
        }

        for (Schema fromSchema : from.getSchemas()) {
            Schema toSchema = to.getSchema(fromSchema.getName());
            if (toSchema != null) {
                throw new MetadataException(QueryPlugin.Event.TEIID31228, QueryPlugin.Util
                        .gs(QueryPlugin.Event.TEIID31228, fromSchema.getName(), to.getName(), to.getVersion()));
            }
        }

        if (importRoles) {
            for (Role fromRole : from.getRoles()) {
                Role toRole = to.getRole(fromRole.getName());
                if (toRole != null) {
                    throw new MetadataException(QueryPlugin.Event.TEIID31230, QueryPlugin.Util
                            .gs(QueryPlugin.Event.TEIID31230, fromRole.getName(), to.getName(), to.getVersion()));
                }
            }
        }
    }    
    
    private void merge(Database from, Database to, boolean importRoles) {
        
        for (DataWrapper fromWrapper : from.getDataWrappers()) {
            if (to.getDataWrapper(fromWrapper.getName()) == null) {
                dataWrapperCreated(fromWrapper);
            }
        }
        
        for (Server fromServer : from.getServers()) {
            if (to.getServer(fromServer.getName()) == null ) {
                serverCreated(fromServer);
            }
        }

        for (Schema fromSchema : from.getSchemas()) {
            if (to.getSchema(fromSchema.getName()) == null) {
                ArrayList<String> servers = new ArrayList<String>();
                for (Server server:fromSchema.getServers()) {
                    servers.add(server.getName());
                }
                Schema s = new Schema();
                s.setName(fromSchema.getName());
                s.setPhysical(fromSchema.isPhysical());
                s.setProperties(fromSchema.getProperties());
                schemaCreated(s, servers);
                importSchema(fromSchema);
            }
        }

        if (importRoles) {
            for (Role fromRole : from.getRoles()) {
                if (to.getRole(fromRole.getName()) == null) {
                    roleCreated(fromRole);
                }            
            }
            for (Grant g : from.getGrants()) {
                grantCreated(g);
            }
        }
    }
    
}
