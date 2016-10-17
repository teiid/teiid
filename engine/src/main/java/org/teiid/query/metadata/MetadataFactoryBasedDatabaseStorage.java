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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Server;

public class MetadataFactoryBasedDatabaseStorage implements DatabaseStorage {
    private MetadataFactory factory;
    private DatabaseStore store;

    public MetadataFactoryBasedDatabaseStorage(final MetadataFactory factory) {
        this.factory = factory;
        DatabaseStore store = new DatabaseStore() {
            @Override
            public Map<String, Datatype> getRuntimeTypes() {
                return factory.getDataTypes();
            }
            @Override
            public Map<String, Datatype> getBuiltinDataTypes() {
                return factory.getBuiltinDataTypes();
            } 
            @Override
            protected void deployCurrentVDB() {
                // snub out default behavior
            }            
        };
        setStore(store);
    }

    @Override
    public void load() {  
        Database db = new Database(this.factory.getVdbName(), this.factory.getVdbVersion());
        getStore().databaseCreated(db);
        
        getStore().dataWrapperCreated(new DataWrapper("none"));
        Server server = new Server("none");
        server.setDataWrapper("none");
        
        getStore().serverCreated(server);
        if (this.factory.getSchema().isPhysical()) {
            Server s = new Server(this.factory.getSchema().getName());
            s.setDataWrapper("none");
            getStore().serverCreated(s);
        }
        List<String> servers = Collections.emptyList();
        if (this.factory.getSchema().isPhysical()){
        	servers = Arrays.asList("none");
        }
        getStore().schemaCreated(this.factory.getSchema(), servers);
    }
    
    @Override
    public void setStore(DatabaseStore store) {
        this.store = store;
    }
    
    @Override
    public DatabaseStore getStore() {
        return this.store;
    }

    @Override
    public void setProperties(String properties) {
    }

    @Override
    public void startRecording(boolean save) {
    }

    @Override
    public void stopRecording() {
    }
}
