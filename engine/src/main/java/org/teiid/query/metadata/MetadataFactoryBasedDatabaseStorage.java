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

import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Server;

public class MetadataFactoryBasedDatabaseStorage implements DatabaseStorage {
    private static final String NONE = "none"; //$NON-NLS-1$
    private MetadataFactory factory;

    public MetadataFactoryBasedDatabaseStorage(final MetadataFactory factory) {
        this.factory = factory;
    }

    @Override
    public void load(DatabaseStore store) {  
        Database db = new Database(this.factory.getVdbName(), this.factory.getVdbVersion());
        store.databaseCreated(db);
        
        store.dataWrapperCreated(new DataWrapper(NONE));
        Server server = new Server(NONE);
        server.setDataWrapper(NONE);
        
        store.serverCreated(server);
        if (this.factory.getSchema().isPhysical()) {
            Server s = new Server(this.factory.getSchema().getName());
            s.setDataWrapper(NONE);
            store.serverCreated(s);
        }
        List<String> servers = Collections.emptyList();
        if (this.factory.getSchema().isPhysical()){
        	servers = Arrays.asList(NONE);
        }
        store.schemaCreated(this.factory.getSchema(), servers);
    }
    
    @Override
    public void setProperties(String properties) {
    }
    
    @Override
    public void drop(Database database) {
        
    }
    
    @Override
    public void save(Database database) {
        
    }
    
    @Override
    public void restore(DatabaseStore store, Database database) {
        
    }

}
