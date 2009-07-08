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

package com.metamatrix.admin.server;

import java.util.Collection;
import java.util.Map;

import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.registry.ClusteredRegistryState;

/**
 * @since 5.0
 * This implementation needed for testing purposes, to override the usage of
 * singleton RuntimeMetadataCatalog with FakeRuntimeMetadataCatalog
 */
public class FakeServerConfigAdminImpl extends ServerConfigAdminImpl {

    /**
     * constructor
     */
    public FakeServerConfigAdminImpl(ServerAdminImpl parent,ClusteredRegistryState registry) {
    	super(parent, registry);
    }
    
    protected Collection getVirtualDatabases( ) throws VirtualDatabaseException {
        return FakeRuntimeMetadataCatalog.getVirtualDatabases();
    }

    protected Collection getModels(VirtualDatabaseID vdbId) throws VirtualDatabaseException {
        return FakeRuntimeMetadataCatalog.getModels(vdbId);
    }

    protected void setConnectorBindingNames(VirtualDatabaseID vdbId, Map mapModelsToConnBinds) throws VirtualDatabaseException {
    	FakeRuntimeMetadataCatalog.setConnectorBindingNames(vdbId, mapModelsToConnBinds, getUserName());
	}
	
	protected void setVDBState(VirtualDatabaseID vdbID, int siState) throws VirtualDatabaseException {
		FakeRuntimeMetadataCatalog.setVDBStatus(vdbID, (short)siState, getUserName());
	}

}
