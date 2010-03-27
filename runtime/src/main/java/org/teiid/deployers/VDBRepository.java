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
package org.teiid.deployers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.deployers.spi.DeploymentException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.metadata.TransformationMetadata;
import org.teiid.runtime.RuntimePlugin;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.vdb.runtime.VDBKey;

/**
 * Repository for VDBs
 */
public class VDBRepository implements Serializable{
	private static final long serialVersionUID = 312177538191772674L;
	
	private Map<VDBKey, VDBMetaData> vdbRepo = new ConcurrentHashMap<VDBKey, VDBMetaData>();
	private Map<VDBKey, MetadataStore> metadataStoreRepo = new ConcurrentHashMap<VDBKey, MetadataStore>();
	private Map<VDBKey, TransformationMetadata> vdbToQueryMetadata = Collections.synchronizedMap(new HashMap<VDBKey, TransformationMetadata>());
	
	
	public void addVDB(VDBMetaData vdb) throws DeploymentException {
		if (getVDB(vdb.getName(), vdb.getVersion()) != null) {
			throw new DeploymentException(RuntimePlugin.Util.getString("duplicate_vdb", vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
		}
		this.vdbRepo.put(vdbId(vdb), vdb);
	}
	
	public VDBMetaData getVDB(String name, int version) {
		return this.vdbRepo.get(new VDBKey(name, version));
	}
	
	public List<VDBMetaData> getVDBs(){
		return new ArrayList(this.vdbRepo.values());
	}

    protected VDBKey vdbId(VDBMetaData vdb) {
        return new VDBKey(vdb.getName(), vdb.getVersion());
    } 	
		
	public VDBMetaData getActiveVDB(String vdbName) throws VirtualDatabaseException {
    	int latestVersion = 0;
        for (VDBKey key:this.vdbRepo.keySet()) {
            if(key.getName().equalsIgnoreCase(vdbName)) {
            	VDBMetaData vdb = this.vdbRepo.get(key);
                if (vdb.getStatus() == VDB.Status.ACTIVE_DEFAULT) {
                	latestVersion = vdb.getVersion();
                	break;
                }            	
                // Make sure the VDB Name and version number are the only parts of this vdb key
                latestVersion = Math.max(latestVersion, Integer.parseInt(key.getVersion()));
            }
        }
        if(latestVersion == 0) {
            throw new VirtualDatabaseException(RuntimePlugin.Util.getString("VDBService.VDB_does_not_exist._2", vdbName, "latest")); //$NON-NLS-1$ //$NON-NLS-2$ 
        }

        VDBMetaData vdb = getVDB(vdbName, latestVersion);
        if (vdb.getStatus() == VDB.Status.ACTIVE || vdb.getStatus() == VDB.Status.ACTIVE_DEFAULT) {
        	return vdb;            
        }
        throw new VirtualDatabaseException(RuntimePlugin.Util.getString("VDBService.VDB_does_not_exist._2", vdbName, latestVersion)); //$NON-NLS-1$
	}
	

	public TransformationMetadata getMetadata(String vdbName, int vdbVersion) {
		return this.vdbToQueryMetadata.get(new VDBKey(vdbName, vdbVersion));
	}
	
	public void addMetadata(VDBMetaData vdb, TransformationMetadata metadata) {
		this.vdbToQueryMetadata.put(vdbId(vdb), metadata);
	}

	public void addMetadataStore(VDBMetaData vdb, MetadataStore store) {
		this.metadataStoreRepo.put(vdbId(vdb), store);
	}	
	
	public MetadataStore getMetadataStore(String vdbName, int vdbVersion) {
		return this.metadataStoreRepo.get(new VDBKey(vdbName, vdbVersion));
	}
	
	public synchronized void removeVDB(String vdbName, int vdbVersion) {
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		this.vdbRepo.remove(key);
		this.metadataStoreRepo.remove(key);
		this.vdbToQueryMetadata.remove(key);
	}	
	
	public Map<String, Datatype> getBuiltinDatatypes() {
		Collection<Datatype> datatypes = getMetadataStore(CoreConstants.SYSTEM_VDB, 1).getDatatypes();
		Map<String, Datatype> datatypeMap = new HashMap<String, Datatype>();
		for (Class<?> typeClass : DataTypeManager.getAllDataTypeClasses()) {
			for (Datatype datatypeRecordImpl : datatypes) {
				if (datatypeRecordImpl.getJavaClassName().equals(typeClass.getName())) {
					datatypeMap.put(DataTypeManager.getDataTypeName(typeClass), datatypeRecordImpl);
					break;
				}
			}
		}
		return datatypeMap;
	}
}
