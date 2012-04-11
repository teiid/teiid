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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.TranslatorException;
import org.teiid.vdb.runtime.VDBKey;


/**
 * Repository for VDBs
 */
public class VDBRepository implements Serializable{
	private static final long serialVersionUID = 312177538191772674L;
	
	private NavigableMap<VDBKey, CompositeVDB> vdbRepo = new ConcurrentSkipListMap<VDBKey, CompositeVDB>();
	private MetadataStore systemStore;
	private MetadataStore odbcStore;
	private boolean odbcEnabled = false;
	private List<VDBLifeCycleListener> listeners = new CopyOnWriteArrayList<VDBLifeCycleListener>();
	private SystemFunctionManager systemFunctionManager;
	private Map<String, Datatype> datatypeMap = new HashMap<String, Datatype>();
	
	
	public void addVDB(VDBMetaData vdb, MetadataStore metadataStore, UDFMetaData udf, ConnectorManagerRepository cmr) throws VirtualDatabaseException {
		if (getVDB(vdb.getName(), vdb.getVersion()) != null) {
			 throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40035, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40035, vdb.getName(), vdb.getVersion()));
		}
		
		// get the system VDB metadata store
		if (this.systemStore == null) {
			 throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40036, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40036));
		}	
		
		if (this.odbcEnabled && odbcStore == null) {
			this.odbcStore = getODBCMetadataStore();
		}
		
		CompositeVDB cvdb = null;
		if (this.odbcStore == null) {
			cvdb = new CompositeVDB(vdb, metadataStore, udf, this.systemFunctionManager.getSystemFunctions(), cmr, this.systemStore);
		}
		else {
			cvdb = new CompositeVDB(vdb, metadataStore, udf, this.systemFunctionManager.getSystemFunctions(), cmr, this.systemStore, odbcStore);
		}
		this.vdbRepo.put(vdbId(vdb), cvdb); 
		notifyAdd(vdb.getName(), vdb.getVersion(), cvdb);
	}

	public VDBMetaData getVDB(String name, int version) {
		CompositeVDB v = this.vdbRepo.get(new VDBKey(name, version));
		if (v != null) {
			return v.getVDB();
		}
		return null;
	}
	
	public List<VDBMetaData> getVDBs(){
		ArrayList<VDBMetaData> vdbs = new ArrayList<VDBMetaData>();
		for(CompositeVDB cVDB:this.vdbRepo.values()) {
			vdbs.add(cVDB.getVDB());
		}
		return vdbs;
	}
	
	/**
	 * This returns the all the VDBS that loaded and still loading or stalled due to data source unavailability.
	 * @return
	 */
	public List<VDBMetaData> getAllDeployedVDBs(){
		ArrayList<VDBMetaData> vdbs = new ArrayList<VDBMetaData>();
		for(CompositeVDB cVDB:this.vdbRepo.values()) {
			if (!cVDB.isMetadataloadFinished()) {
				vdbs.add(cVDB.buildVDB());
			}
			else {
				vdbs.add(cVDB.getVDB());
			}
		}
		return vdbs;
	}	

    protected VDBKey vdbId(VDBMetaData vdb) {
        return new VDBKey(vdb.getName(), vdb.getVersion());
    } 	
		
	public VDBMetaData getVDB(String vdbName) {
    	int latestVersion = 0;
        for (VDBKey key:this.vdbRepo.tailMap(new VDBKey(vdbName, 0)).keySet()) {
            if(!key.getName().equalsIgnoreCase(vdbName)) {
            	break;
            }
        	VDBMetaData vdb = this.vdbRepo.get(key).getVDB();
        	switch (vdb.getConnectionType()) {
        	case ANY:
        		latestVersion = Math.max(vdb.getVersion(), latestVersion);
        		break;
        	case BY_VERSION:
                if (latestVersion == 0) {
            		latestVersion = vdb.getVersion();
                }            	
                break;
        	}
        }
        if(latestVersion == 0) {
            return null; 
        }

        return getVDB(vdbName, latestVersion);
	}
	
	public MetadataStore getSystemStore() {
		return systemStore;
	}
	
	public MetadataStore getODBCStore() {
		return this.odbcStore;
	}	
	
	public void setSystemStore(MetadataStore store) {
		this.systemStore = store;
		Collection<Datatype> datatypes = this.systemStore.getDatatypes().values();
		
		for (String typeName : DataTypeManager.getAllDataTypeNames()) {
			
			boolean found = false;
			for (Datatype datatypeRecordImpl : datatypes) {
				if (datatypeRecordImpl.getRuntimeTypeName().equalsIgnoreCase(typeName)) {
					datatypeMap.put(typeName, datatypeRecordImpl);
					found = true;
					break;
				}
			}
			
			if (!found) {
				for (Datatype datatypeRecordImpl : datatypes) {
					if (datatypeRecordImpl.getJavaClassName().equals(DataTypeManager.getDataTypeClass(typeName))) {
						datatypeMap.put(typeName, datatypeRecordImpl);
						break;
					}			
				}
			}
		}
		
		// add alias types
		addAliasType(datatypes, DataTypeManager.DataTypeAliases.BIGINT);
		addAliasType(datatypes, DataTypeManager.DataTypeAliases.DECIMAL);
		addAliasType(datatypes, DataTypeManager.DataTypeAliases.REAL);
		addAliasType(datatypes, DataTypeManager.DataTypeAliases.SMALLINT);
		addAliasType(datatypes, DataTypeManager.DataTypeAliases.TINYINT);
		addAliasType(datatypes, DataTypeManager.DataTypeAliases.VARCHAR);
		
	}
	
	private void addAliasType(Collection<Datatype> datatypes, String alias) {
		Class<?> typeClass = DataTypeManager.getDataTypeClass(alias);
		for (Datatype datatypeRecordImpl : datatypes) {
			if (datatypeRecordImpl.getJavaClassName().equals(typeClass.getName())) {
				datatypeMap.put(alias, datatypeRecordImpl);
				break;
			}
		}
	}

	private MetadataStore getODBCMetadataStore() {
		try {
			PgCatalogMetadataStore pg = new PgCatalogMetadataStore(CoreConstants.ODBC_MODEL, getBuiltinDatatypes());
			return pg.asMetadataStore();
		} catch (TranslatorException e) {
			LogManager.logError(LogConstants.CTX_DQP, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40002));
		}
		return null;
	}
	
	public void odbcEnabled() {
		this.odbcEnabled = true;
	}
	
	public boolean removeVDB(String vdbName, int vdbVersion) {
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		CompositeVDB removed = this.vdbRepo.remove(key);
		if (removed != null) {
			// if this VDB was part of another VDB; then remove them.
			for (CompositeVDB other:this.vdbRepo.values()) {
				if (other.hasChildVdb(key)) {
					notifyRemove(other.getVDB().getName(), other.getVDB().getVersion(), other);
	
					other.removeChild(key);
	
					notifyAdd(other.getVDB().getName(), other.getVDB().getVersion(), other);
				}
			}
			notifyRemove(key.getName(), key.getVersion(), removed);
			return true;
		}
		return false;
	}	
	
	public Map<String, Datatype> getBuiltinDatatypes() {
		return datatypeMap;
	}
	
	public void mergeVDBs(String sourceVDBName, int sourceVDBVersion, String targetVDBName, int targetVDBVersion) throws AdminException{
		CompositeVDB source = this.vdbRepo.get(new VDBKey(sourceVDBName, sourceVDBVersion));
		if (source == null) {
			 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40037, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40037, sourceVDBName, sourceVDBVersion));
		}
		
		CompositeVDB target = this.vdbRepo.get(new VDBKey(targetVDBName, targetVDBVersion));
		if (target == null) {
			 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40038, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40038, sourceVDBName, sourceVDBVersion));
		}		

		notifyRemove(targetVDBName, targetVDBVersion, target);
		// merge them
		target.addChild(source);
		
		notifyAdd(targetVDBName, targetVDBVersion, target);
		finishDeployment(targetVDBName, targetVDBVersion);		
	}
	
	// this is called by mc
	public void start() {
		if (this.odbcEnabled) {
			this.odbcStore = getODBCMetadataStore();
		}
	}
	
	public void finishDeployment(String name, int version) {
		CompositeVDB v = this.vdbRepo.get(new VDBKey(name, version));
		if (v!= null) {
			boolean valid = false;
			v.setMetaloadFinished(true);
			VDBMetaData metdataAwareVDB = v.getVDB();			
			ValidatorReport report = MetadataValidator.validate(metdataAwareVDB, metdataAwareVDB.removeAttachment(MetadataStore.class));
			
			if (!report.hasItems()) {
				valid  = true;					
			}
			else {
				LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40073, name, version));
			}
			
			// check the data sources available
			if (valid) {
				valid = hasValidDataSources(metdataAwareVDB);
			}
			
			if (valid) {
				metdataAwareVDB.setStatus(VDB.Status.ACTIVE);
			}
			else {
				metdataAwareVDB.setStatus(VDB.Status.INACTIVE);
			}
				
			LogManager.logInfo(LogConstants.CTX_RUNTIME, (VDB.Status.ACTIVE == metdataAwareVDB.getStatus())?RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,name, version):RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40006,name, version));
			notifyFinished(name, version, v);
		}
	}
	
	boolean hasValidDataSources(VDBMetaData vdb) {
		ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
		
		for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
	    	if (model.isSource()) {
		    	List<SourceMappingMetadata> mappings = model.getSourceMappings();
				for (SourceMappingMetadata mapping:mappings) {
					ConnectorManager cm = cmr.getConnectorManager(mapping.getName());
					if (cm != null) {
						String msg = cm.getStausMessage();
						if (msg != null && msg.length() > 0) {
							model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), cm.getStausMessage());
							LogManager.logInfo(LogConstants.CTX_RUNTIME, cm.getStausMessage());
						}
					}					
				}
	    	}			
		}
		return vdb.isValid();
	}
	
	
	private void notifyFinished(String name, int version, CompositeVDB v) {
		for(VDBLifeCycleListener l:this.listeners) {
			l.finishedDeployment(name, version, v);
		}
	}
	
	public void addListener(VDBLifeCycleListener listener) {
		this.listeners.add(listener);
	}
	
	public void removeListener(VDBLifeCycleListener listener) {
		this.listeners.remove(listener);
	}
	
	private void notifyAdd(String name, int version, CompositeVDB vdb) {
		for(VDBLifeCycleListener l:this.listeners) {
			l.added(name, version, vdb);
		}
	}
	
	private void notifyRemove(String name, int version, CompositeVDB vdb) {
		for(VDBLifeCycleListener l:this.listeners) {
			l.removed(name, version, vdb);
		}
	}
	
	public void setSystemFunctionManager(SystemFunctionManager mgr) {
		this.systemFunctionManager = mgr;
	}	
}
