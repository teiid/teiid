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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.TranslatorException;
import org.teiid.vdb.runtime.VDBKey;


/**
 * Repository for VDBs
 */
public class VDBRepository implements Serializable{
	private static final int DEPLOY_TIMEOUT = 60000;
	private static final int LOAD_TIMEOUT = 600000;

	private static final long serialVersionUID = 312177538191772674L;
	
	private NavigableMap<VDBKey, CompositeVDB> vdbRepo = new ConcurrentSkipListMap<VDBKey, CompositeVDB>();
	private MetadataStore systemStore;
	private MetadataStore odbcStore;
	private boolean odbcEnabled = false;
	private List<VDBLifeCycleListener> listeners = new CopyOnWriteArrayList<VDBLifeCycleListener>();
	private SystemFunctionManager systemFunctionManager;
	private Map<String, Datatype> datatypeMap = new HashMap<String, Datatype>();
	private ReentrantLock lock = new ReentrantLock();
	private Condition vdbAdded = lock.newCondition();
	
	public void addVDB(VDBMetaData vdb, MetadataStore metadataStore, LinkedHashMap<String, Resource> visibilityMap, UDFMetaData udf, ConnectorManagerRepository cmr) throws VirtualDatabaseException {
		VDBKey key = vdbId(vdb);
		
		// get the system VDB metadata store
		if (this.systemStore == null) {
			 throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40022, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40022));
		}	
		
		if (this.odbcEnabled && odbcStore == null) {
			this.odbcStore = getODBCMetadataStore();
		}

		MetadataStore[] stores = null;
		if (this.odbcStore == null) {
			stores = new MetadataStore[] {this.systemStore};
		} else {
			stores = new MetadataStore[] {this.systemStore, odbcStore};
		}
		CompositeVDB cvdb = new CompositeVDB(vdb, metadataStore, visibilityMap, udf, this.systemFunctionManager.getSystemFunctions(), cmr, stores);
		cvdb.buildCompositeState(this);
		lock.lock();
		try {
			if (vdbRepo.containsKey(key)) {
				 throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40035, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40035, vdb.getName(), vdb.getVersion()));
			}
			this.vdbRepo.put(key, cvdb);
			vdbAdded.signalAll();
		} finally {
			lock.unlock();
		}
		notifyAdd(vdb.getName(), vdb.getVersion(), cvdb);
	}
	
	public void waitForFinished(String vdbName, int vdbVersion) throws InterruptedException {
		CompositeVDB cvdb = null;
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		Date toWait = new Date(System.currentTimeMillis() + DEPLOY_TIMEOUT);
		lock.lock();
		try {
			while (cvdb == null) {
				cvdb = this.vdbRepo.get(key);
				if (cvdb == null && !vdbAdded.awaitUntil(toWait)) {
					return; //TODO: should there be a message/exception
				}
			}
		} finally {
			lock.unlock();
		}
		VDBMetaData vdb = cvdb.getVDB();
		synchronized (vdb) {
			while (vdb.isLoading()) {
				vdb.wait(LOAD_TIMEOUT);
			}
		}
	}

	CompositeVDB getCompositeVDB(String name, int version) {
		return this.vdbRepo.get(new VDBKey(name, version));
	}
	
	public VDBMetaData getVDB(String name, int version) {
		CompositeVDB v = getCompositeVDB(name, version);
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
	
    protected VDBKey vdbId(VDBMetaData vdb) {
        return new VDBKey(vdb.getName(), vdb.getVersion());
    } 	
		
	public VDBMetaData getVDB(String vdbName) {
    	int latestVersion = 0;
    	VDBMetaData result = null;
        for (Map.Entry<VDBKey, CompositeVDB> entry:this.vdbRepo.tailMap(new VDBKey(vdbName, 0)).entrySet()) {
            if(!entry.getKey().getName().equalsIgnoreCase(vdbName)) {
            	break;
            }
        	VDBMetaData vdb = entry.getValue().getVDB();
        	switch (vdb.getConnectionType()) {
        	case ANY:
        		if (vdb.getVersion() > latestVersion) {
        			latestVersion = vdb.getVersion();
        			result = vdb;
        		}
        		break;
        	case BY_VERSION:
                if (latestVersion == 0) {
            		latestVersion = vdb.getVersion();
            		result = vdb;
                }            	
                break;
        	}
        }
        return result;
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
	
	public VDBMetaData removeVDB(String vdbName, int vdbVersion) {
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		CompositeVDB removed = this.vdbRepo.remove(key);
		if (removed == null) {
			return null;
		}
		notifyRemove(key.getName(), key.getVersion(), removed);
		return removed.getVDB();
	}	
	
	public Map<String, Datatype> getBuiltinDatatypes() {
		return datatypeMap;
	}
	
	// this is called by mc
	public void start() {
		if (this.odbcEnabled) {
			this.odbcStore = getODBCMetadataStore();
		}
	}
	
	public void finishDeployment(String name, int version) {
		CompositeVDB v = this.vdbRepo.get(new VDBKey(name, version));
		if (v == null) {
			return;
		}
		boolean valid = false;
		v.metadataLoadFinished();
		VDBMetaData metdataAwareVDB = v.getVDB();			
		synchronized (metdataAwareVDB) {
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
				metdataAwareVDB.setStatus(VDB.Status.INVALID);
			}
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,name, version, metdataAwareVDB.getStatus()));
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
