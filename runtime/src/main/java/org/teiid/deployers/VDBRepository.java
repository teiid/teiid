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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataStore;
import org.teiid.net.ConnectionException;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;


/**
 * Repository for VDBs
 */
public class VDBRepository implements Serializable{
	private static final long serialVersionUID = 312177538191772674L;
	private static final int DEFAULT_TIMEOUT_MILLIS = PropertiesUtils.getIntProperty(System.getProperties(), "org.teiid.clientVdbLoadTimeoutMillis", 300000); //$NON-NLS-1$
	
	private NavigableMap<VDBKey, CompositeVDB> vdbRepo = new ConcurrentSkipListMap<VDBKey, CompositeVDB>();
	private NavigableMap<VDBKey, VDBMetaData> pendingDeployments = new ConcurrentSkipListMap<VDBKey, VDBMetaData>();

	private MetadataStore systemStore = SystemMetadata.getInstance().getSystemStore();
	private MetadataStore odbcStore;
	private boolean odbcEnabled = false;
	private Set<VDBLifeCycleListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<VDBLifeCycleListener, Boolean>());
	private SystemFunctionManager systemFunctionManager;
	private Map<String, Datatype> datatypeMap = SystemMetadata.getInstance().getRuntimeTypeMap();
	private ReentrantLock lock = new ReentrantLock();
	private Condition vdbAdded = lock.newCondition();
	
	public void addVDB(VDBMetaData vdb, MetadataStore metadataStore, LinkedHashMap<String, VDBResources.Resource> visibilityMap, UDFMetaData udf, ConnectorManagerRepository cmr) throws VirtualDatabaseException {
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
		CompositeVDB cvdb = new CompositeVDB(vdb, metadataStore, visibilityMap, udf, this.systemFunctionManager.getSystemFunctions(), cmr, this, stores);
		lock.lock();
		try {
			if (vdbRepo.containsKey(key)) {
				 throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40035, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40035, vdb.getName(), vdb.getVersion()));
			}
			this.vdbRepo.put(key, cvdb);
			this.pendingDeployments.remove(key);
			vdbAdded.signalAll();
		} finally {
			lock.unlock();
		}
		notifyAdd(vdb.getName(), vdb.getVersion(), cvdb);
	}
	
	public void waitForFinished(String vdbName, int vdbVersion, int timeOutMillis) throws ConnectionException {
		CompositeVDB cvdb = null;
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		long timeOutNanos = 0;
		if (timeOutMillis >= 0) {
			timeOutNanos = TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MILLIS);
		} else {
			//TODO allow a configurable default
			timeOutNanos = TimeUnit.MINUTES.toNanos(10);
		}
		lock.lock();
		try {
			while ((cvdb = this.vdbRepo.get(key)) == null) {
				if (timeOutNanos <= 0) {
					throw new ConnectionException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40096, timeOutMillis, vdbName, vdbVersion)); 
				}
				timeOutNanos = this.vdbAdded.awaitNanos(timeOutNanos);
				
			}
		} catch (InterruptedException e) {
			return;
		} finally {
			lock.unlock();
		}
		VDBMetaData vdb = cvdb.getVDB();
		long finishNanos = System.nanoTime() + timeOutNanos;
		synchronized (vdb) {
			while (vdb.getStatus() != Status.ACTIVE) {
				if (timeOutNanos <= 0) {
					throw new ConnectionException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40097, timeOutMillis, vdbName, vdbVersion, vdb.getValidityErrors())); 
				}
				try {
					vdb.wait(timeOutNanos);
				} catch (InterruptedException e) {
					return;
				}
				timeOutNanos = finishNanos - System.nanoTime();
			}
		}
	}

	CompositeVDB getCompositeVDB(VDBKey key) {
		return this.vdbRepo.get(key);
	}
	
	/**
	 * A live vdb may be loading or active
	 * @param name
	 * @param version
	 * @return
	 */
	public VDBMetaData getLiveVDB(String name, int version) {
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
		//there is a minor chance of a duplicate entry
		//but we're not locking to prevent that
		vdbs.addAll(pendingDeployments.values());
		return vdbs;
	}
	
	Collection<CompositeVDB> getCompositeVDBs() {
		return vdbRepo.values();
	}
	
    protected VDBKey vdbId(VDBMetaData vdb) {
        return new VDBKey(vdb.getName(), vdb.getVersion());
    } 	
		
    /**
     * A live vdb may be loading or active
     * @param vdbName
     * @return
     */
	public VDBMetaData getLiveVDB(String vdbName) {
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
	}
	
	private MetadataStore getODBCMetadataStore() {
		try {
			PgCatalogMetadataStore pg = new PgCatalogMetadataStore(CoreConstants.ODBC_MODEL, getRuntimeTypeMap());
			return pg.asMetadataStore();
		} catch (MetadataException e) {
			LogManager.logError(LogConstants.CTX_DQP, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40002));
		}
		return null;
	}
	
	public void odbcEnabled() {
		this.odbcEnabled = true;
	}
	
	public VDBMetaData removeVDB(String vdbName, int vdbVersion) {
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		return removeVDB(key);
	}

	private VDBMetaData removeVDB(VDBKey key) {
		this.pendingDeployments.remove(key);
		CompositeVDB removed = this.vdbRepo.remove(key);
		if (removed == null) {
			return null;
		}
		removed.getVDB().setStatus(Status.REMOVED);
		notifyRemove(key.getName(), key.getVersion(), removed);
		return removed.getVDB();
	}	
	
	public Map<String, Datatype> getRuntimeTypeMap() {
		return datatypeMap;
	}
	
	// this is called by mc
	public void start() {
		if (this.odbcEnabled) {
			this.odbcStore = getODBCMetadataStore();
		}
	}
	
	public void finishDeployment(String name, int version) {
		VDBKey key = new VDBKey(name, version);
		CompositeVDB v = this.vdbRepo.get(key);
		if (v == null) {
			return;
		}
		VDBMetaData metadataAwareVDB = v.getVDB();			
		if (v.getOriginalVDB().getStatus() == Status.FAILED) {
			if (v.getOriginalVDB() != metadataAwareVDB && metadataAwareVDB.getStatus() == Status.LOADING) {
				metadataAwareVDB.setStatus(Status.FAILED);
			}
			return;
		}
		v.metadataLoadFinished();
		synchronized (metadataAwareVDB) {
			try {
				ValidatorReport report = new MetadataValidator().validate(metadataAwareVDB, metadataAwareVDB.removeAttachment(MetadataStore.class));
	
				if (report.hasItems()) {
					LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40073, name, version));
					if (!metadataAwareVDB.isPreview() && !processMetadataValidatorReport(key, report)) {
						metadataAwareVDB.setStatus(Status.FAILED);
						LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,name, version, metadataAwareVDB.getStatus()));
						notifyFinished(name, version, v);
						return;
					}
				} 
				validateDataSources(metadataAwareVDB);
				metadataAwareVDB.setStatus(Status.ACTIVE);
				LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,name, version, metadataAwareVDB.getStatus()));
				notifyFinished(name, version, v);
			} finally {
				if (metadataAwareVDB.getStatus() != Status.ACTIVE && metadataAwareVDB.getStatus() != Status.FAILED) {
					//guard against an unexpected exception - probably bad validation logic
					metadataAwareVDB.setStatus(Status.FAILED);
					notifyFinished(name, version, v);
				}
			}
		}
	}
	
	/**
	 * @param key 
	 * @param report
	 * @return if the deployment should finish  
	 */
	protected boolean processMetadataValidatorReport(VDBKey key, ValidatorReport report) {
		return false;
	}

	void validateDataSources(VDBMetaData vdb) {
		ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
		
		for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
	    	if (model.isSource()) {
		    	Collection<SourceMappingMetadata> mappings = model.getSourceMappings();
				for (SourceMappingMetadata mapping:mappings) {
					ConnectorManager cm = cmr.getConnectorManager(mapping.getName());
					if (cm != null) {
						String msg = cm.getStausMessage();
						if (msg != null && msg.length() > 0) {
							model.addRuntimeError(msg);
							model.setMetadataStatus(Model.MetadataStatus.FAILED);
							LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);
						}
					}					
				}
	    	}			
		}
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

	public void addPendingDeployment(VDBMetaData deployment) {
		VDBKey key = vdbId(deployment);
		this.pendingDeployments.put(key, deployment);
	}

	public VDBMetaData getVDB(String vdbName, int vdbVersion) {
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		CompositeVDB cvdb = this.vdbRepo.get(key);
		if (cvdb != null) {
			return cvdb.getVDB();
		}
		return this.pendingDeployments.get(key);
	}	
}
