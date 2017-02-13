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
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.BufferManager;
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
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.runtime.MaterializationManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;


/**
 * Repository for VDBs
 */
public class VDBRepository implements Serializable{
	private static final String LIFECYCLE_CONTEXT = LogConstants.CTX_RUNTIME + ".VDBLifeCycleListener"; //$NON-NLS-1$
	private static final long serialVersionUID = 312177538191772674L;
	private static final int DEFAULT_TIMEOUT_MILLIS = PropertiesUtils.getIntProperty(System.getProperties(), "org.teiid.clientVdbLoadTimeoutMillis", 300000); //$NON-NLS-1$
	private static final boolean ADD_PG_METADATA = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.addPGMetadata", true); //$NON-NLS-1$
	
	private NavigableMap<VDBKey, CompositeVDB> vdbRepo = new ConcurrentSkipListMap<VDBKey, CompositeVDB>();
	private NavigableMap<VDBKey, VDBMetaData> pendingDeployments = new ConcurrentSkipListMap<VDBKey, VDBMetaData>();

	private MetadataStore systemStore = SystemMetadata.getInstance().getSystemStore();
	private MetadataStore odbcStore;
	private Set<VDBLifeCycleListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<VDBLifeCycleListener, Boolean>());
	private SystemFunctionManager systemFunctionManager;
	private Map<String, Datatype> datatypeMap = SystemMetadata.getInstance().getRuntimeTypeMap();
	private ReentrantLock lock = new ReentrantLock();
	private Condition vdbAdded = lock.newCondition();
	private boolean dataRolesRequired;
	private MetadataException odbcException;
    private BufferManager bufferManager;
    private ObjectReplicator objectReplictor;
    private DatabaseStore databaseStore;
	
    public void addVDB(VDBMetaData vdb, MetadataStore metadataStore,
            LinkedHashMap<String, VDBResources.Resource> visibilityMap, UDFMetaData udf, ConnectorManagerRepository cmr)
            throws VirtualDatabaseException {
        
		// get the system VDB metadata store
		if (this.systemStore == null) {
            throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40022,
                    RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40022));
		}	
		
		if (dataRolesRequired && vdb.getDataPolicyMap().isEmpty()) {
            throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40143,
                    RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40143, vdb));
		}
		
		boolean pgMetadataEnabled = ADD_PG_METADATA;
		String includePgMetadata = vdb.getPropertyValue("include-pg-metadata");
		if (includePgMetadata != null) {
		    pgMetadataEnabled = Boolean.parseBoolean(includePgMetadata);
		}

		if (pgMetadataEnabled && odbcException != null) {
			throw odbcException;
		}
		MetadataStore[] stores = null;
		if (pgMetadataEnabled) {
		    stores = new MetadataStore[] {this.systemStore, odbcStore};
		} else {
		    stores = new MetadataStore[] {this.systemStore};
		}
        CompositeVDB cvdb = new CompositeVDB(vdb, metadataStore, visibilityMap, udf,
                this.systemFunctionManager.getSystemFunctions(), cmr, this, stores);
		lock.lock();
		try {
			VDBKey vdbKey = cvdb.getVDBKey();
			if (vdbKey.isAtMost()) {
                throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40145,
                        RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40145, vdbKey));
			}
			if (vdbRepo.containsKey(vdbKey)) {
                throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40035,
                        RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40035, vdb.getName(), vdb.getVersion()));
			}
			vdb.setVersion(vdbKey.getVersion()); //canonicalize the version
			vdb.addAttchment(VDBKey.class, vdbKey); //save the key for comparisons
			vdb.setStatus(Status.LOADING);
			this.vdbRepo.put(vdbKey, cvdb);
			this.pendingDeployments.remove(vdbKey);
			vdbAdded.signalAll();
		} finally {
			lock.unlock();
		}
		notifyAdd(vdb.getName(), vdb.getVersion(), cvdb);
	}
	
	public void waitForFinished(VDBKey key, int timeOutMillis) throws ConnectionException {
		CompositeVDB cvdb = null;
		if (timeOutMillis < 0) {
			timeOutMillis = DEFAULT_TIMEOUT_MILLIS;
		}
		long timeOutNanos = TimeUnit.MILLISECONDS.toNanos(timeOutMillis);
		lock.lock();
		try {
			while ((cvdb = this.vdbRepo.get(key)) == null) {
				if (timeOutNanos <= 0) {
					throw new ConnectionException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40096, timeOutMillis, key.getName(), key.getVersion())); 
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
				long millis = timeOutNanos/1000000;
				if (millis <= 0) {
					throw new ConnectionException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40097, timeOutMillis, key.getName(), key.getVersion(), vdb.getValidityErrors())); 
				}
				try {
					vdb.wait(millis);
				} catch (InterruptedException e) {
					return;
				}
				timeOutNanos = finishNanos - System.nanoTime();
			}
		}
	}

	public CompositeVDB getCompositeVDB(VDBKey key) {
		return this.vdbRepo.get(key);
	}
	
	/**
	 * A live vdb may be loading or active
	 * @param name
	 * @param version
	 * @return
	 */
	public VDBMetaData getLiveVDB(String name, Object version) {
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
	
    /**
     * A live vdb may be loading or active
     * @param vdbName
     * @return
     */
	public VDBMetaData getLiveVDB(String vdbName) {
    	VDBMetaData result = null;
    	VDBKey key = new VDBKey(vdbName, null);
    	if (!key.isAtMost()) {
    		CompositeVDB v = this.vdbRepo.get(key);
    		if (v != null) {
    			return v.getVDB();
    		}
    		return null;
    	}
        for (Map.Entry<VDBKey, CompositeVDB> entry:this.vdbRepo.tailMap(key).entrySet()) {
        	if(!key.acceptsVerion(entry.getKey())) {
            	break;
            }
        	VDBMetaData vdb = entry.getValue().getVDB();
        	switch (vdb.getConnectionType()) {
        	case ANY:
    			result = vdb;
        		break;
        	case BY_VERSION:
        	case NONE:
        		if (result == null || result.getConnectionType() == ConnectionType.NONE) {
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
			ValidatorReport report = new ValidatorReport("Function Validation"); //$NON-NLS-1$
			FunctionMetadataValidator.validateFunctionMethods(pg.getSchema().getFunctions().values(), report);
			if(report.hasItems()) {
			    throw new MetadataException(report.getFailureMessage());
			}
			return pg.asMetadataStore();
		} catch (MetadataException e) {
			this.odbcException = e;
			LogManager.logError(LogConstants.CTX_DQP, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40002));
		}
		return null;
	}
	
	public VDBMetaData removeVDB(String vdbName, Object vdbVersion) {
        VDBKey key = new VDBKey(vdbName, vdbVersion);
        return removeVDB(key);
    }

	private VDBMetaData removeVDB(VDBKey key) {
		notifyBeforeRemove(key.getName(), key.getVersion(), this.vdbRepo.get(key));
		this.pendingDeployments.remove(key);
		CompositeVDB removed = this.vdbRepo.remove(key);
		if (removed == null) {
			return null;
		}
		removed.getVDB().setStatus(Status.REMOVED);
        // stop object replication
        if (this.objectReplictor != null) {
            GlobalTableStore gts = removed.getVDB().getAttachment(GlobalTableStore.class);
            this.objectReplictor.stop(gts);
        }
		notifyRemove(key.getName(), key.getVersion(), removed);
		return removed.getVDB();
	}	
	
	public Map<String, Datatype> getRuntimeTypeMap() {
		return datatypeMap;
	}
	
	// this is called by mc
	public void start() {
	    this.odbcStore = getODBCMetadataStore();
	}
	
	public void finishDeployment(String name, String version) {
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
		synchronized (metadataAwareVDB) {
			try {
				try {
					v.metadataLoadFinished();
				} catch (MetadataException e) {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40073, name, version, e.getMessage())); //$NON-NLS-1$
					if (!metadataAwareVDB.isPreview()) {
						ValidatorReport report = new ValidatorReport();
						report.addItem(new ValidatorFailure(e.getMessage()));
						if (!processMetadataValidatorReport(key, report)) {
							metadataAwareVDB.setStatus(Status.FAILED);
							notifyFinished(name, version, v);
							return;
						}
					}
				}
				ValidatorReport report = new MetadataValidator().validate(metadataAwareVDB, metadataAwareVDB.removeAttachment(MetadataStore.class));
	
				if (report.hasItems()) {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40073, name, version, report.getItems().iterator().next()));
					if (!metadataAwareVDB.isPreview() && !processMetadataValidatorReport(key, report)) {
						metadataAwareVDB.setStatus(Status.FAILED);
						notifyFinished(name, version, v);
						return;
					}
				} 
				validateDataSources(metadataAwareVDB);
				metadataAwareVDB.setStatus(Status.ACTIVE);
				
				// for  replication of events, temp tables and mat views
                GlobalTableStore gts = CompositeGlobalTableStore.createInstance(v, this.bufferManager, this.objectReplictor);
                metadataAwareVDB.addAttchment(GlobalTableStore.class, gts);
				
                if (this.databaseStore != null) {
                	metadataAwareVDB.addAttchment(DatabaseStore.class, this.databaseStore);
                }
                
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
	
	
	private void notifyFinished(String name, String version, CompositeVDB v) {
		LogManager.logInfo(LIFECYCLE_CONTEXT, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,name, version, v.getVDB().getStatus()));
		VDBLifeCycleListener mm = null;
		for(VDBLifeCycleListener l:this.listeners) {
			if (l instanceof MaterializationManager) {
				mm = l;
				continue; //defer to last
			}
			l.finishedDeployment(name, v);
		}
		if (mm != null) {
			mm.finishedDeployment(name, v);
		}
	}
	
	public void addListener(VDBLifeCycleListener listener) {
		this.listeners.add(listener);
	}
	
	public void removeListener(VDBLifeCycleListener listener) {
		this.listeners.remove(listener);
	}
	
	private void notifyAdd(String name, String version, CompositeVDB vdb) {
		LogManager.logInfo(LIFECYCLE_CONTEXT, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40118,name, version));
		for(VDBLifeCycleListener l:this.listeners) {
			l.added(name, vdb);
		}
	}
	
	private void notifyRemove(String name, String version, CompositeVDB vdb) {
		LogManager.logInfo(LIFECYCLE_CONTEXT, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40119,name, version));
		for(VDBLifeCycleListener l:this.listeners) {
			l.removed(name, vdb);
		}
	}
	
	private void notifyBeforeRemove(String name, String version, CompositeVDB vdb) {
		LogManager.logInfo(LIFECYCLE_CONTEXT, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40120,name, version));
		for(VDBLifeCycleListener l:this.listeners) {
			l.beforeRemove(name, vdb);
		}
	}
	
	public SystemFunctionManager getSystemFunctionManager() {
		return this.systemFunctionManager;
	}
	
	public void setSystemFunctionManager(SystemFunctionManager mgr) {
		this.systemFunctionManager = mgr;
	}

	public void addPendingDeployment(VDBMetaData deployment) {
		deployment.setStatus(Status.LOADING);
		VDBKey key = new VDBKey(deployment.getName(), deployment.getVersion());
		this.pendingDeployments.put(key, deployment);
	}

	public VDBMetaData getVDB(String vdbName, Object vdbVersion) {
		VDBKey key = new VDBKey(vdbName, vdbVersion);
		CompositeVDB cvdb = this.vdbRepo.get(key);
		if (cvdb != null) {
			return cvdb.getVDB();
		}
		return this.pendingDeployments.get(key);
	}	
	
	public boolean isDataRolesRequired() {
		return dataRolesRequired;
	}
	
	public void setDataRolesRequired(boolean requireDataRoles) {
		this.dataRolesRequired = requireDataRoles;
	}

    public void setBufferManager(BufferManager value) {
        this.bufferManager = value;
    }

    public void setObjectReplicator(ObjectReplicator value) {
        this.objectReplictor = value;
    }

	NavigableMap<VDBKey, CompositeVDB> getVdbRepo() {
        return vdbRepo;
    }
	
}
