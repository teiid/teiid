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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.deployers.spi.DeploymentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.TableStats;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.TransformationMetadata.Resource;
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
	private MetadataRepository metadataRepository;
	
	public MetadataRepository getMetadataRepository() {
		return metadataRepository;
	}
	
	public void addVDB(VDBMetaData vdb, MetadataStoreGroup stores, LinkedHashMap<String, Resource> visibilityMap, UDFMetaData udf, ConnectorManagerRepository cmr) throws DeploymentException {
		if (getVDB(vdb.getName(), vdb.getVersion()) != null) {
			throw new DeploymentException(RuntimePlugin.Util.getString("duplicate_vdb", vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
		}
		
		// get the system VDB metadata store
		if (this.systemStore == null) {
			throw new DeploymentException(RuntimePlugin.Util.getString("system_vdb_load_error")); //$NON-NLS-1$
		}	
		
		if (this.odbcEnabled && odbcStore == null) {
			this.odbcStore = getODBCMetadataStore();
		}
		CompositeVDB cvdb = null;
		if (this.odbcStore == null) {
			cvdb = new CompositeVDB(vdb, stores, visibilityMap, udf, this.systemFunctionManager.getSystemFunctions(), cmr, this.systemStore);
		}
		else {
			cvdb = new CompositeVDB(vdb, stores, visibilityMap, udf, this.systemFunctionManager.getSystemFunctions(), cmr, this.systemStore, odbcStore);
		}
		this.vdbRepo.put(vdbId(vdb), cvdb); 
		notifyAdd(vdb.getName(), vdb.getVersion(), cvdb);
	}

	private void updateFromMetadataRepository(CompositeVDB cvdb) {
		if (metadataRepository == null) {
			return;
		}
		String vdbName = cvdb.getVDB().getName();
		int vdbVersion = cvdb.getVDB().getVersion();
		LinkedList<MetadataStore> allStores = new LinkedList<MetadataStore>(cvdb.getMetadataStores().getStores());
		allStores.addAll(Arrays.asList(cvdb.getAdditionalStores()));
		metadataRepository.startLoadVdb(vdbName, vdbVersion);
		for (MetadataStore metadataStore : allStores) {
			Collection<AbstractMetadataRecord> records = new LinkedHashSet<AbstractMetadataRecord>();
			for (Schema schema : metadataStore.getSchemas().values()) {
				records.add(schema);
				for (Table t : schema.getTables().values()) {
					records.add(t);
					records.addAll(t.getColumns());
					records.addAll(t.getAllKeys());
					if (t.isPhysical()) {
						TableStats stats = metadataRepository.getTableStats(vdbName, vdbVersion, t);
						if (stats != null) {
							t.setTableStats(stats);
						}
						for (Column c : t.getColumns()) {
							ColumnStats cStats = metadataRepository.getColumnStats(vdbName, vdbVersion, c);
							if (cStats != null) {
								c.setColumnStats(cStats);
							}
						}
					} else {
						String def = metadataRepository.getViewDefinition(vdbName, vdbVersion, t);
						if (def != null) {
							t.setSelectTransformation(def);
						}
						if (t.supportsUpdate()) {
							def = metadataRepository.getInsteadOfTriggerDefinition(vdbName, vdbVersion, t, Table.TriggerEvent.INSERT);
							if (def != null) {
								t.setInsertPlan(def);
							}
							Boolean enabled = metadataRepository.isInsteadOfTriggerEnabled(vdbName, vdbVersion, t, Table.TriggerEvent.INSERT);
							if (enabled != null) {
								t.setInsertPlanEnabled(enabled);
							}
							def = metadataRepository.getInsteadOfTriggerDefinition(vdbName, vdbVersion, t, Table.TriggerEvent.UPDATE);
							if (def != null) {
								t.setUpdatePlan(def);
							}
							enabled = metadataRepository.isInsteadOfTriggerEnabled(vdbName, vdbVersion, t, Table.TriggerEvent.UPDATE);
							if (enabled != null) {
								t.setUpdatePlanEnabled(enabled);
							}
							def = metadataRepository.getInsteadOfTriggerDefinition(vdbName, vdbVersion, t, Table.TriggerEvent.DELETE);
							if (def != null) {
								t.setDeletePlan(def);
							}
							enabled = metadataRepository.isInsteadOfTriggerEnabled(vdbName, vdbVersion, t, Table.TriggerEvent.DELETE);
							if (enabled != null) {
								t.setDeletePlanEnabled(enabled);
							}
						}
					}
				}
				for (Procedure p : schema.getProcedures().values()) {
					records.add(p);
					records.addAll(p.getParameters());
					if (p.getResultSet() != null) {
						records.addAll(p.getResultSet().getColumns());
					}
					if (p.isVirtual() && !p.isFunction()) {
						String proc = metadataRepository.getProcedureDefinition(vdbName, vdbVersion, p);
						if (proc != null) {
							p.setQueryPlan(proc);								
						}
					}
				}
			}
			for (AbstractMetadataRecord abstractMetadataRecord : records) {
				LinkedHashMap<String, String> p = metadataRepository.getProperties(vdbName, vdbVersion, abstractMetadataRecord);
				if (p != null) {
					abstractMetadataRecord.setProperties(p);
				}
			}
		}
		metadataRepository.endLoadVdb(vdbName, vdbVersion);
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
	}
	
	public void setMetadataRepository(MetadataRepository metadataRepository) {
		this.metadataRepository = metadataRepository;
	}

	private MetadataStore getODBCMetadataStore() {
		try {
			PgCatalogMetadataStore pg = new PgCatalogMetadataStore(CoreConstants.ODBC_MODEL, getBuiltinDatatypes(), new Properties());
			return  pg.getMetadataStore();
		} catch (TranslatorException e) {
			LogManager.logError(LogConstants.CTX_DQP, RuntimePlugin.Util.getString("failed_to_load_odbc_metadata")); //$NON-NLS-1$
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
		Collection<Datatype> datatypes = this.systemStore.getDatatypes();
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
	
	public void mergeVDBs(String sourceVDBName, int sourceVDBVersion, String targetVDBName, int targetVDBVersion) throws AdminException{
		CompositeVDB source = this.vdbRepo.get(new VDBKey(sourceVDBName, sourceVDBVersion));
		if (source == null) {
			throw new AdminProcessingException(RuntimePlugin.Util.getString("vdb_not_found", sourceVDBName, sourceVDBVersion)); //$NON-NLS-1$
		}
		
		CompositeVDB target = this.vdbRepo.get(new VDBKey(targetVDBName, targetVDBVersion));
		if (target == null) {
			throw new AdminProcessingException(RuntimePlugin.Util.getString("vdb_not_found", sourceVDBName, sourceVDBVersion)); //$NON-NLS-1$
		}		

		notifyRemove(targetVDBName, targetVDBVersion, target);
		// merge them
		target.addChild(source);
		
		notifyAdd(targetVDBName, targetVDBVersion, target);
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
			updateFromMetadataRepository(v);
			v.update();
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
