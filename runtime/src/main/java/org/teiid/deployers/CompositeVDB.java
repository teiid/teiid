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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDBImport;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.metadata.*;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;

/**
 * Represents the runtime state of a vdb that may aggregate several vdbs.
 */
public class CompositeVDB {
	private VDBMetaData vdb;
	private MetadataStore store;
	private LinkedHashMap<String, Resource> visibilityMap;
	private UDFMetaData udf;
	private LinkedHashMap<VDBKey, CompositeVDB> children;
	private MetadataStore[] additionalStores;
	private ConnectorManagerRepository cmr;
	private FunctionTree systemFunctions;
	private boolean metadataloadFinished = false;
	private VDBMetaData mergedVDB;
	
	public CompositeVDB(VDBMetaData vdb, MetadataStore metadataStore, LinkedHashMap<String, Resource> visibilityMap, UDFMetaData udf, FunctionTree systemFunctions, ConnectorManagerRepository cmr, MetadataStore... additionalStores) {
		this.vdb = vdb;
		this.store = metadataStore;
		this.visibilityMap = visibilityMap;
		this.udf = udf;
		this.systemFunctions = systemFunctions;
		this.cmr = cmr;
		this.additionalStores = additionalStores;
		this.mergedVDB = vdb;
	}
	
	private static TransformationMetadata buildTransformationMetaData(VDBMetaData vdb, LinkedHashMap<String, Resource> visibilityMap, MetadataStore store, UDFMetaData udf, FunctionTree systemFunctions, MetadataStore[] additionalStores) {
		Collection <FunctionTree> udfs = new ArrayList<FunctionTree>();
		if (udf != null) {			
			for (Map.Entry<String, Collection<FunctionMethod>> entry : udf.getFunctions().entrySet()) {
				udfs.add(new FunctionTree(entry.getKey(), new UDFSource(entry.getValue()), true));
			}
		}
		
		CompositeMetadataStore compositeStore = new CompositeMetadataStore(store);
		for (MetadataStore s:additionalStores) {
			compositeStore.merge(s);
			for (Schema schema:s.getSchemas().values()) {
				if (!schema.getFunctions().isEmpty()) {
					udfs.add(new FunctionTree(schema.getName(), new UDFSource(schema.getFunctions().values()), true));
				}
			}
		}
		
		TransformationMetadata metadata =  new TransformationMetadata(vdb, compositeStore, visibilityMap, systemFunctions, udfs);
				
		return metadata;
	}
	
	public VDBMetaData getVDB() {
		return this.mergedVDB;
	}
	
	void buildCompositeState(VDBRepository vdbRepository) throws VirtualDatabaseException {
		if (vdb.getVDBImports().isEmpty()) {
			this.vdb.addAttchment(ConnectorManagerRepository.class, this.cmr);
			return;
		}
		
		VDBMetaData newMergedVDB = new VDBMetaData();
		newMergedVDB.setName(this.vdb.getName());
		newMergedVDB.setVersion(this.vdb.getVersion());
		newMergedVDB.setModels(this.vdb.getModelMetaDatas().values());
		newMergedVDB.setDataPolicies(this.vdb.getDataPolicies());
		newMergedVDB.setDescription(this.vdb.getDescription());
		newMergedVDB.setStatus(this.vdb.getStatus());
		newMergedVDB.setJAXBProperties(this.vdb.getJAXBProperties());
		newMergedVDB.setConnectionType(this.vdb.getConnectionType());
		ConnectorManagerRepository mergedRepo = new ConnectorManagerRepository();
		mergedRepo.getConnectorManagers().putAll(this.cmr.getConnectorManagers());
		newMergedVDB.addAttchment(ConnectorManagerRepository.class, mergedRepo);
		this.children = new LinkedHashMap<VDBKey, CompositeVDB>();
		newMergedVDB.setImportedModels(new TreeSet<String>(String.CASE_INSENSITIVE_ORDER));
		for (VDBImport vdbImport : vdb.getVDBImports()) {
			CompositeVDB importedVDB = vdbRepository.getCompositeVDB(vdbImport.getName(), vdbImport.getVersion());
			if (importedVDB == null) {
				throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40083, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40083, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion()));
			}
			VDBMetaData childVDB = importedVDB.getVDB();
			this.children.put(new VDBKey(childVDB.getName(), childVDB.getVersion()), importedVDB);
			
			if (vdbImport.isImportDataPolicies()) {
				for (DataPolicy role : importedVDB.getVDB().getDataPolicies()) {
					if (vdb.addDataPolicy((DataPolicyMetadata)role) != null) {
						throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40084, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40084, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion(), role.getName()));
					}
				}
			}
			
			// add models
			for (Model m:importedVDB.getVDB().getModels()) {
				if (newMergedVDB.addModel((ModelMetaData)m) != null) {
					throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40085, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40085, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion(), m.getName()));
				}
				newMergedVDB.getImportedModels().add(m.getName());
			}
			for (Map.Entry<String, ConnectorManager> entry : importedVDB.cmr.getConnectorManagers().entrySet()) {
				if (mergedRepo.getConnectorManagers().put(entry.getKey(), entry.getValue()) != null) {
					throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40086, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40086, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion(), entry.getKey()));
				}
			}
		}
		this.mergedVDB = newMergedVDB;
	}
	
	private UDFMetaData getUDF() {
		UDFMetaData mergedUDF = new UDFMetaData();
		if (this.udf != null) {
			mergedUDF.addFunctions(this.udf);
		}
		
		for (Schema schema:store.getSchemas().values()) {
			Collection<FunctionMethod> funcs = schema.getFunctions().values();
			mergedUDF.addFunctions(schema.getName(), funcs);
		}

		if (this.cmr != null) {
			//system scoped common source functions
			for (ConnectorManager cm:this.cmr.getConnectorManagers().values()) {
				List<FunctionMethod> funcs = cm.getPushDownFunctions();
				mergedUDF.addFunctions(CoreConstants.SYSTEM_MODEL, funcs);
			}
		}
		
		if (this.children != null) {
			//udf model functions - also scoped to the model
			for (CompositeVDB child:this.children.values()) {
				UDFMetaData funcs = child.getUDF();
				if (funcs != null) {
					mergedUDF.addFunctions(funcs);
				}
			}		
		}
		return mergedUDF;
	}
	
	/**
	 * TODO: we are not checking for collisions here.
	 */
	private LinkedHashMap<String, Resource> getVisibilityMap() {
		if (this.children == null || this.children.isEmpty()) {
			return this.visibilityMap;
		}
		
		LinkedHashMap<String, Resource> mergedvisibilityMap = new LinkedHashMap<String, Resource>();
		if (this.visibilityMap != null) {
			mergedvisibilityMap.putAll(this.visibilityMap);
		}
		for (CompositeVDB child:this.children.values()) {
			LinkedHashMap<String, Resource> vm = child.getVisibilityMap();
			if ( vm != null) {
				mergedvisibilityMap.putAll(vm);
			}
		}		
		return mergedvisibilityMap;
	}
	
	private MetadataStore getMetadataStore() {
		if (this.children == null || this.children.isEmpty()) {
			return this.store;
		}		
		
		MetadataStore mergedStore = new MetadataStore();
		if (this.store != null) {
			mergedStore.merge(this.store);
		}
		for (CompositeVDB child:this.children.values()) {
			MetadataStore childStore = child.getMetadataStore();
			if ( childStore != null) {
				mergedStore.merge(childStore);
			}
		}		
		return mergedStore;
	}
		
	private static void updateFromMetadataRepository(VDBMetaData vdb, Schema schema, DefaultMetadataRepository metadataRepository) {
		String vdbName = vdb.getName();
		int vdbVersion = vdb.getVersion();
		Collection<AbstractMetadataRecord> records = new LinkedHashSet<AbstractMetadataRecord>();
						
		metadataRepository.startLoadVdb(vdbName, vdbVersion);
		
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
	
		for (AbstractMetadataRecord abstractMetadataRecord : records) {
			LinkedHashMap<String, String> p = metadataRepository.getProperties(vdbName, vdbVersion, abstractMetadataRecord);
			if (p != null) {
				abstractMetadataRecord.setProperties(p);
			}
		}
		metadataRepository.endLoadVdb(vdbName, vdbVersion);
	}	
	
	public synchronized void metadataLoadFinished() {
		if (this.metadataloadFinished) {
			return;
		}
		this.metadataloadFinished = true;
		
		MetadataStore mergedStore = getMetadataStore();
		
		for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
			MetadataRepository repo = model.getAttachment(MetadataRepository.class);
			if (repo instanceof DefaultMetadataRepository) {
				updateFromMetadataRepository(mergedVDB, mergedStore.getSchema(model.getName()), (DefaultMetadataRepository)repo);
			}
		}
		
		TransformationMetadata metadata = buildTransformationMetaData(mergedVDB, getVisibilityMap(), mergedStore, getUDF(), systemFunctions, this.additionalStores);
		mergedVDB.addAttchment(QueryMetadataInterface.class, metadata);
		mergedVDB.addAttchment(TransformationMetadata.class, metadata);
		mergedVDB.addAttchment(MetadataStore.class, mergedStore);
	}
	
}
