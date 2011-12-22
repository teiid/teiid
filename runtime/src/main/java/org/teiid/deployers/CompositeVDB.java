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
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.vdb.runtime.VDBKey;



public class CompositeVDB {
	private VDBMetaData vdb;
	private MetadataStoreGroup stores;
	private LinkedHashMap<String, Resource> visibilityMap;
	private UDFMetaData udf;
	private LinkedHashMap<VDBKey, CompositeVDB> children;
	private MetadataStore[] additionalStores;
	private ConnectorManagerRepository cmr;
	private FunctionTree systemFunctions;
	
	// used as cached item to avoid rebuilding
	private VDBMetaData mergedVDB;
	
	public CompositeVDB(VDBMetaData vdb, MetadataStoreGroup stores, LinkedHashMap<String, Resource> visibilityMap, UDFMetaData udf, FunctionTree systemFunctions, ConnectorManagerRepository cmr, MetadataStore... additionalStores) {
		this.vdb = vdb;
		this.stores = stores;
		this.visibilityMap = visibilityMap;
		this.udf = udf;
		this.systemFunctions = systemFunctions;
		this.cmr = cmr;
		this.additionalStores = additionalStores;
		this.vdb.addAttchment(ConnectorManagerRepository.class, cmr);
		this.mergedVDB = vdb;
		update();
	}
	
	public synchronized void addChild(CompositeVDB child) {
		if (this.children == null) {
			this.children = new LinkedHashMap<VDBKey, CompositeVDB>();
		}
		VDBMetaData childVDB = child.getVDB();
		this.children.put(new VDBKey(childVDB.getName(), childVDB.getVersion()), child);
		this.mergedVDB = null;
	}
	
	public synchronized void removeChild(VDBKey child) {
		if (this.children != null) {
			this.children.remove(child);
		}
		this.mergedVDB = null;
	}	
	
	synchronized void update() {
		TransformationMetadata metadata = buildTransformationMetaData(mergedVDB, getVisibilityMap(), getMetadataStores(), getUDF(), systemFunctions, this.additionalStores);
		mergedVDB.addAttchment(QueryMetadataInterface.class, metadata);
		mergedVDB.addAttchment(TransformationMetadata.class, metadata);	
	}
	
	private static TransformationMetadata buildTransformationMetaData(VDBMetaData vdb, LinkedHashMap<String, Resource> visibilityMap, MetadataStoreGroup stores, UDFMetaData udf, FunctionTree systemFunctions, MetadataStore[] additionalStores) {
		Collection <FunctionTree> udfs = new ArrayList<FunctionTree>();
		if (udf != null) {			
			for (Map.Entry<String, Collection<FunctionMethod>> entry : udf.getFunctions().entrySet()) {
				udfs.add(new FunctionTree(entry.getKey(), new UDFSource(entry.getValue()), true));
			}
		}
		
		CompositeMetadataStore compositeStore = new CompositeMetadataStore(stores.getStores());
		for (MetadataStore s:additionalStores) {
			compositeStore.addMetadataStore(s);
			for (Schema schema:s.getSchemas().values()) {
				if (!schema.getFunctions().isEmpty()) {
					udfs.add(new FunctionTree(schema.getName(), new UDFSource(schema.getFunctions().values()), true));
				}
			}
		}
		
		TransformationMetadata metadata =  new TransformationMetadata(vdb, compositeStore, visibilityMap, systemFunctions, udfs);
				
		return metadata;
	}
	
	public MetadataStore[] getAdditionalStores() {
		return additionalStores;
	}
	
	public synchronized VDBMetaData getVDB() {
		if (this.mergedVDB == null) {
			this.mergedVDB = buildVDB();
			update();
		}
		return this.mergedVDB;
	}
	
	public synchronized boolean hasChildVdb(VDBKey child) {
		if (this.children != null) {
			return this.children.containsKey(child);
		}
		return false;
	}
	
	private VDBMetaData buildVDB() {
		VDBMetaData newMergedVDB = new VDBMetaData();
		newMergedVDB.setName(this.vdb.getName());
		newMergedVDB.setVersion(this.vdb.getVersion());
		newMergedVDB.setModels(this.vdb.getModels());
		newMergedVDB.setDataPolicies(this.vdb.getDataPolicies());
		newMergedVDB.setDescription(this.vdb.getDescription());
		newMergedVDB.setStatus(this.vdb.getStatus());
		newMergedVDB.setJAXBProperties(this.vdb.getJAXBProperties());
		newMergedVDB.setConnectionType(this.vdb.getConnectionType());
		ConnectorManagerRepository mergedRepo = new ConnectorManagerRepository();
		mergedRepo.getConnectorManagers().putAll(this.cmr.getConnectorManagers());
		if (this.children != null) {
			for (CompositeVDB child:this.children.values()) {
				
				// add models
				for (Model m:child.getVDB().getModels()) {
					newMergedVDB.addModel((ModelMetaData)m);
				}
				
				for (DataPolicy p:child.getVDB().getDataPolicies()) {
					newMergedVDB.addDataPolicy((DataPolicyMetadata)p);
				}
				mergedRepo.getConnectorManagers().putAll(child.cmr.getConnectorManagers());
			}
		}
		newMergedVDB.addAttchment(ConnectorManagerRepository.class, mergedRepo);
		return newMergedVDB;
	}
	
	private UDFMetaData getUDF() {
		UDFMetaData mergedUDF = new UDFMetaData();
		if (this.udf != null) {
			mergedUDF.addFunctions(this.udf);
		}
		if (this.stores != null) {
			//schema scoped source functions - this is only a dynamic vdb concept
			for(MetadataStore store:this.stores.getStores()) {
				for (Schema schema:store.getSchemas().values()) {
					Collection<FunctionMethod> funcs = schema.getFunctions().values();
					mergedUDF.addFunctions(schema.getName(), funcs);
				}
			}
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
	
	private synchronized LinkedHashMap<String, Resource> getVisibilityMap() {
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
	
	public synchronized MetadataStoreGroup getMetadataStores() {
		if (this.children == null || this.children.isEmpty()) {
			return this.stores;
		}		
		
		MetadataStoreGroup mergedStores = new MetadataStoreGroup();
		if (this.stores != null) {
			mergedStores.addStores(this.stores.getStores());
		}
		for (CompositeVDB child:this.children.values()) {
			MetadataStoreGroup childStores = child.getMetadataStores();
			if ( childStores != null) {
				mergedStores.addStores(childStores.getStores());
			}
		}		
		return mergedStores;
	}
}
