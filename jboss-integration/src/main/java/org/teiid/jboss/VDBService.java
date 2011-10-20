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
package org.teiid.jboss;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

import javax.xml.stream.XMLStreamException;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.*;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.*;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidException;
import org.teiid.deployers.*;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.services.BufferServiceImpl;
import org.teiid.translator.DelegatingExecutionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

class VDBService implements Service<VDBMetaData> {
	private VDBMetaData vdb;
	private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
	private final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	private final InjectedValue<ObjectSerializer> serializerInjector = new InjectedValue<ObjectSerializer>();
	private final InjectedValue<BufferServiceImpl> bufferServiceInjector = new InjectedValue<BufferServiceImpl>();
	private final InjectedValue<ObjectReplicator> objectReplicatorInjector = new InjectedValue<ObjectReplicator>();
	private boolean undeployInProgress = false;
	
	public VDBService(VDBMetaData metadata) {
		this.vdb = metadata;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		TranslatorRepository repo = new TranslatorRepository();

		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = this.vdb.getAttachment(UDFMetaData.class);
		IndexMetadataFactory indexFactory = this.vdb.getAttachment(IndexMetadataFactory.class);
		
		// add required connector managers; if they are not already there
		for (Translator t: this.vdb.getOverrideTranslators()) {
			VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;
			
			String type = data.getType();
			Translator parent = getTranslatorRepository().getTranslatorMetaData(type);
			
			Set<String> keys = parent.getProperties().stringPropertyNames();
			for (String key:keys) {
				if (data.getPropertyValue(key) == null && parent.getPropertyValue(key) != null) {
					data.addProperty(key, parent.getPropertyValue(key));
				}
			}
			repo.addTranslatorMetadata(data.getName(), data);
		}

		createConnectorManagers(cmr, repo, this.vdb);
				
		// check to see if the vdb has been modified when server is down; if it is then clear the old files
		// This is no longer required as VDB can not modified in place in deployment directory as they held inside the data dir
		//if (vdbModifiedTime != -1L && getSerializer().isStale(this.vdb, vdbModifiedTime)) {
		//	getSerializer().removeAttachments(this.vdb);
		//	LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB ", vdb.getName(), " old cached metadata has been removed"); //$NON-NLS-1$ //$NON-NLS-2$				
		//}
				
		boolean asynchLoad = false;
		boolean preview = this.vdb.isPreview();
		
		// if store is null and vdb dynamic vdb then try to get the metadata
		MetadataStoreGroup store  = null;
		if (this.vdb.isDynamic()) {
			store = new MetadataStoreGroup();
			asynchLoad = buildDynamicMetadataStore(this.vdb, store, cmr);
		}
		else if (indexFactory != null){
			store = getSerializer().loadSafe(getSerializer().buildVDBFile(this.vdb), MetadataStoreGroup.class);
			if (store == null) {
				store = new MetadataStoreGroup();
				try {
					store.addStore(indexFactory.getMetadataStore(getVDBRepository().getSystemStore().getDatatypes()));
				} catch (IOException e) {
					throw new StartException(e);
				}
			}
			else {
				LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB ", vdb.getName(), " was loaded from cached metadata"); //$NON-NLS-1$ //$NON-NLS-2$				
			}
		}
		
		// allow empty vdbs for enabling the preview functionality
		if (preview && store == null) {
			store = new MetadataStoreGroup();
		}
		
		if (store == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("failed_matadata_load", this.vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
		}
		
		LinkedHashMap<String, Resource> visibilityMap = null;
				
		if (indexFactory != null) {
			visibilityMap = indexFactory.getEntriesPlusVisibilities();
		}
				
		try {
			// add transformation metadata to the repository.
			getVDBRepository().addVDB(this.vdb, store, visibilityMap, udf, cmr);
		} catch (VirtualDatabaseException e) {
			throw new StartException(e);
		}
		
		boolean valid = true;
		synchronized (this.vdb) {
			if (indexFactory != null) {
				try {
					if (getSerializer().saveAttachment(getSerializer().buildVDBFile(this.vdb),store, false)) {
						LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB ", vdb.getName(), " metadata has been cached to data folder"); //$NON-NLS-1$ //$NON-NLS-2$
					}
				} catch (IOException e1) {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, e1, RuntimePlugin.Util.getString("vdb_save_failed", vdb.getName()+"."+vdb.getVersion())); //$NON-NLS-1$ //$NON-NLS-2$			
				}
			}
			if (!preview) {
				valid = validateSources(cmr, vdb);
				
				// Check if the VDB is fully configured.
				if (!valid) {
					vdb.setStatus(VDB.Status.INACTIVE);
				} else if (!asynchLoad) {
					//if asynch this will be set by the loading thread
					getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion());
					vdb.setStatus(VDB.Status.ACTIVE);
				}
			}
			else {
				vdb.setStatus(VDB.Status.ACTIVE);
			}
		}
		this.vdb.removeAttachment(UDFMetaData.class);
		this.vdb.removeAttachment(MetadataStoreGroup.class);
		this.vdb.removeAttachment(IndexMetadataFactory.class);	
		
		// add object replication to temp/matview tables
		GlobalTableStore gts = new GlobalTableStoreImpl(getBuffermanager(), vdb.getAttachment(TransformationMetadata.class));
		if (getObjectReplicatorInjector().getValue() != null) {
			try {
				gts = getObjectReplicatorInjector().getValue().replicate(vdb.getName() + vdb.getVersion(), GlobalTableStore.class, gts, 300000);
			} catch (Exception e) {
				LogManager.logError(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("replication_failed", gts)); //$NON-NLS-1$
			}
		}
		vdb.addAttchment(GlobalTableStore.class, gts);		
		
		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_deployed",vdb, valid?"active":"inactive")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$		
	}

	@Override
	public void stop(StopContext context) {
		
		// stop object replication
		if (getObjectReplicatorInjector().getValue() != null) {
			GlobalTableStore gts = vdb.getAttachment(GlobalTableStore.class);
			getObjectReplicatorInjector().getValue().stop(gts);
		}		
		
		getVDBRepository().removeVDB(this.vdb.getName(), this.vdb.getVersion());
		this.vdb.setRemoved(true);

		// service stopped not due to shutdown then clean-up the data files
		if (undeployInProgress) {
			getSerializer().removeAttachments(vdb); 
			LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+vdb.getName()+" metadata removed"); //$NON-NLS-1$ //$NON-NLS-2$
		}

		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_undeployed", this.vdb)); //$NON-NLS-1$
	}

	@Override
	public VDBMetaData getValue() throws IllegalStateException,IllegalArgumentException {
		return this.vdb;
	}
	
	private void createConnectorManagers(ConnectorManagerRepository cmr, TranslatorRepository repo, final VDBMetaData deployment) throws StartException {
		IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map = new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>();
		
		for (Model model:deployment.getModels()) {
			for (String source:model.getSourceNames()) {
				if (cmr.getConnectorManager(source) != null) {
					continue;
				}

				String name = model.getSourceTranslatorName(source);
				ConnectorManager cm = new ConnectorManager(name, model.getSourceConnectionJndiName(source));
				try {
					ExecutionFactory<Object, Object> ef = getExecutionFactory(name, repo, getTranslatorRepository(), deployment, map, new HashSet<String>());
					cm.setExecutionFactory(ef);
					cm.setModelName(model.getName());
					cmr.addConnectorManager(source, cm);
				} catch (TranslatorNotFoundException e) {
					if (e.getCause() != null) {
						throw new StartException(e.getCause());
					}
					throw new StartException(e.getMessage());
				}
			}
		}
	}
	
	static ExecutionFactory<Object, Object> getExecutionFactory(String name, TranslatorRepository vdbRepo, TranslatorRepository repo, VDBMetaData deployment, IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map, HashSet<String> building) throws TranslatorNotFoundException {
		if (!building.add(name)) {
			throw new TranslatorNotFoundException(RuntimePlugin.Util.getString("recursive_delegation", deployment.getName(), deployment.getVersion(), building)); //$NON-NLS-1$
		}
		VDBTranslatorMetaData translator = vdbRepo.getTranslatorMetaData(name);
		if (translator == null) {
			translator = repo.getTranslatorMetaData(name);
		}
		if (translator == null) {
			throw new TranslatorNotFoundException(RuntimePlugin.Util.getString("translator_not_found", deployment.getName(), deployment.getVersion(), name)); //$NON-NLS-1$
		}
		try {
		ExecutionFactory<Object, Object> ef = map.get(translator);
		if ( ef == null) {
			ef = TranslatorUtil.buildExecutionFactory(translator);
			if (ef instanceof DelegatingExecutionFactory) {
				DelegatingExecutionFactory delegator = (DelegatingExecutionFactory)ef;
				String delegateName = delegator.getDelegateName();
				if (delegateName != null) {
					ExecutionFactory<Object, Object> delegate = getExecutionFactory(delegateName, vdbRepo, repo, deployment, map, building);
					((DelegatingExecutionFactory) ef).setDelegate(delegate);
				}
			}
			map.put(translator, ef);
		}
		return ef;
		} catch(TeiidException e) {
			throw new TranslatorNotFoundException(e);
		}
	}

	
	private boolean validateSources(ConnectorManagerRepository cmr, VDBMetaData deployment) {
		boolean valid = true;
		for(Model m:deployment.getModels()) {
			ModelMetaData model = (ModelMetaData)m;
			List<SourceMappingMetadata> mappings = model.getSourceMappings();
			for (SourceMappingMetadata mapping:mappings) {
				ConnectorManager cm = cmr.getConnectorManager(mapping.getName());
				String msg = cm.getStausMessage();
				if (msg != null && msg.length() > 0) {
					valid = false;
					model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), cm.getStausMessage());
					LogManager.logInfo(LogConstants.CTX_RUNTIME, cm.getStausMessage());
				}
			}
			
			// in the dynamic case the metadata may be still loading.
			if (!model.getErrors().isEmpty()) {
				valid = false;
			}
		}
		return valid;
	}

    private boolean buildDynamicMetadataStore(final VDBMetaData vdb, final MetadataStoreGroup vdbStore, final ConnectorManagerRepository cmr) throws StartException {
    	boolean asynch = false;
    	// make sure we are configured correctly first
		for (final ModelMetaData model:vdb.getModelMetaDatas().values()) {
	    	if (model.getSourceNames().isEmpty()) {
	    		throw new StartException(RuntimePlugin.Util.getString("fail_to_deploy", vdb.getName()+"-"+vdb.getVersion(), model.getName())); //$NON-NLS-1$ //$NON-NLS-2$
	    	}
			    	
	    	final boolean cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata")); //$NON-NLS-1$ //$NON-NLS-2$
	    	final File cacheFile = getSerializer().buildModelFile(this.vdb, model.getName());
	    	boolean loaded = false;
	    	if (cache) {
				MetadataStore store = getSerializer().loadSafe(cacheFile, MetadataStore.class);
				if (store != null) {
					vdbStore.addStore(store);
					loaded = true;
				}
	    	}
	    	
	    	if (!loaded) {
	    		Runnable job = new Runnable() {
					@Override
					public void run() {
						Boolean loadStatus = loadMetadata(vdb, model, cache, cacheFile, vdbStore, cmr);
						//if (loadStatus == null) {
							//TODO: a source is up, but we failed.  should we retry or poll?
						//} 
						if (loadStatus == null || !loadStatus) {
							//defer the load to the status checker if/when a source is available/redeployed
							model.addAttchment(Runnable.class, this);
						}	    				
					}
	    		};	    		
	    		Executor executor = getExecutor();
	    		if (executor == null) {
	    			job.run();
	    		}
	    		else {
		    		asynch = true;
		    		executor.execute(job);
	    		}
	    	}
		}
		return asynch;
	}	
    
    /**
     * @return true if loaded, null if not loaded - but a cm is available, else false
     */
    private Boolean loadMetadata(VDBMetaData vdb, ModelMetaData model, boolean cache, File cacheFile, MetadataStoreGroup vdbStore, ConnectorManagerRepository cmr) {
		String msg = RuntimePlugin.Util.getString("model_metadata_loading", vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date())); //$NON-NLS-1$ 
		model.addError(ModelMetaData.ValidationError.Severity.ERROR.toString(), msg); 
		LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);

    	String exceptionMessage = null;
    	Boolean loaded = false;
    	for (String sourceName: model.getSourceNames()) {
    		ConnectorManager cm = cmr.getConnectorManager(sourceName);
    		String status = cm.getStausMessage();
			if (status != null && status.length() > 0) {
				exceptionMessage = status;
				continue;
			}
			loaded = null;
    		try {
    			MetadataStore store = cm.getMetadata(model.getName(), getVDBRepository().getBuiltinDatatypes(), model.getProperties());
    			if (cache) {
    				getSerializer().saveAttachment(cacheFile, store, false);
    			}
    			vdbStore.addStore(store);
    			loaded = true;
    			break;
			} catch (TranslatorException e) {
				//TODO: we aren't effectively differentiating the type of load error - connectivity vs. metadata
				if (exceptionMessage == null) {
					exceptionMessage = e.getMessage();
				}
			} catch (IOException e) {
				if (exceptionMessage == null) {
					exceptionMessage = e.getMessage();
				}				
			}
    	}
    	
    	synchronized (vdb) {
	    	if (loaded == null || !loaded) {
	    		vdb.setStatus(VDB.Status.INACTIVE);
	    		String failed_msg = RuntimePlugin.Util.getString(loaded==null?"failed_to_retrive_metadata":"nosources_to_retrive_metadata", vdb.getName(), vdb.getVersion(), model.getName()); //$NON-NLS-1$ //$NON-NLS-2$ 
		    	model.addError(ModelMetaData.ValidationError.Severity.ERROR.toString(), failed_msg); 
		    	if (exceptionMessage != null) {
		    		model.addError(ModelMetaData.ValidationError.Severity.ERROR.toString(), exceptionMessage);     		
		    	}
		    	LogManager.logWarning(LogConstants.CTX_RUNTIME, failed_msg);
	    	} else {
	    		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("metadata_loaded",vdb.getName(), vdb.getVersion(), model.getName())); //$NON-NLS-1$
	    		model.clearErrors();
	    		if (vdb.isValid()) {
	    			getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion());
					vdb.setStatus(VDB.Status.ACTIVE);
					LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_activated",vdb.getName(), vdb.getVersion())); //$NON-NLS-1$    			
	    		}
	    	}
    	}
    	
    	return loaded;
    }
		
	public InjectedValue<VDBRepository> getVDBRepositoryInjector(){
		return this.vdbRepositoryInjector;
	}
	
	private VDBRepository getVDBRepository() {
		return vdbRepositoryInjector.getValue();
	}
	
	public InjectedValue<TranslatorRepository> getTranslatorRepositoryInjector(){
		return this.translatorRepositoryInjector;
	}
	
	private TranslatorRepository getTranslatorRepository() {
		return this.translatorRepositoryInjector.getValue();
	}
	
	public InjectedValue<Executor> getExecutorInjector(){
		return this.executorInjector;
	}
	
	private Executor getExecutor() {
		return this.executorInjector.getValue();
	}
	
	public InjectedValue<ObjectSerializer> getSerializerInjector() {
		return serializerInjector;
	}
	
	private ObjectSerializer getSerializer() {
		return serializerInjector.getValue();
	}
	
	public InjectedValue<BufferServiceImpl> getBufferServiceInjector() {
		return bufferServiceInjector;
	}
	
	private BufferManager getBuffermanager() {
		return getBufferServiceInjector().getValue().getBufferManager();
	}
	
	public InjectedValue<ObjectReplicator> getObjectReplicatorInjector() {
		return objectReplicatorInjector;
	}	
	
	public void undeployInProgress() {
		this.undeployInProgress = true;
	}
	
	public void addDataRole(String policyName, String mappedRole) throws AdminProcessingException{
		DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
		
		if (policy == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, this.vdb.getName(), this.vdb.getVersion())); //$NON-NLS-1$
		}		
		
		policy.addMappedRoleName(mappedRole);
		save();
	}
	
	public void remoteDataRole(String policyName, String mappedRole) throws AdminProcessingException{
		DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
		
		if (policy == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, this.vdb.getName(), this.vdb.getVersion())); //$NON-NLS-1$
		}		
		
		policy.removeMappedRoleName(mappedRole);
		save();
	}	
	
	public void addAnyAuthenticated(String policyName) throws AdminProcessingException{
		DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
		
		if (policy == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, this.vdb.getName(), this.vdb.getVersion())); //$NON-NLS-1$
		}		
		
		policy.setAnyAuthenticated(true);
		save();
	}	
	
	public void removeAnyAuthenticated(String policyName) throws AdminProcessingException{
		DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
		
		if (policy == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, this.vdb.getName(), this.vdb.getVersion())); //$NON-NLS-1$
		}		
		
		policy.setAnyAuthenticated(false);
		save();
	}		
	
	public void changeConnectionType(ConnectionType type) throws AdminProcessingException {
		this.vdb.setConnectionType(type);
		save();
	}
	
	public void assignDatasource(String modelName, String sourceName, String translatorName, String dsName) throws AdminProcessingException{
		ModelMetaData model = this.vdb.getModel(modelName);
		
		if (model == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("model_not_found", modelName, this.vdb.getName(), this.vdb.getVersion())); //$NON-NLS-1$
		}
		
		SourceMappingMetadata source = model.getSourceMapping(sourceName);
		if(source == null) {
			throw new AdminProcessingException(IntegrationPlugin.Util.getString("source_not_found", sourceName, modelName, this.vdb.getName(), this.vdb.getVersion())); //$NON-NLS-1$
		}
		source.setTranslatorName(translatorName);
		source.setConnectionJndiName(dsName);
		save();
	}
	
	private void save() throws AdminProcessingException{
		try {
			ObjectSerializer os = getSerializer();
			VDBMetadataParser.marshell(this.vdb, os.getVdbXmlOutputStream(this.vdb));
		} catch (IOException e) {
			throw new AdminProcessingException(e);
		} catch (XMLStreamException e) {
			throw new AdminProcessingException(e);
		}
	}
	
	@SuppressWarnings("serial")
	static class TranslatorNotFoundException extends TeiidException {
		public TranslatorNotFoundException(String msg) {
			super(msg);
		}
		public TranslatorNotFoundException(Throwable t) {
			super(t);
		}
	}
}
