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
import java.util.Date;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.xml.stream.XMLStreamException;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidException;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.MetadataStoreGroup;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
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
	private ContainerLifeCycleListener shutdownListener;
	private VDBLifeCycleListener vdbListener;
	
	public VDBService(VDBMetaData metadata, ContainerLifeCycleListener shutdownListener) {
		this.vdb = metadata;
		this.shutdownListener = shutdownListener;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		TranslatorRepository repo = new TranslatorRepository();
		this.vdb.addAttchment(TranslatorRepository.class, repo);
		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = this.vdb.getAttachment(UDFMetaData.class);
		IndexMetadataFactory indexFactory = this.vdb.getAttachment(IndexMetadataFactory.class);
		
		// add required connector managers; if they are not already there
		for (Translator t: this.vdb.getOverrideTranslators()) {
			VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;
			
			String type = data.getType();
			VDBTranslatorMetaData parent = getTranslatorRepository().getTranslatorMetaData(type);
			data.setModuleName(parent.getModuleName());
			
			Set<String> keys = parent.getProperties().stringPropertyNames();
			for (String key:keys) {
				if (data.getPropertyValue(key) == null && parent.getPropertyValue(key) != null) {
					data.addProperty(key, parent.getPropertyValue(key));
				}
			}
			repo.addTranslatorMetadata(data.getName(), data);
		}

		createConnectorManagers(cmr, repo, this.vdb);
		
		this.vdbListener = new VDBLifeCycleListener() {
			@Override
			public void added(String name, int version, CompositeVDB vdb) {
			}

			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
			}

			@Override
			public void finishedDeployment(String name, int version,CompositeVDB vdb) {
				// add object replication to temp/matview tables
				GlobalTableStore gts = new GlobalTableStoreImpl(getBuffermanager(), vdb.getVDB().getAttachment(TransformationMetadata.class));
				if (getObjectReplicatorInjector().getValue() != null) {
					try {
						gts = getObjectReplicatorInjector().getValue().replicate(name + version, GlobalTableStore.class, gts, 300000);
					} catch (Exception e) {
						LogManager.logError(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50023, gts)); 
					}
				}
				vdb.getVDB().addAttchment(GlobalTableStore.class, gts);
			}
		};
		
		getVDBRepository().addListener(this.vdbListener);
				
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
					throw new StartException(IntegrationPlugin.Event.TEIID50031.name(), e);
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
			LogManager.logError(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50024, this.vdb.getName(), vdb.getVersion()));
		}
		
		LinkedHashMap<String, Resource> visibilityMap = null;
				
		if (indexFactory != null) {
			visibilityMap = indexFactory.getEntriesPlusVisibilities();
		}
				
		try {
			// add transformation metadata to the repository.
			getVDBRepository().addVDB(this.vdb, store, visibilityMap, udf, cmr);
		} catch (VirtualDatabaseException e) {
			throw new StartException(IntegrationPlugin.Event.TEIID50032.name(), e);
		}
		
		boolean valid = true;
		synchronized (this.vdb) {
			if (indexFactory != null) {
				try {
					if (getSerializer().saveAttachment(getSerializer().buildVDBFile(this.vdb),store, false)) {
						LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB ", vdb.getName(), " metadata has been cached to data folder"); //$NON-NLS-1$ //$NON-NLS-2$
					}
				} catch (IOException e1) {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, e1, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50044, vdb.getName(), vdb.getVersion()));		
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
				
		LogManager.logInfo(LogConstants.CTX_RUNTIME, valid?RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,vdb.getName(), vdb.getVersion()):RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40006,vdb.getName(), vdb.getVersion()));		
	}

	@Override
	public void stop(StopContext context) {
		
		// stop object replication
		if (getObjectReplicatorInjector().getValue() != null) {
			GlobalTableStore gts = vdb.getAttachment(GlobalTableStore.class);
			getObjectReplicatorInjector().getValue().stop(gts);
		}		
		getVDBRepository().removeListener(this.vdbListener);
		getVDBRepository().removeVDB(this.vdb.getName(), this.vdb.getVersion());
		this.vdb.setRemoved(true);

		// service stopped not due to shutdown then clean-up the data files
		if (!this.shutdownListener.isShutdownInProgress()) {
			getSerializer().removeAttachments(vdb); 
			LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+vdb.getName()+" metadata removed"); //$NON-NLS-1$ //$NON-NLS-2$
		}

		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50026, this.vdb));
	}

	@Override
	public VDBMetaData getValue() throws IllegalStateException,IllegalArgumentException {
		return this.vdb;
	}
	
	private void createConnectorManagers(ConnectorManagerRepository cmr, TranslatorRepository repo, final VDBMetaData deployment) throws StartException {
		IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map = new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>();
		
		for (Model model:deployment.getModels()) {
			List<String> sourceNames = model.getSourceNames();
			if (sourceNames.size() != new HashSet<String>(sourceNames).size()) {
				throw new StartException(IntegrationPlugin.Util.getString("duplicate_source_name", model.getName(), deployment.getName(), deployment.getVersion(), IntegrationPlugin.Event.TEIID50033)); //$NON-NLS-1$
			}
			for (String source:sourceNames) {
				ConnectorManager cm = cmr.getConnectorManager(source);
				String name = model.getSourceTranslatorName(source);
				String connection = model.getSourceConnectionJndiName(source);
				if (cm != null) {
					if (!cm.getTranslatorName().equals(name)
							|| !cm.getConnectionName().equals(connection)) {
						throw new StartException(IntegrationPlugin.Util.getString("source_name_mismatch", source, deployment.getName(), deployment.getVersion(),IntegrationPlugin.Event.TEIID50034)); //$NON-NLS-1$
					}
					continue;
				}

				cm = new ConnectorManager(name, connection);
				try {
					ExecutionFactory<Object, Object> ef = getExecutionFactory(name, repo, getTranslatorRepository(), deployment, map, new HashSet<String>());
					cm.setExecutionFactory(ef);
					cmr.addConnectorManager(source, cm);
				} catch (TranslatorNotFoundException e) {
					if (e.getCause() != null) {
						throw new StartException(IntegrationPlugin.Event.TEIID50035.name(), e.getCause());
					}
					throw new StartException(IntegrationPlugin.Event.TEIID50035.name()+" "+e.getMessage()); //$NON-NLS-1$
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
				
		        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		        if (translator.getModuleName() != null) {
			        try {
			        	final ModuleIdentifier moduleId = ModuleIdentifier.create(translator.getModuleName());
			        	final Module module = Module.getCallerModuleLoader().loadModule(moduleId);
			        	classloader = module.getClassLoader();
			        } catch (ModuleLoadException e) {
			            throw new TeiidException(e, RuntimePlugin.Util.getString("failed_load_module", translator.getModuleName(), translator.getName())); //$NON-NLS-1$
			        }		
		        }
				
				ef = TranslatorUtil.buildExecutionFactory(translator, classloader);
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
	    		String failed_msg = IntegrationPlugin.Util.gs(loaded==null?IntegrationPlugin.Event.TEIID50036:IntegrationPlugin.Event.TEIID50030, vdb.getName(), vdb.getVersion(), model.getName());
		    	model.addError(ModelMetaData.ValidationError.Severity.ERROR.toString(), failed_msg); 
		    	if (exceptionMessage != null) {
		    		model.addError(ModelMetaData.ValidationError.Severity.ERROR.toString(), exceptionMessage);     		
		    	}
		    	LogManager.logWarning(LogConstants.CTX_RUNTIME, failed_msg);
	    	} else {
	    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50029,vdb.getName(), vdb.getVersion(), model.getName())); 
	    		model.clearErrors();
	    		if (vdb.isValid()) {
	    			getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion());
					vdb.setStatus(VDB.Status.ACTIVE);
					LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(RuntimePlugin.Event.TEIID40003,vdb.getName(), vdb.getVersion()));
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
