package org.teiid.jboss;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;

import javax.resource.spi.work.WorkManager;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.vfs.VirtualFile;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.TeiidException;
import org.teiid.deployers.*;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.DelegatingExecutionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class VDBService implements Service<VDBMetaData> {
	private VDBMetaData vdb;
	private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
	private final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	private final InjectedValue<ObjectSerializer> serializerInjector = new InjectedValue<ObjectSerializer>();
	
	public VDBService(VDBMetaData metadata) {
		this.vdb = metadata;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		TranslatorRepository repo = new TranslatorRepository();
		
		// add required connector managers; if they are not already there
		for (Translator t: vdb.getOverrideTranslators()) {
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

		createConnectorManagers(cmr, repo, vdb);

		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = vdb.getAttachment(UDFMetaData.class);
		MetadataStoreGroup store = vdb.getAttachment(MetadataStoreGroup.class);
		
		boolean asynchLoad = false;
		boolean preview = vdb.isPreview();
		
		// if store is null and vdb dynamic vdb then try to get the metadata
		if (store == null && vdb.isDynamic()) {
			store = new MetadataStoreGroup();
			asynchLoad = buildDynamicMetadataStore(vdb, store, cmr);
		}
		
		// allow empty vdbs for enabling the preview functionality
		if (preview && store == null) {
			store = new MetadataStoreGroup();
		}
		
		if (store == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("failed_matadata_load", vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
		}
		
		LinkedHashMap<String, Resource> visibilityMap = null;
		IndexMetadataFactory indexFactory = vdb.getAttachment(IndexMetadataFactory.class);		
		if (indexFactory != null) {
			visibilityMap = indexFactory.getEntriesPlusVisibilities();
		}
				
		try {
			// add transformation metadata to the repository.
			getVDBRepository().addVDB(vdb, store, visibilityMap, udf, cmr);
		} catch (VirtualDatabaseException e) {
			throw new StartException(e);
		}
		
		boolean valid = true;
		synchronized (vdb) {
			if (indexFactory != null) {
				try {
					saveMetadataStore(vdb, store);
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
		
		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_deployed",vdb, valid?"active":"inactive")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$		
	}

	@Override
	public void stop(StopContext context) {
		
		getVDBRepository().removeVDB(vdb.getName(), vdb.getVersion());
		vdb.setRemoved(true);
		
		deleteMetadataStore(vdb);

		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_undeployed", vdb)); //$NON-NLS-1$
		
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
				ExecutionFactory<Object, Object> ef = getExecutionFactory(name, repo, deployment, map, new HashSet<String>());
				cm.setExecutionFactory(ef);
				cm.setModelName(model.getName());
				cmr.addConnectorManager(source, cm);
			}
		}
	}
	
	private ExecutionFactory<Object, Object> getExecutionFactory(String name, TranslatorRepository repo, VDBMetaData deployment, IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map, HashSet<String> building) throws StartException {
		if (!building.add(name)) {
			throw new StartException(RuntimePlugin.Util.getString("recursive_delegation", deployment.getName(), deployment.getVersion(), building)); //$NON-NLS-1$
		}
		VDBTranslatorMetaData translator = repo.getTranslatorMetaData(name);
		if (translator == null) {
			translator = getTranslatorRepository().getTranslatorMetaData(name);
		}
		if (translator == null) {
			throw new StartException(RuntimePlugin.Util.getString("translator_not_found", deployment.getName(), deployment.getVersion(), name)); //$NON-NLS-1$
		}
		try {
		ExecutionFactory<Object, Object> ef = map.get(translator);
		if ( ef == null) {
			ef = TranslatorUtil.buildExecutionFactory(translator);
			if (ef instanceof DelegatingExecutionFactory) {
				DelegatingExecutionFactory delegator = (DelegatingExecutionFactory)ef;
				String delegateName = delegator.getDelegateName();
				if (delegateName != null) {
					ExecutionFactory<Object, Object> delegate = getExecutionFactory(delegateName, repo, deployment, map, building);
					((DelegatingExecutionFactory) ef).setDelegate(delegate);
				}
			}
			map.put(translator, ef);
		}
		return ef;
		} catch(TeiidException e) {
			throw new StartException(e);
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
	    	final File cacheFile = buildCachedModelFileName(vdb, model.getName());
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
    				getSerializer().saveAttachment(cacheFile, store);
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
    
	private void saveMetadataStore(VDBMetaData vdb, MetadataStoreGroup store) throws IOException {
		File cacheFileName = buildCachedVDBFileName(getSerializer(), vdb);
		if (!cacheFileName.exists()) {
			getSerializer().saveAttachment(cacheFileName,store);
			LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+vdb.getName()+" metadata has been cached to "+ cacheFileName); //$NON-NLS-1$ //$NON-NLS-2$
		}		
	}
	
	private void deleteMetadataStore(VDBMetaData vdb) {
		if (!unit.exists() || !shutdownListener.isShutdownInProgress()) {
			getSerializer().removeAttachments(vdb.getName()+"_"+vdb.getVersion()); //$NON-NLS-1$
			LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+vdb.getName()+" metadata removed"); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
	
    
	private File buildCachedModelFileName(VDBMetaData vdb, String modelName) {
		return getSerializer().getAttachmentPath(vdb.getName()+"_"+vdb.getVersion(), vdb.getName()+"_"+vdb.getVersion()+"_"+modelName); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}    
	
	static File buildCachedVDBFileName(ObjectSerializer serializer, VDBMetaData vdb) {
		return serializer.getAttachmentPath(vdb.getName()+"_"+vdb.getVersion(), vdb.getName()+"_"+vdb.getVersion()); //$NON-NLS-1$ //$NON-NLS-2$
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
}
