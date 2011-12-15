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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.util.threadpool.ThreadPool;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
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


public class VDBDeployer extends AbstractSimpleRealDeployer<VDBMetaData> {
	private VDBRepository vdbRepository;
	private TranslatorRepository translatorRepository;
	private ObjectSerializer serializer;
	private ContainerLifeCycleListener shutdownListener;
	private ThreadPool threadPool;
	
	public VDBDeployer() {
		super(VDBMetaData.class);
		setInput(VDBMetaData.class);
		setOutput(VDBMetaData.class);
		setRelativeOrder(3001); // after the data sources
	}

	@Override
	public void deploy(DeploymentUnit unit, VDBMetaData deployment) throws DeploymentException {
		if (this.vdbRepository.removeVDB(deployment.getName(), deployment.getVersion())) {
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("redeploying_vdb", deployment)); //$NON-NLS-1$ 
		}
		
		TranslatorRepository repo = new TranslatorRepository();
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		deployment.addAttchment(TranslatorRepository.class, repo);
		boolean preview = deployment.isPreview();
		
		if (!preview) {
			List<String> errors = deployment.getValidityErrors();
			if (errors != null && !errors.isEmpty()) {
				throw new DeploymentException(RuntimePlugin.Util.getString("validity_errors_in_vdb", deployment)); //$NON-NLS-1$
			}
		}
		
		// get the metadata store of the VDB (this is build in parse stage)
		MetadataStoreGroup store = unit.getAttachment(MetadataStoreGroup.class);
		
		// add required connector managers; if they are not already there
		for (Translator t: deployment.getOverrideTranslators()) {
			VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;
			
			String type = data.getType();
			Translator parent = this.translatorRepository.getTranslatorMetaData(type);
			if ( parent == null) {
				throw new DeploymentException(RuntimePlugin.Util.getString("translator_type_not_found", unit.getName())); //$NON-NLS-1$
			}
			
			Set<String> keys = parent.getProperties().stringPropertyNames();
			for (String key:keys) {
				if (data.getPropertyValue(key) == null && parent.getPropertyValue(key) != null) {
					data.addProperty(key, parent.getPropertyValue(key));
				}
			}
			repo.addTranslatorMetadata(data.getName(), data);
		}
		createConnectorManagers(cmr, repo, deployment);
		boolean asynchLoad = false;
		// if store is null and vdb dynamic vdb then try to get the metadata
		if (store == null && deployment.isDynamic()) {
			store = new MetadataStoreGroup();
			asynchLoad = buildDynamicMetadataStore((VFSDeploymentUnit)unit, deployment, store, cmr);
		}
		
		// allow empty vdbs for enabling the preview functionality
		if (preview && store == null) {
			store = new MetadataStoreGroup();
		}
		
		if (store == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("failed_matadata_load", deployment.getName(), deployment.getVersion())); //$NON-NLS-1$
		}
		
		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = unit.getAttachment(UDFMetaData.class);
		
		LinkedHashMap<String, Resource> visibilityMap = null;
		IndexMetadataFactory indexFactory = unit.getAttachment(IndexMetadataFactory.class);		
		if (indexFactory != null) {
			visibilityMap = indexFactory.getEntriesPlusVisibilities();
		}
				
		// add the metadata objects as attachments
		deployment.removeAttachment(IndexMetadataFactory.class);
		deployment.removeAttachment(UDFMetaData.class);
		deployment.removeAttachment(MetadataStoreGroup.class);
		
		// add transformation metadata to the repository.
		this.vdbRepository.addVDB(deployment, store, visibilityMap, udf, cmr);
		
		boolean valid = true;
		synchronized (deployment) {
			if (indexFactory != null) {
				try {
					saveMetadataStore((VFSDeploymentUnit)unit, deployment, store);
				} catch (IOException e1) {
					LogManager.logWarning(LogConstants.CTX_RUNTIME, e1, RuntimePlugin.Util.getString("vdb_save_failed", deployment.getName()+"."+deployment.getVersion())); //$NON-NLS-1$ //$NON-NLS-2$			
				}
			}
			if (!preview) {
				valid = validateSources(cmr, deployment);
				
				// Check if the VDB is fully configured.
				if (!valid) {
					deployment.setStatus(VDB.Status.INACTIVE);
				} else if (!asynchLoad) {
					//if asynch this will be set by the loading thread
					this.vdbRepository.finishDeployment(deployment.getName(), deployment.getVersion());
					deployment.setStatus(VDB.Status.ACTIVE);
				}
			}
			else {
				deployment.setStatus(VDB.Status.ACTIVE);
			}
		}
		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_deployed",deployment, valid?"active":"inactive")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	private void createConnectorManagers(ConnectorManagerRepository cmr, TranslatorRepository repo, final VDBMetaData deployment) throws DeploymentException {
		IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map = new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>();
		
		for (Model model:deployment.getModels()) {
			List<String> sourceNames = model.getSourceNames();
			if (sourceNames.size() != new HashSet<String>(sourceNames).size()) {
				throw new DeploymentException(RuntimePlugin.Util.getString("duplicate_source_name", model.getName(), deployment.getName(), deployment.getVersion())); //$NON-NLS-1$
			}
			for (String source:sourceNames) {
				ConnectorManager cm = cmr.getConnectorManager(source);
				String name = model.getSourceTranslatorName(source);
				String connection = model.getSourceConnectionJndiName(source);
				if (cm != null) {
					if (!cm.getTranslatorName().equals(name)
							|| !cm.getConnectionName().equals(connection)) {
						throw new DeploymentException(RuntimePlugin.Util.getString("source_name_mismatch", source, deployment.getName(), deployment.getVersion())); //$NON-NLS-1$
					}
					continue;
				}

				cm = new ConnectorManager(name, connection);
				ExecutionFactory<Object, Object> ef = getExecutionFactory(name, repo, deployment, map, new HashSet<String>());
				cm.setExecutionFactory(ef);
				cm.setModelName(model.getName());
				cmr.addConnectorManager(source, cm);
			}
		}
	}
	
	private ExecutionFactory<Object, Object> getExecutionFactory(String name, TranslatorRepository repo, VDBMetaData deployment, IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map, HashSet<String> building) throws DeploymentException {
		if (!building.add(name)) {
			throw new DeploymentException(RuntimePlugin.Util.getString("recursive_delegation", deployment.getName(), deployment.getVersion(), building)); //$NON-NLS-1$
		}
		Translator translator = repo.getTranslatorMetaData(name);
		if (translator == null) {
			translator = this.translatorRepository.getTranslatorMetaData(name);
		}
		if (translator == null) {
			throw new DeploymentException(RuntimePlugin.Util.getString("translator_not_found", deployment.getName(), deployment.getVersion(), name)); //$NON-NLS-1$
		}
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

	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}
	
	@Override
	public void undeploy(DeploymentUnit unit, VDBMetaData deployment) {
		super.undeploy(unit, deployment);
		
		if (this.vdbRepository != null) {
			this.vdbRepository.removeVDB(deployment.getName(), deployment.getVersion());
		}
		
		deployment.setRemoved(true);
		
		try {
			deleteMetadataStore((VFSDeploymentUnit)unit);
		} catch (IOException e) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_delete_failed", e.getMessage())); //$NON-NLS-1$
		}

		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_undeployed", deployment)); //$NON-NLS-1$
	}

	public void setObjectSerializer(ObjectSerializer serializer) {
		this.serializer = serializer;
	}		
	
	private void saveMetadataStore(VFSDeploymentUnit unit, VDBMetaData vdb, MetadataStoreGroup store) throws IOException {
		File cacheFileName = buildCachedVDBFileName(this.serializer, unit, vdb);
		if (!cacheFileName.exists()) {
			this.serializer.saveAttachment(cacheFileName,store);
			LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+unit.getRoot().getName()+" metadata has been cached to "+ cacheFileName); //$NON-NLS-1$ //$NON-NLS-2$
		}		
	}
	
	private void deleteMetadataStore(VFSDeploymentUnit unit) throws IOException {
		if (!unit.getRoot().exists() || !shutdownListener.isShutdownInProgress()) {
			this.serializer.removeAttachments(unit);
			LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+unit.getRoot().getName()+" metadata removed"); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
	
    private boolean buildDynamicMetadataStore(final VFSDeploymentUnit unit, final VDBMetaData vdb, final MetadataStoreGroup vdbStore, final ConnectorManagerRepository cmr) throws DeploymentException {
    	boolean asynch = false;
    	// make sure we are configured correctly first
		for (final ModelMetaData model:vdb.getModelMetaDatas().values()) {
	    	if (model.getSourceNames().isEmpty()) {
	    		throw new DeploymentException(RuntimePlugin.Util.getString("fail_to_deploy", vdb.getName()+"-"+vdb.getVersion(), model.getName())); //$NON-NLS-1$ //$NON-NLS-2$
	    	}
			    	
	    	final boolean cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata")); //$NON-NLS-1$ //$NON-NLS-2$
	    	final File cacheFile = buildCachedModelFileName(unit, vdb, model.getName());
	    	boolean loaded = false;
	    	if (cache) {
				MetadataStore store = this.serializer.loadSafe(cacheFile, MetadataStore.class);
				if (store != null) {
					vdbStore.addStore(store);
					loaded = true;
				}
	    	}
	    	
	    	if (!loaded) {
	    		asynch = true;
	    		threadPool.run(new Runnable() {
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
	    		});
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
    			MetadataStore store = cm.getMetadata(model.getName(), this.vdbRepository.getBuiltinDatatypes(), model.getProperties());
    			if (cache) {
    				this.serializer.saveAttachment(cacheFile, store);
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
	    			this.vdbRepository.finishDeployment(vdb.getName(), vdb.getVersion());
					vdb.setStatus(VDB.Status.ACTIVE);
					LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_activated",vdb.getName(), vdb.getVersion())); //$NON-NLS-1$    			
	    		}
	    	}
    	}
    	
    	return loaded;
    }
    
	private File buildCachedModelFileName(VFSDeploymentUnit unit, VDBMetaData vdb, String modelName) {
		return this.serializer.getAttachmentPath(unit, vdb.getName()+"_"+vdb.getVersion()+"_"+modelName); //$NON-NLS-1$ //$NON-NLS-2$
	}    
	
	static File buildCachedVDBFileName(ObjectSerializer serializer, VFSDeploymentUnit unit, VDBMetaData vdb) {
		return serializer.getAttachmentPath(unit, vdb.getName()+"_"+vdb.getVersion()); //$NON-NLS-1$
	} 	
	
	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}	
	
	public void setContainerLifeCycleListener(ContainerLifeCycleListener listener) {
		shutdownListener = listener;
	}
	
	public void setThreadPool(ThreadPool pool) {
		this.threadPool = pool;
	}
}
