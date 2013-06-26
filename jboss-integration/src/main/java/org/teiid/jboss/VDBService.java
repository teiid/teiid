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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.ModelController;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.deployers.CompositeGlobalTableStore;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.RuntimeVDB;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.jboss.rest.ResteasyEnabler;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.index.IndexMetadataRepository;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.runtime.AbstractVDBDeployer;
import org.teiid.translator.DelegatingExecutionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

class VDBService extends AbstractVDBDeployer implements Service<RuntimeVDB> {
	private VDBMetaData vdb;
	private RuntimeVDB runtimeVDB;
	protected final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	protected final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
	protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	protected final InjectedValue<ObjectSerializer> serializerInjector = new InjectedValue<ObjectSerializer>();
	protected final InjectedValue<BufferManager> bufferManagerInjector = new InjectedValue<BufferManager>();
	protected final InjectedValue<ObjectReplicator> objectReplicatorInjector = new InjectedValue<ObjectReplicator>();
	protected final InjectedValue<VDBStatusChecker> vdbStatusCheckInjector = new InjectedValue<VDBStatusChecker>();
	
	// for REST deployment
	protected final InjectedValue<ModelController> controllerValue = new InjectedValue<ModelController>();
	
	private VDBLifeCycleListener vdbListener;
	private VDBLifeCycleListener restEasyListener;
	private VDBResources vdbResources;
	
	public VDBService(VDBMetaData metadata, VDBResources vdbResources) {
		this.vdb = metadata;
		this.vdbResources = vdbResources;
	}
	
	@Override
	public void start(final StartContext context) throws StartException {
		
		ConnectorManagerRepository cmr = new ConnectorManagerRepository();
		TranslatorRepository repo = new TranslatorRepository();
		
		this.vdb.addAttchment(TranslatorRepository.class, repo);

		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = this.vdb.getAttachment(UDFMetaData.class);
		
		// add required connector managers; if they are not already there
		for (Translator t: this.vdb.getOverrideTranslators()) {
			VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;
			
			String type = data.getType();
			VDBTranslatorMetaData parent = getTranslatorRepository().getTranslatorMetaData(type);
			data.setModuleName(parent.getModuleName());
			data.addAttchment(ClassLoader.class, parent.getAttachment(ClassLoader.class));
			data.setParent(parent);
			repo.addTranslatorMetadata(data.getName(), data);
		}

		createConnectorManagers(cmr, repo, this.vdb);
		final ServiceBuilder<Void> vdbService = addVDBFinishedService(context);
		this.vdbListener = new VDBLifeCycleListener() {
			@Override
			public void added(String name, int version, CompositeVDB cvdb) {
			}

			@Override
			public void removed(String name, int version, CompositeVDB cvdb) {
			}

			@Override
			public void finishedDeployment(String name, int version, CompositeVDB cvdb) {
				if (!name.equals(VDBService.this.vdb.getName()) || version != VDBService.this.vdb.getVersion()) {
					return;
				}
				//clear out the indexmetadatarepository as it holds state that is no longer necessary
				repositories.put("index", new IndexMetadataRepository()); //$NON-NLS-1$ 
				VDBMetaData vdbInstance = cvdb.getVDB();
				if (vdbInstance.getStatus().equals(Status.ACTIVE)) {
					// add object replication to temp/matview tables
					GlobalTableStore gts = CompositeGlobalTableStore.createInstance(cvdb, getBuffermanager(), objectReplicatorInjector.getValue());

					vdbInstance.addAttchment(GlobalTableStore.class, gts);
					vdbService.install();
				}
			}
		};
		
		getVDBRepository().addListener(this.vdbListener);
		
		this.restEasyListener = new ResteasyEnabler(this.vdb.getName(), this.vdb.getVersion(), controllerValue.getValue(), executorInjector.getValue());
		getVDBRepository().addListener(this.restEasyListener);
				
		MetadataStore store = new MetadataStore();
		
		try {
			//check to see if there is an index file.  if there is then we assume
			//that index is the default metadata repo
			MetadataRepository<?, ?> defaultRepo = null;
			for (String s : this.vdbResources.getEntriesPlusVisibilities().keySet()) {
				if (s.endsWith(VDBResources.INDEX_EXT)) {
					defaultRepo = super.getMetadataRepository("index"); //$NON-NLS-1$
					break;
				}
			}
			this.assignMetadataRepositories(vdb, defaultRepo); 
			// add transformation metadata to the repository.
			getVDBRepository().addVDB(this.vdb, store, vdbResources.getEntriesPlusVisibilities(), udf, cmr);
		} catch (VirtualDatabaseException e) {
			throw new StartException(e);
		}		
		
		this.vdb.removeAttachment(UDFMetaData.class);
		try {
			loadMetadata(this.vdb, cmr, store, this.vdbResources);
		} catch (TranslatorException e) {
			throw new StartException(e);
		}
				
		this.runtimeVDB = buildRuntimeVDB(this.vdb, context.getController().getServiceContainer());		
	}

	private RuntimeVDB buildRuntimeVDB(final VDBMetaData vdbMetadata, final ServiceContainer serviceContainer) {
		RuntimeVDB.VDBModificationListener modificationListener = new RuntimeVDB.VDBModificationListener() {
			@Override
			public void dataRoleChanged(String policyName) throws AdminProcessingException {
				save();
			}
			@Override
			public void connectionTypeChanged() throws AdminProcessingException {
				save();
			}
			@Override
			public void dataSourceChanged(String modelName, String sourceName,String translatorName, String dsName) throws AdminProcessingException {
				save();
			}
			@Override
			public void onRestart(List<String> modelNames) {
				ServiceController<?> switchSvc = serviceContainer.getService(TeiidServiceNames.vdbSwitchServiceName(vdbMetadata.getName(), vdbMetadata.getVersion()));
		        if (switchSvc != null) {
		        	if (!modelNames.isEmpty()) {
						for (String model:modelNames) {
							deleteModelCache(model);
						}		        	
		        	}
		        	else {
		        		for (String model:vdbMetadata.getModelMetaDatas().keySet()) {
		        			deleteModelCache(model);
		        		}
		        	}
		            switchSvc.setMode(ServiceController.Mode.REMOVE);
		        }		        
			}			
		};
		return new RuntimeVDB(vdbMetadata, modificationListener) {
			protected VDBStatusChecker getVDBStatusChecker() {
				return VDBService.this.vdbStatusCheckInjector.getValue();
			}
		};
	}
	
	Service<Void> createVoidService() {
		return new Service<Void>() {
			@Override
			public Void getValue() throws IllegalStateException, IllegalArgumentException {
				return null;
			}
			@Override
			public void start(StartContext sc)throws StartException {}
			@Override
			public void stop(StopContext sc) {}
		};		
	}

	private ServiceBuilder<Void> addVDBFinishedService(StartContext context) {
		ServiceContainer serviceContainer = context.getController().getServiceContainer();
		final ServiceController<?> controller = serviceContainer.getService(TeiidServiceNames.vdbFinishedServiceName(vdb.getName(), vdb.getVersion()));
        if (controller != null) {
            controller.setMode(ServiceController.Mode.REMOVE);
        }
        return serviceContainer.addService(TeiidServiceNames.vdbFinishedServiceName(vdb.getName(), vdb.getVersion()), createVoidService());
	}

	@Override
	public void stop(StopContext context) {
		ServiceController<?> switchSvc = context.getController().getServiceContainer().getService(TeiidServiceNames.vdbSwitchServiceName(vdb.getName(), vdb.getVersion()));
        if (switchSvc != null) {
            switchSvc.setMode(ServiceController.Mode.REMOVE);
        }
		// stop object replication
		if (this.objectReplicatorInjector.getValue() != null) {
			GlobalTableStore gts = vdb.getAttachment(GlobalTableStore.class);
			this.objectReplicatorInjector.getValue().stop(gts);
		}		
		getVDBRepository().removeVDB(this.vdb.getName(), this.vdb.getVersion());
		getVDBRepository().removeListener(this.vdbListener);
		getVDBRepository().removeListener(this.restEasyListener);
		final ServiceController<?> controller = context.getController().getServiceContainer().getService(TeiidServiceNames.vdbFinishedServiceName(vdb.getName(), vdb.getVersion()));
        if (controller != null) {
            controller.setMode(ServiceController.Mode.REMOVE);
        }	        
		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50026, this.vdb));
	}

	@Override
	public RuntimeVDB getValue() throws IllegalStateException,IllegalArgumentException {
		return this.runtimeVDB;
	}
	
	private void createConnectorManagers(ConnectorManagerRepository cmr, final TranslatorRepository repo, final VDBMetaData deployment) throws StartException {
		final IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map = new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>();
		
		try {
			ConnectorManagerRepository.ExecutionFactoryProvider provider = new ConnectorManagerRepository.ExecutionFactoryProvider() {
				
				@Override
				public ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException {
					return VDBService.getExecutionFactory(name, repo, getTranslatorRepository(), deployment, map, new HashSet<String>());
				}
			};
			cmr.setProvider(provider);
			cmr.createConnectorManagers(deployment, provider);
		} catch (ConnectorManagerException e) {
			if (e.getCause() != null) {
				throw new StartException(IntegrationPlugin.Event.TEIID50035.name()+" "+e.getMessage(), e.getCause()); //$NON-NLS-1$
			}
			throw new StartException(e.getMessage());
		}
	}
	
	@SuppressWarnings({"rawtypes","unchecked"})
	static ExecutionFactory<Object, Object> getExecutionFactory(String name, TranslatorRepository vdbRepo, TranslatorRepository repo, VDBMetaData deployment, IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map, HashSet<String> building) throws ConnectorManagerException {
		if (!building.add(name)) {
			throw new ConnectorManagerException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50076, deployment.getName(), deployment.getVersion(), building));
		}
		VDBTranslatorMetaData translator = vdbRepo.getTranslatorMetaData(name);
		if (translator == null) {
			translator = repo.getTranslatorMetaData(name);
		}
		if (translator == null) {
			throw new ConnectorManagerException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50078, deployment.getName(), deployment.getVersion(), name));
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
						((DelegatingExecutionFactory<Object, Object>) ef).setDelegate(delegate);
					}
				}
				map.put(translator, ef);
			}
			return ef;
		} catch(TeiidException e) {
			throw new ConnectorManagerException(e);
		}
	}

	@Override
	@SuppressWarnings({"rawtypes","unchecked"})
    protected void loadMetadata(final VDBMetaData vdb, final ModelMetaData model, final ConnectorManagerRepository cmr, final MetadataRepository metadataRepo, final MetadataStore vdbMetadataStore, final AtomicInteger loadCount, final VDBResources vdbResources) {

    	String msg = IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50029,vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date()));
    	model.setMetadataStatus(Model.MetadataStatus.LOADING);
		model.addRuntimeMessage(Severity.INFO, msg); 
		LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);

		Runnable job = new Runnable() {
			@Override
			public void run() {
				
				boolean cached = false;
				Exception ex = null;
				TranslatorException te = null;
				
				// if this is not the first time trying to load metadata
				if (model.getMetadataStatus() != Model.MetadataStatus.LOADING) {
					model.setMetadataStatus(Model.MetadataStatus.RETRYING);
				}
				
				// designer based models define data types based on their built in data types, which are system vdb data types
				Map<String, Datatype> datatypes = getVDBRepository().getRuntimeTypeMap();
				Map<String, Datatype> builtin = getVDBRepository().getSystemStore().getDatatypes();
				final File cachedFile = getSerializer().buildModelFile(vdb, model.getName());
				MetadataFactory factory = getSerializer().loadSafe(cachedFile, MetadataFactory.class);
				if (factory != null) {
					factory.correctDatatypes(datatypes, builtin);
					cached = true;
					LogManager.logTrace(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " was loaded from cached metadata"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				} else {
					factory = createMetadataFactory(vdb, model, vdbResources.getEntriesPlusVisibilities());
					ExecutionFactory ef = null;
					Object cf = null;
					
					for (ConnectorManager cm : getConnectorManagers(model, cmr)) {
						if (ex != null) {
							LogManager.logDetail(LogConstants.CTX_RUNTIME, ex, "Failed to get metadata, trying next source."); //$NON-NLS-1$
							ex = null;
							te = null;
						}
						try {
							if (cm != null) {
								ef = cm.getExecutionFactory();
								cf = cm.getConnectionFactory();
							}
						} catch (TranslatorException e) {
							LogManager.logDetail(LogConstants.CTX_RUNTIME, e, "Failed to get a connection factory for metadata load."); //$NON-NLS-1$
							te = e;
						}
						try {
							metadataRepo.loadMetadata(factory, ef, cf);		
							LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50030,vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date())));
							break;
						} catch (Exception e) {
							ex = e;
						}
					}
				}
		    					
				synchronized (vdb) {
			    	if (ex == null) {
			    		if (!cached) {
				    		// cache the schema to disk
							cacheMetadataStore(model, factory);
			    		}
						
						metadataLoaded(vdb, model, vdbMetadataStore, loadCount, factory, true);
			    	} else {
			    		String errorMsg = ex.getMessage()==null?ex.getClass().getName():ex.getMessage();
			    		if (te != null) {
			    			errorMsg += ": " + te.getMessage(); //$NON-NLS-1$ 
			    		}
			    		model.addRuntimeError(errorMsg);
			    		model.setMetadataStatus(Model.MetadataStatus.FAILED);
			    		//log the exception of a non-teiid runtime exception as it may indicate a problem 
						LogManager.logWarning(LogConstants.CTX_RUNTIME, (ex instanceof RuntimeException && !(ex instanceof TeiidRuntimeException))?ex:null, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50036,vdb.getName(), vdb.getVersion(), model.getName(), errorMsg));
						if (ex instanceof RuntimeException) {
							metadataLoaded(vdb, model, vdbMetadataStore, loadCount, factory, false);
						} else {
							//defer the load to the status checker if/when a source is available/redeployed
							model.addAttchment(Runnable.class, this);
						}
			    	}
		    	}
			}
		};	    		
		
		Executor executor = getExecutor();
		if (executor == null) {
			job.run();
		}
		else {
    		executor.execute(job);
		}
	}	
    
	private void cacheMetadataStore(final ModelMetaData model, MetadataFactory schema) {
		boolean cache = true;
		if (vdb.isXmlDeployment()) {
			cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata")); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		String prop = vdb.getPropertyValue("cache-metadata"); //$NON-NLS-1$
		if (prop != null) {
			cache = Boolean.valueOf(prop);	
		}
		prop = model.getPropertyValue("cache-metadata"); //$NON-NLS-1$
		if (prop != null) {
			cache = Boolean.valueOf(prop);	
		}
				
		if (cache) {
			final File cachedFile = getSerializer().buildModelFile(vdb, model.getName());
			try {
				getSerializer().saveAttachment(cachedFile, schema, false);
			} catch (IOException e) {
				LogManager.logWarning(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50044, vdb.getName(), vdb.getVersion(), model.getName()));
			}
		}
	}    
	
	private void deleteModelCache(String modelName) {
		final File cachedFile = getSerializer().buildModelFile(vdb, modelName);
		getSerializer().removeAttachment(cachedFile);
	}

	protected VDBRepository getVDBRepository() {
		return vdbRepositoryInjector.getValue();
	}
	
	private TranslatorRepository getTranslatorRepository() {
		return this.translatorRepositoryInjector.getValue();
	}
	
	private Executor getExecutor() {
		return this.executorInjector.getValue();
	}
	
	private ObjectSerializer getSerializer() {
		return serializerInjector.getValue();
	}
	
	private BufferManager getBuffermanager() {
		return bufferManagerInjector.getValue();
	}
	
	private void save() throws AdminProcessingException{
		try {
			ObjectSerializer os = getSerializer();
			VDBMetadataParser.marshell(this.vdb, os.getVdbXmlOutputStream(this.vdb));
		} catch (IOException e) {
			 throw new AdminProcessingException(IntegrationPlugin.Event.TEIID50048, e);
		} catch (XMLStreamException e) {
			 throw new AdminProcessingException(IntegrationPlugin.Event.TEIID50049, e);
		}
	}
	
	/**
	 * Override for module based loading
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected MetadataRepository<?, ?> getMetadataRepository(String repoType) throws VirtualDatabaseException {
		MetadataRepository<?, ?> repo = super.getMetadataRepository(repoType);
		if (repo != null) {
			return repo;
		}
		final Module module;
        ClassLoader moduleLoader = this.getClass().getClassLoader();
        ModuleLoader ml = Module.getCallerModuleLoader();
        if (repoType != null && ml != null) {
	        try {
            	module = ml.loadModule(ModuleIdentifier.create(repoType));
            	moduleLoader = module.getClassLoader();
	        } catch (ModuleLoadException e) {
	            throw new VirtualDatabaseException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50057, repoType));
	        }
        }
        
        final ServiceLoader<MetadataRepository> serviceLoader =  ServiceLoader.load(MetadataRepository.class, moduleLoader);
        if (serviceLoader != null) {
        	for (MetadataRepository loader:serviceLoader) {
        		MetadataRepository old = this.repositories.putIfAbsent(repoType, loader);
        		return old!=null?old:loader;
        	}
        }
		return null;
	}	

}
