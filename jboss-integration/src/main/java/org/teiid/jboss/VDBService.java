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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.stream.XMLStreamException;

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
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidException;
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
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.index.IndexMetadataRepository;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
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
	
	private VDBLifeCycleListener vdbListener;
	private LinkedHashMap<String, Resource> visibilityMap;
	
	public VDBService(VDBMetaData metadata, LinkedHashMap<String, Resource> visibilityMap) {
		this.vdb = metadata;
		this.visibilityMap = visibilityMap;
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
			
			Set<String> keys = parent.getProperties().stringPropertyNames();
			for (String key:keys) {
				if (data.getPropertyValue(key) == null && parent.getPropertyValue(key) != null) {
					data.addProperty(key, parent.getPropertyValue(key));
				}
			}
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
				VDBMetaData vdbInstance = cvdb.getVDB();
				// add object replication to temp/matview tables
				GlobalTableStore gts = new GlobalTableStoreImpl(getBuffermanager(), vdbInstance.getAttachment(TransformationMetadata.class));
				if (objectReplicatorInjector.getValue() != null) {
					try {
						gts = objectReplicatorInjector.getValue().replicate(name + version, GlobalTableStore.class, gts, 300000);
					} catch (Exception e) {
						LogManager.logError(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50023, gts)); 
					}
				}
				vdbInstance.addAttchment(GlobalTableStore.class, gts);
				vdbService.install();
			}
		};
		
		getVDBRepository().addListener(this.vdbListener);
				
		MetadataStore store = new MetadataStore();
		
		try {
			this.assignMetadataRepositories(vdb, super.getMetadataRepository("index")); //$NON-NLS-1$
			// add transformation metadata to the repository.
			getVDBRepository().addVDB(this.vdb, store, visibilityMap, udf, cmr);
		} catch (VirtualDatabaseException e) {
			throw new StartException(e);
		}		
		
		this.vdb.removeAttachment(UDFMetaData.class);
		try {
			loadMetadata(this.vdb, cmr, store);
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
		getVDBRepository().removeListener(this.vdbListener);
		getVDBRepository().removeVDB(this.vdb.getName(), this.vdb.getVersion());
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
			cmr.createConnectorManagers(deployment, new ConnectorManagerRepository.ExecutionFactoryProvider() {
				
				@Override
				public ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException {
					return VDBService.getExecutionFactory(name, repo, getTranslatorRepository(), deployment, map, new HashSet<String>());
				}
			});
		} catch (ConnectorManagerException e) {
			if (e.getCause() != null) {
				throw new StartException(IntegrationPlugin.Event.TEIID50035.name()+" "+e.getMessage(), e.getCause()); //$NON-NLS-1$
			}
			throw new StartException(e.getMessage());
		}
	}
	
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
				
		        ClassLoader classloader = translator.getAttachment(ClassLoader.class);
		        if (classloader == null) {
		        	classloader = Thread.currentThread().getContextClassLoader();
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
			throw new ConnectorManagerException(e);
		}
	}

    protected void loadMetadata(final VDBMetaData vdb, final ModelMetaData model, final ConnectorManagerRepository cmr, final MetadataRepository metadataRepo, final MetadataStore vdbMetadataStore, final AtomicInteger loadCount) {

    	String msg = IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50029,vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date())); 
		model.addRuntimeMessage(Severity.INFO, msg); 
		LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);

		Runnable job = new Runnable() {
			@Override
			public void run() {
				
				boolean metadataLoaded = false;
				boolean cached = false;
				Exception ex = null;
				
				final File cachedFile = getSerializer().buildModelFile(vdb, model.getName());
				MetadataFactory factory = getSerializer().loadSafe(cachedFile, MetadataFactory.class);
				if (factory != null) {
					metadataLoaded = true;
					cached = true;
					LogManager.logTrace(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " was loaded from cached metadata"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				}
				
				if (!metadataLoaded) {
					boolean indexStore = (metadataRepo instanceof IndexMetadataRepository);
					// designer based models define data types based on their built in data types, which are system vdb data types
					Map<String, Datatype> datatypes = indexStore?getVDBRepository().getSystemStore().getDatatypes():getVDBRepository().getBuiltinDatatypes();
					factory = new MetadataFactory(vdb.getName(), vdb.getVersion(), model.getName(), datatypes, model.getProperties(), model.getSchemaText());
					factory.getSchema().setPhysical(model.isSource());
					
					ExecutionFactory ef = null;
					Object cf = null;
					
					try {
						ConnectorManager cm = getConnectorManager(model, cmr);
						if (cm != null) {
							ef = cm.getExecutionFactory();
							cf = cm.getConnectionFactory();
						}
					} catch (TranslatorException e) {
						//cf not available
					}
					try {
						metadataRepo.loadMetadata(factory, ef, cf);		
						LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50030,vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date())));
						metadataLoaded = true;
					} catch (Exception e) {
						ex = e;
					}
				}
		    					
				synchronized (vdb) {
			    	if (metadataLoaded) {
			    		if (!cached) {
				    		// cache the schema to disk
							cacheMetadataStore(model, factory);
			    		}
						
						metadataLoaded(vdb, model, vdbMetadataStore, loadCount, factory, true);
			    	} else {
			    		model.addRuntimeError(ex.getMessage()); 
						LogManager.logWarning(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50036,vdb.getName(), vdb.getVersion(), model.getName(), ex.getMessage()));
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
    
    // if is not dynamic always cache; else check for the flag (this may need to be revisited with index vdb)
	private void cacheMetadataStore(final ModelMetaData model, MetadataFactory schema) {
		boolean cache = !vdb.isDynamic();
		if (vdb.isDynamic()) {
			cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata")); //$NON-NLS-1$ //$NON-NLS-2$
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
	@SuppressWarnings("unchecked")
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
        		return loader;
        	}
        }
		return null;
	}	

}
