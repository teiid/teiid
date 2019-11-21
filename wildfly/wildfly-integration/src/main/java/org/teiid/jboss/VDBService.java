/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.jboss;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.Executor;

import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.server.moduleservice.ServiceModuleLoader;
import org.jboss.msc.service.LifecycleContext;
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
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.RuntimeVDB;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.index.IndexMetadataRepository;
import org.teiid.query.metadata.VDBResources;
import org.teiid.runtime.AbstractVDBDeployer;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.vdb.runtime.VDBKey;

class VDBService extends AbstractVDBDeployer implements Service<RuntimeVDB> {
    private VDBMetaData vdb;
    private RuntimeVDB runtimeVDB;
    protected final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
    protected final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
    protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
    protected final InjectedValue<ObjectSerializer> serializerInjector = new InjectedValue<ObjectSerializer>();
    protected final InjectedValue<VDBStatusChecker> vdbStatusCheckInjector = new InjectedValue<VDBStatusChecker>();

    private VDBLifeCycleListener vdbListener;
    private VDBResources vdbResources;
    private VDBKey vdbKey;

    public VDBService(VDBMetaData metadata, VDBResources vdbResources, ContainerLifeCycleListener shutdownListener) {
        this.vdb = metadata;
        this.vdbKey = new VDBKey(metadata.getName(), metadata.getVersion());
        this.vdbResources = vdbResources;
    }

    @Override
    public void start(final StartContext context) throws StartException {

        ConnectorManagerRepository cmr = new ConnectorManagerRepository();
        TranslatorRepository repo = new TranslatorRepository();

        this.vdb.addAttachment(TranslatorRepository.class, repo);

        // check if this is a VDB with index files, if there are then build the TransformationMetadata
        UDFMetaData udf = this.vdb.getAttachment(UDFMetaData.class);

        // add required connector managers; if they are not already there
        for (Translator t: this.vdb.getOverrideTranslators()) {
            VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;

            String type = data.getType();
            VDBTranslatorMetaData parent = getTranslatorRepository().getTranslatorMetaData(type);
            data.setModuleName(parent.getModuleName());
            data.addAttachment(ClassLoader.class, parent.getAttachment(ClassLoader.class));
            data.setParent(parent);
            repo.addTranslatorMetadata(data.getName(), data);
        }

        createConnectorManagers(cmr, repo, this.vdb);
        this.vdbListener = new VDBLifeCycleListener() {
            @Override
            public void added(String name, CompositeVDB cvdb) {
            }
            @Override
            public void beforeRemove(String name, CompositeVDB cvdb) {
            }
            @Override
            public void removed(String name, CompositeVDB cvdb) {
            }

            @Override
            public void finishedDeployment(String name, CompositeVDB cvdb) {
                if (!VDBService.this.vdbKey.equals(cvdb.getVDBKey())) {
                    return;
                }
                //clear out the indexmetadatarepository as it holds state that is no longer necessary
                repositories.put("index", new IndexMetadataRepository()); //$NON-NLS-1$
                VDBMetaData vdbInstance = cvdb.getVDB();
                if (vdbInstance.getStatus().equals(Status.ACTIVE)) {
                    //need to construct/install the service in a single thread
                    final ServiceBuilder<Void> vdbService = addVDBFinishedService(context);
                    vdbService.install();
                }
            }
        };

        getVDBRepository().addListener(this.vdbListener);

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
            cleanup(context);
            throw new StartException(e);
        }

        this.vdb.removeAttachment(UDFMetaData.class);
        try {
            loadMetadata(this.vdb, cmr, store, this.vdbResources);
        } catch (TranslatorException e) {
            cleanup(context);
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
            @Override
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

    void cleanup(LifecycleContext context) {
        getVDBRepository().removeVDB(this.vdb.getName(), this.vdb.getVersion());
        getVDBRepository().removeListener(this.vdbListener);
        //getVDBRepository().removeListener(this.restEasyListener);
        final ServiceController<?> controller = context.getController().getServiceContainer().getService(TeiidServiceNames.vdbFinishedServiceName(vdb.getName(), vdb.getVersion()));
        if (controller != null) {
            controller.setMode(ServiceController.Mode.REMOVE);
        }
        LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50026, this.vdb));
    }

    @Override
    public void stop(StopContext context) {
        cleanup(context);
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
                    return TranslatorUtil.getExecutionFactory(name, repo, getTranslatorRepository(), deployment, map, new HashSet<String>());
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

    @Override
    protected MetadataFactory getCachedMetadataFactory(
            final VDBMetaData vdb, final ModelMetaData model) {
        final File cachedFile = getSerializer().buildModelFile(vdb, model.getName());
        MetadataFactory factory = getSerializer().loadSafe(cachedFile, MetadataFactory.class);
        return factory;
    }

    @Override
    protected void cacheMetadataFactory(VDBMetaData v, ModelMetaData model,
            MetadataFactory schema) {
        boolean cache = true;
        if (vdb.isXmlDeployment()) {
            cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata")); //$NON-NLS-1$ //$NON-NLS-2$
            if (cache) {
                LogManager.logDetail(LogConstants.CTX_RUNTIME, "using VDB metadata caching value", vdb.getPropertyValue("UseConnectorMetadata"), "Note that UseConnectorMetadata is deprecated.  Use cache-metadata instead."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }

        String prop = vdb.getPropertyValue("cache-metadata"); //$NON-NLS-1$
        if (prop != null) {
            cache = Boolean.valueOf(prop);
        }
        prop = model.getPropertyValue("cache-metadata"); //$NON-NLS-1$
        if (prop != null) {
            LogManager.logDetail(LogConstants.CTX_RUNTIME, model, "using metadata caching value", prop); //$NON-NLS-1$
            cache = Boolean.valueOf(prop);
        }

        if (cache) {
            final File cachedFile = getSerializer().buildModelFile(vdb, model.getName());
            try {
                getSerializer().saveAttachment(cachedFile, schema, false);
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50044, vdb.getName(), vdb.getVersion(), model.getName()));
            }
        }
    }

    private void deleteModelCache(String modelName) {
        final File cachedFile = getSerializer().buildModelFile(vdb, modelName);
        getSerializer().removeAttachment(cachedFile);
    }

    @Override
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

    private void save() throws AdminProcessingException {
        try {
            ObjectSerializer os = getSerializer();
            VDBMetadataParser.marshall(this.vdb, os.getVdbXmlOutputStream(this.vdb));
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
        try {
            repo = TeiidAdd.loadService(MetadataRepository.class, repoType, vdb.getAttachment(ServiceModuleLoader.class));
            repo = TeiidAdd.wrapWithClassLoaderProxy(MetadataRepository.class, repo);
        } catch (OperationFailedException e) {
            throw new VirtualDatabaseException(IntegrationPlugin.Event.TEIID50057, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50057, repoType));
        }
        MetadataRepository old = this.repositories.putIfAbsent(repoType, repo);
        return old!=null?old:repo;
    }

    @Override
    protected void runMetadataJob(VDBMetaData vdbMetaData, ModelMetaData model, Runnable job) {
        getExecutor().execute(wrap(job));
    }

    private Runnable wrap(Runnable job) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    job.run();
                } catch (IllegalStateException e) {
                    if (vdb.getStatus() != Status.FAILED && vdb.getStatus() != Status.REMOVED) {
                        throw e;
                    }
                    LogManager.logDetail(LogConstants.CTX_RUNTIME, e, "Could not load metadata for a removed or failed deployment."); //$NON-NLS-1$
                }
            }
        };
    }

    @Override
    protected boolean retryLoad(VDBMetaData v, ModelMetaData model,
            Runnable job) {
        VDBStatusChecker marked = model.removeAttachment(VDBStatusChecker.class);

        if (marked != null) {
            getExecutor().execute(wrap(job));
        } else {
            //defer the load to the status checker if/when a source is available/redeployed
            model.addAttachment(Runnable.class, wrap(job));
        }
        return true;
    }
}
