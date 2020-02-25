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

package org.teiid.runtime;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.VDBResource;
import org.teiid.metadatastore.DeploymentBasedDatabaseStore;
import org.teiid.query.metadata.*;
import org.teiid.query.metadata.DatabaseStore.Mode;
import org.teiid.query.parser.QueryParser;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public abstract class AbstractVDBDeployer {

    public static final boolean ALLOW_INFORMATION_SCHEMA = Boolean.valueOf(PropertiesUtils.getHierarchicalProperty("org.teiid.allow_information_schema", Boolean.FALSE.toString())); //$NON-NLS-1$

    /**
     * A wrapper to add a stateful text config
     */
    private static class MetadataRepositoryWrapper<F, C> implements MetadataRepository<F, C> {

        private MetadataRepository<F, C> repo;
        private String text;

        public MetadataRepositoryWrapper(MetadataRepository<F, C> repo, String text) {
            this.repo = repo;
            this.text = text;
        }

        @Override
        public void loadMetadata(MetadataFactory factory,
                ExecutionFactory<F, C> executionFactory, F connectionFactory) throws TranslatorException {
            repo.loadMetadata(factory, executionFactory, connectionFactory, this.text);
        }

    };

    protected ConcurrentSkipListMap<String, MetadataRepository<?, ?>> repositories = new ConcurrentSkipListMap<String, MetadataRepository<?, ?>>(String.CASE_INSENSITIVE_ORDER);

    public AbstractVDBDeployer() {
        repositories.put("ddl", new DDLMetadataRepository()); //$NON-NLS-1$
        repositories.put("native", new NativeMetadataRepository()); //$NON-NLS-1$
        repositories.put("ddl-file", new DDLFileMetadataRepository()); //$NON-NLS-1$
        repositories.put("udf", new UDFMetadataRepository()); //$NON-NLS-1$
    }

    public void addMetadataRepository(String name, MetadataRepository<?, ?> metadataRepository) {
        this.repositories.put(name, metadataRepository);
    }

    protected void assignMetadataRepositories(VDBMetaData deployment, MetadataRepository<?, ?> defaultRepo) throws VirtualDatabaseException {
        for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
            if (model.getModelType() != Type.OTHER && (model.getName() == null || model.getName().indexOf('.') >= 0)
                    || model.getName().equalsIgnoreCase(CoreConstants.SYSTEM_MODEL)
                    || model.getName().equalsIgnoreCase(CoreConstants.SYSTEM_ADMIN_MODEL)
                    || model.getName().equalsIgnoreCase(CoreConstants.ODBC_MODEL)
                    || (!ALLOW_INFORMATION_SCHEMA && model.getName().equalsIgnoreCase(CoreConstants.INFORMATION_SCHEMA))) {
                throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40121, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40121, model.getName(), deployment.getName(), deployment.getVersion()));
            }
            if (model.isSource() && model.getSourceNames().isEmpty()) {
                throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40093, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40093, model.getName(), deployment.getName(), deployment.getVersion()));
            }
            if (model.getModelType() == Type.FUNCTION || model.getModelType() == Type.OTHER) {
                continue;
            }
            MetadataRepository<?, ?> repo = getMetadataRepository(deployment, model, defaultRepo);
            //handle multi-source column creation
            if (model.isSupportsMultiSourceBindings() && Boolean.valueOf(model.getPropertyValue("multisource.addColumn"))) { //$NON-NLS-1$
                List<MetadataRepository<?, ?>> repos = new ArrayList<MetadataRepository<?, ?>>(2);
                repos.add(repo);
                String columnName = model.getPropertyValue(MultiSourceMetadataWrapper.MULTISOURCE_COLUMN_NAME);
                repos.add(new MultiSourceMetadataRepository(columnName==null?MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME:columnName));
                repo = new ChainingMetadataRepository(repos);
            }
            model.addAttachment(MetadataRepository.class, repo);
        }
    }

    private MetadataRepository<?, ?> getMetadataRepository(VDBMetaData vdb, ModelMetaData model, MetadataRepository<?, ?> defaultRepo) throws VirtualDatabaseException {
        if (model.getSourceMetadataType().isEmpty()) {
            if (defaultRepo != null) {
                return defaultRepo;
            }
            if (model.isSource()) {
                return new ChainingMetadataRepository(Arrays.asList(new NativeMetadataRepository(), new DirectQueryMetadataRepository()));
            }
            throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), null));
        }

        List<MetadataRepository<?, ?>> repos = new ArrayList<MetadataRepository<?,?>>(2);

        for (int i = 0; i < model.getSourceMetadataType().size(); i++) {
            String schemaTypes = model.getSourceMetadataType().get(i);

            StringTokenizer st = new StringTokenizer(schemaTypes, ","); //$NON-NLS-1$
            while (st.hasMoreTokens()) {
                String repoType = st.nextToken().trim();
                MetadataRepository<?, ?> current = getMetadataRepository(repoType);
                if (current == null) {
                    throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), repoType));
                }
                if (model.getSourceMetadataText().size() > i) {
                    current = new MetadataRepositoryWrapper(current, model.getSourceMetadataText().get(i));
                }
                repos.add(current);
            }
        }

        if (model.getModelType() == ModelMetaData.Type.PHYSICAL) {
            repos.add(new DirectQueryMetadataRepository());
        }
        if (model.getModelType() == ModelMetaData.Type.VIRTUAL) {
            repos.add(new MaterializationMetadataRepository());
        }
        if (repos.size() == 1) {
            return repos.get(0);
        }
        return new ChainingMetadataRepository(repos);
    }

    protected List<ConnectorManager> getConnectorManagers(final ModelMetaData model, final ConnectorManagerRepository cmr) {
        if (model.isSource()) {
            Collection<SourceMappingMetadata> mappings = model.getSources().values();
            List<ConnectorManager> result = new ArrayList<ConnectorManager>(mappings.size());
            for (SourceMappingMetadata mapping:mappings) {
                result.add(cmr.getConnectorManager(mapping.getName()));
            }
            return result;
        }
        //return a single null to give us something to loop over
        return Collections.singletonList(null);
    }

    protected void loadMetadata(VDBMetaData vdb, ConnectorManagerRepository cmr,
            MetadataStore store, VDBResources vdbResources) throws TranslatorException {
        //add the system types
        store.addDataTypes(SystemMetadata.getInstance().getRuntimeTypeMap());
        //add domains if defined
        String value = vdb.getPropertyValue(VDBMetaData.TEIID_DOMAINS);
        if (value != null) {
            //use a temporary store/db to retrieve the domains
            DatabaseStore dbStore = new DatabaseStore() {
                @Override
                public Map<String, Datatype> getRuntimeTypes() {
                    return getVDBRepository().getRuntimeTypeMap();
                }
            };
            dbStore.startEditing(true);
            dbStore.databaseCreated(new Database("x", "1")); //$NON-NLS-1$ //$NON-NLS-2$
            dbStore.databaseSwitched("x", "1"); //$NON-NLS-1$ //$NON-NLS-2$
            dbStore.setMode(Mode.DOMAIN);
            QueryParser.getQueryParser().parseDDL(dbStore, new StringReader(value));
            dbStore.stopEditing();
            store.addDataTypes(dbStore.getDatabase("x", "1").getMetadataStore().getDatatypes()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        // load metadata from the models
        AtomicInteger loadCount = new AtomicInteger();
        for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
            if (model.getModelType() == Model.Type.PHYSICAL || model.getModelType() == Model.Type.VIRTUAL) {
                loadCount.incrementAndGet();
            }
        }
        if (loadCount.get() == 0) {
            processVDBDDL(vdb, store, cmr, vdbResources);
            getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion());
            return;
        }

        for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
            MetadataRepository metadataRepository = model.getAttachment(MetadataRepository.class);
            if (model.getModelType() == Model.Type.PHYSICAL || model.getModelType() == Model.Type.VIRTUAL) {
                loadMetadata(vdb, model, cmr, metadataRepository, store, loadCount, vdbResources);
                LogManager.logTrace(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " was being loaded from its repository"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            else {
                LogManager.logTrace(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " skipped being loaded because of its type ", model.getModelType()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        }
    }

    protected abstract VDBRepository getVDBRepository();

    @SuppressWarnings({"rawtypes","unchecked"})
    private void loadMetadata(final VDBMetaData vdb, final ModelMetaData model, final ConnectorManagerRepository cmr, final MetadataRepository metadataRepo, final MetadataStore vdbMetadataStore, final AtomicInteger loadCount, final VDBResources vdbResources) throws TranslatorException {

        String msg = RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID50029,vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date()));
        model.setMetadataStatus(Model.MetadataStatus.LOADING);
        model.addRuntimeMessage(Severity.INFO, msg);
        LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);

        final Runnable job = new Runnable() {

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
                Map<String, Datatype> datatypes = vdbMetadataStore.getDatatypes();
                MetadataFactory factory = getCachedMetadataFactory(vdb, model);
                if (factory != null) {
                    factory.correctDatatypes(datatypes);
                    cached = true;
                    LogManager.logDetail(LogConstants.CTX_RUNTIME, "Model ", model.getName(), "in VDB ", vdb.getName(), " was loaded from cached metadata"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                } else {
                    factory = createMetadataFactory(vdb, vdbMetadataStore, model, vdbResources==null?Collections.EMPTY_MAP:vdbResources.getEntriesPlusVisibilities());
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
                        ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
                        try {
                            LogManager.logDetail(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID50104,vdb.getName(), vdb.getVersion(), model.getName(), cm != null?cm.getTranslatorName():null, cm != null?cm.getConnectionName():null));
                            Thread.currentThread().setContextClassLoader(metadataRepo.getClass().getClassLoader());
                            metadataRepo.loadMetadata(factory, ef, cf);
                            LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID50030,vdb.getName(), vdb.getVersion(), model.getName(), SimpleDateFormat.getInstance().format(new Date())));
                            break;
                        } catch (Exception e) {
                            factory = createMetadataFactory(vdb, vdbMetadataStore, model, vdbResources==null?Collections.EMPTY_MAP:vdbResources.getEntriesPlusVisibilities());
                            ex = e;
                        } finally {
                            Thread.currentThread().setContextClassLoader(originalCL);
                        }
                    }
                }

                synchronized (vdb) {
                    if (ex == null) {
                        if (!cached) {
                            // cache the schema to disk
                            cacheMetadataFactory(vdb, model, factory);
                        }

                        metadataLoaded(vdb, model, vdbMetadataStore, loadCount, factory, true, cmr, vdbResources);
                    } else {
                        String errorMsg = ex.getMessage()==null?ex.getClass().getName():ex.getMessage();
                        if (te != null) {
                            errorMsg += ": " + te.getMessage(); //$NON-NLS-1$
                        }
                        model.addAttachment(Exception.class, ex);
                        model.addRuntimeError(errorMsg);
                        model.setMetadataStatus(Model.MetadataStatus.FAILED);
                        LogManager.logWarning(LogConstants.CTX_RUNTIME, ex, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID50036,vdb.getName(), vdb.getVersion(), model.getName(), errorMsg));
                        if (ex instanceof RuntimeException || !retryLoad(vdb, model, this)) {
                            metadataLoaded(vdb, model, vdbMetadataStore, loadCount, factory, false, cmr, vdbResources);
                        }
                    }
                }
            }
        };

        runMetadataJob(vdb, model, job);
    }

    protected abstract void runMetadataJob(VDBMetaData vdb, ModelMetaData model, Runnable job) throws TranslatorException;

    /**
     * Return true if we can retry the load
     * @param vdb
     * @param model
     * @param job
     * @return
     */
    protected abstract boolean retryLoad(VDBMetaData vdb, final ModelMetaData model, Runnable job);

    protected abstract MetadataFactory getCachedMetadataFactory(final VDBMetaData vdb, final ModelMetaData model);

    protected abstract void cacheMetadataFactory(VDBMetaData vdb, final ModelMetaData model, MetadataFactory schema);

    private void metadataLoaded(final VDBMetaData vdb,
            final ModelMetaData model,
            final MetadataStore vdbMetadataStore,
            final AtomicInteger loadCount, MetadataFactory factory, boolean success, ConnectorManagerRepository cmr, VDBResources vdbResources) {
        boolean failed = false;
        if (success) {
            // merge into VDB metadata
            factory.mergeInto(vdbMetadataStore);

            //TODO: this is not quite correct, the source may be missing
            model.clearRuntimeMessages();
            model.setMetadataStatus(Model.MetadataStatus.LOADED);
        } else {
            model.setMetadataStatus(Model.MetadataStatus.FAILED);
            synchronized (vdb) {
                failed = vdb.getStatus() != Status.FAILED;
                vdb.setStatus(Status.FAILED);
            }
            //TODO: abort the other loads
        }

        if ((loadCount.decrementAndGet() == 0 && vdb.getStatus() != Status.FAILED) || failed) {
            if (vdb.getStatus() != Status.FAILED) {
                processVDBDDL(vdb, vdbMetadataStore, cmr, vdbResources);
            }
            getVDBRepository().finishDeployment(vdb.getName(), vdb.getVersion());
        }
    }

    private void processVDBDDL(final VDBMetaData vdb,
            final MetadataStore vdbMetadataStore, final ConnectorManagerRepository cmr, final VDBResources vdbResources) {
        if (vdb.getStatus() == Status.FAILED) {
            return;
        }
        String ddl = vdb.getPropertyValue(VDBMetaData.TEIID_DDL);
        if (ddl != null) {
            final Database database = DatabaseUtil.convert(vdb, vdbMetadataStore);
            CompositeMetadataStore compositeStore = new CompositeMetadataStore(vdbMetadataStore);
            final TransformationMetadata metadata = new TransformationMetadata(vdb, compositeStore, null,
                    getVDBRepository().getSystemFunctionManager().getSystemFunctions(), null).getDesignTimeMetadata();

            DeploymentBasedDatabaseStore deploymentStore = new DeploymentBasedDatabaseStore(getVDBRepository()) {

                @Override
                protected TransformationMetadata getTransformationMetadata() {
                    return metadata;
                }

                @Override
                public void importSchema(String schemaName, String serverType,
                        String serverName, String foreignSchemaName,
                        List<String> includeTables, List<String> excludeTables,
                        Map<String, String> properties) {
                    ModelMetaData model = vdb.getModel(schemaName);
                    Properties modelProperties = new Properties();
                    modelProperties.putAll(model.getPropertiesMap());
                    modelProperties.putAll(properties);
                    if (!includeTables.isEmpty()) {
                        modelProperties.put("importer.includeTables", StringUtil.join(includeTables, ",")); //$NON-NLS-1$
                    }
                    if (!excludeTables.isEmpty()) {
                        modelProperties.put("importer.excludeTables", StringUtil.join(excludeTables, ",")); //$NON-NLS-1$
                    }
                    if (foreignSchemaName != null) {
                        modelProperties.put("importer.schemaName", foreignSchemaName); //$NON-NLS-1$
                    }
                    MetadataFactory factory = DatabaseStore.createMF(this, getSchema(schemaName), true, modelProperties);
                    factory.setParser(new QueryParser());
                    if (vdbResources != null) {
                        factory.setVdbResources(vdbResources.getEntriesPlusVisibilities());
                    }
                    factory.setVDBClassLoader(vdb.getAttachment(ClassLoader.class));
                    MetadataRepository baseRepo = model.getAttachment(MetadataRepository.class);

                    MetadataRepository metadataRepository;
                    try {
                        metadataRepository = getMetadataRepository(serverType);
                        if (metadataRepository == null) {
                            throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40094, model.getName(), vdb.getName(), vdb.getVersion(), serverType));
                        }
                    } catch (VirtualDatabaseException e1) {
                        throw new MetadataException(e1);
                    }

                    metadataRepository = new ChainingMetadataRepository(Arrays.asList(new MetadataRepositoryWrapper(metadataRepository, null), baseRepo));

                    ExecutionFactory ef = null;
                    Object cf = null;

                    Exception te = null;
                    for (ConnectorManager cm : getConnectorManagers(model, cmr)) {
                        if (te != null) {
                            LogManager.logDetail(LogConstants.CTX_RUNTIME, te, "Failed to get metadata, trying next source."); //$NON-NLS-1$
                            te = null;
                        }
                        try {
                            if (cm != null) {
                                ef = cm.getExecutionFactory();
                                cf = cm.getConnectionFactory();
                            }
                        } catch (TranslatorException e) {
                            LogManager.logDetail(LogConstants.CTX_RUNTIME, e, "Failed to get a connection factory for metadata load."); //$NON-NLS-1$
                        }

                        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.TRACE)) {
                            LogManager.logTrace(LogConstants.CTX_RUNTIME, "CREATE SCHEMA", factory.getSchema().getName(), ";\n", DDLStringVisitor.getDDLString(factory.getSchema(), null, null)); //$NON-NLS-1$ //$NON-NLS-2$
                        }

                        try {
                            metadataRepository.loadMetadata(factory, ef, cf);
                            break;
                        } catch (Exception e) {
                            te = e;
                            factory = DatabaseStore.createMF(this, getSchema(schemaName), true, modelProperties);
                            factory.setParser(new QueryParser());
                            if (vdbResources != null) {
                                factory.setVdbResources(vdbResources.getEntriesPlusVisibilities());
                            }
                        }
                    }
                    if (te != null) {
                        if (te instanceof RuntimeException) {
                            throw (RuntimeException)te;
                        }
                        throw new MetadataException(te);
                    }
                }
            };
            deploymentStore.startEditing(false);
            deploymentStore.databaseCreated(database);
            deploymentStore.databaseSwitched(database.getName(), database.getVersion());
            deploymentStore.setMode(Mode.SCHEMA);
            try {
                QueryParser.getQueryParser().parseDDL(deploymentStore, new StringReader(ddl));
            } finally {
                deploymentStore.stopEditing();
            }

            DatabaseUtil.copyDatabaseGrantsAndRoles(database, vdb);
        }
    }

    protected MetadataFactory createMetadataFactory(VDBMetaData vdb, MetadataStore store,
            ModelMetaData model, Map<String, ? extends VDBResource> vdbResources) {
        Map<String, Datatype> datatypes = store.getDatatypes();
        MetadataFactory factory = new MetadataFactory(vdb.getName(), vdb.getVersion(), datatypes, model);
        factory.getSchema().setPhysical(model.isSource());
        factory.setParser(new QueryParser()); //for thread safety each factory gets it's own instance.
        factory.setVdbResources(vdbResources);
        factory.setVDBClassLoader(vdb.getAttachment(ClassLoader.class));
        return factory;
    }

    /**
     * @throws VirtualDatabaseException
     */
    protected MetadataRepository<?, ?> getMetadataRepository(String repoType) throws VirtualDatabaseException {
        return repositories.get(repoType);
    }
}
