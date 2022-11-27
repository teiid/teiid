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
package org.teiid.deployers;

import java.util.*;
import java.util.concurrent.Future;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.VDBImport;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;

/**
 * Represents the runtime state of a vdb that may aggregate several vdbs.
 */
public class CompositeVDB {

    private static final boolean WIDEN_COMPARISON_TO_STRING = PropertiesUtils.getHierarchicalProperty("org.teiid.widenComparisonToString", false, Boolean.class); //$NON-NLS-1$
    private static final boolean HIDDEN_METADATA_RESOLVABLE = PropertiesUtils.getHierarchicalProperty("org.teiid.hiddenMetadataResolvable", true, Boolean.class); //$NON-NLS-1$

    private VDBMetaData vdb;
    private MetadataStore store;
    private LinkedHashMap<String, VDBResources.Resource> visibilityMap;
    private UDFMetaData udf;
    LinkedHashMap<VDBKey, CompositeVDB> children;
    private MetadataStore[] additionalStores;
    private ConnectorManagerRepository cmr;
    private FunctionTree systemFunctions;
    private boolean metadataloadFinished = false;
    private VDBMetaData mergedVDB;
    private VDBMetaData originalVDB;
    private Collection<Future<?>> tasks = Collections.synchronizedSet(new HashSet<Future<?>>());
    private VDBKey vdbKey;

    public CompositeVDB(VDBMetaData vdb, MetadataStore metadataStore,
            LinkedHashMap<String, VDBResources.Resource> visibilityMap, UDFMetaData udf, FunctionTree systemFunctions,
            ConnectorManagerRepository cmr, VDBRepository vdbRepository, MetadataStore... additionalStores)
            throws VirtualDatabaseException {
        this.vdb = vdb;
        this.store = metadataStore;
        this.visibilityMap = visibilityMap;
        this.udf = udf;
        this.systemFunctions = systemFunctions;
        this.cmr = cmr;
        this.additionalStores = additionalStores;
        this.mergedVDB = vdb;
        this.originalVDB = vdb;
        this.vdbKey = new VDBKey(originalVDB.getName(), originalVDB.getVersion());
        buildCompositeState(vdbRepository);
        this.mergedVDB.addAttachment(VDBKey.class, this.vdbKey);
    }

    private static TransformationMetadata buildTransformationMetaData(VDBMetaData vdb,
            LinkedHashMap<String, VDBResources.Resource> visibilityMap, MetadataStore store, UDFMetaData udf,
            FunctionTree systemFunctions, MetadataStore[] additionalStores, boolean allowEnv) {

        Collection <FunctionTree> udfs = new ArrayList<FunctionTree>();
        if (udf != null) {
            for (Map.Entry<String, UDFSource> entry : udf.getFunctions().entrySet()) {
                udfs.add(new FunctionTree(entry.getKey(), entry.getValue(), true));
            }
        }

        //add functions for procedures
        for (Schema schema:store.getSchemas().values()) {
            if (!schema.getProcedures().isEmpty()) {
                FunctionTree ft = FunctionTree.getFunctionProcedures(schema);
                if (ft != null) {
                    udfs.add(ft);
                }
            }
        }

        CompositeMetadataStore compositeStore = new CompositeMetadataStore(store);
        for (MetadataStore s:additionalStores) {
            compositeStore.merge(s);
            for (Schema schema:s.getSchemas().values()) {
                if (!schema.getFunctions().isEmpty()) {
                    UDFSource source = new UDFSource(schema.getFunctions().values());
                    if (udf != null) {
                        source.setClassLoader(udf.getClassLoader());
                    }
                    udfs.add(new FunctionTree(schema.getName(), source, true));
                }
                if (!schema.getProcedures().isEmpty()) {
                    FunctionTree ft = FunctionTree.getFunctionProcedures(schema);
                    if (ft != null) {
                        udfs.add(ft);
                    }
                }
            }
        }

        TransformationMetadata metadata =  new TransformationMetadata(vdb, compositeStore, visibilityMap, systemFunctions, udfs);
        metadata.setAllowENV(allowEnv);
        metadata.setLongRanks(AggregateSymbol.LONG_RANKS);
        metadata.setUseOutputNames(false);
        metadata.setWidenComparisonToString(WIDEN_COMPARISON_TO_STRING);
        metadata.setHiddenResolvable(HIDDEN_METADATA_RESOLVABLE);
        processSourceRoles(metadata, compositeStore, vdb);
        return metadata;
    }

    /**
     * source roles are stored on the metadatastore, which needs merged
     * into the vdb - but we can't modify the existing roles
     * as that would then show up in a vdb export
     */
    private static void processSourceRoles(TransformationMetadata metadata, MetadataStore store, VDBMetaData vdb) {
        LinkedHashMap<String, DataPolicyMetadata> newPolicies = new LinkedHashMap<>();
        for (DataPolicyMetadata policy : vdb.getDataPolicyMap().values()) {
            policy = policy.clone();
            newPolicies.put(policy.getName(), policy);
        }
        metadata.setPolicies(newPolicies);
        if (store.getRoles() == null || store.getRoles().isEmpty() || vdb.getDataPolicyMap() == null) {
            return;
        }
        for (Role role : store.getRoles()) {
            DataPolicyMetadata dpm = newPolicies.get(role.getName());
            if (dpm == null) {
                LogManager.logDetail(LogConstants.CTX_RUNTIME, "Permission added to non-existant role", role.getName()); //$NON-NLS-1$
                continue;
            }
            DatabaseUtil.convert(role, dpm);
        }
    }

    public VDBMetaData getVDB() {
        return this.mergedVDB;
    }

    private void buildCompositeState(VDBRepository vdbRepository) throws VirtualDatabaseException {
        if (vdb.getVDBImports().isEmpty()) {
            this.vdb.addAttachment(ConnectorManagerRepository.class, this.cmr);
            return;
        }

        VDBMetaData newMergedVDB = this.vdb.clone();
        ConnectorManagerRepository mergedRepo = this.cmr;
        if (!this.cmr.isShared()) {
            mergedRepo = new ConnectorManagerRepository();
            mergedRepo.getConnectorManagers().putAll(this.cmr.getConnectorManagers());
        }
        newMergedVDB.addAttachment(ConnectorManagerRepository.class, mergedRepo);
        LinkedHashSet<ClassLoader> toSearch = new LinkedHashSet<>(vdb.getVDBImports().size()+1);
        toSearch.add(this.vdb.getAttachment(ClassLoader.class));
        this.children = new LinkedHashMap<VDBKey, CompositeVDB>();
        newMergedVDB.setImportedModels(new TreeSet<String>(String.CASE_INSENSITIVE_ORDER));
        LinkedHashSet<VDBImport> seen = new LinkedHashSet<>(vdb.getVDBImports());
        Stack<VDBImport> toLoad = new Stack<VDBImport>();
        toLoad.addAll(seen);
        while (!toLoad.isEmpty()) {
            VDBImport vdbImport = toLoad.pop();
            VDBKey key = new VDBKey(vdbImport.getName(), vdbImport.getVersion());
            if (key.isAtMost()) {
                //TODO: could allow partial versions
                throw new VirtualDatabaseException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40144, vdbKey, key));
            }
            CompositeVDB importedVDB = vdbRepository.getCompositeVDB(key);
            if (importedVDB == null) {
                throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40083, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40083, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion()));
            }
            VDBMetaData childVDB = importedVDB.getVDB();
            VDBMetaData originalChildVDB = importedVDB.getOriginalVDB();
            //detect transitive importing vdb
            if (!originalChildVDB.getVDBImports().isEmpty()
                    && originalChildVDB.getVisibilityOverrides().isEmpty()
                    && originalChildVDB.getDataPolicies().isEmpty()
                    && originalChildVDB.getModels().isEmpty()) {
                for (VDBImport childImport : originalChildVDB.getVDBImports()) {
                    if (seen.add(childImport)) {
                        toLoad.push(childImport);
                    } else {
                        LogManager.logDetail(LogConstants.CTX_DQP, "Ommitting the duplicate import of " + childImport + " from " + vdbImport + " for the creation of " + vdb); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    }
                }
                continue;
            }
            newMergedVDB.getVisibilityOverrides().putAll(childVDB.getVisibilityOverrides());
            toSearch.add(childVDB.getAttachment(ClassLoader.class));
            this.children.put(importedVDB.getVDBKey(), importedVDB);

            if (vdbImport.isImportDataPolicies()) {
                for (DataPolicy dp : importedVDB.getVDB().getDataPolicies()) {
                    DataPolicyMetadata role = (DataPolicyMetadata)dp;
                    if (newMergedVDB.addDataPolicy(role) != null) {
                        throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40084, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40084, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion(), role.getName()));
                    }
                    if (role.isGrantAll()) {
                        role.setSchemas(childVDB.getModelMetaDatas().keySet());
                    }
                }
            }

            // add models
            for (ModelMetaData m:childVDB.getModelMetaDatas().values()) {
                if (newMergedVDB.addModel(m) != null) {
                    throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40085, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40085, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion(), m.getName()));
                }
                newMergedVDB.getImportedModels().add(m.getName());
                String visibilityOverride = newMergedVDB.getPropertyValue(m.getName() + ".visible"); //$NON-NLS-1$
                if (visibilityOverride != null) {
                    boolean visible = Boolean.valueOf(visibilityOverride);
                    newMergedVDB.setVisibilityOverride(m.getName(), visible);
                }
            }
            ConnectorManagerRepository childCmr = childVDB.getAttachment(ConnectorManagerRepository.class);
            if (childCmr == null) {
                throw new AssertionError("childVdb does not have a connector manager repository"); //$NON-NLS-1$
            }
            if (!this.cmr.isShared()) {
                for (Map.Entry<String, ConnectorManager> entry : childCmr.getConnectorManagers().entrySet()) {
                    if (mergedRepo.getConnectorManagers().put(entry.getKey(), entry.getValue()) != null) {
                        throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40086, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40086, vdb.getName(), vdb.getVersion(), vdbImport.getName(), vdbImport.getVersion(), entry.getKey()));
                    }
                }
            }
        }
        if (toSearch.iterator().next() != null) {
            CombinedClassLoader ccl = new CombinedClassLoader(toSearch.iterator().next().getParent(), toSearch.toArray(new ClassLoader[toSearch.size()]));
            this.mergedVDB.addAttachment(ClassLoader.class, ccl);
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
    private LinkedHashMap<String, VDBResources.Resource> getVisibilityMap() {
        if (this.children == null || this.children.isEmpty()) {
            return this.visibilityMap;
        }

        LinkedHashMap<String, VDBResources.Resource> mergedvisibilityMap = new LinkedHashMap<String, VDBResources.Resource>();
        for (CompositeVDB child:this.children.values()) {
            LinkedHashMap<String, VDBResources.Resource> vm = child.getVisibilityMap();
            if ( vm != null) {
                mergedvisibilityMap.putAll(vm);
            }
        }
        if (this.visibilityMap != null) {
            mergedvisibilityMap.putAll(this.visibilityMap);
        }
        return mergedvisibilityMap;
    }

    private MetadataStore getMetadataStore() {
        return this.store;
    }

    VDBMetaData getOriginalVDB() {
        return originalVDB;
    }

    public void metadataLoadFinished(boolean allowEnv) {
        if (this.metadataloadFinished) {
            return;
        }
        this.metadataloadFinished = true;

        MetadataStore mergedStore = getMetadataStore();
        //the order of the models is important for resolving ddl
        //TODO we might consider not using the intermediate MetadataStore
        List<Schema> schemas = mergedStore.getSchemaList();
        schemas.clear();
        for (ModelMetaData model : this.vdb.getModelMetaDatas().values()) {
            Schema s = mergedStore.getSchema(model.getName());
            if (s != null) {
                schemas.add(s);
            } else {
                mergedStore.getSchemas().remove(model.getName());
            }
        }
        if (this.children != null && !this.children.isEmpty()) {
            for (CompositeVDB child:this.children.values()) {
                MetadataStore childStore = child.getMetadataStore();
                if ( childStore != null) {
                    mergedStore.merge(childStore);
                }
            }
        }

        TransformationMetadata metadata = buildTransformationMetaData(mergedVDB, getVisibilityMap(), mergedStore, getUDF(), systemFunctions, this.additionalStores, allowEnv);
        QueryMetadataInterface qmi = metadata;
        Map<String, String> multiSourceModels = MultiSourceMetadataWrapper.getMultiSourceModels(mergedVDB);
        if(multiSourceModels != null && !multiSourceModels.isEmpty()) {
            qmi = new MultiSourceMetadataWrapper(metadata, multiSourceModels);
        }
        mergedVDB.addAttachment(QueryMetadataInterface.class, qmi);
        mergedVDB.addAttachment(TransformationMetadata.class, metadata);
        mergedVDB.addAttachment(MetadataStore.class, mergedStore);
    }

    LinkedHashMap<VDBKey, CompositeVDB> getChildren() {
        return children;
    }

    public Collection<Future<?>> clearTasks() {
        ArrayList<Future<?>> copy = new ArrayList<Future<?>>(tasks);
        tasks.clear();
        return copy;
    }

    public void removeTask(Future<?> future) {
        tasks.remove(future);
    }

    public void addTask(Future<?> future) {
        tasks.add(future);
    }

    public VDBKey getVDBKey() {
        return this.vdbKey;
    }

}
