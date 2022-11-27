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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;
import org.teiid.query.tempdata.TempTable;
import org.teiid.runtime.RuntimePlugin;

public class CompositeGlobalTableStore implements GlobalTableStore {

    public static GlobalTableStore createInstance(CompositeVDB vdb, BufferManager bufferManager, ObjectReplicator replicator) {
        VDBMetaData vdbMetadata = vdb.getVDB();
        QueryMetadataInterface metadata = vdbMetadata.getAttachment(TransformationMetadata.class);
        GlobalTableStore gts = new GlobalTableStoreImpl(bufferManager, vdbMetadata, metadata);
        if (replicator != null) {
            try {
                gts = replicator.replicate(vdbMetadata.getFullName(), GlobalTableStore.class, gts, 300000);
            } catch (Exception e) {
                LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40088, gts));
            }
        }
        if (vdb.getChildren() == null) {
            return gts;
        }
        TreeMap<String, GlobalTableStore> stores = new TreeMap<String, GlobalTableStore>(String.CASE_INSENSITIVE_ORDER);
        buildStoreMap(vdb, stores);
        return new CompositeGlobalTableStore(stores, gts, metadata);
    }

    static void buildStoreMap(CompositeVDB vdb,
            TreeMap<String, GlobalTableStore> stores) {
        for (CompositeVDB cvdb : vdb.getChildren().values()) {
            VDBMetaData child = cvdb.getVDB();
            GlobalTableStore childGts = child.getAttachment(GlobalTableStore.class);
            if (cvdb.getChildren() != null) {
                buildStoreMap(cvdb, stores);
            }
            for (String name : child.getModelMetaDatas().keySet()) {
                if (cvdb.getChildren() == null || !child.getImportedModels().contains(name)) {
                    stores.put(name, childGts);
                }
            }
        }
    }

    private Map<String, GlobalTableStore> stores;
    private GlobalTableStore primary;
    private QueryMetadataInterface metadata;

    public CompositeGlobalTableStore(Map<String, GlobalTableStore> stores,
            GlobalTableStore primary, QueryMetadataInterface metadata) {
        this.stores = stores;
        this.primary = primary;
        this.metadata = metadata;
    }

    @Override
    public TempMetadataID getGlobalTempTableMetadataId(Object groupID)
            throws TeiidComponentException, TeiidProcessingException {
        Object mid = metadata.getModelID(groupID);
        String name = metadata.getName(mid);
        return getStore(name).getGlobalTempTableMetadataId(groupID);
    }

    @Override
    public TempMetadataID getGlobalTempTableMetadataId(String matTableName) {
        return getStoreForTable(matTableName).getGlobalTempTableMetadataId(matTableName);
    }

    @Override
    public TempMetadataID getCodeTableMetadataId(String codeTableName,
            String returnElementName, String keyElementName, String matTableName)
            throws TeiidComponentException, QueryMetadataException {
        return getStoreForTable(matTableName).getCodeTableMetadataId(codeTableName, returnElementName, keyElementName, matTableName);
    }

    @Override
    public MatTableInfo getMatTableInfo(String matTableName) {
        return getStoreForTable(matTableName).getMatTableInfo(matTableName);
    }

    @Override
    public TempTable getTempTable(String matTableName) {
        return getStoreForTable(matTableName).getTempTable(matTableName);
    }

    @Override
    public Serializable getAddress() {
        return primary.getAddress();
    }

    @Override
    public List<?> updateMatViewRow(String matTableName, List<?> tuple,
            boolean delete) throws TeiidComponentException {
        return getStoreForTable(matTableName).updateMatViewRow(matTableName, tuple, delete);
    }

    @Override
    public TempTable createMatTable(String matTableName, GroupSymbol group)
            throws TeiidComponentException, QueryMetadataException,
            TeiidProcessingException {
        return getStoreForTable(matTableName).createMatTable(matTableName, group);
    }

    @Override
    public void failedLoad(String matTableName) {
        getStoreForTable(matTableName).failedLoad(matTableName);
    }

    @Override
    public boolean needsLoading(String matTableName, Serializable loadingAddress,
            boolean firstPass, boolean refresh, boolean invalidate) {
        return getStoreForTable(matTableName).needsLoading(matTableName, loadingAddress, firstPass, refresh, invalidate);
    }

    @Override
    public void loaded(String matTableName, TempTable table) {
        getStoreForTable(matTableName).loaded(matTableName, table);
    }

    GlobalTableStore getStoreForTable(String matTableName) {
        String name = matTableName.substring(RelationalPlanner.MAT_PREFIX.length(), matTableName.length());
        name = name.substring(0, name.indexOf('.'));
        return getStore(name);
    }

    GlobalTableStore getStore(String name) {
        GlobalTableStore store = stores.get(name);
        if (store == null) {
            return primary;
        }
        return store;
    }

    GlobalTableStore getPrimary() {
        return primary;
    }

}
