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

package org.teiid.query.tempdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.message.RequestID;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.ReplicatedObject;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;

public class GlobalTableStoreImpl implements GlobalTableStore, ReplicatedObject<String> {

    private static final String TEIID_FBI = "teiid:fbi"; //$NON-NLS-1$

    public enum MatState {
        NEEDS_LOADING,
        LOADING,
        FAILED_LOAD,
        LOADED
    }

    public class MatTableInfo {
        private long updateTime = -1;
        private MatState state = MatState.NEEDS_LOADING;
        private Serializable loadingAddress;
        private long ttl = -1;
        private boolean valid;
        private boolean asynch; //sub state of loading
        private Map<RequestID, WeakReference<RequestWorkItem>> waiters = new HashMap<RequestID, WeakReference<RequestWorkItem>>(2);

        protected MatTableInfo() {}

        private synchronized boolean shouldLoad(Serializable possibleLoadingAddress, boolean firstPass, boolean refresh, boolean invalidate) {
            if (invalidate) {
                LogManager.logDetail(LogConstants.CTX_MATVIEWS, this, "invalidating"); //$NON-NLS-1$
                valid = false;
            }
            switch (state) {
            case NEEDS_LOADING:
            case FAILED_LOAD:
                if (!firstPass) {
                    this.loadingAddress = possibleLoadingAddress;
                    setState(MatState.LOADING, null);
                }
                return true;
            case LOADING:
                if (!firstPass && localAddress instanceof Comparable<?> && ((Comparable)localAddress).compareTo(possibleLoadingAddress) < 0) {
                    this.loadingAddress = possibleLoadingAddress; //ties go to the lowest address
                    return true;
                }
                return false;
            case LOADED:
                if (!firstPass
                        || refresh
                        || (ttl >= 0 && System.currentTimeMillis() - updateTime - ttl > 0)) {
                    if (firstPass) {
                        setState(MatState.NEEDS_LOADING, null);
                    } else {
                        this.loadingAddress = possibleLoadingAddress;
                        setState(MatState.LOADING, null);
                    }
                    return true;
                }
                //other nodes may still need to load
                return localAddress == null || !localAddress.equals(possibleLoadingAddress);
            }
            throw new AssertionError();
        }

        private synchronized void setState(MatState state, Boolean valid) {
            MatState oldState = this.state;
            long timestamp = System.currentTimeMillis();
            LogManager.logDetail(LogConstants.CTX_MATVIEWS, this, "setting matState to", state, valid, timestamp, "old values", oldState, this.valid); //$NON-NLS-1$ //$NON-NLS-2$
            if (valid != null) {
                this.valid = valid;
            }
            this.state = state;
            this.updateTime = System.currentTimeMillis();
            for (WeakReference<RequestWorkItem> request : waiters.values()) {
                RequestWorkItem workItem = request.get();
                if (workItem != null) {
                    workItem.moreWork();
                }
            }
            waiters.clear();
        }

        public synchronized void setAsynchLoad() {
            assert state == MatState.LOADING;
            asynch = true;
        }

        public synchronized void setTtl(long ttl) {
            this.ttl = ttl;
        }

        public synchronized long getUpdateTime() {
            return updateTime;
        }

        public synchronized MatState getState() {
            return state;
        }

        public synchronized boolean isUpToDate() {
            return isValid() && (ttl < 0 || System.currentTimeMillis() - updateTime - ttl <= 0);
        }

        public synchronized boolean isValid() {
            return valid;
        }

        public synchronized long getTtl() {
            return ttl;
        }

        public VDBMetaData getVdbMetaData() {
            return vdbMetaData;
        }

        public synchronized void addWaiter(RequestWorkItem waiter) {
            waiters.put(waiter.getRequestID(), new WeakReference<RequestWorkItem>(waiter));
        }

        public synchronized boolean getAndClearAsynch() {
            boolean result = asynch;
            asynch = false;
            return result;
        }

    }

    private ConcurrentHashMap<String, MatTableInfo> matTables = new ConcurrentHashMap<String, MatTableInfo>();
    private TempTableStore tableStore = new TempTableStore("SYSTEM", TransactionMode.ISOLATE_READS, false); //$NON-NLS-1$
    private BufferManager bufferManager;
    private QueryMetadataInterface metadata;
    private volatile Serializable localAddress;
    private VDBMetaData vdbMetaData;

    public GlobalTableStoreImpl(BufferManager bufferManager, VDBMetaData vdbMetaData, QueryMetadataInterface metadata) {
        this.bufferManager = bufferManager;
        this.vdbMetaData = vdbMetaData;
        this.metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
    }

    public synchronized MatTableInfo getMatTableInfo(final String tableName) {
        MatTableInfo info = matTables.get(tableName);
        if (info == null) {
            info = new MatTableInfo();
            matTables.put(tableName, info);
        }
        return info;
    }

    @Override
    public void failedLoad(String matTableName) {
        MatTableInfo info = getMatTableInfo(matTableName);
        synchronized (info) {
            if (info.state != MatState.LOADED) {
                info.setState(MatState.FAILED_LOAD, null);
            }
        }
    }

    @Override
    public boolean needsLoading(String matTableName, Serializable loadingAddress, boolean firstPass, boolean refresh, boolean invalidate) {
        MatTableInfo info = getMatTableInfo(matTableName);
        return info.shouldLoad(loadingAddress, firstPass, refresh, invalidate);
    }

    @Override
    public TempMetadataID getGlobalTempTableMetadataId(Object viewId)
    throws TeiidProcessingException, TeiidComponentException {
        String matViewName = metadata.getFullName(viewId);
        String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
        GroupSymbol group = new GroupSymbol(matViewName);
        group.setMetadataID(viewId);
        TempMetadataID id = tableStore.getMetadataStore().getTempGroupID(matTableName);
        //define the table preserving the key/index information and ensure that only a single instance exists
        if (id == null) {
            synchronized (viewId) {
                id = tableStore.getMetadataStore().getTempGroupID(matTableName);
                LinkedHashMap<Expression, Integer> newExprs = null;
                if (id == null) {
                    List<ElementSymbol> allCols = ResolverUtil.resolveElementsInGroup(group, metadata);
                    QueryNode qnode = metadata.getVirtualPlan(viewId);
                    if (viewId instanceof Table) {
                        Table t = (Table)viewId;
                        List<KeyRecord> fbis = t.getFunctionBasedIndexes();
                        if (!fbis.isEmpty()) {
                            List<GroupSymbol> groups = Arrays.asList(group);
                            int i = 0;
                            newExprs = new LinkedHashMap<Expression, Integer>();
                            for (KeyRecord keyRecord : fbis) {
                                for (int j = 0; j < keyRecord.getColumns().size(); j++) {
                                    Column c = keyRecord.getColumns().get(j);
                                    if (c.getParent() != keyRecord) {
                                        continue;
                                    }
                                    String exprString = c.getNameInSource();
                                    Expression ex = QueryParser.getQueryParser().parseExpression(exprString);
                                    Integer index = newExprs.get(ex);
                                    if (index == null) {
                                        ResolverVisitor.resolveLanguageObject(ex, groups, metadata);
                                        ex = QueryRewriter.rewriteExpression(ex, null, metadata);
                                        String colName = TEIID_FBI + i;
                                        while (t.getColumnByName(colName) != null) {
                                            colName = TEIID_FBI + (++i);
                                        }
                                        ElementSymbol es = new ElementSymbol(colName);
                                        es.setType(ex.getType());
                                        allCols.add(es);
                                        c.setPosition(allCols.size());
                                        newExprs.put(ex, allCols.size());
                                        ex = (Expression)ex.clone();
                                    } else {
                                        c.setPosition(index);
                                    }
                                }
                            }
                            ResolverUtil.clearGroupInfo(group, metadata);
                            StringBuilder query = new StringBuilder("SELECT "); //$NON-NLS-1$
                            query.append(group).append(".*, "); //$NON-NLS-1$
                            for (Iterator<Expression> iter = newExprs.keySet().iterator(); iter.hasNext();) {
                                query.append(iter.next());
                                if (iter.hasNext()) {
                                    query.append(", "); //$NON-NLS-1$
                                }
                            }
                            query.append(" FROM ").append(group).append(" option nocache ").append(group); //$NON-NLS-1$ //$NON-NLS-2$
                            qnode = new QueryNode(query.toString());
                        }
                    }
                    id = tableStore.getMetadataStore().addTempGroup(matTableName, allCols, false, true);
                    id.setQueryNode(qnode);
                    id.setCardinality((int)metadata.getCardinality(viewId));
                    id.setOriginalMetadataID(viewId);

                    Object pk = metadata.getPrimaryKey(viewId);
                    if (pk != null) {
                        ArrayList<TempMetadataID> primaryKey = resolveIndex(metadata, id, pk);
                        id.setPrimaryKey(primaryKey);
                    }
                    Collection keys = metadata.getUniqueKeysInGroup(viewId);
                    for (Object key : keys) {
                        id.addUniqueKey(resolveIndex(metadata, id, key));
                    }
                    Collection indexes = metadata.getIndexesInGroup(viewId);
                    for (Object index : indexes) {
                        id.addIndex(index, resolveIndex(metadata, id, index));
                    }
                    Collection fks = metadata.getForeignKeysInGroup(viewId);
                    for (Object fk : fks) {
                        //a corner case would be for a self relationship
                        id.addForeignKey(fk, metadata.getPrimaryKeyIDForForeignKeyID(fk), resolveIndex(metadata, id, fk));
                    }
                    if (newExprs != null) {
                        Table table = (Table)viewId;
                        List<KeyRecord> fbis = table.getFunctionBasedIndexes();
                        for (KeyRecord keyRecord : fbis) {
                            id.addIndex(keyRecord, resolveIndex(metadata, id, keyRecord));
                        }
                        GroupSymbol gs = new GroupSymbol(matTableName);
                        gs.setMetadataID(id);
                        SymbolMap map = SymbolMap.createSymbolMap(group, ResolverUtil.resolveElementsInGroup(gs, metadata).subList(0, allCols.size() - newExprs.size()), metadata);
                        LinkedHashMap<Expression, Integer> mappedExprs = new LinkedHashMap<Expression, Integer>();
                        for (Map.Entry<Expression, Integer> entry : newExprs.entrySet()) {
                            Expression ex = (Expression) entry.getKey().clone();
                            ExpressionMappingVisitor.mapExpressions(ex, map.asMap());
                            mappedExprs.put(ex, entry.getValue());
                        }
                        id.getTableData().setFunctionBasedExpressions(mappedExprs);
                    }
                }
            }
        }
        updateCacheHint(viewId, group, id);
        return id;
    }

    @Override
    public TempMetadataID getCodeTableMetadataId(
            String codeTableName, String returnElementName,
            String keyElementName, String matTableName) throws TeiidComponentException,
            QueryMetadataException {
        ElementSymbol keyElement = new ElementSymbol(matTableName + ElementSymbol.SEPARATOR + keyElementName);
        ElementSymbol returnElement = new ElementSymbol(matTableName + ElementSymbol.SEPARATOR + returnElementName);
        keyElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementRuntimeTypeName(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + keyElementName))));
        returnElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementRuntimeTypeName(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + returnElementName))));
        TempMetadataID id = this.tableStore.getMetadataStore().getTempGroupID(matTableName);
        if (id == null) {
            synchronized (this) {
                id = this.tableStore.getMetadataStore().addTempGroup(matTableName, Arrays.asList(keyElement, returnElement), false, true);
                String queryString = Reserved.SELECT + ' ' + new ElementSymbol(keyElementName) + " ," + new ElementSymbol(returnElementName) + ' ' + Reserved.FROM + ' ' + new GroupSymbol(codeTableName); //$NON-NLS-1$
                id.setQueryNode(new QueryNode(queryString));
                id.setPrimaryKey(id.getElements().subList(0, 1));
                CacheHint hint = new CacheHint(true, null);
                id.setCacheHint(hint);
            }
        }
        return id;
    }

    private void updateCacheHint(Object viewId, GroupSymbol group,
            TempMetadataID id) throws TeiidComponentException,
            QueryMetadataException, QueryResolverException,
            QueryValidatorException {
        if (id.getCacheHint() != null && !id.getTableData().updateCacheHint(((Table)viewId).getLastModified())) {
            return;
        }
        //TODO: be stricter about the update strategy (needs synchronized or something better than ms resolution)
        Command c = QueryResolver.resolveView(group, metadata.getVirtualPlan(viewId), SQLConstants.Reserved.SELECT, metadata, false).getCommand();
        CacheHint hint = c.getCacheHint();
        if (hint != null) {
            hint = hint.clone();
        } else {
            hint = new CacheHint();
        }
        //overlay the properties
        String ttlString = metadata.getExtensionProperty(viewId, MaterializationMetadataRepository.MATVIEW_TTL, false);
        if (Boolean.valueOf(metadata.getExtensionProperty(viewId, MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, false))) {
            hint.setTtl(null); //will be managed by the scheduler
        } else if (ttlString != null) {
            hint.setTtl(Long.parseLong(ttlString));
        }
        String memString = metadata.getExtensionProperty(viewId, MaterializationMetadataRepository.MATVIEW_PREFER_MEMORY, false);
        if (memString != null) {
            hint.setPrefersMemory(Boolean.valueOf(memString));
        }
        String updatableString = metadata.getExtensionProperty(viewId, MaterializationMetadataRepository.MATVIEW_UPDATABLE, false);
        if (updatableString != null) {
            hint.setUpdatable(Boolean.valueOf(updatableString));
        }
        String scope = metadata.getExtensionProperty(viewId, MaterializationMetadataRepository.MATVIEW_SCOPE, false);
        if (scope != null) {
            hint.setScope(scope);
        }
        id.setCacheHint(hint);
    }

    static ArrayList<TempMetadataID> resolveIndex(
            QueryMetadataInterface metadata, TempMetadataID id, Object pk)
            throws TeiidComponentException, QueryMetadataException {
        List cols = metadata.getElementIDsInKey(pk);
        ArrayList<TempMetadataID> primaryKey = new ArrayList<TempMetadataID>(cols.size());
        for (Object coldId : cols) {
            int pos = metadata.getPosition(coldId) - 1;
            primaryKey.add(id.getElements().get(pos));
        }
        return primaryKey;
    }

    @Override
    public void loaded(String matTableName, TempTable table) {
        swapTempTable(matTableName, table);
        this.getMatTableInfo(matTableName).setState(MatState.LOADED, true);
    }

    private void swapTempTable(String tempTableName, TempTable tempTable) {
        this.tableStore.getTempTables().put(tempTableName, tempTable);
    }

    @Override
    public List<?> updateMatViewRow(String matTableName, List<?> tuple,
            boolean delete) throws TeiidComponentException {
        TempTable tempTable = tableStore.getTempTable(matTableName);
        if (tempTable != null) {
            TempMetadataID id = tableStore.getMetadataStore().getTempGroupID(matTableName);
            synchronized (id) {
                boolean clone = tempTable.getActive().get() != 0;
                if (clone) {
                    tempTable = tempTable.clone();
                }
                List<?> result = tempTable.updateTuple(tuple, delete);
                if (clone) {
                    swapTempTable(matTableName, tempTable);
                }
                return result;
            }
        }
        return null;
    }

    public TempTableStore getTempTableStore() {
        return this.tableStore;
    }

    @Override
    public TempTable createMatTable(final String tableName, GroupSymbol group) throws TeiidComponentException,
    QueryMetadataException, TeiidProcessingException {
        Create create = getCreateCommand(group, true, metadata);
        TempTable table = tableStore.addTempTable(tableName, create, bufferManager, false, null);
        table.setUpdatable(false);
        CacheHint hint = table.getCacheHint();
        if (hint != null) {
            table.setPreferMemory(hint.isPrefersMemory());
            if (hint.getTtl() != null) {
                getMatTableInfo(tableName).setTtl(hint.getTtl());
            }
            if (!create.getPrimaryKey().isEmpty()) {
                table.setUpdatable(hint.isUpdatable(false));
            }
        }
        return table;
    }

    public static Create getCreateCommand(GroupSymbol group, boolean matview, QueryMetadataInterface metadata)
            throws QueryMetadataException, TeiidComponentException {
        Create create = new Create();
        create.setTable(group);
        List<ElementSymbol> allColumns = ResolverUtil.resolveElementsInGroup(group, metadata);
        create.setElementSymbolsAsColumns(allColumns);
        if (!matview) {
            for (int i = 0; i < allColumns.size(); i++) {
                ElementSymbol es = allColumns.get(i);
                if (!metadata.elementSupports(es.getMetadataID(), SupportConstants.Element.NULL)) {
                    create.getColumns().get(i).setNullType(NullType.No_Nulls);
                    if (es.getType() == DataTypeManager.DefaultDataClasses.INTEGER
                            && metadata.elementSupports(es.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT)) {
                        create.getColumns().get(i).setAutoIncremented(true); //serial
                    }
                }
            }
        }
        Object pk = metadata.getPrimaryKey(group.getMetadataID());
        if (pk != null) {
            List<ElementSymbol> pkColumns = resolveIndex(metadata, allColumns, pk);
            create.getPrimaryKey().addAll(pkColumns);
        }
        return create;
    }

    /**
     * Return a list of ElementSymbols for the given index/key object
     */
    public static List<ElementSymbol> resolveIndex(QueryMetadataInterface metadata, List<ElementSymbol> allColumns, Object pk)
            throws TeiidComponentException, QueryMetadataException {
        Collection<?> pkIds = metadata.getElementIDsInKey(pk);
        List<ElementSymbol> pkColumns = new ArrayList<ElementSymbol>(pkIds.size());
        for (Object col : pkIds) {
            pkColumns.add(allColumns.get(metadata.getPosition(col)-1));
        }
        return pkColumns;
    }

    //begin replication methods

    @Override
    public void setAddress(Serializable address) {
        this.localAddress = address;
    }

    @Override
    public Serializable getAddress() {
        return localAddress;
    }

    @Override
    public void getState(OutputStream ostream) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(ostream);
            for (Map.Entry<String, TempTable> entry : tableStore.getTempTables().entrySet()) {
                sendTable(entry.getKey(), oos, true);
            }
            oos.writeObject(null);
            oos.close();
        } catch (IOException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30217, e);
        } catch (TeiidComponentException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30218, e);
        }
    }

    @Override
    public void setState(InputStream istream) {
        try {
            ObjectInputStream ois = new ObjectInputStream(istream);
            while (true) {
                String tableName = (String)ois.readObject();
                if (tableName == null) {
                    break;
                }
                loadTable(tableName, ois);
            }
            ois.close();
        } catch (Exception e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30219, e);
        }
    }

    @Override
    public void getState(String stateId, OutputStream ostream) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(ostream);
            sendTable(stateId, oos, false);
            oos.close();
        } catch (IOException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30220, e);
        } catch (TeiidComponentException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30221, e);
        }
    }

    private void sendTable(String stateId, ObjectOutputStream oos, boolean writeName)
            throws IOException, TeiidComponentException {
        TempTable tempTable = this.tableStore.getTempTable(stateId);
        if (tempTable == null) {
            return;
        }
        MatTableInfo info = getMatTableInfo(stateId);
        if (!info.isValid()) {
            return;
        }
        if (writeName) {
            oos.writeObject(stateId);
        }
        oos.writeLong(info.updateTime);
        oos.writeObject(info.loadingAddress);
        oos.writeObject(info.state);
        tempTable.writeTo(oos);
    }

    @Override
    public void setState(String stateId, InputStream istream) {
        try {
            ObjectInputStream ois = new ObjectInputStream(istream);
            loadTable(stateId, ois);
            ois.close();
        } catch (Exception e) {
            MatTableInfo info = this.getMatTableInfo(stateId);
            if (!info.isUpToDate()) {
                info.setState(MatState.FAILED_LOAD, null);
            }
            throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30222, e);
        }
    }

    private void loadTable(String stateId, ObjectInputStream ois)
            throws TeiidComponentException, QueryMetadataException,
            IOException,
            ClassNotFoundException, TeiidProcessingException {
        LogManager.logDetail(LogConstants.CTX_DQP, "loading table from remote stream", stateId); //$NON-NLS-1$
        long updateTime = ois.readLong();
        Serializable loadingAddress = (Serializable) ois.readObject();
        MatState state = (MatState)ois.readObject();
        GroupSymbol group = new GroupSymbol(stateId);
        if (stateId.startsWith(RelationalPlanner.MAT_PREFIX)) {
            String viewName = stateId.substring(RelationalPlanner.MAT_PREFIX.length());
            Object viewId = this.metadata.getGroupID(viewName);
            group.setMetadataID(getGlobalTempTableMetadataId(viewId));
        } else {
            String viewName = stateId.substring(TempTableDataManager.CODE_PREFIX.length());
            int index = viewName.lastIndexOf('.');
            String returnElementName = viewName.substring(index + 1);
            viewName = viewName.substring(0, index);
            index = viewName.lastIndexOf('.');
            String keyElementName = viewName.substring(index + 1);
            viewName = viewName.substring(0, index);
            group.setMetadataID(getCodeTableMetadataId(viewName, returnElementName, keyElementName, stateId));
        }
        TempTable tempTable = this.createMatTable(stateId, group);
        tempTable.readFrom(ois);
        MatTableInfo info = this.getMatTableInfo(stateId);
        synchronized (info) {
            swapTempTable(stateId, tempTable);
            info.setState(state, true);
            info.updateTime = updateTime;
            info.loadingAddress = loadingAddress;
        }
    }

    @Override
    public void droppedMembers(Collection<Serializable> addresses) {
        for (MatTableInfo info : this.matTables.values()) {
            synchronized (info) {
                if (info.getState() == MatState.LOADING
                        && addresses.contains(info.loadingAddress)) {
                    info.setState(MatState.FAILED_LOAD, null);
                }
            }
        }
    }

    @Override
    public boolean hasState(String stateId) {
        return this.tableStore.getTempTable(stateId) != null;
    }

    @Override
    public TempMetadataID getGlobalTempTableMetadataId(String matTableName) {
        return this.tableStore.getMetadataStore().getTempGroupID(matTableName);
    }

    @Override
    public TempTable getTempTable(String matTableName) {
        return this.tableStore.getTempTable(matTableName);
    }

}
