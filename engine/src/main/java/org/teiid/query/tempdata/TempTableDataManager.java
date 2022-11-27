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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.security.SessionToken;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.TupleSourceCache;
import org.teiid.events.EventDistributor;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.CacheDirective.Scope;

/**
 * This proxy ProcessorDataManager is used to handle temporary tables.
 *
 * This isn't handled as a connector because of the temporary metadata and
 * the create/drop handling (which doesn't have push down support)
 */
public class TempTableDataManager implements ProcessorDataManager {

    private static final int MIN_ASYNCH_SIZE = 1<<15;

    public interface RequestExecutor {
        void execute(String command, List<?> parameters);
        boolean isShutdown();
    }

    public abstract class ProxyTupleSource implements TupleSource {
        TupleSource actual;

        @Override
        public List<?> nextTuple() throws TeiidComponentException,
                TeiidProcessingException {
            if (actual == null) {
                actual = createTupleSource();
            }
            return actual.nextTuple();
        }

        protected abstract TupleSource createTupleSource() throws TeiidComponentException,
        TeiidProcessingException;

        @Override
        public void closeSource() {
            if (actual != null) {
                actual.closeSource();
            }
        }

    }

    private static final String REFRESHMATVIEWROW = ".refreshmatviewrow"; //$NON-NLS-1$
    private static final String REFRESHMATVIEWROWS = ".refreshmatviewrows"; //$NON-NLS-1$
    private static final String REFRESHMATVIEW = ".refreshmatview"; //$NON-NLS-1$
    public static final String CODE_PREFIX = "#CODE_"; //$NON-NLS-1$
    private static String REFRESH_SQL = SQLConstants.Reserved.CALL + ' ' + CoreConstants.SYSTEM_ADMIN_MODEL + REFRESHMATVIEW + "(?, ?)"; //$NON-NLS-1$

    private ProcessorDataManager processorDataManager;
    private BufferManager bufferManager;
    private SessionAwareCache<CachedResults> cache;
    private RequestExecutor executor;

    private EventDistributor eventDistributor;

    public TempTableDataManager(ProcessorDataManager processorDataManager, BufferManager bufferManager,
            SessionAwareCache<CachedResults> cache){
        this.processorDataManager = processorDataManager;
        this.bufferManager = bufferManager;
        this.cache = cache;
    }

    public void setExecutor(RequestExecutor executor) {
        this.executor = executor;
    }

    public void setEventDistributor(EventDistributor eventDistributor) {
        this.eventDistributor = eventDistributor;
    }

    public TupleSource registerRequest(
        CommandContext context,
        Command command,
        String modelName,
        RegisterRequestParameter parameterObject)
        throws TeiidComponentException, TeiidProcessingException {

         if (parameterObject.info != null) {
             TupleSourceCache tsc = context.getTupleSourceCache();
             if (tsc != null) {
                 return tsc.getSharedTupleSource(context, command, modelName, parameterObject, bufferManager, this);
             }
        }

        TempTableStore tempTableStore = context.getTempTableStore();
        if(tempTableStore != null) {
            try {
                TupleSource result = registerRequest(context, modelName, command);
                if (result != null) {
                    return result;
                }
            } catch (BlockedException e) {
                throw new AssertionError("blocked is not expected"); //$NON-NLS-1$
            }
        }
        return this.processorDataManager.registerRequest(context, command, modelName, parameterObject);
    }

    TupleSource registerRequest(final CommandContext context, String modelName, final Command command) throws TeiidComponentException, TeiidProcessingException {
        final TempTableStore contextStore = context.getTempTableStore();
        if (command instanceof Query) {
            Query query = (Query)command;
            if (modelName != null && !modelName.equals(TempMetadataAdapter.TEMP_MODEL.getID())) {
                return null;
            }
            return registerQuery(context, contextStore, query);
        }
        if (command instanceof ProcedureContainer) {
            if (command instanceof StoredProcedure) {
                StoredProcedure proc = (StoredProcedure)command;
                if (CoreConstants.SYSTEM_ADMIN_MODEL.equals(modelName)) {
                    TupleSource result = handleSystemProcedures(context, proc);
                    if (result != null) {
                        return result;
                    }
                } else if (proc.getGroup().isGlobalTable()) {
                    return handleCachedProcedure(context, proc);
                }
                return null; //it's not a stored procedure we want to handle
            }

            final GroupSymbol group = ((ProcedureContainer)command).getGroup();
            if (!modelName.equals(TempMetadataAdapter.TEMP_MODEL.getID()) || !group.isTempGroupSymbol()) {
                return null;
            }
            return new ProxyTupleSource() {

                @Override
                protected TupleSource createTupleSource() throws TeiidComponentException,
                        TeiidProcessingException {
                    final String groupKey = group.getNonCorrelationName();
                    final TempTable table = contextStore.getOrCreateTempTable(groupKey, command, bufferManager, true, true, context, group);
                    if (command instanceof Insert) {
                        Insert insert = (Insert)command;
                        TupleSource ts = insert.getTupleSource();
                        if (ts == null) {
                            Evaluator eval = new Evaluator(Collections.emptyMap(), TempTableDataManager.this, context);
                            List<Object> values = new ArrayList<Object>(insert.getValues().size());
                            for (Expression expr : (List<Expression>)insert.getValues()) {
                                values.add(eval.evaluate(expr, null));
                            }
                            ts = new CollectionTupleSource(Arrays.asList(values).iterator());
                        }
                        return table.insert(ts, insert.getVariables(), true, insert.isUpsert(), context);
                    }
                    if (command instanceof Update) {
                        final Update update = (Update)command;
                        final Criteria crit = update.getCriteria();
                        return table.update(crit, update.getChangeList());
                    }
                    if (command instanceof Delete) {
                        final Delete delete = (Delete)command;
                        final Criteria crit = delete.getCriteria();
                        if (crit == null) {
                            //TODO: we'll add a real truncate later
                            long rows = table.truncate(false);
                            return CollectionTupleSource.createUpdateCountTupleSource((int)Math.min(Integer.MAX_VALUE, rows));
                        }
                        return table.delete(crit);
                    }
                    throw new AssertionError("unknown command " + command); //$NON-NLS-1$
                }
            };
        }
        if (command instanceof Create) {
            Create create = (Create)command;
            String tempTableName = create.getTable().getName();
            if (contextStore.hasTempTable(tempTableName, true)) {
                 throw new QueryProcessingException(QueryPlugin.Event.TEIID30229, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30229, tempTableName));
            }
            if (create.getTableMetadata() != null) {
                contextStore.addForeignTempTable(tempTableName, create);
            } else {
                contextStore.addTempTable(tempTableName, create, bufferManager, true, context);
            }
            return CollectionTupleSource.createUpdateCountTupleSource(0);
        }
        if (command instanceof Drop) {
            String tempTableName = ((Drop)command).getTable().getName();
            contextStore.removeTempTableByName(tempTableName, context);
            return CollectionTupleSource.createUpdateCountTupleSource(0);
        }
        return null;
    }

    private TupleSource handleCachedProcedure(final CommandContext context,
            StoredProcedure proc) throws TeiidComponentException,
            QueryMetadataException, TeiidProcessingException {
        String fullName = context.getMetadata().getFullName(proc.getProcedureID());
        LogManager.logDetail(LogConstants.CTX_DQP, "processing cached procedure request for", fullName); //$NON-NLS-1$
        LinkedList<Object> vals = new LinkedList<Object>();
        for (SPParameter param : proc.getInputParameters()) {
            vals.add(((Constant)param.getExpression()).getValue());
        }
        //collapse the hash to single byte for the key to restrict the possible results to 256
        int hash = vals.hashCode();
        hash |= (hash >>> 16);
        hash |= (hash >>> 8);
        hash &= 0x000000ff;
        final CacheID cid = new CacheID(new ParseInfo(), fullName + hash, context.getVdbName(),
                context.getVdbVersion(), context.getConnectionId(), context.getUserName());
        cid.setParameters(vals);
        CachedResults results = cache.get(cid);
        if (results != null) {
            TupleBuffer buffer = results.getResults();
            return buffer.createIndexedTupleSource();
        }
        //construct a query with a no cache hint
        final CacheHint hint = proc.getCacheHint();
        proc.setCacheHint(null);
        Option option = new Option();
        option.setNoCache(true);
        option.addNoCacheGroup(fullName);
        proc.setOption(option);
        StoredProcedure cloneProc = (StoredProcedure)proc.clone();
        int i = 0;
        for (SPParameter param : cloneProc.getInputParameters()) {
            param.setExpression(new Reference(i++));
        }
        final QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(cloneProc.toString(), fullName.toUpperCase(), context, vals.toArray());
        final BatchCollector bc = qp.createBatchCollector();

        return new ProxyTupleSource() {
            boolean success = false;

            @Override
            protected TupleSource createTupleSource() throws TeiidComponentException,
                    TeiidProcessingException {
                TupleBuffer tb = bc.collectTuples();
                CachedResults cr = new CachedResults();
                cr.setResults(tb, qp.getProcessorPlan());
                Determinism determinismLevel = qp.getContext().getDeterminismLevel();
                if (hint != null && hint.getDeterminism() != null) {
                    LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Cache hint modified the query determinism from ",determinismLevel, " to ", hint.getDeterminism() }); //$NON-NLS-1$ //$NON-NLS-2$
                    determinismLevel = hint.getDeterminism();
                }
                cache.put(cid, determinismLevel, cr, hint != null?hint.getTtl():null);
                context.setDeterminismLevel(determinismLevel);
                success = true;
                return tb.createIndexedTupleSource();
            }

            @Override
            public void closeSource() {
                super.closeSource();
                qp.closeProcessing();
                if (!success && bc.getTupleBuffer() != null) {
                    bc.getTupleBuffer().remove();
                }
            }
        };
    }

    private TupleSource handleSystemProcedures(final CommandContext context, StoredProcedure proc)
            throws TeiidComponentException, QueryMetadataException,
            QueryProcessingException, QueryResolverException,
            QueryValidatorException, TeiidProcessingException,
            ExpressionEvaluationException {
        final QueryMetadataInterface metadata = context.getMetadata();
        if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEW)) {
            Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(2).getExpression()).getValue());
            TempMetadataID matTableId = context.getGlobalTableStore().getGlobalTempTableMetadataId(groupID);
            final GlobalTableStore globalStore = getGlobalStore(context, matTableId);
            String matViewName = metadata.getFullName(groupID);
            String matTableName = metadata.getFullName(matTableId);
            LogManager.logDetail(LogConstants.CTX_MATVIEWS, "processing refreshmatview for", matViewName); //$NON-NLS-1$
            boolean invalidate = Boolean.TRUE.equals(((Constant)proc.getParameter(3).getExpression()).getValue());
            boolean needsLoading = globalStore.getMatTableInfo(matTableName).getAndClearAsynch();
            if (!needsLoading) {
                needsLoading = globalStore.needsLoading(matTableName, globalStore.getAddress(), true, true, invalidate);
                if (needsLoading) {
                    needsLoading = globalStore.needsLoading(matTableName, globalStore.getAddress(), false, false, invalidate);
                }
            }
            if (!needsLoading) {
                return CollectionTupleSource.createUpdateCountTupleSource(-1);
            }
            GroupSymbol matTable = new GroupSymbol(matTableName);
            matTable.setMetadataID(matTableId);
            return loadGlobalTable(context, matTable, matTableName, globalStore);
        } else if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEWROWS)) {
            final Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(2).getExpression()).getValue());
            TempMetadataID matTableId = context.getGlobalTableStore().getGlobalTempTableMetadataId(groupID);
            final GlobalTableStore globalStore = getGlobalStore(context, matTableId);
            Object pk = metadata.getPrimaryKey(groupID);
            String matViewName = metadata.getFullName(groupID);
            if (pk == null) {
                 throw new QueryProcessingException(QueryPlugin.Event.TEIID30230, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30230, matViewName));
            }
            List<?> ids = metadata.getElementIDsInKey(pk);
            Object[][] params = (Object[][]) ((ArrayImpl) ((Constant)proc.getParameter(3).getExpression()).getValue()).getValues();
            return updateMatviewRows(context, metadata, groupID, globalStore, matViewName, ids, params);
        } else if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEWROW)) {
            final Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(2).getExpression()).getValue());
            TempMetadataID matTableId = context.getGlobalTableStore().getGlobalTempTableMetadataId(groupID);
            final GlobalTableStore globalStore = getGlobalStore(context, matTableId);
            Object pk = metadata.getPrimaryKey(groupID);
            final String matViewName = metadata.getFullName(groupID);
            if (pk == null) {
                 throw new QueryProcessingException(QueryPlugin.Event.TEIID30230, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30230, matViewName));
            }
            List<?> ids = metadata.getElementIDsInKey(pk);
            Constant key = (Constant)proc.getParameter(3).getExpression();
            Object initialValue = key.getValue();
            SPParameter keyOther = proc.getParameter(4);
            Object[] param = null;
            if (keyOther != null) {
                Object[] otherCols = ((ArrayImpl) ((Constant)keyOther.getExpression()).getValue()).getValues();
                if (otherCols != null) {
                    param = new Object[1 + otherCols.length];
                    param[0] = initialValue;
                    for (int i = 0; i < otherCols.length; i++) {
                        param[i+1] = otherCols[i];
                    }
                }
            }
            if (param == null) {
                param = new Object[] {initialValue};
            }

            Object[][] params = new Object[][] {param};

            return updateMatviewRows(context, metadata, groupID, globalStore,
                    matViewName, ids, params);
        }
        return null;
    }

    private TupleSource updateMatviewRows(final CommandContext context,
            final QueryMetadataInterface metadata, final Object groupID,
            final GlobalTableStore globalStore, final String matViewName,
            List<?> ids, Object[][] params) throws QueryProcessingException,
            TeiidComponentException, QueryMetadataException,
            TransformationException {
        final String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
        MatTableInfo info = globalStore.getMatTableInfo(matTableName);
        if (!info.isValid()) {
            return CollectionTupleSource.createUpdateCountTupleSource(-1);
        }
        TempTable tempTable = globalStore.getTempTable(matTableName);
        if (!tempTable.isUpdatable()) {
             throw new QueryProcessingException(QueryPlugin.Event.TEIID30232, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30232, matViewName));
        }

        List<Object[]> converted = new ArrayList<Object[]>();
        for (Object[] param : params) {
            if (param == null || ids.size() != param.length) {
                throw new QueryProcessingException(QueryPlugin.Event.TEIID30231, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30231, matViewName, ids.size(), param == null ? 0 : param.length));
            }
            final Object[] vals = new Object[param.length];
            for (int i = 0; i < ids.size(); i++) {
                Object value = param[i];
                String targetTypeName = metadata.getElementRuntimeTypeName(ids.get(i));
                value = DataTypeManager.transformValue(value, DataTypeManager.getDataTypeClass(targetTypeName));
                vals[i] = value;
            }
            converted.add(vals);
        }
        final Iterator<Object[]> paramIter = converted.iterator();

        Iterator<?> iter = ids.iterator();
        StringBuilder criteria = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            Object id = iter.next();
            if (i != 0) {
                criteria.append(" AND "); //$NON-NLS-1$
            }
            criteria.append(metadata.getFullName(id)).append(" = ?"); //$NON-NLS-1$
        }

        final String queryString = Reserved.SELECT + " * " + Reserved.FROM + ' ' + matViewName + ' ' + Reserved.WHERE + ' ' + //$NON-NLS-1$
            criteria.toString() + ' ' + Reserved.OPTION + ' ' + Reserved.NOCACHE;

        return new ProxyTupleSource() {
            private QueryProcessor qp;
            private TupleSource ts;
            private Object[] params;
            private int count;

            @Override
            protected TupleSource createTupleSource()
                    throws TeiidComponentException,
                    TeiidProcessingException {
                while (true) {
                    if (qp == null) {
                        params = paramIter.next();
                        LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30012, matViewName, Arrays.toString(params)));
                        qp = context.getQueryProcessorFactory().createQueryProcessor(queryString, matViewName.toUpperCase(), context, params);
                        ts = new BatchCollector.BatchProducerTupleSource(qp);
                    }

                    List<?> tuple = ts.nextTuple();
                    boolean delete = false;
                    if (tuple == null) {
                        delete = true;
                        tuple = Arrays.asList(params);
                    } else {
                        tuple = new ArrayList<Object>(tuple); //ensure the list is serializable
                    }
                    List<?> result = globalStore.updateMatViewRow(matTableName, tuple, delete);

                    if (result != null) {
                        count++;
                    }

                    if (eventDistributor != null) {
                        eventDistributor.updateMatViewRow(context.getVdbName(), context.getVdbVersion(), metadata.getName(metadata.getModelID(groupID)), metadata.getName(groupID), tuple, delete);
                    }

                    qp.closeProcessing();
                    qp = null;
                    ts = null;

                    if (!paramIter.hasNext()) {
                        break;
                    }
                }

                return CollectionTupleSource.createUpdateCountTupleSource(count);
            }

            @Override
            public void closeSource() {
                super.closeSource();
                if (qp != null) {
                    qp.closeProcessing();
                }
            }
        };
    }

    private Object validateMatView(QueryMetadataInterface metadata,    String viewName) throws TeiidComponentException,
            TeiidProcessingException {
        try {
            Object groupID = metadata.getGroupID(viewName);
            if (!metadata.hasMaterialization(groupID) || metadata.getMaterialization(groupID) != null) {
                 throw new QueryProcessingException(QueryPlugin.Event.TEIID30233, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30233, viewName));
            }
            return groupID;
        } catch (QueryMetadataException e) {
             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30234, e);
        }
    }

    private TupleSource registerQuery(final CommandContext context,
            final TempTableStore contextStore, final Query query) {
        final GroupSymbol group = query.getFrom().getGroups().get(0);
        if (!group.isTempGroupSymbol()) {
            return null;
        }
        final String tableName = group.getNonCorrelationName();
        if (group.isGlobalTable()) {
            TempMetadataID matTableId = (TempMetadataID)group.getMetadataID();
            final GlobalTableStore globalStore = getGlobalStore(context, matTableId);
            final MatTableInfo info = globalStore.getMatTableInfo(tableName);
            return new ProxyTupleSource() {
                Future<Void> moreWork = null;
                TupleSource loadingTupleSource;
                DQPWorkContext newWorkContext;

                @Override
                protected TupleSource createTupleSource()
                        throws TeiidComponentException,
                        TeiidProcessingException {
                    if (loadingTupleSource != null) {
                        load();
                    } else {
                        boolean load = false;
                        if (!info.isUpToDate()) {
                            boolean invalidate = shouldInvalidate(context.getVdb());
                            load = globalStore.needsLoading(tableName, globalStore.getAddress(), true, false, info.isValid() && invalidate);
                            if (load) {
                                load = globalStore.needsLoading(tableName, globalStore.getAddress(), false, false, info.isValid() && invalidate);
                            }
                            if (!load) {
                                synchronized (info) {
                                    if (!info.isUpToDate()) {
                                        RequestWorkItem workItem = context.getWorkItem();
                                        info.addWaiter(workItem);
                                        if (moreWork != null) {
                                            moreWork.cancel(false);
                                        }
                                        moreWork = workItem.scheduleWork(10000); //fail-safe - attempt again in 10 seconds
                                        throw BlockedException.block("Blocking on mat view load", tableName); //$NON-NLS-1$
                                    }
                                }
                            } else {
                                if (!info.isValid() || executor == null) {
                                    //blocking load
                                    //TODO: we should probably do all loads using a temp session
                                    if (info.getVdbMetaData() != null && context.getDQPWorkContext() != null && !info.getVdbMetaData().getFullName().equals(context.getDQPWorkContext().getVDB().getFullName())) {
                                        assert executor != null;
                                        //load with by pretending we're in the imported vdb
                                        newWorkContext = createWorkContext(context, info.getVdbMetaData());
                                        CommandContext newContext = context.clone();
                                        newContext.setNewVDBState(newWorkContext);
                                        loadingTupleSource = loadGlobalTable(newContext, group, tableName, newContext.getGlobalTableStore());
                                    } else {
                                        loadingTupleSource = loadGlobalTable(context, group, tableName, globalStore);
                                    }
                                    load();
                                } else {
                                    loadViaRefresh(context, tableName, context.getDQPWorkContext().getVDB(), info);
                                }
                            }
                        }
                    }
                    TempTable table = globalStore.getTempTable(tableName);
                    context.accessedDataObject(group.getMetadataID());
                    if (context.isParallel() && query.getCriteria() == null && query.getOrderBy() != null && table.getRowCount() > MIN_ASYNCH_SIZE) {
                        return new AsyncTupleSource(new Callable<TupleSource>() {
                            @Override
                            public TupleSource call() throws Exception {
                                synchronized (this) {
                                    TupleSource result = table.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
                                    cancelMoreWork();
                                    return result;
                                }
                            }
                        }, context);
                    }
                    TupleSource result = table.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
                    cancelMoreWork();
                    return result;
                }

                private void load() throws TeiidComponentException,
                        TeiidProcessingException {
                    try {
                        if (newWorkContext != null) {

                                newWorkContext.runInContext(new Callable<Void>() {
                                    @Override
                                    public Void call() throws Exception {
                                        loadingTupleSource.nextTuple();
                                        return null;
                                    }
                                });
                        } else {
                            loadingTupleSource.nextTuple();
                        }
                    } catch (Throwable e) {
                        rethrow(e);
                    }
                }

                private void cancelMoreWork() {
                    if (moreWork != null) {
                        moreWork.cancel(false);
                        moreWork = null;
                    }
                }

                @Override
                public void closeSource() {
                    if (loadingTupleSource != null) {
                        loadingTupleSource.closeSource();
                    }
                    super.closeSource();
                    cancelMoreWork();
                }

            };
        }
        //it's not expected for a blocked exception to bubble up from here, so return a tuplesource to perform getOrCreateTempTable
        return new ProxyTupleSource() {
            @Override
            protected TupleSource createTupleSource()
                    throws TeiidComponentException, TeiidProcessingException {
                TempTableStore tts = contextStore;

                TempTable tt = tts.getOrCreateTempTable(tableName, query, bufferManager, true, false, context, group);
                if (context.getDataObjects() != null) {
                    Object id = RelationalPlanner.getTrackableGroup(group, context.getMetadata());
                    if (id != null) {
                        context.accessedDataObject(id);
                    }
                }
                if (context.isParallel() && query.getCriteria() == null && query.getOrderBy() != null && tt.getRowCount() > MIN_ASYNCH_SIZE) {
                    return new AsyncTupleSource(new Callable<TupleSource>() {
                        @Override
                        public TupleSource call() throws Exception {
                            synchronized (this) {
                                return tt.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
                            }
                        }
                    }, context);
                }
                return tt.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
            }
        };
    }

    private GlobalTableStore getGlobalStore(final CommandContext context, TempMetadataID matTableId) {
        GlobalTableStore globalStore = context.getGlobalTableStore();
        if (matTableId.getCacheHint() == null || matTableId.getCacheHint().getScope() == null || Scope.VDB.compareTo(matTableId.getCacheHint().getScope()) <= 0) {
            return globalStore;
        }
        throw new AssertionError("session scoping not supported"); //$NON-NLS-1$
    }

    private void loadViaRefresh(final CommandContext context, final String tableName, VDBMetaData vdb, MatTableInfo info) throws TeiidProcessingException, TeiidComponentException {
        info.setAsynchLoad();
        DQPWorkContext workContext = createWorkContext(context, vdb);
        final String viewName = tableName.substring(RelationalPlanner.MAT_PREFIX.length());
        workContext.runInContext(new Runnable() {
            @Override
            public void run() {
                executor.execute(REFRESH_SQL, Arrays.asList(viewName, Boolean.FALSE));
            }
        });
    }

    private DQPWorkContext createWorkContext(final CommandContext context,
            VDBMetaData vdb) {
        SessionMetadata session = createTemporarySession(context.getUserName(), "asynch-mat-view-load", vdb); //$NON-NLS-1$
        session.setSubject(context.getSubject());
        session.setSecurityDomain(context.getSession().getSecurityDomain());
        session.setSecurityContext(context.getSession().getSecurityContext());
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setAdmin(true);
        DQPWorkContext current = context.getDQPWorkContext();
        workContext.setSession(session);
        workContext.setPolicies(current.getAllowedDataPolicies());
        workContext.setSecurityHelper(current.getSecurityHelper());
        return workContext;
    }

    private TupleSource loadGlobalTable(final CommandContext context,
            final GroupSymbol group, final String tableName, final GlobalTableStore globalStore)
            throws TeiidComponentException, TeiidProcessingException {
        LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30013, tableName));
        final QueryMetadataInterface metadata = context.getMetadata();
        final List<ElementSymbol> allColumns = ResolverUtil.resolveElementsInGroup(group, metadata);
        final TempTable table = globalStore.createMatTable(tableName, group);
        table.setUpdatable(false);
        return new ProxyTupleSource() {
            TupleSource insertTupleSource;
            boolean success;
            QueryProcessor qp;
            boolean closed;
            boolean errored;

            @Override
            protected TupleSource createTupleSource() throws TeiidComponentException,
                    TeiidProcessingException {
                long rowCount = -1;
                try {
                    if (insertTupleSource == null) {
                        String fullName = metadata.getFullName(group.getMetadataID());
                        String transformation = metadata.getVirtualPlan(group.getMetadataID()).getQuery();
                        qp = context.getQueryProcessorFactory().createQueryProcessor(transformation, fullName, context);
                        insertTupleSource = new BatchCollector.BatchProducerTupleSource(qp);
                    }
                    table.insert(insertTupleSource, allColumns, false, false, null);
                    table.getTree().compact();
                    rowCount = table.getRowCount();
                    Determinism determinism = qp.getContext().getDeterminismLevel();
                    context.setDeterminismLevel(determinism);
                    //TODO: could pre-process indexes to remove overlap
                    for (Object index : metadata.getIndexesInGroup(group.getMetadataID())) {
                        List<ElementSymbol> columns = GlobalTableStoreImpl.resolveIndex(metadata, allColumns, index);
                        table.addIndex(columns, false);
                    }
                    for (Object key : metadata.getUniqueKeysInGroup(group.getMetadataID())) {
                        List<ElementSymbol> columns = GlobalTableStoreImpl.resolveIndex(metadata, allColumns, key);
                        table.addIndex(columns, true);
                    }
                    CacheHint hint = table.getCacheHint();
                    if (hint != null && table.getPkLength() > 0) {
                        table.setUpdatable(hint.isUpdatable(false));
                    }
                    if (determinism.compareTo(Determinism.VDB_DETERMINISTIC) < 0 && (hint == null || hint.getScope() == null || Scope.VDB.compareTo(hint.getScope()) <= 0)) {
                        LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31143, determinism, tableName)); //$NON-NLS-1$
                    }
                    globalStore.loaded(tableName, table);
                    success = true;
                    LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30014, tableName, rowCount));
                    return CollectionTupleSource.createUpdateCountTupleSource((int)Math.min(Integer.MAX_VALUE, rowCount));
                } catch (BlockedException e) {
                    throw e;
                } catch (Exception e) {
                    errored = true;
                    if (executor == null || !executor.isShutdown()) {
                        //if we're shutting down, no need to log
                        LogManager.logError(LogConstants.CTX_MATVIEWS, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30015, tableName));
                    }
                    closeSource();
                    rethrow(e);
                    throw new AssertionError();
                }
            }

            @Override
            public void closeSource() {
                if (closed) {
                    return;
                }
                if (!errored && !success) {
                    LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31153, tableName));
                }
                closed = true;
                if (!success) {
                    globalStore.failedLoad(tableName);
                    table.remove();
                }
                if (qp != null) {
                    qp.closeProcessing();
                }
                super.closeSource();
            }
        };
    }

    public Object lookupCodeValue(CommandContext context, String codeTableName,
            String returnElementName, String keyElementName, Object keyValue)
            throws BlockedException, TeiidComponentException,
            TeiidProcessingException {
        //we are not using a resolved form of a lookup, so we canonicalize with upper case
        codeTableName = codeTableName.toUpperCase();
        keyElementName = keyElementName.toUpperCase();
        returnElementName = returnElementName.toUpperCase();
        String matTableName = CODE_PREFIX + codeTableName + ElementSymbol.SEPARATOR + keyElementName + ElementSymbol.SEPARATOR + returnElementName;

        TupleSource ts = context.getCodeLookup(matTableName, keyValue);
        if (ts == null) {
            QueryMetadataInterface metadata = context.getMetadata();

            TempMetadataID id = context.getGlobalTableStore().getCodeTableMetadataId(codeTableName,
                    returnElementName, keyElementName, matTableName);

            ElementSymbol keyElement = new ElementSymbol(keyElementName, new GroupSymbol(matTableName));
            ElementSymbol returnElement = new ElementSymbol(returnElementName, new GroupSymbol(matTableName));
            keyElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementRuntimeTypeName(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + keyElementName))));
            returnElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementRuntimeTypeName(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + returnElementName))));

            Query query = RelationalPlanner.createMatViewQuery(id, matTableName, Arrays.asList(returnElement), true);
            query.setCriteria(new CompareCriteria(keyElement, CompareCriteria.EQ, new Constant(keyValue)));

            ts = registerQuery(context, context.getTempTableStore(), query);
        }
        try {
            List<?> row = ts.nextTuple();
            Object result = null;
            if (row != null) {
                result = row.get(0);
            }
            ts.closeSource();
            return result;
        } catch (BlockedException e) {
            context.putCodeLookup(matTableName, keyValue, ts);
            throw e;
        }
    }

    @Override
    public EventDistributor getEventDistributor() {
        return this.eventDistributor;
    }

    /**
     * Create an unauthenticated session
     * @param userName
     * @param app
     * @param vdb
     * @return
     */
    public static SessionMetadata createTemporarySession(String userName, String app, VDBMetaData vdb) {
        long creationTime = System.currentTimeMillis();
        SessionMetadata newSession = new SessionMetadata();
        newSession.setSessionToken(new SessionToken(userName));
        newSession.setSessionId(newSession.getSessionToken().getSessionID());
        newSession.setUserName(userName);
        newSession.setCreatedTime(creationTime);
        newSession.setApplicationName(app);
        newSession.setVDBName(vdb.getName());
        newSession.setVDBVersion(vdb.getVersion());
        newSession.setVdb(vdb);
        newSession.setEmbedded(true);
        return newSession;
    }

    private static void rethrow(Throwable e)
            throws TeiidComponentException,
            TeiidProcessingException {
        if (e instanceof TeiidComponentException) {
            throw (TeiidComponentException)e;
        }
        if (e instanceof TeiidProcessingException) {
            throw (TeiidProcessingException)e;
        }
        if (e instanceof RejectedExecutionException) {
            throw new TeiidComponentException(e);
        }
        if (e instanceof RuntimeException) {
            throw (RuntimeException)e;
        }
        throw new TeiidRuntimeException(e);
    }

    public static boolean shouldInvalidate(VDBMetaData vdb) {
        boolean invalidate = true;
        if (vdb != null) {
            String val = vdb.getPropertyValue("lazy-invalidate"); //$NON-NLS-1$
            if (val != null) {
                invalidate = !Boolean.valueOf(val);
            }
        }
        return invalidate;
    }

}
