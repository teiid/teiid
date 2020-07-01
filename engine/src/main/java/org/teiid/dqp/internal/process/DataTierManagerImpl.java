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

package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.RequestMessage;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorWork;
import org.teiid.dqp.internal.process.AbstractWorkItem.ThreadState;
import org.teiid.dqp.internal.process.DQPCore.CompletionListener;
import org.teiid.dqp.internal.process.RecordTable.ExpandingSimpleIterator;
import org.teiid.dqp.internal.process.RecordTable.SimpleIterator;
import org.teiid.dqp.internal.process.RecordTable.SimpleIteratorWrapper;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.internal.process.TupleSourceCache.CachableVisitor;
import org.teiid.dqp.internal.process.TupleSourceCache.CopyOnReadTupleSource;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.events.EventDistributor;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.NamespaceContainer;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.Table.Type;
import org.teiid.metadata.TableStats;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.CompositeMetadataStore.RecordHolder;
import org.teiid.query.metadata.DDLConstants;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.DdlPlan;
import org.teiid.query.processor.DdlPlan.SetPropertyProcessor;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.tempdata.BaseIndexInfo;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.CacheDirective.Invalidation;
import org.teiid.translator.CacheDirective.Scope;
import org.teiid.translator.TranslatorException;
import org.teiid.vdb.runtime.VDBKey;

/**
 * Full {@link ProcessorDataManager} implementation that
 * controls access to {@link ConnectorManager}s and handles system queries.
 */
public class DataTierManagerImpl implements ProcessorDataManager {

    private static final int MAX_VALUE_LENGTH = 1 << 21;

    private static final class ThreadBoundTask implements Callable<Void>, CompletionListener<Void> {
        private final RequestWorkItem workItem;
        private final TupleSource toRead;
        final AtomicBoolean done = new AtomicBoolean();
        private final DataTierTupleSource dtts;

        private ThreadBoundTask(RequestWorkItem workItem, TupleSource toRead, DataTierTupleSource dtts) {
            this.workItem = workItem;
            this.toRead = toRead;
            this.dtts = dtts;
        }

        @Override
        public Void call() throws TeiidProcessingException, TeiidComponentException {
            //pull the whole thing.  the side effect will be saving
            while (true) {
                try {
                    if (done.get() || toRead.nextTuple() == null) {
                        break;
                    }
                    signalMore();
                } catch (BlockedException e) {
                    //data not available
                    Future<Void> future = dtts.getScheduledFuture();
                    if (future != null) {
                        try {
                            future.get();
                        } catch (Exception e1) {
                            throw new TeiidComponentException(e);
                        }
                    }
                }
            };
            return null;
        }

        private void signalMore() {
            if (!done.get()) {
                synchronized (workItem) {
                    if (workItem.getThreadState() != ThreadState.MORE_WORK) {
                        workItem.moreWork();
                    }
                }
            }
        }

        @Override
        public void onCompletion(FutureWork<Void> future) {
            if (future != null) {
                signalMore();
            }
            toRead.closeSource();
            dtts.fullyCloseSource();
        }
    }

    private enum SystemTables {
        VIRTUALDATABASES,
        SCHEMAS,
        TABLES,
        DATATYPES,
        COLUMNS,
        KEYS,
        PROCEDURES,
        KEYCOLUMNS,
        PROCEDUREPARAMS,
        REFERENCEKEYCOLUMNS,
        PROPERTIES,
        FUNCTIONS,
        FUNCTIONPARAMS,
    }

    private enum SystemAdminTables {
        MATVIEWS,
        VDBRESOURCES,
        TRIGGERS,
        VIEWS,
        STOREDPROCEDURES,
        USAGE,
        SESSIONS,
        REQUESTS,
        TRANSACTIONS
    }

    private enum SystemAdminProcs {
        SETTABLESTATS,
        SETCOLUMNSTATS,
        SETPROPERTY,
        LOGMSG,
        ISLOGGABLE,
        CANCELREQUEST,
        TERMINATESESSION,
        TERMINATETRANSACTION,
        SCHEMASOURCES
    }

    private enum SystemProcs {
        ARRAYITERATE
    }

    private static final TreeMap<String, Integer> levelMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
    static {
        levelMap.put("OFF", MessageLevel.NONE); //$NON-NLS-1$
        levelMap.put("FATAL", MessageLevel.CRITICAL); //$NON-NLS-1$
        levelMap.put("ERROR", MessageLevel.ERROR); //$NON-NLS-1$
        levelMap.put("WARN", MessageLevel.WARNING); //$NON-NLS-1$
        levelMap.put("INFO", MessageLevel.INFO); //$NON-NLS-1$
        levelMap.put("DEBUG", MessageLevel.DETAIL); //$NON-NLS-1$
        levelMap.put("TRACE", MessageLevel.TRACE); //$NON-NLS-1$
    }

    public static int getLevel(String level) throws TeiidProcessingException {
        Integer intLevel = levelMap.get(level);
        if (intLevel == null) {
             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30546, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30546, level, levelMap.keySet()));
        }
        return intLevel;
    }

    // Resources
    DQPCore requestMgr;
    private BufferManager bufferManager;
    private EventDistributor eventDistributor;
    private boolean detectChangeEvents;

    private Map<SystemTables, BaseExtractionTable<?>> systemTables = new HashMap<SystemTables, BaseExtractionTable<?>>();
    private Map<SystemAdminTables, BaseExtractionTable<?>> systemAdminTables = new HashMap<SystemAdminTables, BaseExtractionTable<?>>();

    private static TreeMap<String, List<String>> PREFIX_MAP = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
    static {
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.STRING, Arrays.asList("'", "'")); //$NON-NLS-1$ //$NON-NLS-2$
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.CHAR, Arrays.asList("'", "'")); //$NON-NLS-1$ //$NON-NLS-2$
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.VARBINARY, Arrays.asList("'X", "'")); //$NON-NLS-1$ //$NON-NLS-2$
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.DATE, Arrays.asList("{'d", "'}")); //$NON-NLS-1$ //$NON-NLS-2$
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.TIME, Arrays.asList("{'t", "'}")); //$NON-NLS-1$ //$NON-NLS-2$
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, Arrays.asList("{'ts", "'}")); //$NON-NLS-1$ //$NON-NLS-2$
        PREFIX_MAP.put(DataTypeManager.DefaultDataTypes.BOOLEAN, Arrays.asList("{'b", "'}")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public DataTierManagerImpl(DQPCore requestMgr, BufferManager bufferMgr, boolean detectChangeEvents) {
        this.requestMgr = requestMgr;
        this.bufferManager = bufferMgr;
        this.detectChangeEvents = detectChangeEvents;
        MetadataStore ms = SystemMetadata.getInstance().getSystemStore();
        TransformationMetadata tm = new TransformationMetadata(null, new CompositeMetadataStore(ms), null, null, null);
        String name = SystemTables.SCHEMAS.name();
        List<ElementSymbol> columns = getColumns(tm, name);
        systemTables.put(SystemTables.SCHEMAS, new RecordExtractionTable<Schema>(new SchemaRecordTable(1, columns), columns) {

            @Override
            public void fillRow(List<Object> row, Schema model,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Schema> iter) {
                row.add(vdb.getName());
                row.add(model.getName());
                row.add(model.isPhysical());
                row.add(model.getUUID());
                row.add(model.getAnnotation());
                row.add(model.getPrimaryMetamodelUri());
            }
        });
        name = SystemTables.TABLES.name();
        columns = getColumns(tm, CoreConstants.SYSTEM_MODEL + "." +name); //$NON-NLS-1$
        systemTables.put(SystemTables.TABLES, new RecordExtractionTable<Table>(new TableSystemTable(1, 2, columns), columns) {

            @Override
            public void fillRow(List<Object> row, Table table,
                    VDBMetaData v, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Table> iter) {
                row.add(v.getName());
                row.add(table.getParent().getName());
                row.add(table.getName());
                row.add(table.getTableType().toString());
                row.add(table.getNameInSource());
                row.add(table.isPhysical());
                row.add(table.supportsUpdate());
                row.add(table.getUUID());
                row.add(table.getCardinality());
                row.add(table.getAnnotation());
                row.add(table.isSystem());
                row.add(table.isMaterialized());
                row.add(table.getParent().getUUID());
            }
        });
        name = SystemAdminTables.MATVIEWS.name();
        columns = getColumns(tm, name);
        systemAdminTables.put(SystemAdminTables.MATVIEWS, new RecordExtractionTable<Table>(new TableSystemTable(1, 2, columns) {
            @Override
            protected boolean isValid(Table s, VDBMetaData vdb,
                    List<Object> rowBuffer, Criteria condition, CommandContext cc)
                    throws TeiidProcessingException, TeiidComponentException {
                if (s == null || !s.isMaterialized()) {
                    return false;
                }
                return super.isValid(s, vdb, rowBuffer, condition, cc);
            }
        }, columns) {

            @Override
            public void fillRow(List<Object> row, Table table,
                    VDBMetaData v, TransformationMetadata m, CommandContext cc, SimpleIterator<Table> iter) {
                String targetSchema = null;
                String matTableName = null;
                String state = null;
                Timestamp updated = null;
                Integer cardinaltity = null;
                Boolean valid = null;
                if (table.getMaterializedTable() == null) {
                    GlobalTableStore globalStore = cc.getGlobalTableStore();
                    matTableName = RelationalPlanner.MAT_PREFIX+table.getFullName().toUpperCase();
                    TempMetadataID id = globalStore.getGlobalTempTableMetadataId(matTableName);
                    MatTableInfo info = globalStore.getMatTableInfo(matTableName);
                    valid = info.isValid();
                    state = info.getState().name();
                    updated = info.getUpdateTime()==-1?null:new Timestamp(info.getUpdateTime());
                    if (id != null) {
                        cardinaltity = (int)Math.min(Integer.MAX_VALUE, id.getCardinality());
                    }
                    //ttl, pref_mem - not part of proper metadata
                } else {
                    Table t = table.getMaterializedTable();
                    matTableName = t.getName();
                    targetSchema = t.getParent().getName();
                }
                row.add(v.getName());
                row.add(table.getParent().getName());
                row.add(table.getName());
                row.add(targetSchema);
                row.add(matTableName);
                row.add(valid);
                row.add(state);
                row.add(updated);
                row.add(cardinaltity);
            }
        });
        name = SystemAdminTables.VDBRESOURCES.name();
        columns = getColumns(tm, name);
        systemAdminTables.put(SystemAdminTables.VDBRESOURCES, new BaseExtractionTable<String>(columns) {

            @Override
            public SimpleIterator<String> createIterator(VDBMetaData vdb,
                    TransformationMetadata metadata, CommandContext cc) throws QueryMetadataException, TeiidComponentException {
                String[] vals = metadata.getVDBResourcePaths();
                return new SimpleIteratorWrapper<String>(Arrays.asList(vals).iterator());
            }

            @Override
            public void fillRow(List<Object> row, String filePath,
                    VDBMetaData v, TransformationMetadata m, CommandContext cc, SimpleIterator<String> iter) {
                row.add(filePath);
                row.add(new BlobType(m.getVDBResourceAsBlob(filePath)));
            }
        });
        name = SystemTables.PROCEDURES.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.PROCEDURES, new RecordExtractionTable<Procedure>(new ProcedureSystemTable(1, 2, columns), columns) {

            @Override
            public void fillRow(List<Object> row, Procedure proc,
                    VDBMetaData v, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Procedure> iter) {
                row.add(v.getName());
                row.add(proc.getParent().getName());
                row.add(proc.getName());
                row.add(proc.getNameInSource());
                row.add(proc.getResultSet() != null);
                row.add(proc.getUUID());
                row.add(proc.getAnnotation());
                row.add(proc.getParent().getUUID());
            }
        });
        name = SystemTables.FUNCTIONS.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.FUNCTIONS, new RecordExtractionTable<FunctionMethod>(new FunctionSystemTable(1, 4, columns), columns) {

            @Override
            public void fillRow(List<Object> row, FunctionMethod proc,
                    VDBMetaData v, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<FunctionMethod> iter) {
                row.add(v.getName());
                if (proc.getParent() != null) {
                    row.add(proc.getParent().getName());
                } else {
                    row.add(CoreConstants.SYSTEM_MODEL);
                }
                row.add(proc.getName());
                row.add(proc.getNameInSource());
                row.add(proc.getUUID());
                row.add(proc.getAnnotation());
                row.add(proc.isVarArgs());
                if (proc.getParent() != null) {
                    row.add(proc.getParent().getUUID());
                } else {
                    String systemUid = null;
                    try {
                        //TODO make this more available
                        systemUid = metadata.getModelID(CoreConstants.SYSTEM_MODEL).getUUID();
                    } catch (TeiidComponentException e) {
                    }
                    row.add(systemUid);
                }
            }
        });
        name = SystemTables.DATATYPES.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.DATATYPES, new RecordExtractionTable<Datatype>(new RecordTable<Datatype>(new int[] {0}, columns.subList(0, 1)) {

            @Override
            public SimpleIterator<Datatype> processQuery(VDBMetaData vdb,
                    CompositeMetadataStore metadataStore, BaseIndexInfo<?> ii, TransformationMetadata metadata, CommandContext commandContext) {
                return processQuery(vdb, metadataStore.getDatatypesExcludingAliases(), ii, commandContext);
            }
        }, columns) {

            @Override
            public void fillRow(List<Object> row, Datatype datatype,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Datatype> iter) {
                row.add(datatype.getName());
                row.add(datatype.isBuiltin());
                row.add(datatype.getType().name());
                row.add(datatype.getName());
                row.add(datatype.getJavaClassName());
                row.add(datatype.getScale());
                row.add(datatype.getLength());
                row.add(datatype.getNullType().toString());
                row.add(datatype.isSigned());
                row.add(datatype.isAutoIncrement());
                row.add(datatype.isCaseSensitive());
                Integer precision = datatype.getPrecision();
                if (datatype.isBuiltin() && !Number.class.isAssignableFrom(DataTypeManager.getDataTypeClass(datatype.getRuntimeTypeName()))) {
                    precision = JDBCSQLTypeInfo.getDefaultPrecision(datatype.getName());
                } else if (precision != null && precision == 0) {
                    precision = JDBCSQLTypeInfo.getDefaultPrecision(datatype.getRuntimeTypeName());
                }
                row.add(precision);
                 row.add(datatype.getRadix());
                 row.add(datatype.getSearchType().toString());
                 row.add(datatype.getUUID());
                 row.add(datatype.getRuntimeTypeName());
                 row.add(datatype.getBasetypeName());
                 row.add(datatype.getAnnotation());
                 row.add(JDBCSQLTypeInfo.getSQLType(datatype.getRuntimeTypeName()));
                 List<String> prefix = PREFIX_MAP.get(datatype.getRuntimeTypeName());
                 if (prefix != null) {
                     row.add(prefix.get(0));
                     row.add(prefix.get(1));
                 } else {
                     row.add(null);
                    row.add(null);
                 }
            }
        });
        name = SystemTables.VIRTUALDATABASES.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.VIRTUALDATABASES, new BaseExtractionTable<VDBMetaData>(columns) {
            @Override
            public SimpleIterator<VDBMetaData> createIterator(VDBMetaData vdb,
                    TransformationMetadata metadata, CommandContext cc)
                    throws QueryMetadataException, TeiidComponentException {
                return new SimpleIteratorWrapper<VDBMetaData>(Arrays.asList(vdb).iterator());
            }
            @Override
            public void fillRow(List<Object> row, VDBMetaData record,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<VDBMetaData> iter) {
                VDBKey key = new VDBKey(record.getName(), 0);
                row.add(key.getName());
                row.add(record.getVersion());
                row.add(record.getDescription());
                row.add(record.getStatusTimestamp(Status.LOADING));
                row.add(record.getStatusTimestamp(Status.ACTIVE));
            }
        });
        name = SystemTables.PROCEDUREPARAMS.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.PROCEDUREPARAMS, new ChildRecordExtractionTable<Procedure, BaseColumn>(new ProcedureSystemTable(1, 2, columns), columns) {
            @Override
            public void fillRow(List<Object> row, BaseColumn param,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<BaseColumn> iter) {
                Datatype dt = param.getDatatype();
                row.add(vdb.getName());
                String type = "ResultSet"; //$NON-NLS-1$
                AbstractMetadataRecord proc = param.getParent();
                boolean isOptional = false;
                int pos = param.getPosition();
                if (param instanceof ProcedureParameter) {
                    ProcedureParameter pp = (ProcedureParameter)param;
                    type = pp.getType().name();
                    isOptional = pp.isOptional();
                    if (((Procedure)proc).getParameters().get(0).getType() == ProcedureParameter.Type.ReturnValue) {
                        pos--;
                    }
                } else {
                    Column pp = (Column)param;
                    proc = param.getParent().getParent();
                }
                row.add(proc.getParent().getName());
                row.add(proc.getName());
                row.add(proc.getUUID());
                row.add(param.getName());
                row.add(param.getRuntimeType());
                row.add(pos);
                row.add(type);
                row.add(isOptional);
                row.add(param.getPrecision());
                row.add(param.getLength());
                row.add(param.getScale());
                row.add(param.getRadix());
                row.add(param.getNullType().toString());
                row.add(param.getUUID());
                row.add(param.getAnnotation());
                addTypeInfo(row, param, dt);
                row.add(param.getDefaultValue());
            }

            @Override
            protected Collection<? extends BaseColumn> getChildren(final Procedure parent, CommandContext cc) {
                Collection<ProcedureParameter> params = parent.getParameters();
                if (parent.getResultSet() == null) {
                    return params;
                }
                //TODO: don't incur the gc cost of the temp list
                Collection<Column> rsColumns = parent.getResultSet().getColumns();
                ArrayList<BaseColumn> result = new ArrayList<BaseColumn>(params.size() + rsColumns.size());
                result.addAll(params);
                result.addAll(rsColumns);
                return result;
            }

        });
        name = SystemTables.FUNCTIONPARAMS.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.FUNCTIONPARAMS, new ChildRecordExtractionTable<FunctionMethod, FunctionParameter>(new FunctionSystemTable(1, 3, columns), columns) {
            @Override
            public void fillRow(List<Object> row, FunctionParameter param,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<FunctionParameter> iter) {
                row.add(vdb.getName());
                FunctionMethod parent = ((ExpandingSimpleIterator<FunctionMethod, FunctionParameter>)iter).getCurrentParent();
                if (parent.getParent() == null) {
                    row.add(CoreConstants.SYSTEM_MODEL);
                } else {
                    row.add(parent.getParent().getName());
                }
                row.add(parent.getName());
                row.add(parent.getUUID());
                row.add(param.getName());
                row.add(param.getRuntimeType());
                row.add(param.getPosition());
                row.add(param.getPosition()==0?"ReturnValue":"In"); //$NON-NLS-1$ //$NON-NLS-2$
                row.add(param.getPrecision());
                row.add(param.getLength());
                row.add(param.getScale());
                row.add(param.getRadix());
                row.add(param.getNullType().toString());
                row.add(param.getUUID());
                row.add(param.getAnnotation());
                addTypeInfo(row, param, param.getDatatype());
            }

            @Override
            protected Collection<? extends FunctionParameter> getChildren(final FunctionMethod parent, CommandContext cc) {
                ArrayList<FunctionParameter> result = new ArrayList<FunctionParameter>(parent.getInputParameters().size() + 1);
                result.addAll(parent.getInputParameters());
                result.add(parent.getOutputParameter());
                return result;
            }

        });
        name = SystemTables.PROPERTIES.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.PROPERTIES, new ChildRecordExtractionTable<AbstractMetadataRecord, Map.Entry<String, String>>(
                new RecordTable<AbstractMetadataRecord>(new int[] {0}, columns.subList(2, 3)) {
                    @Override
                    protected void fillRow(AbstractMetadataRecord s,
                            List<Object> rowBuffer) {
                        rowBuffer.add(s.getUUID());
                    }

                    @Override
                    public SimpleIterator<AbstractMetadataRecord> processQuery(
                            VDBMetaData vdb, CompositeMetadataStore metadataStore,
                            BaseIndexInfo<?> ii, TransformationMetadata metadata, CommandContext commandContext) {
                        return processQuery(vdb, metadataStore.getOids(), ii, commandContext);
                    }

                    @Override
                    protected AbstractMetadataRecord extractRecord(Object val) {
                        if (val != null) {
                            return ((RecordHolder)val).getRecord();
                        }
                        return null;
                    }
                }, columns) {

            @Override
            public void fillRow(List<Object> row, Map.Entry<String,String> entry, VDBMetaData vdb, TransformationMetadata metadata, CommandContext cc, SimpleIterator<Map.Entry<String, String>> iter) {
                String value = entry.getValue();
                Clob clobValue = null;
                if (value != null) {
                    clobValue = new ClobType(new ClobImpl(value));
                }
                row.add(entry.getKey());
                row.add(entry.getValue());
                row.add(((ExpandingSimpleIterator<AbstractMetadataRecord, Entry<String, String>>)iter).getCurrentParent().getUUID());
                row.add(clobValue);
            }

            @Override
            protected Collection<Map.Entry<String,String>> getChildren(AbstractMetadataRecord parent, CommandContext cc) {
                Map<String, String> props = parent.getProperties();
                        return props.entrySet().stream().flatMap((e) -> {
                            String key = e.getKey();
                            String legacyKey = NamespaceContainer.getLegacyKey(key);
                            if (legacyKey != null && !props.containsKey(legacyKey)) {
                                return Stream.of(e,
                                        new AbstractMap.SimpleEntry<String, String>(
                                                legacyKey, e.getValue()));
                            }
                            return Stream.of(e);
                        }).collect(Collectors.toList());
            }
        });
        name = SystemAdminTables.TRIGGERS.name();
        columns = getColumns(tm, name);
        systemAdminTables.put(SystemAdminTables.TRIGGERS, new ChildRecordExtractionTable<Table, Trigger>(new TableSystemTable(1, 2, columns), columns) {
            @Override
            protected void fillRow(List<Object> row, Trigger record, VDBMetaData vdb, TransformationMetadata metadata, CommandContext cc, SimpleIterator<Trigger> iter) {
                Clob clobValue = null;
                if (record.body != null) {
                    clobValue = new ClobType(new ClobImpl(record.body));
                }
                AbstractMetadataRecord table = ((ExpandingSimpleIterator<AbstractMetadataRecord, Trigger>)iter).getCurrentParent();
                row.add(vdb.getName());
                row.add(table.getParent().getName());
                row.add(table.getName());
                row.add(record.name);
                row.add(record.triggerType);
                row.add(record.triggerEvent);
                row.add(record.status);
                row.add(clobValue);
                row.add(table.getUUID());
            }

            @Override
            protected Collection<Trigger> getChildren(Table table, CommandContext cc) {
                ArrayList<Trigger> cols = new ArrayList<Trigger>();
                if (table .isVirtual()) {
                    if (table.getInsertPlan() != null) {
                        cols.add(new Trigger("it", TriggerEvent.INSERT.name(), table.isInsertPlanEnabled(), null, table.getInsertPlan())); //$NON-NLS-1$
                    }
                    if (table.getUpdatePlan() != null) {
                        cols.add(new Trigger("ut", TriggerEvent.UPDATE.name(), table.isUpdatePlanEnabled(), null, table.getUpdatePlan())); //$NON-NLS-1$
                    }
                    if (table.getDeletePlan() != null) {
                        cols.add(new Trigger("dt", TriggerEvent.DELETE.name(), table.isDeletePlanEnabled(), null, table.getDeletePlan())); //$NON-NLS-1$
                    }
                } else {
                    for (org.teiid.metadata.Trigger trigger : table.getTriggers().values()) {
                        cols.add(new Trigger(trigger.getName(), trigger.getEvent().name(), true, SQLConstants.NonReserved.AFTER, trigger.getPlan()));
                    }
                }
                return cols;
            }
        });
        name = SystemAdminTables.VIEWS.name();
        columns = getColumns(tm, CoreConstants.SYSTEM_ADMIN_MODEL + "." +name); //$NON-NLS-1$
        systemAdminTables.put(SystemAdminTables.VIEWS, new RecordExtractionTable<Table>(new TableSystemTable(1, 2, columns) {
            @Override
            protected boolean isValid(Table s, VDBMetaData vdb,
                    List<Object> rowBuffer, Criteria condition, CommandContext cc)
                    throws TeiidProcessingException, TeiidComponentException {
                if (s == null || !s.isVirtual()) {
                    return false;
                }
                return super.isValid(s, vdb, rowBuffer, condition, cc);
            }
        }, columns) {

            @Override
            public void fillRow(List<Object> row, Table table,
                    VDBMetaData v, TransformationMetadata m, CommandContext cc, SimpleIterator<Table> iter) {
                row.add(v.getName());
                row.add(table.getParent().getName());
                row.add(table.getName());
                row.add(new ClobType(new ClobImpl(table.getSelectTransformation())));
                row.add(table.getUUID());
            }
        });
        addSessionsTable(tm);
        addRequestsTable(tm);
        addTransactionsTable(tm);
        name = SystemAdminTables.STOREDPROCEDURES.name();
        columns = getColumns(tm, name);
        systemAdminTables.put(SystemAdminTables.STOREDPROCEDURES, new RecordExtractionTable<Procedure>(new ProcedureSystemTable(1, 2, columns) {
            @Override
            protected boolean isValid(Procedure s, VDBMetaData vdb,
                    List<Object> rowBuffer, Criteria condition, CommandContext cc)
                    throws TeiidProcessingException, TeiidComponentException {
                if (s == null || !s.isVirtual()) {
                    return false;
                }
                return super.isValid(s, vdb, rowBuffer, condition, cc);
            }
        }, columns) {

            @Override
            public void fillRow(List<Object> row, Procedure proc,
                    VDBMetaData v, TransformationMetadata m, CommandContext cc,
                    SimpleIterator<Procedure> iter) {
                row.add(v.getName());
                row.add(proc.getParent().getName());
                row.add(proc.getName());
                row.add(new ClobType(new ClobImpl(proc.getQueryPlan())));
                row.add(proc.getUUID());
            }
        });
        name = SystemTables.COLUMNS.name();
        columns = getColumns(tm, CoreConstants.SYSTEM_MODEL + "." +name); //$NON-NLS-1$
        systemTables.put(SystemTables.COLUMNS, new ChildRecordExtractionTable<Table, Column>(new TableSystemTable(1, 2, columns), columns) {
            @Override
            protected void fillRow(List<Object> row, Column column,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Column> iter) {
                Datatype dt = column.getDatatype();
                row.add(vdb.getName());
                row.add(column.getParent().getParent().getName());
                row.add(column.getParent().getName());
                row.add(column.getName());
                row.add(column.getPosition());
                row.add(column.getNameInSource());
                row.add(column.getRuntimeType());
                row.add(column.getScale());
                row.add(column.getLength());
                row.add(column.isFixedLength());
                row.add(column.isSelectable());
                row.add(column.isUpdatable());
                row.add(column.isCaseSensitive());
                row.add(column.isSigned());
                row.add(column.isCurrency());
                row.add(column.isAutoIncremented());
                row.add(column.getNullType().toString());
                row.add(column.getMinimumValue());
                row.add(column.getMaximumValue());
                row.add(column.getDistinctValues());
                row.add(column.getNullValues());
                row.add(column.getSearchType().toString());
                row.add(column.getFormat());
                row.add(column.getDefaultValue());
                row.add(column.getJavaType().getName());
                row.add(column.getPrecision());
                row.add(column.getCharOctetLength());
                row.add(column.getRadix());
                row.add(column.getUUID());
                row.add(column.getAnnotation());
                row.add(column.getParent().getUUID());
                addTypeInfo(row, column, dt);
            }

            @Override
            protected Collection<Column> getChildren(Table parent, CommandContext cc) {
                return parent.getColumns();
            }

        });
        name = SystemTables.KEYS.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.KEYS, new ChildRecordExtractionTable<Table, KeyRecord>(new TableSystemTable(1, 2, columns), columns) {
            @Override
            protected void fillRow(List<Object> row, KeyRecord key,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<KeyRecord> iter) {
                row.add(vdb.getName());
                row.add(key.getParent().getParent().getName());
                row.add(key.getParent().getName());
                row.add(key.getName());
                row.add(key.getAnnotation());
                row.add(key.getNameInSource());
                row.add(key.getType().toString());
                row.add(false);
                row.add((key instanceof ForeignKey)?((ForeignKey)key).getUniqueKeyID():null);
                row.add(key.getUUID());
                row.add(key.getParent().getUUID());
                row.add(key.getParent().getParent().getUUID());
                row.add(null);
                row.add(null);
                if (key instanceof ForeignKey) {
                    KeyRecord ref = ((ForeignKey)key).getReferenceKey();
                    if (ref != null) {
                        row.set(row.size() - 2, ref.getParent().getUUID());
                        row.set(row.size() - 1, ref.getParent().getParent().getUUID());
                    }
                }
                List<Column> columns2 = key.getColumns();
                Short[] pos = new Short[columns2.size()];
                String[] names = new String[columns2.size()];
                for (int i = 0; i < pos.length; i++) {
                    pos[i] = (short)columns2.get(i).getPosition();
                    names[i] = columns2.get(i).getName();
                }
                row.add(new ArrayImpl((Object[])pos));
                row.add(new ArrayImpl((Object[])names));
            }

            @Override
            protected Collection<KeyRecord> getChildren(Table parent, CommandContext cc) {
                return parent.getAllKeys();
            }

            @Override
            protected boolean isValid(KeyRecord result, CommandContext cc) {
                if (!super.isValid(result, cc)) {
                    return false;
                }
                return isKeyVisible(result, cc);
            }

        });
        name = SystemTables.KEYCOLUMNS.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.KEYCOLUMNS, new ChildRecordExtractionTable<Table, List<?>>(new TableSystemTable(1, 2, columns), columns) {

            @Override
            protected void fillRow(List<Object> row, List<?> record,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<List<?>> iter) {
                row.add(vdb.getName());
                KeyRecord key = (KeyRecord) record.get(0);
                Column column = (Column) record.get(1);
                Integer pos = (Integer) record.get(2);
                row.add(key.getParent().getParent().getName());
                row.add(key.getParent().getName());
                row.add(column.getName());
                row.add(key.getName());
                row.add(key.getType().toString());
                row.add((key instanceof ForeignKey)?((ForeignKey)key).getUniqueKeyID():null);
                row.add(key.getUUID());
                row.add(pos);
                row.add(key.getParent().getUUID());
            }

            @Override
            protected Collection<List<?>> getChildren(Table parent, CommandContext cc) {
                ArrayList<List<?>> cols = new ArrayList<List<?>>();

                for (KeyRecord record : parent.getAllKeys()) {
                    if (!cc.getDQPWorkContext().isAdmin() && !isKeyVisible(record, cc)) {
                        continue;
                    }
                    int i = 1;
                    for (Column col : record.getColumns()) {
                        cols.add(Arrays.asList(record, col, i++));
                    }
                }
                return cols;
            }
        });
        //we key the referencekeycolumns by fk
        name = SystemTables.REFERENCEKEYCOLUMNS.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.REFERENCEKEYCOLUMNS, new ChildRecordExtractionTable<Table, List<?>>(new TableSystemTable(5, 6, columns), columns) {

            @Override
            protected void fillRow(List<Object> row, List<?> record,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<List<?>> iter) {
                row.add(vdb.getName());
                ForeignKey key = (ForeignKey) record.get(0);
                Table pkTable = key.getReferenceKey().getParent();
                Column column = (Column) record.get(1);
                Short pos = (Short) record.get(2);
                row.add(pkTable.getParent().getName());
                row.add(pkTable.getName());
                row.add(key.getReferenceKey().getColumns().get(pos-1).getName());
                row.add(vdb.getName());
                row.add(key.getParent().getParent().getName());
                row.add(key.getParent().getName());
                row.add(column.getName());
                row.add(pos);
                row.add(DatabaseMetaData.importedKeyNoAction);
                row.add(DatabaseMetaData.importedKeyNoAction);
                row.add(key.getName());
                row.add(key.getReferenceKey().getName());
                row.add(DatabaseMetaData.importedKeyInitiallyDeferred);
                row.add(key.getUUID());
            }

            @Override
            protected Collection<List<?>> getChildren(Table parent, CommandContext cc) {
                ArrayList<List<?>> cols = new ArrayList<List<?>>();

                for (KeyRecord record : parent.getForeignKeys()) {
                    if (!cc.getDQPWorkContext().isAdmin() && !isKeyVisible(record, cc)) {
                        continue;
                    }
                    short i = 1;
                    for (Column col : record.getColumns()) {
                        cols.add(Arrays.asList(record, col, i++));
                    }
                }
                return cols;
            }
        });
        name = SystemAdminTables.USAGE.name();
        columns = getColumns(tm, name);
        systemAdminTables.put(SystemAdminTables.USAGE, new ChildRecordExtractionTable<AbstractMetadataRecord, AbstractMetadataRecord>(
                new RecordTable<AbstractMetadataRecord>(new int[] {0}, columns.subList(1, 2)) {
                    @Override
                    protected void fillRow(AbstractMetadataRecord s,
                            List<Object> rowBuffer) {
                        rowBuffer.add(s.getUUID());
                    }

                    @Override
                    public SimpleIterator<AbstractMetadataRecord> processQuery(
                            VDBMetaData vdb, CompositeMetadataStore metadataStore,
                            BaseIndexInfo<?> ii, TransformationMetadata metadata, CommandContext commandContext) {
                        return processQuery(vdb, metadataStore.getOids(), ii, commandContext);
                    }

                    @Override
                    protected AbstractMetadataRecord extractRecord(Object val) {
                        if (val != null) {
                            return ((RecordHolder)val).getRecord();
                        }
                        return null;
                    }
                }, columns) {

            @Override
            public void fillRow(List<Object> row, AbstractMetadataRecord entry, VDBMetaData vdb, TransformationMetadata metadata, CommandContext cc, SimpleIterator<AbstractMetadataRecord> iter) {
                AbstractMetadataRecord currentParent = ((ExpandingSimpleIterator<AbstractMetadataRecord, AbstractMetadataRecord>)iter).getCurrentParent();
                row.add(vdb.getName());
                row.add(currentParent.getUUID());
                row.add(getType(currentParent));
                addNames(row, currentParent);
                row.add(entry.getUUID());
                row.add(getType(entry));
                addNames(row, entry);
            }

            private void addNames(List<Object> row,
                    AbstractMetadataRecord record) {
                if (record instanceof Column) {
                    if (record.getParent().getParent() instanceof Procedure) {
                        //parameter, skip the result set parent
                        row.add(record.getParent().getParent().getParent().getName());
                        row.add(record.getParent().getParent().getName());
                    } else {
                        row.add(record.getParent().getParent().getName());
                        row.add(record.getParent().getName());
                    }
                    row.add(record.getName());
                } else {
                    row.add(record.getParent().getName());
                    row.add(record.getName());
                    row.add(null);
                }
            }

            private String getType(AbstractMetadataRecord record) {
                if (record instanceof Table) {
                    Table t = (Table)record;
                    if (t.getTableType() == Type.Table && t.isVirtual()) {
                        //TODO: this change should be on the Table object as well
                        return "View"; //$NON-NLS-1$
                    }
                    return t.getTableType().name();
                }
                if (record instanceof Procedure) {
                    Procedure p = (Procedure)record;
                    if (p.isFunction()) {
                        return p.getType().name();
                    }
                    if (p.isVirtual()) {
                        return "StoredProcedure"; //$NON-NLS-1$
                    }
                    return "ForeignProcedure"; //$NON-NLS-1$
                }
                return record.getClass().getSimpleName();
            }

            @Override
            protected Collection<AbstractMetadataRecord> getChildren(AbstractMetadataRecord parent, CommandContext cc) {
                return parent.getIncomingObjects();
            }
        });
    }

    private void addSessionsTable(TransformationMetadata tm) {
        String name = SystemAdminTables.SESSIONS.name();
        List<ElementSymbol> columns = getColumns(tm, CoreConstants.SYSTEM_ADMIN_MODEL + "." +name); //$NON-NLS-1$
        systemAdminTables.put(SystemAdminTables.SESSIONS, new BaseExtractionTable<Session>(columns) {
            @Override
            public SimpleIterator<Session> createIterator(VDBMetaData vdb,
                    TransformationMetadata metadata, CommandContext cc)
                    throws QueryMetadataException, TeiidComponentException {
                return new SimpleIteratorWrapper<Session>(cc.getWorkItem()
                        .getDqpCore().getSessionService()
                        .getActiveSessions().iterator()) {
                    @Override
                    protected boolean isValid(Session result) {
                        return result.getVDBName().equals(vdb.getName())
                                && result.getVDBVersion().equals(vdb.getVersion());
                    }
                };
            }
            @Override
            protected void fillRow(List<Object> row, Session record,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Session> iter) {
                row.add(vdb.getName());
                row.add(record.getSessionId());
                row.add(record.getUserName());
                row.add(new Timestamp(record.getCreatedTime()));
                row.add(record.getApplicationName());
                row.add(record.getIPAddress());
            }
        });
    }

    private void addRequestsTable(TransformationMetadata tm) {
        String name = SystemAdminTables.REQUESTS.name();
        List<ElementSymbol> columns = getColumns(tm, CoreConstants.SYSTEM_ADMIN_MODEL + "." +name); //$NON-NLS-1$
        systemAdminTables.put(SystemAdminTables.REQUESTS, new BaseExtractionTable<Request>(columns) {
            @Override
            public SimpleIterator<Request> createIterator(VDBMetaData vdb,
                    TransformationMetadata metadata, CommandContext cc)
                    throws QueryMetadataException, TeiidComponentException {
                return new ExpandingSimpleIterator<Session, Request>(
                    new SimpleIteratorWrapper<Session>(cc
                            .getWorkItem().getDqpCore()
                            .getSessionService().getActiveSessions()
                            .iterator()) {
                        @Override
                        protected boolean isValid(Session result) {
                            return result.getVDBName().equals(vdb.getName())
                                    && result.getVDBVersion().equals(vdb.getVersion());
                        }
                    })
                {
                    @Override
                    protected SimpleIterator<Request> getChildIterator(
                            Session parent) {
                        return new SimpleIteratorWrapper<Request>(
                                cc.getWorkItem().getDqpCore()
                                        .getRequestsForSession(
                                                parent.getSessionId())
                                        .iterator());
                    }
                };
            }

            @Override
            protected void fillRow(List<Object> row, Request record,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Request> iter) {
                row.add(vdb.getName());
                row.add(record.getSessionId());
                row.add(record.getExecutionId());
                row.add(new ClobType(new ClobImpl(record.getCommand())));
                row.add(new Timestamp(record.getStartTime()));
                row.add(record.getTransactionId());
                row.add(record.getState().name());
                row.add(record.getThreadState().name());
                row.add(record.isSourceRequest());
            }
        });
    }

    private void addTransactionsTable(TransformationMetadata tm) {
        String name = SystemAdminTables.TRANSACTIONS.name();
        List<ElementSymbol> columns = getColumns(tm, CoreConstants.SYSTEM_ADMIN_MODEL + "." +name); //$NON-NLS-1$
        systemAdminTables.put(SystemAdminTables.TRANSACTIONS, new BaseExtractionTable<Transaction>(columns) {
            @Override
            public SimpleIterator<Transaction> createIterator(VDBMetaData vdb,
                    TransformationMetadata metadata, CommandContext cc)
                    throws QueryMetadataException, TeiidComponentException {
                return new SimpleIteratorWrapper<Transaction>(cc.getWorkItem()
                        .getDqpCore().getTransactions().iterator()) {
                    @Override
                    protected boolean isValid(Transaction result) {
                        if (result.getAssociatedSession() == null) {
                            return true;
                        }
                        Session s = cc.getWorkItem().getDqpCore().getSessionService().getActiveSession(result.getAssociatedSession());
                        if (s == null) {
                            return false;
                        }
                        return s.getVDBName().equals(vdb.getName())
                                && s.getVDBVersion().equals(vdb.getVersion());
                    }
                };
            }

            @Override
            protected void fillRow(List<Object> row, Transaction record,
                    VDBMetaData vdb, TransformationMetadata metadata,
                    CommandContext cc, SimpleIterator<Transaction> iter) {
                row.add(record.getId());
                row.add(record.getAssociatedSession());
                row.add(new Timestamp(record.getCreatedTime()));
                row.add(record.getScope());
            }
        });
    }

    private boolean isKeyVisible(KeyRecord record, CommandContext cc) {
        if (record instanceof ForeignKey && !cc.getAuthorizationValidator().isAccessible(((ForeignKey)record).getReferenceKey(), cc)) {
            return false;
        }
        for (Column c : record.getColumns()) {
            if (!cc.getAuthorizationValidator().isAccessible(c, cc)) {
                return false;
            }
        }
        return true;
    }

    private void addTypeInfo(List<Object> row, BaseColumn column,
            Datatype dt) {
        String typeName = column.getRuntimeType();
        if (dt != null) {
            if (dt.isBuiltin() || dt.getType() == Datatype.Type.Domain) {
                typeName = dt.getName();
            } else {
                //some of the designer UDT types conflict with our type names,
                //so use the runtime type instead
                typeName = dt.getRuntimeTypeName();
            }
            int arrayDimensions = column.getArrayDimensions();
            while (arrayDimensions-- > 0) {
                typeName += "[]"; //$NON-NLS-1$
            }
        }
        row.add(typeName);
        row.add(JDBCSQLTypeInfo.getSQLType(column.getRuntimeType()));
        Integer columnSize = null;
        if (column.getArrayDimensions() == 0) {
            columnSize = column.getPrecision();
            if (columnSize == 0) {
                columnSize = column.getLength();
            }
            if (typeName != null) {
                Class<?> dataTypeClass = DataTypeManager.getDataTypeClass(typeName);
                if (!Number.class.isAssignableFrom(dataTypeClass)) {
                    if (java.util.Date.class.isAssignableFrom(dataTypeClass) || column.getLength() <= 0) {
                        columnSize = JDBCSQLTypeInfo.getDefaultPrecision(column.getRuntimeType());
                    } else {
                        columnSize = column.getLength();
                    }
                } else if (column.getPrecision() <= 0) {
                    columnSize = JDBCSQLTypeInfo.getDefaultPrecision(column.getRuntimeType());
                }
            }
        }
        row.add(columnSize);
    }

    private List<ElementSymbol> getColumns(TransformationMetadata tm,
            String name) {
        GroupSymbol gs = new GroupSymbol(name);
        try {
            ResolverUtil.resolveGroup(gs, tm);
            List<ElementSymbol> columns = ResolverUtil.resolveElementsInGroup(gs, tm);
            return columns;
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    public boolean detectChangeEvents() {
        return detectChangeEvents;
    }

    public void setEventDistributor(EventDistributor eventDistributor) {
        this.eventDistributor = eventDistributor;
    }

    public EventDistributor getEventDistributor() {
        return eventDistributor;
    }

    public TupleSource registerRequest(CommandContext context, Command command, String modelName, final RegisterRequestParameter parameterObject) throws TeiidComponentException, TeiidProcessingException {
        RequestWorkItem workItem = context.getWorkItem();
        Assertion.isNotNull(workItem);
        if(CoreConstants.SYSTEM_MODEL.equals(modelName) || CoreConstants.SYSTEM_ADMIN_MODEL.equals(modelName)) {
            return processSystemQuery(context, command, workItem.getDqpWorkContext());
        }

        AtomicRequestMessage aqr = createRequest(workItem, command, modelName, parameterObject.connectorBindingId, parameterObject.nodeID);
        aqr.setCommandContext(context);
        if (parameterObject.fetchSize > 0) {
            aqr.setFetchSize(2*parameterObject.fetchSize);
        }
        if (parameterObject.limit > 0) {
            aqr.setFetchSize(Math.min(parameterObject.limit, aqr.getFetchSize()));
        }
        aqr.setCopyStreamingLobs(parameterObject.copyStreamingLobs);
        Collection<GroupSymbol> accessedGroups = null;
        if (context.getDataObjects() != null) {
            QueryMetadataInterface metadata = context.getMetadata();
            accessedGroups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, false);
            boolean usedModel = false;
            for (GroupSymbol gs : accessedGroups) {
                context.accessedDataObject(gs.getMetadataID());

                //check the source/tables/procs for determinism level
                Object mid = gs.getMetadataID();
                if (mid instanceof TempMetadataID) {
                    TempMetadataID tid = (TempMetadataID)mid;
                    if (tid.getOriginalMetadataID() != null) {
                        mid = tid.getOriginalMetadataID();
                    }
                }
                String specificProp = metadata.getExtensionProperty(mid, AbstractMetadataRecord.RELATIONAL_PREFIX + DDLConstants.DETERMINISM, false);
                if (specificProp == null) {
                    if (!usedModel) {
                        Object modelId = metadata.getModelID(mid);
                        String prop = metadata.getExtensionProperty(modelId, AbstractMetadataRecord.RELATIONAL_PREFIX + DDLConstants.DETERMINISM, false);

                        if (prop != null) {
                            usedModel = true;
                            //set model property
                            context.setDeterminismLevel(Determinism.valueOf(prop.toUpperCase()));
                        }
                    }
                    continue;
                }
                context.setDeterminismLevel(Determinism.valueOf(specificProp.toUpperCase()));
            }
        }
        ConnectorManagerRepository cmr = workItem.getDqpWorkContext().getVDB().getAttachment(ConnectorManagerRepository.class);
        ConnectorManager connectorManager = cmr.getConnectorManager(aqr.getConnectorName());
        if (connectorManager == null) {
            //can happen if sources are removed
            if (RelationalNodeUtil.hasOutputParams(command)) {
                throw new AssertionError("A source is required to execute a procedure returning parameters"); //$NON-NLS-1$
            }
            LogManager.logDetail(LogConstants.CTX_DQP, "source", aqr.getConnectorName(), "no longer exists, returning dummy results"); //$NON-NLS-1$ //$NON-NLS-2$
            return CollectionTupleSource.createNullTupleSource();
        }
        ConnectorWork work;
        try {
            work = connectorManager.registerRequest(aqr);
        } catch (TranslatorException te) {
            throw new TeiidProcessingException(te);
        }
        if (!work.isForkable()) {
            aqr.setSerial(true);
        }
        CacheID cid = null;
        CacheDirective cd = null;
        if (workItem.getRsCache() != null && command.areResultsCachable()) {
            CachableVisitor cv = new CachableVisitor();
            PreOrPostOrderNavigator.doVisit(command, cv, PreOrPostOrderNavigator.PRE_ORDER, true);
            if (cv.cacheable) {
                try {
                    cd = work.getCacheDirective();
                } catch (TranslatorException e) {
                    throw new TeiidProcessingException(QueryPlugin.Event.TEIID30504, e, aqr.getConnectorName() + ": " + e.getMessage()); //$NON-NLS-1$
                }
                if (cd != null) {
                    if (cd.getScope() == Scope.NONE) {
                        parameterObject.doNotCache = true;
                    } else {
                        String cmdString = command.toString();
                        if (cmdString.length() < 100000) { //TODO: this check won't be needed if keys aren't exclusively held in memory
                            cid = new CacheID(workItem.getDqpWorkContext(), ParseInfo.DEFAULT_INSTANCE, cmdString);
                            cid.setParameters(cv.parameters);
                            if (cd.getInvalidation() == null || cd.getInvalidation() == Invalidation.NONE) {
                                CachedResults cr = workItem.getRsCache().get(cid);
                                if (cr != null && (cr.getRowLimit() == 0 || (parameterObject.limit > 0 && cr.getRowLimit() >= parameterObject.limit))) {
                                    parameterObject.doNotCache = true;
                                    LogManager.logDetail(LogConstants.CTX_DQP, "Using cache entry for", cid); //$NON-NLS-1$
                                    work.close();
                                    return cr.getResults().createIndexedTupleSource();
                                }
                            } else if (cd.getInvalidation() == Invalidation.IMMEDIATE) {
                                workItem.getRsCache().remove(cid, CachingTupleSource.getDeterminismLevel(cd.getScope()));
                            }
                        }
                    }
                } else {
                    LogManager.logTrace(LogConstants.CTX_DQP, aqr.getAtomicRequestID(), "no cache directive"); //$NON-NLS-1$
                }
            } else {
                LogManager.logTrace(LogConstants.CTX_DQP, aqr.getAtomicRequestID(), "command not cachable"); //$NON-NLS-1$
            }
        }
        DataTierTupleSource dtts = new DataTierTupleSource(aqr, workItem, work, this, parameterObject.limit);
        TupleSource result = dtts;
        TupleBuffer tb = null;
        if (cid != null) {
            tb = getBufferManager().createTupleBuffer(aqr.getCommand().getProjectedSymbols(), aqr.getCommandContext().getConnectionId(), TupleSourceType.PROCESSOR);
            result = new CachingTupleSource(this, tb, (DataTierTupleSource)result, cid, parameterObject, cd, accessedGroups, workItem);
        }
        if (work.isThreadBound()) {
            result = handleThreadBound(workItem, aqr, work, cid, result, dtts, tb);
        } else if (!aqr.isSerial()) {
            dtts.addWork();
        }
        return result;
    }

    /**
     * thread bound work is tricky for our execution model
     *
     * the strategy here is that
     *
     * - if the result is not already a copying tuplesource (from caching)
     * then wrap in a copying tuple source
     *
     * - submit a workitem that will pull the results/fill the buffer,
     *
     * - return a tuplesource off of the buffer for use by the caller
     */
    private TupleSource handleThreadBound(final RequestWorkItem workItem,
            AtomicRequestMessage aqr, ConnectorWork work, CacheID cid,
            TupleSource result, DataTierTupleSource dtts, TupleBuffer tb) throws AssertionError,
            TeiidComponentException, TeiidProcessingException {
        if (workItem.useCallingThread) {
            //in any case we want the underlying work done in the thread accessing the connectorworkitem
            aqr.setSerial(true);
            return result; //simple case, just rely on the client using the same thread
        }
        if (tb == null) {
            tb = getBufferManager().createTupleBuffer(aqr.getCommand().getProjectedSymbols(), aqr.getCommandContext().getConnectionId(), TupleSourceType.PROCESSOR);
        }
        final TupleSource ts = tb.createIndexedTupleSource(cid == null);
        if (cid == null) {
            result = new CopyOnReadTupleSource(tb, result) {

                @Override
                public void closeSource() {
                    ts.closeSource();
                }
            };
        }
        final ThreadBoundTask callable = new ThreadBoundTask(workItem, result, dtts);

        //if serial we have to fully perform the operation with the current thread
        //but we do so lazily just in case the results aren't needed
        if (aqr.isSerial()) {
            return new TupleSource() {
                boolean processed = false;
                @Override
                public List<?> nextTuple() throws TeiidComponentException,
                        TeiidProcessingException {
                    if (!processed) {
                        callable.call();
                        callable.onCompletion(null);
                        processed = true;
                    }
                    return ts.nextTuple();
                }

                @Override
                public void closeSource() {
                    if (!processed) {
                        callable.onCompletion(null);
                        processed = true;
                    }
                    ts.closeSource();
                }
            };
        }
        aqr.setSerial(true);
        final FutureWork<Void> future = workItem.addWork(callable, callable, 100);
        final TupleBuffer buffer = tb;
        //return a thread-safe TupleSource
        return new TupleSource() {
            boolean checkedDone;
            @Override
            public List<?> nextTuple() throws TeiidComponentException,
                    TeiidProcessingException {
                //check the future to see if there was an exception to relay
                //TODO: could refactor as completion listener
                if (!checkedDone && future.isDone()) {
                    checkedDone = true;
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        throw new TeiidComponentException(e);
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof TeiidComponentException) {
                            throw (TeiidComponentException)e.getCause();
                        }
                        if (e.getCause() instanceof TeiidProcessingException) {
                            throw (TeiidProcessingException)e.getCause();
                        }
                        throw new TeiidComponentException(e);
                    }
                }
                synchronized (buffer) {
                    return ts.nextTuple();
                }
            }

            @Override
            public void closeSource() {
                synchronized (buffer) {
                    ts.closeSource();
                }
                callable.done.set(true);
            }
        };
    }

    /**
     * @param command
     * @return
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    private TupleSource processSystemQuery(CommandContext context, Command command,
            DQPWorkContext workContext) throws TeiidComponentException, TeiidProcessingException {
        String vdbName = workContext.getVdbName();
        String vdbVersion = workContext.getVdbVersion();
        final VDBMetaData vdb = workContext.getVDB();
        TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
        CompositeMetadataStore metadata = indexMetadata.getMetadataStore();
        if (command instanceof Query) {
            Query query = (Query)command;
            UnaryFromClause ufc = (UnaryFromClause)query.getFrom().getClauses().get(0);
            GroupSymbol group = ufc.getGroup();
            if (StringUtil.startsWithIgnoreCase(group.getNonCorrelationName(), CoreConstants.SYSTEM_ADMIN_MODEL)) {
                final SystemAdminTables sysTable = SystemAdminTables.valueOf(group.getNonCorrelationName().substring(CoreConstants.SYSTEM_ADMIN_MODEL.length() + 1).toUpperCase());
                BaseExtractionTable<?> et = systemAdminTables.get(sysTable);
                return et.processQuery(query, vdb, indexMetadata, context);
            }
            final SystemTables sysTable = SystemTables.valueOf(group.getNonCorrelationName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
            BaseExtractionTable<?> et = systemTables.get(sysTable);
            return et.processQuery(query, vdb, indexMetadata, context);
        }
        Collection<List<?>> rows = new ArrayList<List<?>>();
        StoredProcedure proc = (StoredProcedure)command;
        for (SPParameter param : proc.getInputParameters()) {
            ProcedurePlan.checkNotNull(param.getParameterSymbol(), ((Constant)param.getExpression()).getValue(), context.getMetadata());
        }
        if (StringUtil.startsWithIgnoreCase(proc.getProcedureCallableName(), CoreConstants.SYSTEM_ADMIN_MODEL)) {
            final SystemAdminProcs sysProc = SystemAdminProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_ADMIN_MODEL.length() + 1).toUpperCase());
            switch (sysProc) {
            case TERMINATESESSION:
            {
                String session = (String)((Constant)proc.getParameter(2).getExpression()).getValue();
                boolean terminated = context.getWorkItem().getDqpCore().getSessionService().terminateSession(session, context.getSession().getSessionId());
                rows.add(Arrays.asList(terminated));
                return new CollectionTupleSource(rows.iterator());
            }
            case TERMINATETRANSACTION:
            {
                String transactionId = (String)((Constant)proc.getParameter(1).getExpression()).getValue();
                try {
                    context.getWorkItem().getDqpCore().terminateTransaction(transactionId);
                } catch (AdminException e) {
                    throw new TeiidProcessingException(e);
                }
                return new CollectionTupleSource(rows.iterator());
            }
            case CANCELREQUEST:
            {
                String session = (String)((Constant)proc.getParameter(2).getExpression()).getValue();
                long executionId = (Long)((Constant)proc.getParameter(3).getExpression()).getValue();
                boolean cancelled = context.getWorkItem().getDqpCore().cancelRequest(session, executionId);
                rows.add(Arrays.asList(cancelled));
                return new CollectionTupleSource(rows.iterator());
            }
            case LOGMSG:
            case ISLOGGABLE:
                String level = (String)((Constant)proc.getParameter(2).getExpression()).getValue();
                String logContext = (String)((Constant)proc.getParameter(3).getExpression()).getValue();
                Object message = null;
                if (sysProc == SystemAdminProcs.LOGMSG) {
                    message = ((Constant)proc.getParameter(4).getExpression()).getValue();
                }
                int msgLevel = getLevel(level);
                boolean logged = false;
                if (LogManager.isMessageToBeRecorded(logContext, msgLevel)) {
                    if (message == null) {
                        message = "null"; //$NON-NLS-1$
                    }
                    LogManager.log(msgLevel, logContext, message);
                    logged = true;
                }
                if (proc.returnParameters()) {
                    rows.add(Arrays.asList(logged));
                }
                return new CollectionTupleSource(rows.iterator());
            case SETPROPERTY:
                try {
                    String uuid = (String)((Constant)proc.getParameter(2).getExpression()).getValue();
                    String key = (String)((Constant)proc.getParameter(3).getExpression()).getValue();
                    Clob value = (Clob)((Constant)proc.getParameter(4).getExpression()).getValue();
                    key = MetadataFactory.resolvePropertyKey(key);
                    String strVal = null;
                    String result = null;
                    if (value != null) {
                        if (value.length() > MAX_VALUE_LENGTH) {
                             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30548, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30548, MAX_VALUE_LENGTH));
                        }
                        strVal = ObjectConverterUtil.convertToString(value.getCharacterStream());
                    }
                    final AbstractMetadataRecord target = getByUuid(metadata, uuid);
                    if (target == null) {
                         throw new TeiidProcessingException(QueryPlugin.Event.TEIID30549, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30549, uuid));
                    }
                    AbstractMetadataRecord schema = target;
                    while (!(schema instanceof Schema) && schema.getParent() != null) {
                        schema = schema.getParent();
                    }
                    if (schema instanceof Schema && vdb.getImportedModels().contains(((Schema)schema).getName())) {
                        throw new TeiidProcessingException(QueryPlugin.Event.TEIID31098, QueryPlugin.Util.getString("ValidationVisitor.invalid_alter", uuid)); //$NON-NLS-1$
                    }
                    result = new SetPropertyProcessor(getMetadataRepository(target, vdb), eventDistributor).setProperty(vdb, target, key, strVal);
                    if (proc.returnParameters()) {
                        if (result == null) {
                            rows.add(Arrays.asList((Clob)null));
                        } else {
                            rows.add(Arrays.asList(new ClobType(new ClobImpl(result))));
                        }
                    }
                    return new CollectionTupleSource(rows.iterator());
                } catch (SQLException e) {
                     throw new TeiidProcessingException(QueryPlugin.Event.TEIID30550, e);
                } catch (IOException e) {
                     throw new TeiidProcessingException(QueryPlugin.Event.TEIID30551, e);
                }
            case SCHEMASOURCES:
                String schemaName = (String)((Constant)proc.getParameter(1).getExpression()).getValue();
                ModelMetaData mmd = vdb.getModel(schemaName);
                if (mmd != null && mmd.isSource()) {
                    for (SourceMappingMetadata smm : mmd.getSourceMappings()) {
                        rows.add(Arrays.asList(smm.getName(), smm.getConnectionJndiName()));
                    }
                }
                return new CollectionTupleSource(rows.iterator());
            }
            final Table table = indexMetadata.getGroupID((String)((Constant)proc.getParameter(1).getExpression()).getValue());
            MetadataRepository metadataRepository = getMetadataRepository(table, vdb);
            switch (sysProc) {
            case SETCOLUMNSTATS:
                final String columnName = (String)((Constant)proc.getParameter(2).getExpression()).getValue();
                Column c = null;
                for (Column col : table.getColumns()) {
                    if (col.getName().equalsIgnoreCase(columnName)) {
                        c = col;
                        break;
                    }
                }
                if (c == null) {
                     throw new TeiidProcessingException(QueryPlugin.Event.TEIID30552, columnName + TransformationMetadata.NOT_EXISTS_MESSAGE);
                }
                Number distinctVals = (Number)((Constant)proc.getParameter(3).getExpression()).getValue();
                Number nullVals = (Number)((Constant)proc.getParameter(4).getExpression()).getValue();
                String max = (String) ((Constant)proc.getParameter(5).getExpression()).getValue();
                String min = (String) ((Constant)proc.getParameter(6).getExpression()).getValue();
                final ColumnStats columnStats = new ColumnStats();
                columnStats.setDistinctValues(distinctVals);
                columnStats.setNullValues(nullVals);
                columnStats.setMaximumValue(max);
                columnStats.setMinimumValue(min);
                if (metadataRepository != null) {
                    metadataRepository.setColumnStats(vdbName, vdbVersion, c, columnStats);
                }
                DdlPlan.setColumnStats(vdb, c, columnStats);
                if (eventDistributor != null) {
                    eventDistributor.setColumnStats(vdbName, vdbVersion, table.getParent().getName(), table.getName(), columnName, columnStats);
                }
                break;
            case SETTABLESTATS:
                Constant val = (Constant)proc.getParameter(2).getExpression();
                final Number cardinality = (Number)val.getValue();
                TableStats tableStats = new TableStats();
                tableStats.setCardinality(cardinality);
                if (metadataRepository != null) {
                    metadataRepository.setTableStats(vdbName, vdbVersion, table, tableStats);
                }
                DdlPlan.setTableStats(vdb, table, tableStats);
                if (eventDistributor != null) {
                    eventDistributor.setTableStats(vdbName, vdbVersion, table.getParent().getName(), table.getName(), tableStats);
                }
                break;
            }
            return new CollectionTupleSource(rows.iterator());
        }
        final SystemProcs sysTable = SystemProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
        switch (sysTable) {
        case ARRAYITERATE:
            Object array = ((Constant)proc.getParameter(1).getExpression()).getValue();
            if (array != null) {
                final Object[] vals;
                if (array instanceof Object[]) {
                    vals = (Object[])array;
                } else {
                    ArrayImpl arrayImpl = (ArrayImpl)array;
                    vals = arrayImpl.getValues();
                }
                return new CollectionTupleSource(new Iterator<List<?>> () {
                    int index = 0;
                    @Override
                    public boolean hasNext() {
                        return index < vals.length;
                    }

                    @Override
                    public List<?> next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        return Arrays.asList(vals[index++]);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                });
            }
        }
        return new CollectionTupleSource(rows.iterator());
    }

    public MetadataRepository getMetadataRepository(AbstractMetadataRecord target, VDBMetaData vdb) {
        String modelName = null;
        while (target.getParent() != null) {
            target = target.getParent();
        }
        modelName = target.getName();
        if (modelName != null) {
            ModelMetaData model = vdb.getModel(modelName);
            if (model != null) {
                return model.getAttachment(MetadataRepository.class);
            }
        }
        return null;
    }

    public static AbstractMetadataRecord getByUuid(CompositeMetadataStore metadata,
            String uuid) {
        RecordHolder holder = metadata.getOids().get(uuid);
        if (holder != null && uuid.equals(holder.getRecord().getUUID())) {
            return holder.getRecord();
        }
        return null;
    }

    private AtomicRequestMessage createRequest(RequestWorkItem workItem,
            Command command, String modelName, String connectorBindingId, int nodeID)
            throws TeiidComponentException {
        RequestMessage request = workItem.requestMsg;
        // build the atomic request based on original request + context info
        AtomicRequestMessage aqr = new AtomicRequestMessage(request, workItem.getDqpWorkContext(), nodeID);
        aqr.setCommand(command);
        aqr.setModelName(modelName);
        aqr.setMaxResultRows(requestMgr.getMaxSourceRows());
        aqr.setExceptionOnMaxRows(requestMgr.isExceptionOnMaxSourceRows());
        aqr.setPartialResults(request.supportsPartialResults());
        aqr.setSerial(requestMgr.getUserRequestSourceConcurrency() == 1);
        aqr.setTransactionContext(workItem.getTransactionContext());
        aqr.setBufferManager(this.getBufferManager());
        if (connectorBindingId == null) {
            VDBMetaData vdb = workItem.getDqpWorkContext().getVDB();
            ModelMetaData model = vdb.getModel(modelName);
            List<String> bindings = model.getSourceNames();
            if (bindings == null || bindings.size() != 1) {
                // this should not happen, but it did occur when setting up the SystemAdmin models
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30554, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30554, modelName, workItem.getDqpWorkContext().getVdbName(), workItem.getDqpWorkContext().getVdbVersion()));
            }
            connectorBindingId = bindings.get(0);
            Assertion.isNotNull(connectorBindingId, "could not obtain connector id"); //$NON-NLS-1$
        }
        aqr.setConnectorName(connectorBindingId);
        return aqr;
    }

    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
        throw new UnsupportedOperationException();
    }

    BufferManager getBufferManager() {
        return this.bufferManager;
    }

    static class Trigger {
        String name;
        String triggerType = "INSTEAD OF"; //$NON-NLS-1$
        String triggerEvent;
        String status;
        String body;

        Trigger(String name, String event, boolean status, String triggerType, String body){
            this.name = name;
            this.triggerEvent = event;
            this.status = status?"ENABLED":"DISABLED"; //$NON-NLS-1$ //$NON-NLS-2$
            this.body = body;
            if (triggerType != null) {
                this.triggerType = triggerType;
            }
        }
    }
}
