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

package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.teiid.adminapi.impl.ModelMetaData;
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
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
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
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.*;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.CompositeMetadataStore.RecordHolder;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
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
			signalMore();
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
		PROPERTIES
	}
	
	private enum SystemAdminTables {
		MATVIEWS,
		VDBRESOURCES,
		TRIGGERS
	}
	
	private enum SystemAdminProcs {
		SETTABLESTATS,
		SETCOLUMNSTATS,
		SETPROPERTY,
		LOGMSG,
		ISLOGGABLE,
	}
	
	private enum SystemProcs {
		GETXMLSCHEMAS,
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
        		row.add(null);
        	}
		});
        name = SystemTables.TABLES.name();
        columns = getColumns(tm, name);
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
				row.add(null);
			}
		});
        name = SystemAdminTables.MATVIEWS.name();
        columns = getColumns(tm, name);
        systemAdminTables.put(SystemAdminTables.MATVIEWS, new RecordExtractionTable<Table>(new TableSystemTable(1, 2, columns) {
        	@Override
        	protected boolean isValid(Table s, VDBMetaData vdb,
        			List<Object> rowBuffer, Criteria condition)
        			throws TeiidProcessingException, TeiidComponentException {
        		if (s == null || !s.isMaterialized()) {
        			return false;
        		}
        		return super.isValid(s, vdb, rowBuffer, condition);
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
					if (id != null && id.getCacheHint() != null && id.getCacheHint().getScope() != null && Scope.VDB.compareTo(id.getCacheHint().getScope()) > 0) {
						//consult the session store instead
						globalStore = cc.getSessionScopedStore(false);
						if (globalStore == null) {
							globalStore = cc.getGlobalTableStore();
						}
					}
					MatTableInfo info = globalStore.getMatTableInfo(matTableName);
					valid = info.isValid();
					state = info.getState().name();
					updated = info.getUpdateTime()==-1?null:new Timestamp(info.getUpdateTime());
					if (id != null) {
						cardinaltity = id.getCardinality();
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
				row.add(null);
			}
		});
        name = SystemTables.DATATYPES.name();
        columns = getColumns(tm, name);
        systemTables.put(SystemTables.DATATYPES, new RecordExtractionTable<Datatype>(new RecordTable<Datatype>(new int[] {0}, columns) {
        	
        	@Override
        	public SimpleIterator<Datatype> processQuery(VDBMetaData vdb,
        			CompositeMetadataStore metadataStore, BaseIndexInfo<?> ii) {
        		return processQuery(vdb, metadataStore.getDatatypes(), ii);
        	}
        }, columns) {
			
        	@Override
        	public void fillRow(List<Object> row, Datatype datatype,
        			VDBMetaData vdb, TransformationMetadata metadata,
        			CommandContext cc, SimpleIterator<Datatype> iter) {
        		row.add(datatype.getName());
				row.add(datatype.isBuiltin());
				row.add(datatype.isBuiltin());
				row.add(datatype.getName());
				row.add(datatype.getJavaClassName());
				row.add(datatype.getScale());
				row.add(datatype.getLength());
				row.add(datatype.getNullType().toString());
				row.add(datatype.isSigned());
				row.add(datatype.isAutoIncrement());
				row.add(datatype.isCaseSensitive());
				row.add(datatype.getPrecision());
 				row.add(datatype.getRadix());
 				row.add(datatype.getSearchType().toString()); 
 				row.add(datatype.getUUID());
 				row.add(datatype.getRuntimeTypeName());
 				row.add(datatype.getBasetypeName());
 				row.add(datatype.getAnnotation());
 				row.add(null);
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
        		row.add(record.getName());
        		row.add(String.valueOf(record.getVersion()));
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
				if (param instanceof ProcedureParameter) {
					ProcedureParameter pp = (ProcedureParameter)param;
					type = pp.getType().name();
					isOptional = pp.isOptional();
				} else {
					Column pp = (Column)param;
					proc = param.getParent().getParent();
				}
				row.add(proc.getParent().getName());
				row.add(proc.getName());
				row.add(param.getName());
				row.add(dt!=null?dt.getRuntimeTypeName():null);
				row.add(param.getPosition());
				row.add(type);
				row.add(isOptional);
				row.add(param.getPrecision());
				row.add(param.getLength());
				row.add(param.getScale());
				row.add(param.getRadix());
				row.add(param.getNullType().toString());
				row.add(param.getUUID());
				row.add(param.getAnnotation());
				row.add(null);
        	}
        	
        	@Override
        	protected Collection<? extends BaseColumn> getChildren(final Procedure parent) {
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
        					BaseIndexInfo<?> ii) {
        				return processQuery(vdb, metadataStore.getOids(), ii);
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
				row.add(null);
				row.add(clobValue);
        	}
        	
        	@Override
        	protected Collection<Map.Entry<String,String>> getChildren(AbstractMetadataRecord parent) {
        		return parent.getProperties().entrySet();
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
        	protected Collection<Trigger> getChildren(Table table) {
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
        		}
        		return cols;
        	}
        });        
        name = SystemTables.COLUMNS.name();
        columns = getColumns(tm, name);
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
        		row.add(dt!=null?dt.getRuntimeTypeName():null);
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
        		row.add(dt!=null?dt.getJavaClassName():null);
        		row.add(column.getPrecision());
        		row.add(column.getCharOctetLength());
        		row.add(column.getRadix());
        		row.add(column.getUUID());
        		row.add(column.getAnnotation());
        		row.add(null);
        	}
        	
        	@Override
        	protected Collection<Column> getChildren(Table parent) {
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
        		row.add(null);
        	}
        	
        	@Override
        	protected Collection<KeyRecord> getChildren(Table parent) {
        		return parent.getAllKeys();
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
        		row.add(null);
        	}
        	
        	@Override
        	protected Collection<List<?>> getChildren(Table parent) {
        		ArrayList<List<?>> cols = new ArrayList<List<?>>();
        		
        		for (KeyRecord record : parent.getAllKeys()) {
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
        		Table pkTable = key.getPrimaryKey().getParent();
        		Column column = (Column) record.get(1);
        		Short pos = (Short) record.get(2);
				row.add(pkTable.getParent().getName());
				row.add(pkTable.getName());
				row.add(key.getPrimaryKey().getColumns().get(pos-1).getName());
				row.add(vdb.getName());
				row.add(key.getParent().getParent().getName());
				row.add(key.getParent().getName());
				row.add(column.getName());
				row.add(pos);
				row.add(DatabaseMetaData.importedKeyNoAction);
				row.add(DatabaseMetaData.importedKeyNoAction);
				row.add(key.getName());
				row.add(key.getPrimaryKey().getName());
				row.add(DatabaseMetaData.importedKeyInitiallyDeferred);
        	}
        	
        	@Override
        	protected Collection<List<?>> getChildren(Table parent) {
        		ArrayList<List<?>> cols = new ArrayList<List<?>>();
        		
        		for (KeyRecord record : parent.getForeignKeys()) {
        			short i = 1;
        			for (Column col : record.getColumns()) {
        				cols.add(Arrays.asList(record, col, i++));
					}
				}
        		return cols;
        	}
        });
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
		Collection<GroupSymbol> accessedGroups = null;
		if (context.getDataObjects() != null) {
			accessedGroups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, false);
			for (GroupSymbol gs : accessedGroups) {
				context.accessedDataObject(gs.getMetadataID());
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
		ConnectorWork work = connectorManager.registerRequest(aqr);
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
						processed = true;
					}
					return ts.nextTuple();
				}
				
				@Override
				public void closeSource() {
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
	 * @param workItem
	 * @return
	 * @throws TeiidComponentException
	 * @throws TeiidProcessingException 
	 */
	private TupleSource processSystemQuery(CommandContext context, Command command,
			DQPWorkContext workContext) throws TeiidComponentException, TeiidProcessingException {
		String vdbName = workContext.getVdbName();
		int vdbVersion = workContext.getVdbVersion();
		VDBMetaData vdb = workContext.getVDB();
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
		if (StringUtil.startsWithIgnoreCase(proc.getProcedureCallableName(), CoreConstants.SYSTEM_ADMIN_MODEL)) {
			final SystemAdminProcs sysProc = SystemAdminProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_ADMIN_MODEL.length() + 1).toUpperCase());
			switch (sysProc) {
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
					if (message != null) {
						LogManager.log(msgLevel, logContext, message);
					}
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
					String strVal = null;
					String result = null;
					if (value != null) {
						if (value.length() > MAX_VALUE_LENGTH) {
							 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30548, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30548, MAX_VALUE_LENGTH));
						}
						strVal = ObjectConverterUtil.convertToString(value.getCharacterStream());
					}
					AbstractMetadataRecord target = getByUuid(metadata, uuid);
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
					if (getMetadataRepository(target, vdb) != null) {
						getMetadataRepository(target, vdb).setProperty(vdbName, vdbVersion, target, key, strVal);
					}
					result = target.setProperty(key, strVal);
					if (eventDistributor != null) {
						eventDistributor.setProperty(vdbName, vdbVersion, uuid, key, strVal);
					}
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
			}
			Table table = indexMetadata.getGroupID((String)((Constant)proc.getParameter(1).getExpression()).getValue());
			switch (sysProc) {
			case SETCOLUMNSTATS:
				String columnName = (String)((Constant)proc.getParameter(2).getExpression()).getValue();
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
				ColumnStats columnStats = new ColumnStats();
				columnStats.setDistinctValues(distinctVals);
				columnStats.setNullValues(nullVals);
				columnStats.setMaximumValue(max);
				columnStats.setMinimumValue(min);
				if (getMetadataRepository(table, vdb) != null) {
					getMetadataRepository(table, vdb).setColumnStats(vdbName, vdbVersion, c, columnStats);
				}
				c.setColumnStats(columnStats);
				if (eventDistributor != null) {
					eventDistributor.setColumnStats(vdbName, vdbVersion, table.getParent().getName(), table.getName(), columnName, columnStats);
				}
				break;
			case SETTABLESTATS:
				Constant val = (Constant)proc.getParameter(2).getExpression();
				Number cardinality = (Number)val.getValue();
				TableStats tableStats = new TableStats();
				tableStats.setCardinality(cardinality);
				if (getMetadataRepository(table, vdb) != null) {
					getMetadataRepository(table, vdb).setTableStats(vdbName, vdbVersion, table, tableStats);
				}
				table.setTableStats(tableStats);
				if (eventDistributor != null) {
					eventDistributor.setTableStats(vdbName, vdbVersion, table.getParent().getName(), table.getName(), tableStats);
				}
				break;
			}
			table.setLastModified(System.currentTimeMillis());
			return new CollectionTupleSource(rows.iterator());
		}
		final SystemProcs sysTable = SystemProcs.valueOf(proc.getProcedureCallableName().substring(CoreConstants.SYSTEM_MODEL.length() + 1).toUpperCase());
		switch (sysTable) {
		case GETXMLSCHEMAS:
			try {
				Object groupID = indexMetadata.getGroupID((String)((Constant)proc.getParameter(1).getExpression()).getValue());
				List<SQLXMLImpl> schemas = indexMetadata.getXMLSchemas(groupID);
				for (SQLXMLImpl schema : schemas) {
					rows.add(Arrays.asList(new XMLType(schema)));
				}
			} catch (QueryMetadataException e) {
				 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30553, e);
			}
			break;
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
		
		Trigger(String name, String event, boolean status, String time, String body){
			this.name = name;
			this.triggerEvent = event;
			this.status = status?"ENABLED":"DISABLED"; //$NON-NLS-1$ //$NON-NLS-2$  
			this.body = body;
		}
	}    
}
