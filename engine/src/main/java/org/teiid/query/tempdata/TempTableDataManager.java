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

package org.teiid.query.tempdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

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
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
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
	
	public interface RequestExecutor {
		void execute(String command, List<?> parameters);
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
		        		return table.insert(ts, insert.getVariables(), true, context);
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
		        			int rows = table.truncate(false);
		                    return CollectionTupleSource.createUpdateCountTupleSource(rows);
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
    		if (contextStore.hasTempTable(tempTableName)) {
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
    	if (command instanceof AlterTempTable) {
    		AlterTempTable att = (AlterTempTable)command;
    		TempTable tt = contextStore.getTempTable(att.getTempTable());
    		Assertion.isNotNull(tt, "Table doesn't exist"); //$NON-NLS-1$
    		tt.setUpdatable(false);
    		if (att.getIndexColumns() != null && tt.getRowCount() > 2*tt.getTree().getPageSize(true)) {
    			for (List<ElementSymbol> cols : att.getIndexColumns()) {
    				tt.addIndex(cols, false);
    			}
    		}
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
			boolean needsLoading = globalStore.needsLoading(matTableName, globalStore.getAddress(), true, true, invalidate);
			if (!needsLoading) {
				return CollectionTupleSource.createUpdateCountTupleSource(-1);
			}
			GroupSymbol matTable = new GroupSymbol(matTableName);
			matTable.setMetadataID(matTableId);
			return loadGlobalTable(context, matTable, matTableName, globalStore);
		} else if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEWROW)) {
			final Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(2).getExpression()).getValue());
			TempMetadataID matTableId = context.getGlobalTableStore().getGlobalTempTableMetadataId(groupID);
			final GlobalTableStore globalStore = getGlobalStore(context, matTableId);
			Object pk = metadata.getPrimaryKey(groupID);
			String matViewName = metadata.getFullName(groupID);
			if (pk == null) {
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30230, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30230, matViewName));
			}
			List<?> ids = metadata.getElementIDsInKey(pk);
			if (ids.size() > 1) {
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30231, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30231, matViewName));
			}
			final String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
			MatTableInfo info = globalStore.getMatTableInfo(matTableName);
			if (!info.isValid()) {
				return CollectionTupleSource.createUpdateCountTupleSource(-1);
			}
			TempTable tempTable = globalStore.getTempTable(matTableName);
			if (!tempTable.isUpdatable()) {
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30232, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30232, matViewName));
			}
			Constant key = (Constant)proc.getParameter(3).getExpression();
			LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30012, matViewName, key));
			Object id = ids.iterator().next();
			String targetTypeName = metadata.getElementType(id);
			final Object value = DataTypeManager.transformValue(key.getValue(), DataTypeManager.getDataTypeClass(targetTypeName));
			String queryString = Reserved.SELECT + " * " + Reserved.FROM + ' ' + matViewName + ' ' + Reserved.WHERE + ' ' + //$NON-NLS-1$
				metadata.getFullName(id) + " = ?" + ' ' + Reserved.OPTION + ' ' + Reserved.NOCACHE; //$NON-NLS-1$
			final QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(queryString, matViewName.toUpperCase(), context, value);
			final TupleSource ts = new BatchCollector.BatchProducerTupleSource(qp);
			return new ProxyTupleSource() {

				@Override
				protected TupleSource createTupleSource()
						throws TeiidComponentException,
						TeiidProcessingException {
					List<?> tuple = ts.nextTuple();
					boolean delete = false;
					if (tuple == null) {
						delete = true;
						tuple = Arrays.asList(value);
					} else {
						tuple = new ArrayList<Object>(tuple); //ensure the list is serializable 
					}
					List<?> result = globalStore.updateMatViewRow(matTableName, tuple, delete);
					if (eventDistributor != null) {
						eventDistributor.updateMatViewRow(context.getVdbName(), context.getVdbVersion(), metadata.getName(metadata.getModelID(groupID)), metadata.getName(groupID), tuple, delete);
					}
					return CollectionTupleSource.createUpdateCountTupleSource(result != null ? 1 : 0);
				}
				
				@Override
				public void closeSource() {
					super.closeSource();
					qp.closeProcessing();
				}
				
			};
		}
		return null;
	}

	private Object validateMatView(QueryMetadataInterface metadata,	String viewName) throws TeiidComponentException,
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
							load = globalStore.needsLoading(tableName, globalStore.getAddress(), true, false, false);
							if (load) {
								load = globalStore.needsLoading(tableName, globalStore.getAddress(), false, false, false);
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
					TupleSource result = table.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
					cancelMoreWork();
					return result;
				}

				private void load() throws TeiidComponentException,
						TeiidProcessingException {
					if (newWorkContext != null) {
						try {
							newWorkContext.runInContext(new Callable<Void>() {
								@Override
								public Void call() throws Exception {
									loadingTupleSource.nextTuple();
									return null;
								}
							});
						} catch (Throwable e) {
							rethrow(e);
						}
					} else {
						loadingTupleSource.nextTuple();
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
				return tt.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
			}
		};
	}

	private GlobalTableStore getGlobalStore(final CommandContext context, TempMetadataID matTableId) {
		GlobalTableStore globalStore = context.getGlobalTableStore();
		if (matTableId.getCacheHint() == null || matTableId.getCacheHint().getScope() == null || Scope.VDB.compareTo(matTableId.getCacheHint().getScope()) <= 0) {
			return globalStore;
		}
		return context.getSessionScopedStore(true);
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
		
			@Override
			protected TupleSource createTupleSource() throws TeiidComponentException,
					TeiidProcessingException {
				int rowCount = -1;
				try {
					if (insertTupleSource == null) {
						String fullName = metadata.getFullName(group.getMetadataID());
						String transformation = metadata.getVirtualPlan(group.getMetadataID()).getQuery();
						qp = context.getQueryProcessorFactory().createQueryProcessor(transformation, fullName, context);
						insertTupleSource = new BatchCollector.BatchProducerTupleSource(qp);
					}
					table.insert(insertTupleSource, allColumns, false, null);
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
					return CollectionTupleSource.createUpdateCountTupleSource(rowCount);
				} catch (BlockedException e) {
					throw e;
				} catch (Exception e) {
					LogManager.logError(LogConstants.CTX_MATVIEWS, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30015, tableName));
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
		//we are not using a resolved form of a lookup, so we canonicallize with upper case
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
	    	keyElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + keyElementName))));
	    	returnElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + returnElementName))));
	    	
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
		if (e instanceof RuntimeException) {
			throw (RuntimeException)e;
		}
		throw new TeiidRuntimeException(e);
	}
}
