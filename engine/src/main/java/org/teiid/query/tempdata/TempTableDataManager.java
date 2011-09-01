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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.events.EventDistributor;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataRepository;
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
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;
import org.teiid.query.util.CommandContext;

/**
 * This proxy ProcessorDataManager is used to handle temporary tables.
 * 
 * This isn't handled as a connector because of the temporary metadata and 
 * the create/drop handling (which doesn't have push down support)
 */
public class TempTableDataManager implements ProcessorDataManager {
	
    private static final String REFRESHMATVIEWROW = ".refreshmatviewrow"; //$NON-NLS-1$
	private static final String REFRESHMATVIEW = ".refreshmatview"; //$NON-NLS-1$
	public static final String CODE_PREFIX = "#CODE_"; //$NON-NLS-1$
	
	private ProcessorDataManager processorDataManager;
    private BufferManager bufferManager;
	private SessionAwareCache<CachedResults> cache;
    private Executor executor;
    
    private EventDistributor eventDistributor;

    public TempTableDataManager(ProcessorDataManager processorDataManager, BufferManager bufferManager, 
    		Executor executor, SessionAwareCache<CachedResults> cache){
        this.processorDataManager = processorDataManager;
        this.bufferManager = bufferManager;
        this.executor = executor;
        this.cache = cache;
    }
    
    public void setEventDistributor(EventDistributor eventDistributor) {
		this.eventDistributor = eventDistributor;
	}
    
	public TupleSource registerRequest(
		CommandContext context,
		Command command,
		String modelName,
		String connectorBindingId, int nodeID, int limit)
		throws TeiidComponentException, TeiidProcessingException {          

		TempTableStore tempTableStore = context.getTempTableStore();
        if(tempTableStore != null) {
            TupleSource result = registerRequest(context, modelName, command);
            if (result != null) {
            	return result;
            }
        }
        return this.processorDataManager.registerRequest(context, command, modelName, connectorBindingId, nodeID, limit);
	}
	        
    TupleSource registerRequest(CommandContext context, String modelName, Command command) throws TeiidComponentException, TeiidProcessingException {
    	TempTableStore contextStore = context.getTempTableStore();
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
        	
        	GroupSymbol group = ((ProcedureContainer)command).getGroup();
        	if (!group.isTempGroupSymbol()) {
        		return null;
        	}
        	final String groupKey = group.getNonCorrelationName().toUpperCase();
            final TempTable table = contextStore.getOrCreateTempTable(groupKey, command, bufferManager, true);
        	if (command instanceof Insert) {
        		Insert insert = (Insert)command;
        		TupleSource ts = insert.getTupleSource();
        		if (ts == null) {
        			List<Object> values = new ArrayList<Object>(insert.getValues().size());
        			for (Expression expr : (List<Expression>)insert.getValues()) {
        				values.add(Evaluator.evaluate(expr));
					}
        			ts = new CollectionTupleSource(Arrays.asList(values).iterator());
        		}
        		return table.insert(ts, insert.getVariables(), true);
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
        			//because we are non-transactional, just use a truncate
        			int rows = table.truncate();
                    return CollectionTupleSource.createUpdateCountTupleSource(rows);
        		}
        		return table.delete(crit);
        	}
        }
    	if (command instanceof Create) {
    		Create create = (Create)command;
    		String tempTableName = create.getTable().getCanonicalName();
    		if (contextStore.hasTempTable(tempTableName)) {
                throw new QueryProcessingException(QueryPlugin.Util.getString("TempTableStore.table_exist_error", tempTableName));//$NON-NLS-1$
            }
    		contextStore.addTempTable(tempTableName, create, bufferManager, true);
            return CollectionTupleSource.createUpdateCountTupleSource(0);	
    	}
    	if (command instanceof Drop) {
    		String tempTableName = ((Drop)command).getTable().getCanonicalName();
    		contextStore.removeTempTableByName(tempTableName);
            return CollectionTupleSource.createUpdateCountTupleSource(0);
    	}
    	if (command instanceof AlterTempTable) {
    		AlterTempTable att = (AlterTempTable)command;
    		TempTable tt = contextStore.getOrCreateTempTable(att.getTempTable().toUpperCase(), command, bufferManager, true);
    		tt.setUpdatable(false);
    		if (att.getIndexColumns() != null) {
    			tt.addIndex(att.getIndexColumns(), false);
    		}
    		return CollectionTupleSource.createUpdateCountTupleSource(0);
    	}
        return null;
    }

	private TupleSource handleCachedProcedure(CommandContext context,
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
		CacheID cid = new CacheID(new ParseInfo(), fullName + hash, context.getVdbName(), 
				context.getVdbVersion(), context.getConnectionID(), context.getUserName());
		cid.setParameters(vals);
		CachedResults results = cache.get(cid);
		if (results != null) {
			TupleBuffer buffer = results.getResults();
			return buffer.createIndexedTupleSource();
		}
		//construct a query with a no cache hint
		//note that it's safe to use the stringified form of the parameters because
		//it's not possible to use xml/clob/blob/object
		CacheHint hint = proc.getCacheHint();
		proc.setCacheHint(null);
		Option option = new Option();
		option.setNoCache(true);
		option.addNoCacheGroup(fullName);
		proc.setOption(option);
		Determinism determinismLevel = context.resetDeterminismLevel();
		QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(proc.toString(), fullName.toUpperCase(), context);
		qp.setNonBlocking(true);
		qp.getContext().setDataObjects(null);
		BatchCollector bc = qp.createBatchCollector();
		TupleBuffer tb = bc.collectTuples();
		CachedResults cr = new CachedResults();
		cr.setResults(tb, qp.getProcessorPlan());
		cr.setHint(hint);
		if (hint != null && hint.getDeterminism() != null) {
			LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Cache hint modified the query determinism from ",determinismLevel, " to ", hint.getDeterminism() }); //$NON-NLS-1$ //$NON-NLS-2$
			determinismLevel = hint.getDeterminism();
		}
		cache.put(cid, determinismLevel, cr, hint != null?hint.getTtl():null);
		context.setDeterminismLevel(determinismLevel);
		return tb.createIndexedTupleSource();
	}

	private TupleSource handleSystemProcedures(CommandContext context, StoredProcedure proc)
			throws TeiidComponentException, QueryMetadataException,
			QueryProcessingException, QueryResolverException,
			QueryValidatorException, TeiidProcessingException,
			ExpressionEvaluationException {
		QueryMetadataInterface metadata = context.getMetadata();
		GlobalTableStore globalStore = context.getGlobalTableStore();
		if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEW)) {
			Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(1).getExpression()).getValue());
			Object matTableId = globalStore.getGlobalTempTableMetadataId(groupID);
			String matViewName = metadata.getFullName(groupID);
			String matTableName = metadata.getFullName(matTableId);
			LogManager.logDetail(LogConstants.CTX_MATVIEWS, "processing refreshmatview for", matViewName); //$NON-NLS-1$
			boolean invalidate = Boolean.TRUE.equals(((Constant)proc.getParameter(2).getExpression()).getValue());
			boolean needsLoading = globalStore.needsLoading(matTableName, globalStore.getLocalAddress(), true, true, invalidate);
			if (!needsLoading) {
				return CollectionTupleSource.createUpdateCountTupleSource(-1);
			}
			GroupSymbol matTable = new GroupSymbol(matTableName);
			matTable.setMetadataID(matTableId);
			int rowCount = loadGlobalTable(context, matTable, matTableName, globalStore);
			return CollectionTupleSource.createUpdateCountTupleSource(rowCount);
		} else if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEWROW)) {
			Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(1).getExpression()).getValue());
			Object pk = metadata.getPrimaryKey(groupID);
			String matViewName = metadata.getFullName(groupID);
			if (pk == null) {
				throw new QueryProcessingException(QueryPlugin.Util.getString("TempTableDataManager.row_refresh_pk", matViewName)); //$NON-NLS-1$
			}
			List<?> ids = metadata.getElementIDsInKey(pk);
			if (ids.size() > 1) {
				throw new QueryProcessingException(QueryPlugin.Util.getString("TempTableDataManager.row_refresh_composite", matViewName)); //$NON-NLS-1$
			}
			String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
			MatTableInfo info = globalStore.getMatTableInfo(matTableName);
			if (!info.isValid()) {
				return CollectionTupleSource.createUpdateCountTupleSource(-1);
			}
			TempTable tempTable = globalStore.getTempTableStore().getTempTable(matTableName);
			if (!tempTable.isUpdatable()) {
				throw new QueryProcessingException(QueryPlugin.Util.getString("TempTableDataManager.row_refresh_updatable", matViewName)); //$NON-NLS-1$
			}
			Constant key = (Constant)proc.getParameter(2).getExpression();
			LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.getString("TempTableDataManager.row_refresh", matViewName, key)); //$NON-NLS-1$
			String queryString = Reserved.SELECT + " * " + Reserved.FROM + ' ' + matViewName + ' ' + Reserved.WHERE + ' ' + //$NON-NLS-1$
				metadata.getFullName(ids.iterator().next()) + " = ?" + ' ' + Reserved.OPTION + ' ' + Reserved.NOCACHE; //$NON-NLS-1$
			QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(queryString, matViewName.toUpperCase(), context, key.getValue());
			qp.setNonBlocking(true);
			qp.getContext().setDataObjects(null);
			TupleSource ts = new BatchCollector.BatchProducerTupleSource(qp);
			List<?> tuple = ts.nextTuple();
			boolean delete = false;
			if (tuple == null) {
				delete = true;
				tuple = Arrays.asList(key.getValue());
			} else {
				tuple = new ArrayList<Object>(tuple); //ensure the list is serializable 
			}
			List<?> result = globalStore.updateMatViewRow(matTableName, tuple, delete);
			if (eventDistributor != null) {
				this.eventDistributor.updateMatViewRow(context.getVdbName(), context.getVdbVersion(), metadata.getName(metadata.getModelID(groupID)), metadata.getName(groupID), tuple, delete);
			}
			return CollectionTupleSource.createUpdateCountTupleSource(result != null ? 1 : 0);
		}
		return null;
	}

	private Object validateMatView(QueryMetadataInterface metadata,	String viewName) throws TeiidComponentException,
			TeiidProcessingException {
		try {
			Object groupID = metadata.getGroupID(viewName);
			if (!metadata.hasMaterialization(groupID) || metadata.getMaterialization(groupID) != null) {
				throw new QueryProcessingException(QueryPlugin.Util.getString("TempTableDataManager.not_implicit_matview", viewName)); //$NON-NLS-1$
			}
			return groupID;
		} catch (QueryMetadataException e) {
			throw new TeiidProcessingException(e);
		}
	}

	private TupleSource registerQuery(final CommandContext context,
			TempTableStore contextStore, Query query)
			throws TeiidComponentException, QueryMetadataException,
			TeiidProcessingException, ExpressionEvaluationException,
			QueryProcessingException {
		final GroupSymbol group = query.getFrom().getGroups().get(0);
		if (!group.isTempGroupSymbol()) {
			return null;
		}
		final String tableName = group.getNonCorrelationName().toUpperCase();
		boolean remapColumns = !tableName.equalsIgnoreCase(group.getName());
		TempTable table = null;
		if (group.isGlobalTable()) {
			final GlobalTableStore globalStore = context.getGlobalTableStore();
			final MatTableInfo info = globalStore.getMatTableInfo(tableName);
			boolean load = false;
			while (!info.isUpToDate()) {
				load = globalStore.needsLoading(tableName, globalStore.getLocalAddress(), true, false, false);
				if (load) {
					load = globalStore.needsLoading(tableName, globalStore.getLocalAddress(), false, false, false);
					if (load) {
						break;
					}
				}
				synchronized (info) {
					try {
						info.wait(30000);
					} catch (InterruptedException e) {
						throw new TeiidComponentException(e);
					}
				}
			}
			if (load) {
				if (!info.isValid()) {
					//blocking load
					loadGlobalTable(context, group, tableName, globalStore);
				} else {
					loadAsynch(context, group, tableName, globalStore);
				}
			} 
			table = globalStore.getTempTableStore().getOrCreateTempTable(tableName, query, bufferManager, false);
			context.accessedDataObject(group.getMetadataID());
		} else {
			table = contextStore.getOrCreateTempTable(tableName, query, bufferManager, true);
			if (context.getDataObjects() != null) {
				Object id = RelationalPlanner.getTrackableGroup(group, context.getMetadata());
				if (id != null) {
					context.accessedDataObject(group.getMetadataID());
				}
			}
		}
		if (remapColumns) {
			//convert to the actual table symbols (this is typically handled by the languagebridgefactory
			ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
				@Override
				public Expression replaceExpression(Expression element) {
					if (element instanceof ElementSymbol) {
						ElementSymbol es = (ElementSymbol)element;
						es.getGroupSymbol().setName(tableName);
						es.getGroupSymbol().setDefinition(null);
					}
					return element;
				}
			};
			PostOrderNavigator.doVisit(query, emv);
		}
		return table.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
	}

	private void loadAsynch(final CommandContext context,
			final GroupSymbol group, final String tableName, final GlobalTableStore globalStore) {
		Callable<Integer> toCall = new Callable<Integer>() {
			@Override
			public Integer call() throws Exception {
				return loadGlobalTable(context, group, tableName, globalStore);
			}
		};
		FutureTask<Integer> task = new FutureTask<Integer>(toCall);
		executor.execute(task);
	}

	private int loadGlobalTable(CommandContext context,
			GroupSymbol group, final String tableName, GlobalTableStore globalStore)
			throws TeiidComponentException, TeiidProcessingException {
		LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.getString("TempTableDataManager.loading", tableName)); //$NON-NLS-1$
		QueryMetadataInterface metadata = context.getMetadata();
		List<ElementSymbol> allColumns = ResolverUtil.resolveElementsInGroup(group, metadata); 
		TempTable table = globalStore.createMatTable(tableName, group);
		table.setUpdatable(false);
		int rowCount = -1;
		try {
			String fullName = metadata.getFullName(group.getMetadataID());
			String transformation = metadata.getVirtualPlan(group.getMetadataID()).getQuery();
			QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(transformation, fullName, context);
			qp.setNonBlocking(true);
			qp.getContext().setDataObjects(null);
			TupleSource ts = new BatchCollector.BatchProducerTupleSource(qp);
			
			table.insert(ts, allColumns, false);
			table.getTree().compact();
			rowCount = table.getRowCount();
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
				table.setUpdatable(hint.isUpdatable());
			}
		} catch (TeiidComponentException e) {
			LogManager.logError(LogConstants.CTX_MATVIEWS, e, QueryPlugin.Util.getString("TempTableDataManager.failed_load", tableName)); //$NON-NLS-1$
			throw e;
		} catch (TeiidProcessingException e) {
			LogManager.logError(LogConstants.CTX_MATVIEWS, e, QueryPlugin.Util.getString("TempTableDataManager.failed_load", tableName)); //$NON-NLS-1$
			throw e;
		} finally {
			if (rowCount == -1) {
				globalStore.failedLoad(tableName);
			} else {
				globalStore.loaded(tableName, table);
				LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.getString("TempTableDataManager.loaded", tableName, rowCount)); //$NON-NLS-1$
			}
		}
		return rowCount;
	}

	public Object lookupCodeValue(CommandContext context, String codeTableName,
			String returnElementName, String keyElementName, Object keyValue)
			throws BlockedException, TeiidComponentException,
			TeiidProcessingException {
    	String matTableName = CODE_PREFIX + (codeTableName + ElementSymbol.SEPARATOR + keyElementName + ElementSymbol.SEPARATOR + returnElementName).toUpperCase(); 
    	QueryMetadataInterface metadata = context.getMetadata();

    	TempMetadataID id = context.getGlobalTableStore().getCodeTableMetadataId(codeTableName,
				returnElementName, keyElementName, matTableName);
    	
    	ElementSymbol keyElement = new ElementSymbol(matTableName + ElementSymbol.SEPARATOR + keyElementName);
    	ElementSymbol returnElement = new ElementSymbol(matTableName + ElementSymbol.SEPARATOR + returnElementName);
    	keyElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + keyElementName))));
    	returnElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + returnElementName))));
    	
    	Query query = RelationalPlanner.createMatViewQuery(id, matTableName, Arrays.asList(returnElement), true);
    	query.setCriteria(new CompareCriteria(keyElement, CompareCriteria.EQ, new Constant(keyValue)));
    	
    	TupleSource ts = registerQuery(context, context.getTempTableStore(), query);
    	List<?> row = ts.nextTuple();
    	Object result = null;
    	if (row != null) {
    		result = row.get(0);
    	}
    	ts.closeSource();
    	return result;
    }

	@Override
	public EventDistributor getEventDistributor() {
		return this.eventDistributor;
	}

	@Override
	public MetadataRepository getMetadataRepository() {
		return this.processorDataManager.getMetadataRepository();
	}
}
