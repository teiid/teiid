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
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPWorkContext;
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
            TupleSource result = registerRequest(context, modelName, command);
            if (result != null) {
            	return result;
            }
        }
        return this.processorDataManager.registerRequest(context, command, modelName, parameterObject);
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
        	if (!modelName.equals(TempMetadataAdapter.TEMP_MODEL.getID()) || !group.isTempGroupSymbol()) {
        		return null;
        	}
        	final String groupKey = group.getNonCorrelationName();
            final TempTable table = contextStore.getOrCreateTempTable(groupKey, command, bufferManager, true, true, context);
        	if (command instanceof Insert) {
        		Insert insert = (Insert)command;
        		TupleSource ts = insert.getTupleSource();
        		if (ts == null) {
        			Evaluator eval = new Evaluator(Collections.emptyMap(), this, context);
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
				context.getVdbVersion(), context.getConnectionId(), context.getUserName());
		cid.setParameters(vals);
		CachedResults results = cache.get(cid);
		if (results != null) {
			TupleBuffer buffer = results.getResults();
			return buffer.createIndexedTupleSource();
		}
		//construct a query with a no cache hint
		CacheHint hint = proc.getCacheHint();
		proc.setCacheHint(null);
		Option option = new Option();
		option.setNoCache(true);
		option.addNoCacheGroup(fullName);
		proc.setOption(option);
		Determinism determinismLevel = context.resetDeterminismLevel();
		StoredProcedure cloneProc = (StoredProcedure)proc.clone();
		int i = 0;
		for (SPParameter param : cloneProc.getInputParameters()) {
			param.setExpression(new Reference(i++));
		}
		QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(cloneProc.toString(), fullName.toUpperCase(), context, vals.toArray());
		qp.setNonBlocking(true);
		qp.getContext().setDataObjects(null);
		BatchCollector bc = qp.createBatchCollector();
		TupleBuffer tb = bc.collectTuples();
		CachedResults cr = new CachedResults();
		cr.setResults(tb, qp.getProcessorPlan());
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
			Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(2).getExpression()).getValue());
			Object matTableId = globalStore.getGlobalTempTableMetadataId(groupID);
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
			int rowCount = loadGlobalTable(context, matTable, matTableName, globalStore);
			return CollectionTupleSource.createUpdateCountTupleSource(rowCount);
		} else if (StringUtil.endsWithIgnoreCase(proc.getProcedureCallableName(), REFRESHMATVIEWROW)) {
			Object groupID = validateMatView(metadata, (String)((Constant)proc.getParameter(2).getExpression()).getValue());
			Object pk = metadata.getPrimaryKey(groupID);
			String matViewName = metadata.getFullName(groupID);
			if (pk == null) {
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30230, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30230, matViewName));
			}
			List<?> ids = metadata.getElementIDsInKey(pk);
			if (ids.size() > 1) {
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30231, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30231, matViewName));
			}
			String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
			MatTableInfo info = globalStore.getMatTableInfo(matTableName);
			if (!info.isValid()) {
				return CollectionTupleSource.createUpdateCountTupleSource(-1);
			}
			TempTable tempTable = globalStore.getTempTableStore().getTempTable(matTableName);
			if (!tempTable.isUpdatable()) {
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30232, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30232, matViewName));
			}
			Constant key = (Constant)proc.getParameter(3).getExpression();
			LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30012, matViewName, key));
			Object id = ids.iterator().next();
			String targetTypeName = metadata.getElementType(id);
			Object value = DataTypeManager.transformValue(key.getValue(), DataTypeManager.getDataTypeClass(targetTypeName));
			String queryString = Reserved.SELECT + " * " + Reserved.FROM + ' ' + matViewName + ' ' + Reserved.WHERE + ' ' + //$NON-NLS-1$
				metadata.getFullName(id) + " = ?" + ' ' + Reserved.OPTION + ' ' + Reserved.NOCACHE; //$NON-NLS-1$
			QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(queryString, matViewName.toUpperCase(), context, value);
			qp.setNonBlocking(true);
			qp.getContext().setDataObjects(null);
			TupleSource ts = new BatchCollector.BatchProducerTupleSource(qp);
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
				 throw new QueryProcessingException(QueryPlugin.Event.TEIID30233, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30233, viewName));
			}
			return groupID;
		} catch (QueryMetadataException e) {
			 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30234, e);
		}
	}

	private TupleSource registerQuery(final CommandContext context,
			TempTableStore contextStore, Query query)
			throws TeiidComponentException, QueryMetadataException,
			TeiidProcessingException, ExpressionEvaluationException,
			QueryProcessingException {
		GroupSymbol group = query.getFrom().getGroups().get(0);
		if (!group.isTempGroupSymbol()) {
			return null;
		}
		final String tableName = group.getNonCorrelationName();
		TempTable table = null;
		if (group.isGlobalTable()) {
			final GlobalTableStore globalStore = context.getGlobalTableStore();
			final MatTableInfo info = globalStore.getMatTableInfo(tableName);
			boolean load = false;
			while (!info.isUpToDate()) {
				load = globalStore.needsLoading(tableName, globalStore.getAddress(), true, false, false);
				if (load) {
					load = globalStore.needsLoading(tableName, globalStore.getAddress(), false, false, false);
					if (load) {
						break;
					}
				}
				synchronized (info) {
					try {
						info.wait(30000);
					} catch (InterruptedException e) {
						 throw new TeiidComponentException(QueryPlugin.Event.TEIID30235, e);
					}
				}
			}
			if (load) {
				if (!info.isValid() || executor == null) {
					//blocking load
					loadGlobalTable(context, group, tableName, globalStore);
				} else {
					info.setAsynchLoad();
					loadAsynch(context, tableName);
				}
			} 
			table = globalStore.getTempTableStore().getOrCreateTempTable(tableName, query, bufferManager, false, false, context);
			context.accessedDataObject(group.getMetadataID());
		} else {
			table = contextStore.getOrCreateTempTable(tableName, query, bufferManager, true, false, context);
			if (context.getDataObjects() != null) {
				Object id = RelationalPlanner.getTrackableGroup(group, context.getMetadata());
				if (id != null) {
					context.accessedDataObject(group.getMetadataID());
				}
			}
		}
		return table.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
	}

	private void loadAsynch(final CommandContext context, final String tableName) {
		SessionMetadata session = createTemporarySession(context.getUserName(), "asynch-mat-view-load", context.getDQPWorkContext().getVDB()); //$NON-NLS-1$
		session.setSubject(context.getSubject());
		session.setSecurityDomain(context.getSession().getSecurityDomain());
		session.setSecurityContext(context.getSession().getSecurityContext());
		DQPWorkContext workContext = new DQPWorkContext();
		workContext.setAdmin(true);
		DQPWorkContext current = context.getDQPWorkContext();
		workContext.setSession(session);
		workContext.setPolicies(current.getAllowedDataPolicies());
		workContext.setSecurityHelper(current.getSecurityHelper());
		final String viewName = tableName.substring(RelationalPlanner.MAT_PREFIX.length());
		workContext.runInContext(new Runnable() {

			@Override
			public void run() {
				executor.execute(REFRESH_SQL, Arrays.asList(viewName, Boolean.FALSE.toString()));
			}
			
		});
	}

	private int loadGlobalTable(CommandContext context,
			GroupSymbol group, final String tableName, GlobalTableStore globalStore)
			throws TeiidComponentException, TeiidProcessingException {
		LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30013, tableName));
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
			
			table.insert(ts, allColumns, false, null);
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
				table.setUpdatable(hint.isUpdatable(false));
			}
		} catch (TeiidComponentException e) {
			LogManager.logError(LogConstants.CTX_MATVIEWS, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30015, tableName));
			throw e;
		} catch (TeiidProcessingException e) {
			LogManager.logError(LogConstants.CTX_MATVIEWS, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30015, tableName));
			throw e;
		} finally {
			if (rowCount == -1) {
				globalStore.failedLoad(tableName);
			} else {
				globalStore.loaded(tableName, table);
				LogManager.logInfo(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30014, tableName, rowCount));
			}
		}
		return rowCount;
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
    	QueryMetadataInterface metadata = context.getMetadata();

    	TempMetadataID id = context.getGlobalTableStore().getCodeTableMetadataId(codeTableName,
				returnElementName, keyElementName, matTableName);
    	
    	ElementSymbol keyElement = new ElementSymbol(keyElementName, new GroupSymbol(matTableName));
    	ElementSymbol returnElement = new ElementSymbol(returnElementName, new GroupSymbol(matTableName));
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
}
