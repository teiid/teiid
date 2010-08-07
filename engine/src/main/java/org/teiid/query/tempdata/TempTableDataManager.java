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
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.execution.QueryExecPlugin;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.relational.RelationalPlanner;
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
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.tempdata.TempTableStore.MatState;
import org.teiid.query.tempdata.TempTableStore.MatTableInfo;
import org.teiid.query.util.CommandContext;

/**
 * This proxy ProcessorDataManager is used to handle temporary tables.
 */
public class TempTableDataManager implements ProcessorDataManager {
	
    private static final String CODE_PREFIX = "#CODE_"; //$NON-NLS-1$
	private ProcessorDataManager processorDataManager;
    private BufferManager bufferManager;

    /**
     * Constructor takes the "real" ProcessorDataManager that this object will be a proxy to,
     * and will pass most calls through to transparently.  Only when a request is registered for
     * a temp group will this proxy do it's thing.
     * @param processorDataManager the real ProcessorDataManager that this object is a proxy to
     */
    public TempTableDataManager(ProcessorDataManager processorDataManager, BufferManager bufferManager){
        this.processorDataManager = processorDataManager;
        this.bufferManager = bufferManager;
    }

	public TupleSource registerRequest(
		CommandContext context,
		Command command,
		String modelName,
		String connectorBindingId, int nodeID)
		throws TeiidComponentException, TeiidProcessingException {          

		TempTableStore tempTableStore = context.getTempTableStore();
        if(tempTableStore != null) {
            TupleSource result = registerRequest(context, command);
            if (result != null) {
            	return result;
            }
        }
        return this.processorDataManager.registerRequest(context, command, modelName, connectorBindingId, nodeID);
	}
	        
    public TupleSource registerRequest(CommandContext context, Command command) throws TeiidComponentException, TeiidProcessingException {
    	TempTableStore contextStore = context.getTempTableStore();
        if (command instanceof Query) {
            Query query = (Query)command;
            return registerQuery(context, contextStore, query);
        }
        if (command instanceof ProcedureContainer) {
        	GroupSymbol group = ((ProcedureContainer)command).getGroup();
        	if (!group.isTempGroupSymbol()) {
        		return null;
        	}
        	final String groupKey = group.getNonCorrelationName().toUpperCase();
            final TempTable table = contextStore.getOrCreateTempTable(groupKey, command, bufferManager, false);
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
        		return table.insert(ts, insert.getVariables());
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
                throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_exist_error", tempTableName));//$NON-NLS-1$
            }
    		contextStore.addTempTable(tempTableName, create, bufferManager);
            return CollectionTupleSource.createUpdateCountTupleSource(0);	
    	}
    	if (command instanceof Drop) {
    		String tempTableName = ((Drop)command).getTable().getCanonicalName();
    		contextStore.removeTempTableByName(tempTableName);
            return CollectionTupleSource.createUpdateCountTupleSource(0);
    	}
        return null;
    }

	private TupleSource registerQuery(CommandContext context,
			TempTableStore contextStore, Query query)
			throws TeiidComponentException, QueryMetadataException,
			TeiidProcessingException, ExpressionEvaluationException,
			QueryProcessingException {
		GroupSymbol group = query.getFrom().getGroups().get(0);
		if (!group.isTempGroupSymbol()) {
			return null;
		}
		final String tableName = group.getNonCorrelationName().toUpperCase();
		TempTable table = null;
		if (group.isGlobalTable()) {
			TempTableStore globalStore = context.getGlobalTableStore();
			MatTableInfo info = globalStore.getMatTableInfo(tableName);
			boolean load = info.shouldLoad();
			if (load) {
				QueryMetadataInterface metadata = context.getMetadata();
				Create create = new Create();
				create.setTable(group);
				create.setColumns(ResolverUtil.resolveElementsInGroup(group, metadata));
				Object pk = metadata.getPrimaryKey(group.getMetadataID());
				if (pk != null) {
					for (Object col : metadata.getElementIDsInKey(pk)) {
						create.getPrimaryKey().add(create.getColumns().get(metadata.getPosition(col)-1));
					}
				}
				table = globalStore.addTempTable(tableName, create, bufferManager);
				CacheHint hint = table.getCacheHint();
				table.setUpdatable(false);
				table.setPreferMemory(hint.getPrefersMemory());
				boolean success = false;
				try {
					//TODO: order by primary key nulls first - then have an insert ordered optimization
					//TODO: use the getCommand logic in RelationalPlanner to reuse commands for this.
					String transformation = metadata.getVirtualPlan(group.getMetadataID()).getQuery();
		    		QueryProcessor qp = context.getQueryProcessorFactory().createQueryProcessor(transformation, group.getCanonicalName(), context);
		    		qp.setNonBlocking(true);
		    		TupleSource ts = new BatchCollector.BatchProducerTupleSource(qp);
		    		//TODO: if this insert fails, it's unnecessary to do the undo processing
		    		table.insert(ts, table.getColumns());
		    		success = true;
				} finally {
					if (!success) {
						globalStore.removeTempTableByName(tableName);
					}
					info.setState(success?MatState.LOADED:MatState.FAILED_LOAD);
					if (table.getCacheHint().getTtl() != null) {
						info.setTtl(table.getCacheHint().getTtl());
					}
				}
			} else {
				table = globalStore.getOrCreateTempTable(tableName, query, bufferManager, false);
			}
		} else {
			table = contextStore.getOrCreateTempTable(tableName, query, bufferManager, true);
		}
		//convert to the actual table symbols (this is typically handled by the languagebridgefactory
		ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
			@Override
			public Expression replaceExpression(Expression element) {
				if (element instanceof ElementSymbol) {
					ElementSymbol es = (ElementSymbol)element;
					((ElementSymbol) element).setName(tableName + ElementSymbol.SEPARATOR + es.getShortName());
				}
				return element;
			}
		};
		PostOrderNavigator.doVisit(query, emv);
		return table.createTupleSource(query.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
	}

	public Object lookupCodeValue(CommandContext context, String codeTableName,
			String returnElementName, String keyElementName, Object keyValue)
			throws BlockedException, TeiidComponentException,
			TeiidProcessingException {
    	ElementSymbol keyElement = new ElementSymbol(keyElementName);
    	ElementSymbol returnElement = new ElementSymbol(returnElementName);
    	
    	QueryMetadataInterface metadata = context.getMetadata();
    	
    	keyElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + keyElementName))));
    	returnElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + returnElementName))));
    	
    	String matTableName = CODE_PREFIX + (codeTableName + ElementSymbol.SEPARATOR + keyElementName + ElementSymbol.SEPARATOR + returnElementName).toUpperCase(); 
    	TempMetadataID id = context.getGlobalTableStore().getMetadataStore().addTempGroup(matTableName, Arrays.asList(keyElement, returnElement), false, true);
    	String queryString = Reserved.SELECT + ' ' + keyElementName + " ," + returnElementName + ' ' + Reserved.FROM + ' ' + codeTableName; //$NON-NLS-1$ 
    	id.setQueryNode(new QueryNode(matTableName, queryString));
    	id.setPrimaryKey(id.getElements().subList(0, 1));
    	CacheHint hint = new CacheHint(true, null);
    	id.setCacheHint(hint);
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

}
