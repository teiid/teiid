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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.execution.QueryExecPlugin;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.resolver.command.TempTableResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.util.CommandContext;

/** 
 * @since 5.5
 */
public class TempTableStore {

	private BufferManager buffer;
    private TempMetadataStore tempMetadataStore = new TempMetadataStore(new ConcurrentHashMap<String, TempMetadataID>());
    private Map<String, TempTable> groupToTupleSourceID = new ConcurrentHashMap<String, TempTable>();
    private String sessionID;
    private TempTableStore parentTempTableStore;
    
    public TempTableStore(BufferManager buffer, String sessionID) {
        this.buffer = buffer;
        this.sessionID = sessionID;
    }
    
    public void setParentTempTableStore(TempTableStore parentTempTableStore) {
		this.parentTempTableStore = parentTempTableStore;
	}

    void addTempTable(String tempTableName, Create create) {
    	List<ElementSymbol> columns = create.getColumns();
    	
        //add metadata
        TempMetadataID id = tempMetadataStore.addTempGroup(tempTableName, columns, false, true);
        TempTableResolver.addPrimaryKey(create, id);
    	columns = new ArrayList<ElementSymbol>(create.getColumns());
        if (!create.getPrimaryKey().isEmpty()) {
    		//reorder the columns to put the key in front
    		List<ElementSymbol> primaryKey = create.getPrimaryKey();
    		columns.removeAll(primaryKey);
    		columns.addAll(0, primaryKey);
    	}
        TempTable tempTable = new TempTable(id, buffer, columns, create.getPrimaryKey().size(), sessionID);
        groupToTupleSourceID.put(tempTableName, tempTable);
    }

    public void removeTempTableByName(String tempTableName) {
        tempMetadataStore.removeTempGroup(tempTableName);
        TempTable table = this.groupToTupleSourceID.remove(tempTableName);
        if(table != null) {
            table.remove();
        }      
    }
    
    public TempMetadataStore getMetadataStore() {
        return tempMetadataStore;
    }
        
    public TupleSource registerRequest(CommandContext context, Command command) throws TeiidComponentException, TeiidProcessingException{
        if (command instanceof Query) {
            Query query = (Query)command;
            GroupSymbol group = query.getFrom().getGroups().get(0);
            if (!group.isTempGroupSymbol()) {
            	return null;
            }
            final String tableName = group.getNonCorrelationName().toUpperCase();
            TempTable table = getOrCreateTempTable(tableName, command, true);
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
            return table.createTupleSource(command.getProjectedSymbols(), query.getCriteria(), query.getOrderBy());
        }
        if (command instanceof ProcedureContainer) {
        	GroupSymbol group = ((ProcedureContainer)command).getGroup();
        	if (!group.isTempGroupSymbol()) {
        		return null;
        	}
        	final String groupKey = group.getNonCorrelationName().toUpperCase();
            final TempTable table = getOrCreateTempTable(groupKey, command, false);
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
    		if(tempMetadataStore.getTempGroupID(tempTableName) != null) {
                throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_exist_error", tempTableName));//$NON-NLS-1$
            }
    		addTempTable(tempTableName, create);
            return CollectionTupleSource.createUpdateCountTupleSource(0);	
    	}
    	if (command instanceof Drop) {
    		String tempTableName = ((Drop)command).getTable().getCanonicalName();
            removeTempTableByName(tempTableName);
            return CollectionTupleSource.createUpdateCountTupleSource(0);
    	}
        return null;
    }
    
    public void removeTempTables() {
        for (String name : groupToTupleSourceID.keySet()) {
            removeTempTableByName(name);
        }
    }
    
    private TempTable getOrCreateTempTable(String tempTableID, Command command, boolean delegate) throws QueryProcessingException{
    	TempTable tsID = groupToTupleSourceID.get(tempTableID);
        if(tsID != null) {
            return tsID;
        }
        if(delegate && this.parentTempTableStore != null){
    		tsID = this.parentTempTableStore.groupToTupleSourceID.get(tempTableID);
    	    if(tsID != null) {
    	        return tsID;
    	    }
        }
        //allow implicit temp group definition
        List<ElementSymbol> columns = null;
        if (command instanceof Insert) {
            Insert insert = (Insert)command;
            GroupSymbol group = insert.getGroup();
            if(group.isImplicitTempGroupSymbol()) {
                columns = insert.getVariables();
            }
        }
        if (columns == null) {
        	throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_doesnt_exist_error", tempTableID)); //$NON-NLS-1$
        }
        Create create = new Create();
        create.setTable(new GroupSymbol(tempTableID));
        create.setColumns(columns);
        addTempTable(tempTableID, create);       
        return groupToTupleSourceID.get(tempTableID);
    }
    
    public Set<String> getAllTempTables() {
        return new HashSet<String>(this.groupToTupleSourceID.keySet());
    }
    
}
