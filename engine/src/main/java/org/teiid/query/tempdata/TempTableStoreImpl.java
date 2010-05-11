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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.execution.QueryExecPlugin;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;


/** 
 * @since 5.5
 */
public class TempTableStoreImpl implements TempTableStore {
	
    private abstract class UpdateTupleSource implements TupleSource {
		private final String groupKey;
		private final TupleBuffer oldBuffer;
		private final TupleSource ts;
		protected final Map lookup;
		private final TupleBuffer newBuffer;
		protected final Evaluator eval;
		private final Criteria crit;
		protected int updateCount = 0;
		private boolean done;
		private List<?> currentTuple;

		private UpdateTupleSource(String groupKey, TupleBuffer tsId, Criteria crit) throws TeiidComponentException {
			this.groupKey = groupKey;
			this.oldBuffer = tsId;
			this.ts = tsId.createIndexedTupleSource();
    		List columns = tsId.getSchema();
			this.lookup = RelationalNode.createLookupMap(columns);
			this.newBuffer = buffer.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
			this.eval = new Evaluator(lookup, null, null);
			this.crit = crit;
		}

		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			if (done) {
				return null;
			}
			
			//still have to worry about blocked exceptions...
			while (currentTuple != null || (currentTuple = ts.nextTuple()) != null) {
				if (eval.evaluate(crit, currentTuple)) {
					tuplePassed(currentTuple);
				} else {
					tupleFailed(currentTuple);
				}
				currentTuple = null;
			}
			newBuffer.close();
			groupToTupleSourceID.put(groupKey, newBuffer);
	        oldBuffer.remove();
		    done = true;
			return Arrays.asList(updateCount);
		}
		
		protected void addTuple(List<?> tuple) throws TeiidComponentException {
			newBuffer.addTuple(tuple);
		}

		protected abstract void tuplePassed(List<?> tuple) throws ExpressionEvaluationException, BlockedException, TeiidComponentException;
		
		protected abstract void tupleFailed(List<?> tuple) throws TeiidComponentException;

		@Override
		public List<SingleElementSymbol> getSchema() {
			return Command.getUpdateCommandSymbol();
		}

		@Override
		public void closeSource() {
			
		}
		
		@Override
		public int available() {
			return 0;
		}
	}

	private BufferManager buffer;
    private TempMetadataStore tempMetadataStore = new TempMetadataStore();
    private Map<String, TupleBuffer> groupToTupleSourceID = new HashMap<String, TupleBuffer>();
    private String sessionID;
    private TempTableStore parentTempTableStore;
    
    public TempTableStoreImpl(BufferManager buffer, String sessionID, TempTableStore parentTempTableStore) {
        this.buffer = buffer;
        this.sessionID = sessionID;
        this.parentTempTableStore = parentTempTableStore;
    }

    public void addTempTable(String tempTableName, List columns, boolean removeExistingTable) throws TeiidComponentException, QueryProcessingException{        
        if(tempMetadataStore.getTempGroupID(tempTableName) != null) {
            if(!removeExistingTable) {
                throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_exist_error", tempTableName));//$NON-NLS-1$
            }
            removeTempTableByName(tempTableName);
        }
        
        //add metadata
        tempMetadataStore.addTempGroup(tempTableName, columns, false, true);
        //create tuple source
        TupleBuffer tupleBuffer = buffer.createTupleBuffer(columns, sessionID, TupleSourceType.PROCESSOR);
        tupleBuffer.setFinal(true); //final, but not closed so that we can append on insert
        groupToTupleSourceID.put(tempTableName, tupleBuffer);
    }

    public void removeTempTableByName(String tempTableName) throws TeiidComponentException {
        tempMetadataStore.removeTempGroup(tempTableName);
        TupleBuffer tsId = this.groupToTupleSourceID.remove(tempTableName);
        if(tsId != null) {
            tsId.remove();
        }      
    }
    
    public TempMetadataStore getMetadataStore() {
        return tempMetadataStore;
    }
        
    public TupleSource registerRequest(Command command) throws TeiidComponentException, ExpressionEvaluationException, QueryProcessingException{
        if (command instanceof Query) {
            Query query = (Query)command;
            GroupSymbol group = (GroupSymbol)query.getFrom().getGroups().get(0);
            if (!group.isTempGroupSymbol()) {
            	return null;
            }
            TupleBuffer tsId = getTupleSourceID(group.getNonCorrelationName().toUpperCase(), command);
            return tsId.createIndexedTupleSource();
        }
        if (command instanceof ProcedureContainer) {
        	GroupSymbol group = ((ProcedureContainer)command).getGroup();
        	if (!group.isTempGroupSymbol()) {
        		return null;
        	}
        	final String groupKey = group.getNonCorrelationName().toUpperCase();
            final TupleBuffer tsId = getTupleSourceID(groupKey, command);
        	if (command instanceof Insert) {
        		return addTuple((Insert)command, tsId);
        	}
        	if (command instanceof Update) {
        		final Update update = (Update)command;
        		final Criteria crit = update.getCriteria();
        		return new UpdateTupleSource(groupKey, tsId, crit) {
        			@Override
        			protected void tuplePassed(List<?> tuple)
        					throws ExpressionEvaluationException,
        					BlockedException, TeiidComponentException {
        				List<Object> newTuple = new ArrayList<Object>(tuple);
	        			for (Map.Entry<ElementSymbol, Expression> entry : update.getChangeList().getClauseMap().entrySet()) {
	        				newTuple.set((Integer)lookup.get(entry.getKey()), eval.evaluate(entry.getValue(), tuple));
	        			}
	        			updateCount++;
	        			addTuple(newTuple);
        			}
        			
        			protected void tupleFailed(java.util.List<?> tuple) throws TeiidComponentException {
        				addTuple(tuple);
        			}
        		};
        	}
        	if (command instanceof Delete) {
        		final Delete delete = (Delete)command;
        		final Criteria crit = delete.getCriteria();
        		if (crit == null) {
        			int rows = tsId.getRowCount();
                    addTempTable(groupKey, tsId.getSchema(), true);
                    return CollectionTupleSource.createUpdateCountTupleSource(rows);
        		}
        		return new UpdateTupleSource(groupKey, tsId, crit) {
        			@Override
        			protected void tuplePassed(List<?> tuple)
        					throws ExpressionEvaluationException,
        					BlockedException, TeiidComponentException {
        				updateCount++;
        			}
        			
        			protected void tupleFailed(java.util.List<?> tuple) throws TeiidComponentException {
        				addTuple(tuple);
        			}
        		};
        	}
        }
    	if (command instanceof Create) {
    		addTempTable(((Create)command).getTable().getName().toUpperCase(), ((Create)command).getColumns(), false);
            return CollectionTupleSource.createUpdateCountTupleSource(0);	
    	}
    	if (command instanceof Drop) {
    		String tempTableName = ((Drop)command).getTable().getName().toUpperCase();
            removeTempTableByName(tempTableName);
            return CollectionTupleSource.createUpdateCountTupleSource(0);
    	}
        return null;
    }
    
    public void removeTempTables() throws TeiidComponentException{
        for (String name : new ArrayList<String>( groupToTupleSourceID.keySet() )) {
            removeTempTableByName(name);
        }
    }
    
    private TupleBuffer getTupleSourceID(String tempTableID, Command command) throws TeiidComponentException, QueryProcessingException{
        TupleBuffer tsID = groupToTupleSourceID.get(tempTableID);
        if(tsID != null) {
            return tsID;
        }
        if(this.parentTempTableStore != null){
    		tsID = this.parentTempTableStore.getTupleSourceID(tempTableID);
    	    if(tsID != null) {
    	        return tsID;
    	    }
        }
        //allow implicit temp group definition
        List columns = null;
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
        addTempTable(tempTableID, columns, true);       
        return groupToTupleSourceID.get(tempTableID);
    }
    
    private TupleSource addTuple(Insert insert, TupleBuffer tsId) throws TeiidComponentException, ExpressionEvaluationException {
        GroupSymbol group = insert.getGroup();
        int tuplesAdded = 0;
        try {
            List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, new TempMetadataAdapter(null, tempMetadataStore));
            
            List<List<Object>> tuples = getBulkRows(insert, elements);
            
            for (List<Object> list : tuples) {
				tsId.addTuple(list);
			}
            //TODO: this leads to fragmented batches, which may require recreating the table
            tsId.saveBatch();
            
            tuplesAdded = tuples.size();
        } catch (QueryMetadataException err) {
            throw new TeiidComponentException(err);
        }        
        
        return CollectionTupleSource.createUpdateCountTupleSource(tuplesAdded);
    }

	public static List<List<Object>> getBulkRows(Insert insert, List<ElementSymbol> elements) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
		int bulkRowCount = 1;
		if (insert.isBulk()) {
			Constant c = (Constant)insert.getValues().get(0);
			bulkRowCount = ((List<?>)c.getValue()).size();
		}
		
		List<List<Object>> tuples = new ArrayList<List<Object>>(bulkRowCount);
		
		for (int row = 0; row < bulkRowCount; row++) {
			List<Object> currentRow = new ArrayList<Object>(insert.getValues().size());
			for (ElementSymbol symbol : elements) {
                int index = insert.getVariables().indexOf(symbol);
                Object value = null;
                if (index != -1) {
                	if (insert.isBulk()) {
	                	Constant multiValue = (Constant)insert.getValues().get(index);
	    		    	value = ((List<?>)multiValue.getValue()).get(row);
                	} else {
                		Expression expr = (Expression)insert.getValues().get(index);
                        value = Evaluator.evaluate(expr);
                	}
                }
                currentRow.add(value);
            }
		    tuples.add(currentRow);
		}
		return tuples;
	}
    
    public boolean hasTempTable(Command command){
        switch (command.getType()) {
            case Command.TYPE_INSERT:
            {
                Insert insert = (Insert)command;
                GroupSymbol group = insert.getGroup();
                return group.isTempGroupSymbol();
            }
            case Command.TYPE_QUERY:
            {
                if(command instanceof Query) {
                    Query query = (Query)command;
                    GroupSymbol group = (GroupSymbol)query.getFrom().getGroups().get(0);
                    return group.isTempGroupSymbol(); 
                }
                break;
            }
            case Command.TYPE_CREATE:
                return true;
            case Command.TYPE_DROP:
                return true;
        }
        return false;
    }
    
    public Set<String> getAllTempTables() {
        return new HashSet<String>(this.groupToTupleSourceID.keySet());
    }
    
    public TupleBuffer getTupleSourceID(String tempTableName) {
    	return groupToTupleSourceID.get(tempTableName.toUpperCase());
    } 
}
