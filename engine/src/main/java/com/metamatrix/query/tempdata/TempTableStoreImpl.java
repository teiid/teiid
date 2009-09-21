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

package com.metamatrix.query.tempdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.processor.proc.UpdateCountTupleSource;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.TupleCollector;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Create;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Drop;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.TypeRetrievalUtil;

/** 
 * @since 5.5
 */
public class TempTableStoreImpl implements TempTableStore {
	
    private abstract class UpdateTupleSource implements TupleSource {
		private final String groupKey;
		private final TupleSourceID tsId;
		private final TupleSource ts;
		protected final Map lookup;
		private final TupleCollector tc;
		private final TupleSourceID newTs;
		protected final Evaluator eval;
		private final Criteria crit;
		protected int updateCount = 0;
		private boolean done;

		private UpdateTupleSource(String groupKey, TupleSourceID tsId, Criteria crit) throws TupleSourceNotFoundException, MetaMatrixComponentException {
			this.groupKey = groupKey;
			this.tsId = tsId;
			this.ts = buffer.getTupleSource(tsId);
    		List columns = buffer.getTupleSchema(tsId);
			this.lookup = RelationalNode.createLookupMap(columns);
			this.newTs = buffer.createTupleSource(columns, TypeRetrievalUtil.getTypeNames(columns), sessionID, TupleSourceType.PROCESSOR);
			this.tc = new TupleCollector(newTs, buffer);
			this.eval = new Evaluator(lookup, null, null);
			this.crit = crit;
		}

		@Override
		public List<?> nextTuple() throws MetaMatrixComponentException,
				MetaMatrixProcessingException {
			if (done) {
				return null;
			}
			
			List<?> tuple = null;
			//still have to worry about blocked exceptions...
			while ((tuple = ts.nextTuple()) != null) {
				if (eval.evaluate(crit, tuple)) {
					tuplePassed(tuple);
				} else {
					tupleFailed(tuple);
				}
			}
			tc.close();
			groupToTupleSourceID.put(groupKey, newTs);
			try {
		        buffer.removeTupleSource(tsId);
		    }catch(TupleSourceNotFoundException e) {
		    	
		    }
		    done = true;
			return Arrays.asList(updateCount);
		}
		
		protected void addTuple(List<?> tuple) throws MetaMatrixComponentException {
			try {
				tc.addTuple(tuple);
			} catch (TupleSourceNotFoundException e) {
				throw new MetaMatrixComponentException(e);
			}
		}

		protected abstract void tuplePassed(List<?> tuple) throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException;
		
		protected abstract void tupleFailed(List<?> tuple) throws MetaMatrixComponentException;

		@Override
		public List<SingleElementSymbol> getSchema() {
			return Command.getUpdateCommandSymbol();
		}

		@Override
		public void closeSource() throws MetaMatrixComponentException {
			
		}
	}

	private BufferManager buffer;
    private TempMetadataStore tempMetadataStore = new TempMetadataStore();
    private Map<String, TupleSourceID> groupToTupleSourceID = new HashMap<String, TupleSourceID>();
    private String sessionID;
    private TempTableStore parentTempTableStore;
    
    public TempTableStoreImpl(BufferManager buffer, String sessionID, TempTableStore parentTempTableStore) {
        this.buffer = buffer;
        this.sessionID = sessionID;
        this.parentTempTableStore = parentTempTableStore;
    }

    public void addTempTable(String tempTableName, List columns, boolean removeExistingTable) throws MetaMatrixComponentException, QueryProcessingException{        
        if(tempMetadataStore.getTempGroupID(tempTableName) != null) {
            if(!removeExistingTable) {
                throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_exist_error", tempTableName));//$NON-NLS-1$
            }
            removeTempTableByName(tempTableName);
        }
        
        //add metadata
        tempMetadataStore.addTempGroup(tempTableName, columns, false, true);
        //create tuple source
        TupleSourceID tsId = buffer.createTupleSource(columns, TypeRetrievalUtil.getTypeNames(columns), sessionID, TupleSourceType.PROCESSOR);
        try {
            buffer.setStatus(tsId, TupleSourceStatus.FULL);
        }catch(TupleSourceNotFoundException e) {
            //should never happen because the TupleSourceID is just created
            Assertion.failed("Could not find local tuple source for inserting into temp table."); //$NON-NLS-1$
        }
        groupToTupleSourceID.put(tempTableName, tsId);
    }

    public void removeTempTable(Command command) throws MetaMatrixComponentException{
        if(command.getType() == Command.TYPE_DROP) {
            String tempTableName = ((Drop)command).getTable().getName().toUpperCase();
            removeTempTableByName(tempTableName);
        }
    }
    
    public void removeTempTableByName(String tempTableName) throws MetaMatrixComponentException {
        tempMetadataStore.removeTempGroup(tempTableName);
        TupleSourceID tsId = this.groupToTupleSourceID.remove(tempTableName);
        if(tsId != null) {
            try {
                buffer.removeTupleSource(tsId);
            }catch(TupleSourceNotFoundException e) {}
        }      
    }
    
    public TempMetadataStore getMetadataStore() {
        return tempMetadataStore;
    }
        
    public TupleSource registerRequest(Command command) throws MetaMatrixComponentException, ExpressionEvaluationException, QueryProcessingException{
        if (command instanceof Query) {
            Query query = (Query)command;
            GroupSymbol group = (GroupSymbol)query.getFrom().getGroups().get(0);
            if (!group.isTempGroupSymbol()) {
            	return null;
            }
            TupleSourceID tsId = getTupleSourceID(group.getNonCorrelationName().toUpperCase(), command);
            try {
                return buffer.getTupleSource(tsId);
            }catch(TupleSourceNotFoundException e) {
                throw new MetaMatrixComponentException(e);
            }
        }
        if (command instanceof ProcedureContainer) {
        	GroupSymbol group = ((ProcedureContainer)command).getGroup();
        	if (!group.isTempGroupSymbol()) {
        		return null;
        	}
        	final String groupKey = group.getNonCorrelationName().toUpperCase();
            final TupleSourceID tsId = getTupleSourceID(groupKey, command);
        	if (command instanceof Insert) {
        		return addTuple((Insert)command, tsId);
        	}
        	try {
	        	if (command instanceof Update) {
	        		final Update update = (Update)command;
	        		final Criteria crit = update.getCriteria();
	        		return new UpdateTupleSource(groupKey, tsId, crit) {
	        			@Override
	        			protected void tuplePassed(List<?> tuple)
	        					throws ExpressionEvaluationException,
	        					BlockedException, MetaMatrixComponentException {
	        				List<Object> newTuple = new ArrayList<Object>(tuple);
		        			for (Map.Entry<ElementSymbol, Expression> entry : update.getChangeList().getClauseMap().entrySet()) {
		        				newTuple.set((Integer)lookup.get(entry.getKey()), eval.evaluate(entry.getValue(), tuple));
		        			}
		        			updateCount++;
		        			addTuple(newTuple);
	        			}
	        			
	        			protected void tupleFailed(java.util.List<?> tuple) throws MetaMatrixComponentException {
	        				addTuple(tuple);
	        			}
	        		};
	        	}
	        	if (command instanceof Delete) {
	        		final Delete delete = (Delete)command;
	        		final Criteria crit = delete.getCriteria();
	        		return new UpdateTupleSource(groupKey, tsId, crit) {
	        			@Override
	        			protected void tuplePassed(List<?> tuple)
	        					throws ExpressionEvaluationException,
	        					BlockedException, MetaMatrixComponentException {
	        				updateCount++;
	        			}
	        			
	        			protected void tupleFailed(java.util.List<?> tuple) throws MetaMatrixComponentException {
	        				addTuple(tuple);
	        			}
	        		};
	        	}
        	} catch (TupleSourceNotFoundException e) {
        		throw new MetaMatrixComponentException(e);
        	}
        }
    	if (command instanceof Create) {
    		addTempTable(((Create)command).getTable().getName().toUpperCase(), ((Create)command).getColumns(), false);
            return new UpdateCountTupleSource(0);	
    	}
    	if (command instanceof Drop) {
            removeTempTable(command);
            return new UpdateCountTupleSource(0);
    	}
        return null;
    }
    
    public void removeTempTables() throws MetaMatrixComponentException{
        for (String name : new ArrayList<String>( groupToTupleSourceID.keySet() )) {
            removeTempTableByName(name);
        }
    }
    
    private TupleSourceID getTupleSourceID(String tempTableID, Command command) throws MetaMatrixComponentException, QueryProcessingException{
        TupleSourceID tsID = groupToTupleSourceID.get(tempTableID);
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
        switch (command.getType()) {
        case Command.TYPE_QUERY:
            Query query = (Query)command;
            if(query.getInto() != null && query.getInto().getGroup().isImplicitTempGroupSymbol()) {
                columns = query.getSelect().getSymbols();
            }
            break;
        case Command.TYPE_INSERT:
            Insert insert = (Insert)command;
            GroupSymbol group = insert.getGroup();
            if(group.isImplicitTempGroupSymbol()) {
                columns = insert.getVariables();
            }
            break;
        }
        if (columns == null) {
        	throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_doesnt_exist_error", tempTableID)); //$NON-NLS-1$
        }
        addTempTable(tempTableID, columns, true);       
        return groupToTupleSourceID.get(tempTableID);
    }
    
    private TupleSource addTuple(Insert insert, TupleSourceID tsId) throws MetaMatrixComponentException, ExpressionEvaluationException {
        GroupSymbol group = insert.getGroup();
        int tuplesAdded = 0;
        try {
            int rowCount = buffer.getRowCount(tsId);
            TupleBatch tupleBatch;
            List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, new TempMetadataAdapter(null, tempMetadataStore));
            
            List<List<Object>> tuples = getBulkRows(insert, elements);
            
            tuplesAdded = tuples.size();

            // Buffer manager has 1 based index for the tuple sources
            tupleBatch = new TupleBatch((++rowCount), tuples);
            
            buffer.addTupleBatch(tsId, tupleBatch);
        } catch (TupleSourceNotFoundException err) {
            throw new MetaMatrixComponentException(err);
        } catch (BlockedException err) {
            throw new MetaMatrixComponentException(err);
        } catch (QueryMetadataException err) {
            throw new MetaMatrixComponentException(err);
        }        
        
        return new UpdateCountTupleSource(tuplesAdded);
    }

	public static List<List<Object>> getBulkRows(Insert insert, List<ElementSymbol> elements) throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
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
    
    public TupleSourceID getTupleSourceID(String tempTableName) {
    	return groupToTupleSourceID.get(tempTableName.toUpperCase());
    } 
}
