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

package com.metamatrix.query.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CommandContainer;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.TranslatableProcedureContainer;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.util.CommandContext;

public class FakeDataManager implements ProcessorDataManager {
	private Map tuples = new HashMap();
    private static final String LOG_CONTEXT = "FAKE_DATA_MANAGER"; //$NON-NLS-1$
    
    //used to test blocked exception. If true,
    //the first time nextTuple is called on FakeTupleSource,
    //it will throws BlockedExceptiom
    private boolean blockOnce;

    // ---- Cached code table stuff ---- 
    	
    // upper table name + upper key col name + upper ret col name -> map of values
    private Map codeTableValues = new HashMap();
    
    // throw Blocked on first request
    private boolean throwBlocked = false;
    
    // upper table name + upper key col name + upper ret col name -> flag of whether this table has blocked yet
    private Map blockedState = new HashMap();

    // Track history to verify it later
    private List<String> queries = new ArrayList<String>();
    
	public FakeDataManager() {
	}
    
    /**
     * Return string form of all queries run against this FDM 
     * @return List<String>
     */
    public List<String> getQueries() {
        return this.queries;
    }
	        
	public void registerTuples(Object groupID, List elements, List[] data) {
		tuples.put(groupID, new Object[] { elements, data });
	}
	
	public void closeRequest(Object requestID) {
		// does nothing?
    } 
	
	public TupleSource registerRequest(Object processorID, Command command, String modelName, String connectorBindingId, int nodeID)
		throws MetaMatrixComponentException {
        
        LogManager.logTrace(LOG_CONTEXT, new Object[]{"Register Request:", command, ",processorID:", processorID, ",model name:", modelName,",TupleSourceID nodeID:",new Integer(nodeID)}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        
        if ( command instanceof CommandContainer ) {
        	if ( ((CommandContainer) command).getContainedCommands() != null && ((CommandContainer) command).getContainedCommands().size() > 0 ) {
            	for ( Iterator<Command> it = ((CommandContainer) command).getContainedCommands().iterator(); it.hasNext(); ) {
            		this.queries.add(it.next().toString());
            	}
        	} else {
        		this.queries.add(command.toString());
        	}
        } else {
        	this.queries.add(command.toString());
        }

        if (ReferenceCollectorVisitor.getReferences(command).size() > 0) {
            throw new IllegalArgumentException("Found references in the command registered with the DataManager."); //$NON-NLS-1$
        }
		// Get group ID from atomic command
		GroupSymbol group = null;
		if(command instanceof Query){
			group = getQueryGroup((Query)command);    
        }else if(command instanceof SetQuery) {
            SetQuery union = (SetQuery) command;            
            group = getQueryGroup(union.getProjectedQuery());
		} else if (command instanceof ProcedureContainer) {
			group = ((ProcedureContainer) command).getGroup();
		} else if ( command instanceof BatchedUpdateCommand ) {
        	if ( ((CommandContainer) command).getContainedCommands() != null && ((CommandContainer) command).getContainedCommands().size() > 0 ) {
        		if ( command.getSubCommands().get(0) instanceof Update ) {
        			group = ((Update)command.getSubCommands().get(0)).getGroup();
        		}
        	}
		}
		
		Object groupID = group.getMetadataID();
		
		Object[] tupleInfo = (Object[]) tuples.get(groupID);
		List elements = (List) tupleInfo[0];
		List[] tuples = (List[]) tupleInfo[1];
		
		List projectedSymbols = command.getProjectedSymbols();
		int[] columnMap = getColumnMap(elements, projectedSymbols);
		
		/* 
		*  updateCommands is used to hold a list of commands that 
		*  either came from a BatchedUpdateCommand or a signle 
		*  command from an Update command.
		*/
		List<TranslatableProcedureContainer> updateCommands = new ArrayList<TranslatableProcedureContainer>();
		
		// Apply query criteria to tuples
		if(command instanceof Query){
			Query query = (Query)command;
			if(query.getCriteria() != null) {
			    // Build lookupMap from BOTH all the elements and the projected symbols - both may be needed here
	            Map lookupMap = new HashMap();
	            for(int i=0; i<elements.size(); i++) { 
	                Object element = elements.get(i);
                    mapElementToIndex(lookupMap, element, new Integer(i), group);        
	            }
	            for(int i=0; i<projectedSymbols.size(); i++) { 
	            	Object element = projectedSymbols.get(i);
                    mapElementToIndex(lookupMap, element, new Integer(columnMap[i]), group);
	            }
			    
			    List filteredTuples = new ArrayList();
			    for(int i=0; i<tuples.length; i++) {
	                try {
	    				if(new Evaluator(lookupMap, null, null).evaluate(query.getCriteria(), tuples[i])) {
	                        filteredTuples.add(tuples[i]);
	                    }
	                } catch(CriteriaEvaluationException e) {
	                    throw new MetaMatrixComponentException(e, e.getMessage());
	                }
			    }
			    
			    tuples = new List[filteredTuples.size()];
			    filteredTuples.toArray(tuples);
			}
		} else if ( command instanceof TranslatableProcedureContainer ) {
			// add single update command to a list to be executed
			updateCommands.add((TranslatableProcedureContainer)command);
		} else if ( command instanceof BatchedUpdateCommand ) {
        	if ( ((CommandContainer) command).getContainedCommands() != null && ((CommandContainer) command).getContainedCommands().size() > 0 )
				// add all update commands to a list to be executed
        		for ( int i = 0; i < ((CommandContainer) command).getContainedCommands().size(); i++ ) 
        			if ( ((CommandContainer) command).getContainedCommands().get(i) instanceof TranslatableProcedureContainer ) {
        				updateCommands.add(((TranslatableProcedureContainer) ((CommandContainer) command).getContainedCommands().get(i)));
        		}
		}
		
		// if we had update commands added to the list, execute them now
		if ( updateCommands.size() > 0 ) {
		    List<List<Integer>> filteredTuples = new ArrayList<List<Integer>>();
			for ( int c = 0; c < updateCommands.size(); c++ ) {
				TranslatableProcedureContainer update = updateCommands.get(c);
				if ( update.getCriteria() != null ) {
				    // Build lookupMap from BOTH all the elements and the projected symbols - both may be needed here
		            Map<Object, Integer> lookupMap = new HashMap<Object, Integer>();
		            for(int i=0; i<elements.size(); i++) { 
		                Object element = elements.get(i);
	                    mapElementToIndex(lookupMap, element, new Integer(i), group);        
		            }
		            for(int i=0; i<projectedSymbols.size(); i++) { 
		            	Object element = projectedSymbols.get(i);
	                    mapElementToIndex(lookupMap, element, new Integer(columnMap[i]), group);
		            }
				    
				    int updated = 0;
				    for(int i=0; i<tuples.length; i++) {
		                try {
		    				if(new Evaluator(lookupMap, null, null).evaluate(update.getCriteria(), tuples[i])) {
		                        updated++;
		                    }
		                } catch(CriteriaEvaluationException e) {
		                    throw new MetaMatrixComponentException(e, e.getMessage());
		                }
				    }
			    	List<Integer> updateTuple = new ArrayList<Integer>(1);
			    	updateTuple.add( new Integer(updated) );
                    filteredTuples.add(updateTuple);
				}
			}
		    tuples = new List[filteredTuples.size()];
		    filteredTuples.toArray(tuples);
		    elements = new ArrayList<Object>(projectedSymbols);
		    columnMap[0] = 0;
		}		
				
        FakeTupleSource ts= new FakeTupleSource(elements, tuples, projectedSymbols, columnMap);
		if(this.blockOnce){
            ts.setBlockOnce();
		}
        return ts;
	}
    
    private GroupSymbol getQueryGroup(Query query) throws MetaMatrixComponentException {
        GroupSymbol group;
        From from = query.getFrom();
        List groups = from.getGroups();
        if(groups.size() != 1) { 
        	throw new MetaMatrixComponentException("Cannot build fake tuple source for command: " + query);	 //$NON-NLS-1$
        }
        group = (GroupSymbol) groups.get(0);
        Iterator projSymbols = query.getSelect().getProjectedSymbols().iterator();
        while (projSymbols.hasNext()) {
            Object symbol = projSymbols.next();
            if (symbol instanceof ElementSymbol){
                ElementSymbol elementSymbol = (ElementSymbol)symbol;
                GroupSymbol g = elementSymbol.getGroupSymbol();
                if (!g.equals(group)){
                    throw new MetaMatrixComponentException("Illegal symbol " + elementSymbol + " in SELECT of command: " + query);    //$NON-NLS-1$ //$NON-NLS-2$
                }
                if (elementSymbol.getMetadataID() == null){
                    throw new MetaMatrixComponentException("Illegal null metadata ID in ElementSymbol " + elementSymbol + " in SELECT of command: " + query);    //$NON-NLS-1$ //$NON-NLS-2$
                } else if (elementSymbol.getMetadataID() instanceof TempMetadataID){
                    throw new MetaMatrixComponentException("Illegal TempMetadataID in ElementSymbol " + elementSymbol + " in SELECT of command: " + query);    //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
        }
        return group;
    }

    /**
     * @param lookupMap
     * @param element
     * @param integer
     * @param group
     */
    private void mapElementToIndex(Map lookupMap, Object element, Integer index, GroupSymbol group) {
        if (group.getDefinition() != null){
            String groupAlias = group.getCanonicalName();
            ElementSymbol elementSymbol = (ElementSymbol)SymbolMap.getExpression((SingleElementSymbol)element);
            String newName = groupAlias + "." + elementSymbol.getShortName(); //$NON-NLS-1$
            ElementSymbol aliasedElement = new ElementSymbol(newName, elementSymbol.getDisplayFullyQualified());
            aliasedElement.setGroupSymbol(elementSymbol.getGroupSymbol());
            aliasedElement.setMetadataID(elementSymbol.getMetadataID());
            aliasedElement.setType(elementSymbol.getType());
            lookupMap.put(aliasedElement, index);
        } else {
            lookupMap.put(element, index);
        }
    }    
	
	//   columnMap[expectedElementIndex] = allElementIndex
	private int[] getColumnMap(List allElements, List expectedElements) {
		int[] map = new int[expectedElements.size()];
		
		for(int i=0; i<expectedElements.size(); i++) { 
		    SingleElementSymbol symbol = (SingleElementSymbol) expectedElements.get(i);
		    
		    if (symbol instanceof AliasSymbol) {
		        symbol = ((AliasSymbol)symbol).getSymbol();
		    }
		    
		    String shortName = symbol.getShortName();
		    
		    // Find matching short name in all elements
		    boolean foundMatch = false;
		    for(int j=0; j<allElements.size(); j++) { 
				SingleElementSymbol tupleSymbol = (SingleElementSymbol) allElements.get(j);
				if(tupleSymbol.getShortName().equalsIgnoreCase(shortName)) {
				    map[i] = j;
				    foundMatch = true;
				    break;
				}
		    }
		    
		    if(! foundMatch) { 
                map[i] = -1;
		    }
		}
		
		return map;
	}


    public void setThrowBlocked(boolean throwBlocked) {
        this.throwBlocked = throwBlocked;
    }

    public void defineCodeTable(String tableName, String keyCol, String retCol, Map values) {
        String key = tableName.toUpperCase() + keyCol.toUpperCase() + retCol.toUpperCase();
        this.codeTableValues.put(key, values);
        this.blockedState.put(key, Boolean.FALSE);                      
    }
	
    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, MetaMatrixComponentException {
            
            String tableKey = codeTableName.toUpperCase() + keyElementName.toUpperCase() + returnElementName.toUpperCase();
            if(! codeTableValues.containsKey(tableKey)) {
                throw new MetaMatrixComponentException("Unknown code table: " + codeTableName); //$NON-NLS-1$
            }
        
            if(throwBlocked) {
                if(blockedState.get(tableKey).equals(Boolean.FALSE)) { 
                    blockedState.put(tableKey, Boolean.TRUE);
                    throw BlockedException.INSTANCE;
                }
            }
        
            Map values = (Map) codeTableValues.get(tableKey);
            return values.get(keyValue);
    }

    public void setBlockOnce() {
        blockOnce = true;
    }
    
    @Override
    public void clearCodeTables() {
    	
    }

}