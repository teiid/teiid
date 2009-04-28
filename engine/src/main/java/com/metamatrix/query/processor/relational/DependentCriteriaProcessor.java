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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.CollectionValueIterator;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;

public class DependentCriteriaProcessor {
	
    public static class SetState {

        Collection<Object> replacement = new LinkedHashSet<Object>();

        Expression valueExpression;

        ValueIterator valueIterator;

        Object nextValue;

        boolean isNull;
    }

    class TupleState {

        private SortUtility sortUtility;
        private DependentValueSource dvs;
        private List<SetState> dependentSetStates = new LinkedList<SetState>();
        private String valueSource;

        public TupleState(String source) {
        	this.valueSource = source;
        }

        public void sort() throws BlockedException,
                   MetaMatrixComponentException {
            if (dvs == null) {
            	if (sortUtility == null) {
            		List<Expression> sortSymbols = new ArrayList<Expression>(dependentSetStates.size());
	                List<Boolean> sortDirection = new ArrayList<Boolean>(sortSymbols.size());
	                for (int i = 0; i < dependentSetStates.size(); i++) {
	                    sortDirection.add(Boolean.valueOf(OrderBy.ASC));
	                    sortSymbols.add(dependentSetStates.get(i).valueExpression);
	                }
	                DependentValueSource originalVs = (DependentValueSource)dependentNode.getContext().getVariableContext().getGlobalValue(valueSource);
	                TupleSource ts;
					try {
						ts = dependentNode.getBufferManager().getTupleSource(originalVs.getTupleSourceID());
					} catch (TupleSourceNotFoundException e) {
						throw new MetaMatrixComponentException(e);
					}
	                this.sortUtility = new SortUtility(originalVs.getTupleSourceID(), sortSymbols, sortDirection, true, dependentNode.getBufferManager(), dependentNode.getConnectionID());
            	}
            	dvs = new DependentValueSource(sortUtility.sort(), dependentNode.getBufferManager());
            	for (SetState setState : dependentSetStates) {
                    setState.valueIterator = dvs.getValueIterator(setState.valueExpression);
    			}
            }
        }
        
        public void close() throws MetaMatrixComponentException {
            if (dvs != null) {
            	sortUtility = null;
                try {
                    dependentNode.getBufferManager().removeTupleSource(dvs.getTupleSourceID());
                } catch (TupleSourceNotFoundException e) {
                }
                dvs = null;
            }
        }
                
        public List<SetState> getDepedentSetStates() {
            return dependentSetStates;
        }
        
    }
    
    private static final int SORT = 2;
    private static final int SET_PROCESSING = 3;

    //constructor state
    private int maxSetSize;
    private RelationalNode dependentNode;
    private Criteria dependentCrit;

    //initialization state
    private List<Criteria> queryCriteria;
    private Map<Integer, SetState> setStates = new HashMap<Integer, SetState>();
    private LinkedHashMap<String, TupleState> dependentState = new LinkedHashMap<String, TupleState>();
    private List<List<SetState>> sources = new ArrayList<List<SetState>>();

    // processing state
    private int phase = SORT;
    private LinkedList<Integer> restartIndexes = new LinkedList<Integer>();
    private int currentIndex;
    private boolean hasNextCommand;
    protected SubqueryAwareEvaluator eval;

    public DependentCriteriaProcessor(int maxSetSize, RelationalNode dependentNode, Criteria dependentCriteria) throws ExpressionEvaluationException, MetaMatrixComponentException {
        this.maxSetSize = maxSetSize;
        this.dependentNode = dependentNode;
        this.dependentCrit = dependentCriteria;
        this.eval = new SubqueryAwareEvaluator(Collections.emptyMap(), dependentNode.getDataManager(), dependentNode.getContext(), dependentNode.getBufferManager());
        queryCriteria = Criteria.separateCriteriaByAnd(dependentCrit);
        
        for (int i = 0; i < queryCriteria.size(); i++) {
        	Criteria criteria = queryCriteria.get(i);
            if (!(criteria instanceof AbstractSetCriteria)) {
                continue;
            }
            
            if (criteria instanceof SetCriteria) {
                SetCriteria setCriteria = (SetCriteria)criteria;
                if (setCriteria.isNegated() || setCriteria.getNumberOfValues() <= maxSetSize) {
                    continue;
                }
                SetState state = new SetState();
                setStates.put(i, state);
                LinkedHashSet<Object> values = new LinkedHashSet<Object>();
                for (Expression expr : (Collection<Expression>)setCriteria.getValues()) {
					values.add(eval.evaluate(expr, null));
				}
                state.valueIterator = new CollectionValueIterator(values);
                sources.add(Arrays.asList(state));
            } else if (criteria instanceof DependentSetCriteria) {
            	DependentSetCriteria dsc = (DependentSetCriteria)criteria;
                String source = dsc.getContextSymbol();
                
                SetState state = new SetState();
                setStates.put(i, state);
                state.valueExpression = dsc.getValueExpression();
                TupleState ts = dependentState.get(source);
                if (ts == null) {
                	ts = new TupleState(source);
                	dependentState.put(source, ts);
                    sources.add(ts.getDepedentSetStates());
                }
                ts.getDepedentSetStates().add(state);
            } 
        }        
    }

    public void close() throws MetaMatrixComponentException {
        if (dependentState != null) {
            for (TupleState state : dependentState.values()) {
				state.close();
			}
        }
        if (this.eval != null) {
        	this.eval.close();
        }
    }

    public Criteria prepareCriteria() throws MetaMatrixComponentException {

        if (phase == SORT) {
        	for (TupleState state : dependentState.values()) {
                state.sort();
            }

            phase = SET_PROCESSING;
        }

        replaceDependentValueIterators();
        
        LinkedList<Criteria> crits = new LinkedList<Criteria>();
        
        for (int i = 0; i < queryCriteria.size(); i++) {
        	SetState state = this.setStates.get(i);
        	if (state == null) {
        		crits.add((Criteria)queryCriteria.get(i).clone());
        	} else {
        		Criteria crit = replaceDependentCriteria((AbstractSetCriteria)queryCriteria.get(i), state);
        		if (crit == QueryRewriter.FALSE_CRITERIA) {
        			return QueryRewriter.FALSE_CRITERIA;
        		}
        		crits.add(crit);
        	}
        }
        
        if (crits.size() == 1) {
        	return crits.get(0);
        }
        return new CompoundCriteria(CompoundCriteria.AND, crits);
    }
    
    public void consumedCriteria() {
        // flush only the value iterators starting at the restart index
        // it is only safe to do this after the super call to prepare command
        if (restartIndexes.isEmpty()) {
            return;
        }
        
        int restartIndex = restartIndexes.removeLast().intValue();
        
        for (int i = restartIndex; i < sources.size(); i++) {

            List<SetState> source = sources.get(i);

            for (SetState setState : source) {
            	setState.replacement.clear();
            }
        }

        currentIndex = restartIndex;
    }

    /**
     * Replace the dependentSet value iterators with the next set of values from the independent tuple source
     * 
     * @param dependentSets
     * @param replacementValueIterators
     * @throws MetaMatrixComponentException
     */
    private void replaceDependentValueIterators() throws MetaMatrixComponentException {

        for (; currentIndex < sources.size(); currentIndex++) {

            List<SetState> source = sources.get(currentIndex);

            boolean done = false;

            while (!done) {

                boolean isNull = false;
                boolean lessThanMax = true;

                for (SetState state : source) {
                    if (state.nextValue == null && !state.isNull) {
                        if (state.valueIterator.hasNext()) {
                            state.nextValue = state.valueIterator.next();
                            state.isNull = state.nextValue == null;
                        } else {
                            state.valueIterator.reset();
                            done = true; // should be true for each iterator from this source
                            continue;
                        }
                    }

                    isNull |= state.isNull;
                    lessThanMax &= state.replacement.size() < maxSetSize;
                }

                if (done) {
                    if (!restartIndexes.isEmpty() && restartIndexes.getLast().intValue() == currentIndex) {
                        restartIndexes.removeLast();
                    }
                    break;
                }

                if (lessThanMax || isNull) {
                	for (SetState state : source) {
                        if (!isNull) {
                            state.replacement.add(state.nextValue);
                        }
                        state.nextValue = null;
                        state.isNull = false;
                    }
                } else {
                    restartIndexes.add(currentIndex);
                    done = true;
                }
            }
        }

        hasNextCommand = !restartIndexes.isEmpty();
    }

    protected boolean hasNextCommand() {
        return hasNextCommand;
    }
    
    public Criteria replaceDependentCriteria(AbstractSetCriteria crit, SetState state) {
    	if (state.replacement.isEmpty()) {
            // No values - return criteria that is always false
            return QueryRewriter.FALSE_CRITERIA;
    	}
    	if (state.replacement.size() == 1) {
    		return new CompareCriteria(crit.getExpression(), CompareCriteria.EQ, new Constant(state.replacement.iterator().next()));
    	}
        List vals = new ArrayList(state.replacement.size());
        for (Object val : state.replacement) {
            vals.add(new Constant(val));
        }
        
        SetCriteria sc = new SetCriteria();
        sc.setExpression(crit.getExpression());
        sc.setValues(vals);
        return sc;
    }

}
