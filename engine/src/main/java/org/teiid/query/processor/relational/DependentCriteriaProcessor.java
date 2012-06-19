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

package org.teiid.query.processor.relational;

import java.util.*;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.CollectionValueIterator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.ValueIterator;


public class DependentCriteriaProcessor {
	
    public static class SetState {

        Collection<Object> replacement = new LinkedHashSet<Object>();

        Expression valueExpression;

        ValueIterator valueIterator;

        Object nextValue;

        boolean isNull;
        
        float maxNdv = NewCalculateCostUtil.UNKNOWN_VALUE;
        
        boolean overMax;
    }

    class TupleState {

        private SortUtility sortUtility;
        private DependentValueSource dvs;
        private List<SetState> dependentSetStates = new LinkedList<SetState>();
        private String valueSource;
        private DependentValueSource originalVs;

        public TupleState(String source) {
        	this.valueSource = source;
        }

        public void sort() throws BlockedException,
                   TeiidComponentException, TeiidProcessingException {
            if (dvs == null) {
                originalVs = (DependentValueSource)dependentNode.getContext().getVariableContext().getGlobalValue(valueSource);
                if (!originalVs.isDistinct()) {
	            	if (sortUtility == null) {
	            		List<SingleElementSymbol> sortSymbols = new ArrayList<SingleElementSymbol>(dependentSetStates.size());
		                List<Boolean> sortDirection = new ArrayList<Boolean>(sortSymbols.size());
		                for (int i = 0; i < dependentSetStates.size(); i++) {
		                    sortDirection.add(Boolean.valueOf(OrderBy.ASC));
		                    sortSymbols.add((SingleElementSymbol)dependentSetStates.get(i).valueExpression);
		                }
		                this.sortUtility = new SortUtility(originalVs.getTupleBuffer().createIndexedTupleSource(), sortSymbols, sortDirection, Mode.DUP_REMOVE, dependentNode.getBufferManager(), dependentNode.getConnectionID(), originalVs.getTupleBuffer().getSchema());
	            	}
	            	dvs = new DependentValueSource(sortUtility.sort());
                } else {
                	dvs = originalVs;
                }
            	for (SetState setState : dependentSetStates) {
                    setState.valueIterator = dvs.getValueIterator(setState.valueExpression);
                    int distinctCount = dvs.getTupleBuffer().getRowCount();
                    if (setState.maxNdv <= 0 || setState.maxNdv >= distinctCount) {
                    	continue;
                    }
                    if (dvs.getTupleBuffer().getSchema().size() > 1) {
	                    distinctCount = 0;
	                	ValueIterator vi = dvs.getValueIterator(setState.valueExpression);
                    	if (dvs.getTupleBuffer().getSchema().indexOf(setState.valueExpression) == 0) {
        	            	Object last = null;
	                    	while (vi.hasNext()) {
    	                		Object next = vi.next();
        	            		if (next != null && (last == null || ((Comparable)next).compareTo(last) != 0)) {
            	        			distinctCount++;
                	    		}
                    			last = next;
                    		}
                    	} else {
                    		//secondary attributes are not in sorted order, so we use an approximate count
                    		Set<Object> set = new TreeSet<Object>();
                    		int maxSize = Math.min(10000, dvs.getTupleBuffer().getRowCount());
                    		List<Object> buffer = Arrays.asList(new Object[maxSize]);
                    		int i = 0;
	                    	while (vi.hasNext()) {
    	                		Object next = vi.next();
    	                		if (next == null) {
    	                			continue;
    	                		}
        	            		if (set.add(next)) {
            	        			distinctCount++;
                	    		}
        	            		Object old = buffer.set(i++%maxSize, next);
        	            		if (set.size() > maxSize) {
        	            			set.remove(old);
        	            		}
                    		}
                    	}
                	}
                	if (!setState.overMax && distinctCount > setState.maxNdv) {
                		LogManager.logWarning(LogConstants.CTX_DQP, QueryPlugin.Util.getString("DependentCriteriaProcessor.dep_join_backoff", valueSource, setState.valueExpression, setState.maxNdv)); //$NON-NLS-1$
                		setState.overMax = true;
                	}
    			}
            }
        }
        
        public void close() {
            if (dvs != null) {
            	sortUtility = null;
            	if (dvs != originalVs) {
            		dvs.getTupleBuffer().remove();
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
    private int maxPredicates;
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

    public DependentCriteriaProcessor(int maxSetSize, int maxPredicates, RelationalNode dependentNode, Criteria dependentCriteria) throws ExpressionEvaluationException, TeiidComponentException {
        this.maxSetSize = maxSetSize;
        this.maxPredicates = maxPredicates;
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
                if (setCriteria.isNegated() || setCriteria.getNumberOfValues() <= maxSetSize || !setCriteria.isAllConstants()) {
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
                state.maxNdv = dsc.getMaxNdv();
            } 
        }        
    }

    public void close() {
        if (dependentState != null) {
            for (TupleState state : dependentState.values()) {
				state.close();
			}
        }
        if (this.eval != null) {
        	this.eval.close();
        }
    }

    public Criteria prepareCriteria() throws TeiidComponentException, TeiidProcessingException {

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
     * @throws TeiidComponentException
     */
    private void replaceDependentValueIterators() throws TeiidComponentException {
    	int totalPredicates = setStates.size();
    	if (this.maxPredicates > 0) {
        	//We have a bin packing problem if totalPredicates < sources - We'll address that case later.
    		//TODO: better handling for the correlated composite case
    		totalPredicates = Math.max(totalPredicates, this.maxPredicates);
    	}
    	long maxSize = Integer.MAX_VALUE;
    	if (this.maxSetSize > 0) {
    		maxSize = this.maxSetSize;
    		if (this.maxPredicates > 0 && totalPredicates > this.maxPredicates) {
    			//scale the max based upon the number of predicates - this is not perfect, but sufficient for most situations
    			long maxParams = this.maxPredicates * this.maxSetSize;
    			maxSize = Math.max(1, maxParams/totalPredicates);
    		}
    	}
    	int currentPredicates = 0;
    	for (int run = 0; currentPredicates < totalPredicates; run++) {
    		currentPredicates = 0;
    		if (!restartIndexes.isEmpty()) {
    			currentIndex = restartIndexes.removeLast().intValue();
    		}
	        for (int i = 0; i < sources.size(); i++) {

	            List<SetState> source = sources.get(i);

	        	if (i == currentIndex) {
	        		currentIndex++;
		            int doneCount = 0;
		
		            while (doneCount < source.size()) {
		
		                boolean isNull = false;
		                boolean lessThanMax = true;
		
		                for (SetState state : source) {
		                	if (state.overMax) {
		                		doneCount++;
		                		continue;
		                	}
		                    if (state.nextValue == null && !state.isNull) {
		                        if (state.valueIterator.hasNext()) {
		                            state.nextValue = state.valueIterator.next();
		                            state.isNull = state.nextValue == null;
		                        } else {
		                            state.valueIterator.reset();
		                            doneCount++; // should be true for each iterator from this source
		                            continue;
		                        }
		                    }
		
		                    isNull |= state.isNull;
		                    lessThanMax &= state.replacement.size() < maxSize * (run + 1);
		                }
		
		                if (doneCount == source.size()) {
		                    if (!restartIndexes.isEmpty() && restartIndexes.getLast().intValue() == i) {
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
		                    restartIndexes.add(i);
		                    break;
		                }
		            }
	        	}
	            
	            for (SetState setState : source) {
	            	currentPredicates += setState.replacement.size()/maxSize+(setState.replacement.size()%maxSize!=0?1:0);
				}
	        }
	        
            if (restartIndexes.isEmpty()) {
            	break;
            }
    	}

        hasNextCommand = !restartIndexes.isEmpty();
        if (hasNextCommand && dependentState.size() > 1) {
        	for (TupleState state : dependentState.values()) {
				state.originalVs.setUnused(true);
			}
        }
    }

    protected boolean hasNextCommand() {
        return hasNextCommand;
    }
    
    public Criteria replaceDependentCriteria(AbstractSetCriteria crit, SetState state) throws TeiidComponentException {
    	if (state.overMax) {
            DependentValueSource originalVs = (DependentValueSource)dependentNode.getContext().getVariableContext().getGlobalValue(((DependentSetCriteria)crit).getContextSymbol());
            originalVs.setUnused(true);
    		return QueryRewriter.TRUE_CRITERIA;
    	}
    	if (state.replacement.isEmpty()) {
            // No values - return criteria that is always false
            return QueryRewriter.FALSE_CRITERIA;
    	}
    	int numberOfSets = 1;
    	int maxSize = Integer.MAX_VALUE;
    	if (this.maxSetSize > 0) {
    		maxSize = this.maxSetSize;
    		numberOfSets = state.replacement.size()/maxSize + (state.replacement.size()%maxSize!=0?1:0);
    	}
    	Iterator<Object> iter = state.replacement.iterator();
    	ArrayList<Criteria> orCrits = new ArrayList<Criteria>(numberOfSets);
    	
    	for (int i = 0; i < numberOfSets; i++) {
    		if (maxSize == 1 || i + 1 == state.replacement.size()) {
				orCrits.add(new CompareCriteria(crit.getExpression(), CompareCriteria.EQ, newConstant(iter.next())));    				
			} else {
	    		List<Constant> vals = new ArrayList<Constant>(Math.min(state.replacement.size(), maxSize));
				
	    		for (int j = 0; j < maxSize && iter.hasNext(); j++) {
	    			Object val = iter.next();
	                vals.add(newConstant(val));
	            }
	            
	            SetCriteria sc = new SetCriteria();
	            sc.setExpression(crit.getExpression());
	            sc.setValues(vals);
	            orCrits.add(sc);
			}
    	}
    	if (orCrits.size() == 1) {
    		return orCrits.get(0);
    	}
    	return new CompoundCriteria(CompoundCriteria.OR, orCrits);
    }
    
    private Constant newConstant(Object val) {
    	Constant c = new Constant(val);
    	c.setBindEligible(true);
    	return c;
    }

}
