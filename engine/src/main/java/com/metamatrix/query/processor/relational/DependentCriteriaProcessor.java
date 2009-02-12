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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.processor.relational.DependentSourceState.SetState;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.CollectionValueIterator;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;

public class DependentCriteriaProcessor {

    class TupleState implements DependentSourceState {

        private SortUtility sortUtility;
        private TupleSourceID outputID;
        private List dependentSetStates;
        private DependentValueSource valueSource = new DependentValueSource();

        public TupleState(List sortSymbols,
                                    TupleSource ts,
                                    TupleSourceID tsID) throws MetaMatrixComponentException {

            List sortDirection = new ArrayList(sortSymbols.size());

            for (int i = 0; i < sortSymbols.size(); i++) {
                sortDirection.add(Boolean.valueOf(OrderBy.ASC));
            }

            this.sortUtility = new SortUtility(tsID, ts.getSchema(), sortSymbols, sortDirection, true, dependentNode.getBufferManager(),
                                               dependentNode.getConnectionID());
        }

        public void sort() throws BlockedException,
                   MetaMatrixComponentException {
            if (outputID == null) {
                outputID = sortUtility.sort();
            }
        }
        
        public void close() throws MetaMatrixComponentException {
            if (outputID != null) {
                try {
                    dependentNode.getBufferManager().removeTupleSource(outputID);
                } catch (TupleSourceNotFoundException e) {
                    throw new MetaMatrixComponentException(e, e.getMessage());
                }
                outputID = null;
            }
        }
        
        public ValueIterator getValueIterator(SetState setState) {
            return valueSource.getValueIterator(setState.valueExpression);
        }
        
        public void connectValueSource() throws MetaMatrixComponentException {
            try {
                valueSource.setTupleSource(dependentNode.getBufferManager().getTupleSource(outputID), outputID);
            } catch (TupleSourceNotFoundException err) {
                throw new MetaMatrixComponentException(err);
            }
        }

        public List getDepedentSetStates() {
            return dependentSetStates;
        }

        public void setDependentSetStates(List states) {
            this.dependentSetStates = states;
        }
    }
    
    private final static class FixedState implements DependentSourceState {
        
        private List dependentSetStates;
        private Collection values;
        
        public FixedState(SetCriteria crit) {
            this.values = new ArrayList(crit.getValues());
        }

        public void sort() throws BlockedException,
                          MetaMatrixComponentException {
            //do nothing, it should already be sorted
        }

        public void close() throws MetaMatrixComponentException {
            //do nothing
        }

        public ValueIterator getValueIterator(SetState setState) {
            return new CollectionValueIterator(values);
        }

        public void connectValueSource() throws MetaMatrixComponentException {
            //do nothing
        }

        public List getDepedentSetStates() {
            return dependentSetStates;
        }

        public void setDependentSetStates(List states) {
            this.dependentSetStates = states;
        }
    }

    private final static class SimpleValueIteratorSource implements
                                                        ValueIteratorSource {

        private Collection values;

        public SimpleValueIteratorSource(Collection values) {
            this.values = values;
        }

        public ValueIterator getValueIterator(Expression valueExpression) {
            return new CollectionValueIterator(values);
        }

        public boolean isReady() {
            return true;
        }
    }

    private static final int INITIAL = 1;
    private static final int SORT = 2;
    private static final int SET_PROCESSING = 3;

    private int maxSetSize;

    // processing state
    private RelationalNode dependentNode;
    private Criteria dependentCrit;
    private int phase = INITIAL;
    private List sources;
    private LinkedHashMap dependentState;
    private LinkedList restartIndexes;
    private int currentIterator;
    private boolean hasNextCommand;

    public DependentCriteriaProcessor(int maxSetSize, RelationalNode dependentNode, Criteria dependentCriteria) {
        this.maxSetSize = maxSetSize;
        this.dependentNode = dependentNode;
        this.dependentCrit = dependentCriteria;
    }

    public void close() throws MetaMatrixComponentException {
        if (dependentState != null) {
            for (int i = 0; i < sources.size(); i++) {
                DependentSourceState dss = (DependentSourceState)dependentState.get(sources.get(i));
                dss.close();
            }
        }
    }

    public void reset() {
        dependentState = null;
        phase = INITIAL;
    }

    public Criteria prepareCriteria() throws MetaMatrixComponentException {

        if (phase == INITIAL) {
            initializeDependentState();

            phase = SORT;
        }

        if (phase == SORT) {
            sortDependentSources();

            phase = SET_PROCESSING;
        }

        if (!dependentState.isEmpty()) {
            replaceDependentValueIterators();
        }
        
        Criteria result = (Criteria)dependentCrit.clone();

        return result;
    }
    
    public void consumedCriteria() {
        // flush only the value iterators starting at the restart index
        // it is only safe to do this after the super call to prepare command
        if (restartIndexes.isEmpty()) {
            return;
        }
        
        int restartIndex = ((Integer)restartIndexes.removeLast()).intValue();
        
        for (int i = restartIndex; i < sources.size(); i++) {

            DependentSourceState dss = (DependentSourceState)dependentState.get(sources.get(i));

            for (int j = 0; j < dss.getDepedentSetStates().size(); j++) {

                SetState state = (SetState)dss.getDepedentSetStates().get(j);

                state.replacement.clear();
            }
        }

        currentIterator = restartIndex;
    }

    private void initializeDependentState() throws MetaMatrixComponentException {
        hasNextCommand = false;
        dependentState = new LinkedHashMap();
        currentIterator = 0;
        restartIndexes = new LinkedList();

        List queryCriteria = Criteria.separateCriteriaByAnd(dependentCrit);

        for (Iterator i = queryCriteria.iterator(); i.hasNext();) {
            Criteria criteria = (Criteria)i.next();

            if (!(criteria instanceof AbstractSetCriteria)) {
                continue;
            }
            
            Object source = null;
            
            if (criteria instanceof SetCriteria) {
                SetCriteria setCriteria = (SetCriteria)criteria;
                if (setCriteria.getNumberOfValues() <= maxSetSize) {
                    continue;
                }
                source = new Object(); //we just need a consistent hash key

            // jh Case 6435a                
            } else if (criteria instanceof DependentSetCriteria) {
                source = ((DependentSetCriteria)criteria).getValueIteratorSource();                
            } else {
                continue;
            }
            List sets = (List)dependentState.get(source);

            if (sets == null) {
                sets = new LinkedList();
                dependentState.put(source, sets);
            }

            sets.add(criteria);
        }

        sources = new ArrayList(dependentState.keySet());

        for (Iterator i = dependentState.entrySet().iterator(); i.hasNext();) {

            Map.Entry entry = (Map.Entry)i.next();
            
            if (entry.getKey() instanceof DependentValueSource) {

                DependentValueSource dvs = (DependentValueSource)entry.getKey();
    
                List sets = (List)entry.getValue();
    
                List symbols = new ArrayList(sets.size());
    
                for (int j = 0; j < sets.size(); j++) {
    
                    DependentSetCriteria crit = (DependentSetCriteria)sets.get(j);
    
                    SetState state = new SetState();
                    state.valueExpression = crit.getValueExpression();
                    symbols.add(state.valueExpression);
    
                    sets.set(j, state);
    
                    crit.setValueIteratorSource(new SimpleValueIteratorSource(state.replacement));
                }
    
                DependentSourceState dss = new TupleState(symbols, dvs.getTupleSource(), dvs.getTupleSourceID());
    
                entry.setValue(dss);
    
                dss.setDependentSetStates(sets);
            } else {
                
                List sets = (List)entry.getValue();
                
                SetCriteria crit = (SetCriteria)sets.get(0);
                
                DependentSourceState dss = new FixedState(crit);
                
                SetState state = new SetState();
                state.valueExpression = crit.getExpression();

                sets.set(0, state);

                crit.setValues(state.replacement);
                
                entry.setValue(dss);
    
                dss.setDependentSetStates(sets);
            }
        }
    }

    private void sortDependentSources() throws BlockedException,
                                       MetaMatrixComponentException {
        for (int i = 0; i < sources.size(); i++) {
            DependentSourceState dss = (DependentSourceState)dependentState.get(sources.get(i));

            dss.sort();
        }

        // all have been sorted, now create the tuple source iterators
        for (int i = 0; i < sources.size(); i++) {
            DependentSourceState dss = (DependentSourceState)dependentState.get(sources.get(i));

            dss.connectValueSource();

            for (int j = 0; j < dss.getDepedentSetStates().size(); j++) {
                SetState setState = (SetState)dss.getDepedentSetStates().get(j);

                setState.valueIterator = dss.getValueIterator(setState);
            }
        }
    }

    /**
     * Replace the dependentSet value iterators with the next set of values from the independent tuple source
     * 
     * @param dependentSets
     * @param replacementValueIterators
     * @throws MetaMatrixComponentException
     */
    private void replaceDependentValueIterators() throws MetaMatrixComponentException {

        for (; currentIterator < sources.size(); currentIterator++) {

            DependentSourceState dss = (DependentSourceState)dependentState.get(sources.get(currentIterator));

            boolean done = false;

            while (!done) {

                boolean isNull = false;
                boolean lessThanMax = true;

                for (int i = 0; i < dss.getDepedentSetStates().size(); i++) {
                    SetState state = (SetState)dss.getDepedentSetStates().get(i);

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
                    if (!restartIndexes.isEmpty() && ((Integer)restartIndexes.getLast()).intValue() == currentIterator) {
                        restartIndexes.removeLast();
                    }
                    break;
                }

                if (lessThanMax || isNull) {
                    for (int i = 0; i < dss.getDepedentSetStates().size(); i++) {
                        SetState state = (SetState)dss.getDepedentSetStates().get(i);

                        if (!isNull) {
                            state.replacement.add(state.nextValue);
                        }
                        state.nextValue = null;
                        state.isNull = false;
                    }
                } else {
                    restartIndexes.add(new Integer(currentIterator));
                    done = true;
                }
            }
        }

        hasNextCommand = !restartIndexes.isEmpty();
    }

    protected boolean hasNextCommand() {
        return hasNextCommand;
    }

}
