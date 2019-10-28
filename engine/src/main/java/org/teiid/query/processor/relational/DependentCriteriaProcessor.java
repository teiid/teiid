/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.processor.relational;

import java.util.*;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
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
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.ValueIterator;


public class DependentCriteriaProcessor {

    public static class SetState {

        Collection<Constant> replacement = new LinkedHashSet<Constant>();

        Expression valueExpression;

        ValueIterator valueIterator;

        Object nextValue;

        boolean isNull;

        float maxNdv = NewCalculateCostUtil.UNKNOWN_VALUE;

        boolean overMax;

        long replacementSize() {
            return replacement.size() * valueCount;
        }

        long valueCount = 1;

        SetCriteria existingSet;

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
                if (!originalVs.isDistinct() || dependentSetStates.size() != originalVs.getTupleBuffer().getSchema().size()) {
                    if (sortUtility == null) {
                        List<Expression> sortSymbols = new ArrayList<Expression>(dependentSetStates.size());
                        for (int i = 0; i < dependentSetStates.size(); i++) {
                            if (dependentSetStates.get(i).valueExpression instanceof Array) {
                                Array array = (Array)dependentSetStates.get(i).valueExpression;
                                for (Expression ex : array.getExpressions()) {
                                    sortSymbols.add(ex);
                                }
                            } else {
                                sortSymbols.add(dependentSetStates.get(i).valueExpression);
                            }
                        }
                        if (originalVs.isDistinct() && sortSymbols.size() == originalVs.getTupleBuffer().getSchema().size()) {
                            dvs = originalVs;
                        } else {
                            //TODO: should not use the full buffer as it still contains the full source tuples
                            //alternatively if we're already sorted by the join node then processing distinct
                            //does not require a full pass
                            List<Boolean> sortDirection = Collections.nCopies(sortSymbols.size(), OrderBy.ASC);
                            this.sortUtility = new SortUtility(null, sortSymbols, sortDirection, Mode.DUP_REMOVE, dependentNode.getBufferManager(), dependentNode.getConnectionID(), originalVs.getSchema());
                            this.sortUtility.setWorkingBuffer(originalVs.getTupleBuffer());
                            //having this sort throw a blocked exception is a problem for
                            //the logic that declines the source sort operation
                            this.sortUtility.setNonBlocking(true);
                        }
                    }
                    if (sortUtility != null) {
                        dvs = new DependentValueSource(sortUtility.sort());
                    }
                } else {
                    dvs = originalVs;
                }
                for (SetState setState : dependentSetStates) {
                    setState.valueIterator = dvs.getValueIterator(setState.valueExpression);
                    long distinctCount = dvs.getTupleBuffer().getRowCount();
                    if (setState.maxNdv <= 0 || setState.maxNdv >= distinctCount) {
                        continue;
                    }
                    if (!setState.overMax && distinctCount > setState.maxNdv) {
                        LogManager.logWarning(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30011, valueSource, setState.valueExpression, setState.maxNdv));
                        setState.overMax = true;
                    }
                }
            }
        }

        public void close() {
            if (this.sortUtility != null) {
                this.sortUtility.remove();
                sortUtility = null;
            }
            if (dvs != null) {
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
    private boolean pushdown;
    private boolean useBindings;
    private boolean complexQuery;

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

    private int totalPredicates;
    private long maxSize;

    public DependentCriteriaProcessor(int maxSetSize, int maxPredicates, RelationalNode dependentNode, Criteria dependentCriteria) throws ExpressionEvaluationException, TeiidComponentException {
        this.maxSetSize = maxSetSize;
        this.maxPredicates = maxPredicates;
        this.dependentNode = dependentNode;
        this.eval = new SubqueryAwareEvaluator(Collections.emptyMap(), dependentNode.getDataManager(), dependentNode.getContext(), dependentNode.getBufferManager());
        queryCriteria = Criteria.separateCriteriaByAnd(dependentCriteria);

        Map<Expression, SetCriteria> setMap = new HashMap<Expression, SetCriteria>();
        for (int i = 0; i < queryCriteria.size(); i++) {
            Criteria criteria = queryCriteria.get(i);
            if (!(criteria instanceof AbstractSetCriteria)) {
                continue;
            }

            if (criteria instanceof SetCriteria) {
                SetCriteria setCriteria = (SetCriteria)criteria;
                if (setCriteria.isNegated() || !setCriteria.isAllConstants()) {
                    continue;
                }
                setMap.put(setCriteria.getExpression(), setCriteria);
            }
        }

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
                SetCriteria sc = setMap.get(dsc.getExpression());
                if (sc != null) {
                    state.existingSet = sc;
                    int index = queryCriteria.indexOf(sc);
                    queryCriteria.set(index, QueryRewriter.TRUE_CRITERIA);
                    setStates.remove(index);
                }
                state.valueExpression = dsc.getValueExpression();
                if (dsc.hasMultipleAttributes()) {
                    state.valueCount = ((Array)dsc.getExpression()).getExpressions().size();
                }
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
                if (state.dvs.getTupleBuffer().getRowCount() == 0) {
                    return QueryRewriter.FALSE_CRITERIA;
                }
            }

            //init total predicates and max size
            totalPredicates = setStates.size();
            if (this.maxPredicates > 0) {
                //We have a bin packing problem if totalPredicates < sources - We'll address that case later.
                //TODO: better handling for the correlated composite case
                totalPredicates = Math.max(totalPredicates, this.maxPredicates);
            }
            long maxParams = this.maxPredicates * this.maxSetSize;
            maxSize = Integer.MAX_VALUE;
            if (this.maxSetSize > 0) {
                maxSize = this.maxSetSize;
                if (this.maxPredicates > 0 && totalPredicates > this.maxPredicates) {
                    //scale the max based upon the number of predicates - this is not perfect, but sufficient for most situations
                    maxSize = Math.max(1, maxParams/totalPredicates);
                }
            }

            //determine push down handling
            if (pushdown) {
                List<Criteria> newCriteria = new ArrayList<Criteria>();
                long params = 0;
                int sets = 0;
                for (Criteria criteria : queryCriteria) {
                    if (!(criteria instanceof DependentSetCriteria)) {
                        newCriteria.add(criteria);
                        continue;
                    }
                    sets++;
                    DependentSetCriteria dsc = (DependentSetCriteria) criteria;
                    TupleState ts = dependentState.get(dsc.getContextSymbol());
                    DependentValueSource dvs = ts.dvs;
                    // check if this has more rows than we want to push
                    if ((dsc.getMaxNdv() != -1 && dvs.getTupleBuffer().getRowCount() > dsc.getMaxNdv())
                            || (dsc.getMakeDepOptions() != null
                                    && dsc.getMakeDepOptions().getMax() != null
                                    && dvs.getTupleBuffer().getRowCount() > dsc.getMakeDepOptions().getMax())) {
                        continue; // don't try to pushdown
                    }
                    int cols = 1;
                    if (dsc.getExpression() instanceof Array) {
                        cols = ((Array)dsc.getExpression()).getExpressions().size();
                    }
                    dsc = dsc.clone();
                    //determine if this will be more than 1 source query
                    params += cols * dvs.getTupleBuffer().getRowCount();
                    // TODO: this assumes that if any one of the dependent
                    // joins are pushed, then they all are
                    dsc.setDependentValueSource(dvs);
                    newCriteria.add(dsc);
                }
                int maxParamThreshold = 3; //TODO: see if this should be a source tunable parameter
                                           //generally this value accounts for the additional overhead of temp table creation
                if (params > maxParams && (sets > 1 || complexQuery || params > maxParams * maxParamThreshold)) {
                    //use the pushdown only in limited scenarios
                    //only if we will produce more than two source queries
                    //and only if the we could produce a cross set or have a complex query
                    return Criteria.combineCriteria(newCriteria);
                }
            }

            //proceed with set based processing
            phase = SET_PROCESSING;
        }

        replaceDependentValueIterators();

        LinkedList<Criteria> crits = new LinkedList<Criteria>();

        for (int i = 0; i < queryCriteria.size(); i++) {
            SetState state = this.setStates.get(i);
            Criteria criteria = queryCriteria.get(i);
            if (state == null) {
                if (criteria != QueryRewriter.TRUE_CRITERIA) {
                    crits.add((Criteria)criteria.clone());
                }
            } else {
                Criteria crit = replaceDependentCriteria((AbstractSetCriteria)criteria, state);
                if (crit == QueryRewriter.FALSE_CRITERIA) {
                    return QueryRewriter.FALSE_CRITERIA;
                }
                if (crit != QueryRewriter.TRUE_CRITERIA) {
                    crits.add(crit);
                }
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
     * @throws TeiidComponentException
     */
    private void replaceDependentValueIterators() throws TeiidComponentException {
        int currentPredicates = 0;
        for (int run = 0; currentPredicates < totalPredicates; run++) {
            currentPredicates = 0;
            if (!restartIndexes.isEmpty()) {
                currentIndex = restartIndexes.removeLast().intValue();
            }
            int possible = 0;

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
                            lessThanMax &= state.replacementSize() < maxSize * (run + 1);
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
                                    Constant constant = newConstant(state.nextValue, state.valueExpression);
                                    if (state.existingSet == null || state.existingSet.getValues().contains(constant)) {
                                        state.replacement.add(constant);
                                    }
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
                    currentPredicates += setState.replacementSize()/maxSize+(setState.replacementSize()%maxSize!=0?1:0);
                }
                possible+=source.size();
            }

            if (currentPredicates + possible > totalPredicates) {
                break; //don't exceed the max
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
        int setSize = Integer.MAX_VALUE;
        if (this.maxSetSize > 0) {
            setSize = (int) Math.max(1, this.maxSetSize/state.valueCount);
            numberOfSets = state.replacement.size()/setSize + (state.replacement.size()%setSize!=0?1:0);
        }
        Iterator<Constant> iter = state.replacement.iterator();
        ArrayList<Criteria> orCrits = new ArrayList<Criteria>(numberOfSets);

        for (int i = 0; i < numberOfSets; i++) {
            if (setSize == 1 || i + 1 == state.replacement.size()) {
                orCrits.add(new CompareCriteria(crit.getExpression(), CompareCriteria.EQ, iter.next()));
            } else {
                //use an appropriately searchable collection - which is expected by the temp table logic
                Collection<Constant> vals = null;
                if (!DataTypeManager.isHashable(crit.getExpression().getType())) {
                    vals = new TreeSet<Constant>();
                } else {
                    vals = new LinkedHashSet<Constant>();
                }

                for (int j = 0; j < setSize && iter.hasNext(); j++) {
                    Constant val = iter.next();
                    vals.add(val);
                }

                SetCriteria sc = new SetCriteria();
                sc.setExpression(crit.getExpression());
                sc.setValues(vals);
                sc.setAllConstants(true);
                orCrits.add(sc);
            }
        }
        if (orCrits.size() == 1) {
            return orCrits.get(0);
        }
        return new CompoundCriteria(CompoundCriteria.OR, orCrits);
    }

    private Constant newConstant(Object val, Expression ex) {
        Constant c;
        if (ex != null) {
            c = new Constant(val, ex.getType());
        } else {
            c = new Constant(val);
        }
        if (useBindings) {
            //TODO: validate that this is a reasonable approach
            //we are using bind variables since that helps limit the size of the source sql
            //but a prepared plan is really of no use in this scenario
            c.setBindEligible(true);
        }
        return c;
    }

    public void setPushdown(boolean pushdown) {
        this.pushdown = pushdown;
    }

    public void setUseBindings(boolean useBindings) {
        this.useBindings = useBindings;
    }

    public void setComplexQuery(boolean complexQuery) {
        this.complexQuery = complexQuery;
    }

}
