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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.aggregate.*;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.BatchCollector.BatchProducer;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


public class GroupingNode extends SubqueryAwareRelationalNode {

    static class ProjectingTupleSource extends
            BatchCollector.BatchProducerTupleSource {

        private Evaluator eval;
        private List<Expression> collectedExpressions;
        private int[] projectionIndexes;

        ProjectingTupleSource(BatchProducer sourceNode, Evaluator eval, List<Expression> expressions, Map<Expression, Integer> elementMap) {
            super(sourceNode);
            this.eval = eval;
            this.collectedExpressions = expressions;
            this.projectionIndexes = new int[this.collectedExpressions.size()];
            Arrays.fill(this.projectionIndexes, -1);
            for (int i = 0; i < expressions.size(); i++) {
                Integer index = elementMap.get(expressions.get(i));
                if(index != null) {
                    projectionIndexes[i] = index;
                }
            }
        }

        @Override
        protected List<Object> updateTuple(List<?> tuple) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
            int columns = collectedExpressions.size();
            List<Object> exprTuple = new ArrayList<Object>(columns);
            for(int col = 0; col<columns; col++) {
                int index = projectionIndexes[col];
                Object value = null;
                if (index != -1) {
                    value = tuple.get(index);
                } else {
                    // The following call may throw BlockedException, but all state to this point
                    // is saved in class variables so we can start over on building this tuple
                    value = eval.evaluate(collectedExpressions.get(col), tuple);
                }
                exprTuple.add(value);
            }
            return exprTuple;
        }
    }

    // Grouping columns set by the planner
    private List<OrderByItem> orderBy;
    private boolean removeDuplicates;
    private SymbolMap outputMapping;

    // Collection phase
    private int phase = COLLECTION;
    private Map<Expression, Integer> elementMap;                    // Map of incoming symbol to index in source elements
    private LinkedHashMap<Expression, Integer> collectedExpressions;         // Collected Expressions
    private int distinctCols = -1;

    // Sort phase
    private SortUtility sortUtility;
    private TupleBuffer sortBuffer;
    private TupleSource groupTupleSource;

    // Group phase
    private AggregateFunction[][] functions;
    private List<?> lastRow;
    private List<?> currentGroupTuple;
    private boolean doneReading;

    // Group sort
    private STree tree;
    private AggregateFunction[] groupSortfunctions;
    private int[] accumulatorStateCount;
    private TupleSource groupSortTupleSource;
    private int[] projection;

    private static final int COLLECTION = 1;
    private static final int SORT = 2;
    private static final int GROUP = 3;
    private static final int GROUP_SORT = 4;
    private static final int GROUP_SORT_OUTPUT = 5;
    private int[] indexes;
    private boolean rollup;
    private HashMap<Integer, Integer> indexMap;

    public GroupingNode(int nodeID) {
        super(nodeID);
    }

    public void reset() {
        super.reset();

        phase = COLLECTION;

        sortUtility = null;
        sortBuffer = null;

        lastRow = null;
        currentGroupTuple = null;
        doneReading = false;

        if (this.functions != null) {
            for (AggregateFunction[] functions : this.functions) {
                for (AggregateFunction function : functions) {
                    function.reset();
                }
            }
        }
    }

    public void setRemoveDuplicates(boolean removeDuplicates) {
        this.removeDuplicates = removeDuplicates;
    }

    public void setOrderBy(List<OrderByItem> orderBy) {
        this.orderBy = orderBy;
    }

    public void setOutputMapping(SymbolMap outputMapping) {
        this.outputMapping = outputMapping;
    }

    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
            ProcessorDataManager dataMgr) {
        super.initialize(context, bufferManager, dataMgr);

        if (this.functions != null) {
            return;
        }

        // Incoming elements and lookup map for evaluating expressions
        List<? extends Expression> sourceElements = this.getChildren()[0].getElements();
        this.elementMap = createLookupMap(sourceElements);
        this.collectedExpressions = new LinkedHashMap<Expression, Integer>();
        // List should contain all grouping columns / expressions as we need those for sorting
        if(this.orderBy != null) {
            for (OrderByItem item : this.orderBy) {
                Expression ex = SymbolMap.getExpression(item.getSymbol());
                getIndex(ex, this.collectedExpressions);
            }
            if (removeDuplicates) {
                for (Expression ses : sourceElements) {
                    getIndex(ses, collectedExpressions);
                }
                distinctCols = collectedExpressions.size();
            }
        }

        // Construct aggregate function state accumulators
        functions = new AggregateFunction[getElements().size()][];
        for(int i=0; i<getElements().size(); i++) {
            Expression symbol = getElements().get(i);
            if (this.outputMapping != null) {
                symbol = outputMapping.getMappedExpression((ElementSymbol)symbol);
            }
            Class<?> outputType = symbol.getType();
            if(symbol instanceof AggregateSymbol) {
                AggregateSymbol aggSymbol = (AggregateSymbol) symbol;
                functions[i] = new AggregateFunction[rollup?orderBy.size()+1:1];
                for (int j = 0; j < functions[i].length; j++) {
                    functions[i][j] = initAccumulator(aggSymbol, this, this.collectedExpressions);
                }
            } else {
                AggregateFunction af = new ConstantFunction();
                af.setArgIndexes(new int[] {this.collectedExpressions.get(symbol)});
                af.initialize(outputType, new Class<?>[]{symbol.getType()});
                functions[i] = new AggregateFunction[] {af};
            }
        }
    }

    static Integer getIndex(Expression ex, LinkedHashMap<Expression, Integer> expressionIndexes) {
        Integer index = expressionIndexes.get(ex);
        if (index == null) {
            index = expressionIndexes.size();
            expressionIndexes.put(ex, index);
        }
        return index;
    }

    static AggregateFunction initAccumulator(AggregateSymbol aggSymbol,
            RelationalNode node, LinkedHashMap<Expression, Integer> expressionIndexes) {
        int[] argIndexes = new int[aggSymbol.getArgs().length];
        AggregateFunction result = null;
        Expression[] args = aggSymbol.getArgs();
        Class<?>[] inputTypes = new Class[args.length];
        for (int j = 0; j < args.length; j++) {
            inputTypes[j] = args[j].getType();
            argIndexes[j] = getIndex(args[j], expressionIndexes);
        }
        Type function = aggSymbol.getAggregateFunction();
        switch (function) {
        case RANK:
        case DENSE_RANK:
            if (aggSymbol.getType() == DataTypeManager.DefaultDataClasses.LONG) {
                result = new RankingFunctionBig(function);
            } else {
                result = new RankingFunction(function);
            }
            break;
        case ROW_NUMBER: //same as count(*)
            if (aggSymbol.getType() == DataTypeManager.DefaultDataClasses.LONG) {
                result = new CountBig();
                break;
            }
        case CUME_DIST:
        case COUNT:
            result = new Count();
            break;
        case COUNT_BIG:
            result = new CountBig();
            break;
        case SUM:
            result = new Sum();
            break;
        case AVG:
            result = new Avg();
            break;
        case MIN:
            result = new Min();
            break;
        case MAX:
            result = new Max();
            break;
        case XMLAGG:
            result = new XMLAgg();
            break;
        case ARRAY_AGG:
            result = new ArrayAgg();
            break;
        case JSONARRAY_AGG:
            result = new JSONArrayAgg();
            break;
        case TEXTAGG:
            result = new TextAgg((TextLine)args[0]);
            break;
        case STRING_AGG:
            result = new StringAgg(aggSymbol.getType() == DataTypeManager.DefaultDataClasses.BLOB);
            break;
        case FIRST_VALUE:
            result = new FirstLastValue(aggSymbol.getType(), true);
            break;
        case LAST_VALUE:
            result = new FirstLastValue(aggSymbol.getType(), false);
            break;
        case LEAD:
        case LAG:
            result = new LeadLagValue();
            break;
        case NTILE:
            //init with a row function that will also capture the tiles argument
            //the rest of the processing is handled in the window function project node
            result = new Ntile();
            break;
        case PERCENT_RANK:
            //init with a ranking function
            //the rest of the processing is handled in the window function project node
            result = new RankingFunction(Type.RANK);
            break;
        case NTH_VALUE:
            result = new NthValue();
            break;
        case USER_DEFINED:
            try {
                result = new UserDefined(aggSymbol.getFunctionDescriptor());
            } catch (FunctionExecutionException e) {
                throw new TeiidRuntimeException(e);
            }
            break;
        default:
            result = new StatsFunction(function);
        }
        if (aggSymbol.getOrderBy() != null) {
            int numOrderByItems = aggSymbol.getOrderBy().getOrderByItems().size();
            List<OrderByItem> orderByItems = new ArrayList<OrderByItem>(numOrderByItems);
            List<ElementSymbol> schema = createSortSchema(result, inputTypes);
            argIndexes = Arrays.copyOf(argIndexes, argIndexes.length + numOrderByItems);
            for (ListIterator<OrderByItem> iterator = aggSymbol.getOrderBy().getOrderByItems().listIterator(); iterator.hasNext();) {
                OrderByItem item = iterator.next();
                argIndexes[args.length + iterator.previousIndex()] = getIndex(item.getSymbol(), expressionIndexes);
                ElementSymbol element = new ElementSymbol(String.valueOf(iterator.previousIndex()));
                element.setType(item.getSymbol().getType());
                schema.add(element);
                OrderByItem newItem = item.clone();
                newItem.setSymbol(element);
                orderByItems.add(newItem);
            }
            SortingFilter filter = new SortingFilter(result, node.getBufferManager(), node.getConnectionID(), aggSymbol.isDistinct());
            filter.setElements(schema);
            filter.setSortItems(orderByItems);
            result = filter;
        } else if(aggSymbol.isDistinct()) {
            SortingFilter filter = new SortingFilter(result, node.getBufferManager(), node.getConnectionID(), true);
            List<ElementSymbol> elements = createSortSchema(result, inputTypes);
            filter.setElements(elements);
            result = filter;
        }
        result.setArgIndexes(argIndexes);
        if (aggSymbol.getCondition() != null) {
            result.setConditionIndex(getIndex(aggSymbol.getCondition(), expressionIndexes));
        }
        result.initialize(aggSymbol.getType(), inputTypes);
        return result;
    }

    private static List<ElementSymbol> createSortSchema(AggregateFunction af,
            Class<?>[] inputTypes) {
        List<ElementSymbol> elements = new ArrayList<ElementSymbol>(inputTypes.length);
        int[] filteredArgIndexes = new int[inputTypes.length];
        for (int i = 0; i < inputTypes.length; i++) {
            ElementSymbol element = new ElementSymbol("val" + i); //$NON-NLS-1$
            element.setType(inputTypes[i]);
            elements.add(element);
            filteredArgIndexes[i] = i;
        }
        af.setArgIndexes(filteredArgIndexes);
        return elements;
    }

    AggregateFunction[][] getFunctions() {
        return functions;
    }

    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        // Take inputs, evaluate expressions, and build initial tuple source
        if(this.phase == COLLECTION) {
            collectionPhase();
        }

        // If necessary, sort to determine groups (if no group cols, no need to sort)
        if(this.phase == SORT) {
            sortPhase();
        }

        // Walk through the sorted results and for each group, emit a row
        if(this.phase == GROUP) {
            return groupPhase();
        }

        if (this.phase == GROUP_SORT) {
            groupSortPhase();
        }

        if (this.phase == GROUP_SORT_OUTPUT) {
            return groupSortOutputPhase();
        }

        this.terminateBatches();
        return pullBatch();
    }

    public TupleSource getGroupSortTupleSource() {
        final RelationalNode sourceNode = this.getChildren()[0];
        return new ProjectingTupleSource(sourceNode, getEvaluator(elementMap), new ArrayList<Expression>(collectedExpressions.keySet()), elementMap);
    }

    @Override
    public Collection<? extends LanguageObject> getObjects() {
        return this.getChildren()[0].getOutputElements();
    }

    private void collectionPhase() {
        if(this.orderBy == null) {
            // No need to sort
            this.groupTupleSource = getGroupSortTupleSource();
            this.phase = GROUP;
        } else {
            List<NullOrdering> nullOrdering = new ArrayList<NullOrdering>(orderBy.size());
            List<Boolean> sortTypes = new ArrayList<Boolean>(orderBy.size());
            int size = orderBy.size();
            if (this.removeDuplicates) {
                //sort on all inputs
                size = distinctCols;
            }
            int[] sortIndexes = new int[size];
            for (int i = 0; i < size; i++) {
                int index = i;
                if (i < this.orderBy.size()) {
                    OrderByItem item = this.orderBy.get(i);
                    nullOrdering.add(item.getNullOrdering());
                    sortTypes.add(item.isAscending());
                    index = collectedExpressions.get(SymbolMap.getExpression(item.getSymbol()));
                } else {
                    nullOrdering.add(null);
                    sortTypes.add(OrderBy.ASC);
                }
                sortIndexes[i] = index;
            }
            this.indexes = Arrays.copyOf(sortIndexes, orderBy.size());
            if (rollup) {
                this.indexMap = new HashMap<Integer, Integer>();
                for (int i = 0; i < indexes.length; i++) {
                    this.indexMap.put(indexes[i], orderBy.size() - i);
                }
            } else if (!removeDuplicates) {
                boolean groupSort = true;
                List<AggregateFunction> aggs = new ArrayList<AggregateFunction>();
                List<Class<?>> allTypes = new ArrayList<Class<?>>();
                accumulatorStateCount = new int[this.functions.length];
                for (AggregateFunction[] afs : this.functions) {
                    if (afs[0] instanceof ConstantFunction) {
                        continue;
                    }
                    aggs.add(afs[0]);
                    List<? extends Class<?>> types = afs[0].getStateTypes();
                    if (types == null) {
                        groupSort = false;
                        break;
                    }
                    accumulatorStateCount[aggs.size() - 1] = types.size();
                    allTypes.addAll(types);
                }
                if (groupSort) {
                    this.groupSortfunctions = aggs.toArray(new AggregateFunction[aggs.size()]);
                    List<Expression> schema = new ArrayList<Expression>();
                    for (OrderByItem item : this.orderBy) {
                        schema.add(SymbolMap.getExpression(item.getSymbol()));
                    }
                    List<? extends Expression> elements = getElements();
                    this.projection = new int[elements.size()];
                    int index = 0;
                    for (int i = 0; i < elements.size(); i++) {
                        Expression symbol = elements.get(i);
                        if (this.outputMapping != null) {
                            symbol = outputMapping.getMappedExpression((ElementSymbol)symbol);
                        }
                        if (symbol instanceof AggregateSymbol) {
                            projection[i] = schema.size() + index++;
                        } else {
                            projection[i] = schema.indexOf(symbol);
                        }
                    }

                    //add in accumulator value types
                    for (Class<?> type : allTypes) {
                        ElementSymbol es = new ElementSymbol("x");
                        es.setType(type);
                        schema.add(es);
                    }

                    tree = this.getBufferManager().createSTree(schema, this.getConnectionID(), orderBy.size());
                    //non-default order needs to update the comparator
                    tree.getComparator().setNullOrdering(nullOrdering);
                    tree.getComparator().setOrderTypes(sortTypes);

                    this.groupSortTupleSource = this.getGroupSortTupleSource();
                    this.phase = GROUP_SORT;
                    return;
                }
            }

            this.sortUtility = new SortUtility(getGroupSortTupleSource(), removeDuplicates?Mode.DUP_REMOVE_SORT:Mode.SORT, getBufferManager(),
                    getConnectionID(), new ArrayList<Expression>(collectedExpressions.keySet()), sortTypes, nullOrdering, sortIndexes);
            this.phase = SORT;
        }
    }

    /**
     * Process the input and store the partial accumulator values
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    private void groupSortPhase() throws TeiidComponentException, TeiidProcessingException {
        List<?> tuple = null;
        while ((tuple = groupSortTupleSource.nextTuple()) != null) {
            List<?> current = tree.find(tuple);

            boolean update = false;
            List<Object> accumulated = new ArrayList<Object>();
            //not all collected expressions are needed for the key
            for (int i = 0; i < orderBy.size(); i++) {
                accumulated.add(tuple.get(i));
            }
            if (current != null) {
                update = true;
            }
            int index = orderBy.size();
            for (int i = 0; i < this.groupSortfunctions.length; i++) {
                AggregateFunction aggregateFunction = this.groupSortfunctions[i];
                if (update) {
                    aggregateFunction.setState(current, index);
                } else {
                    aggregateFunction.reset();
                }
                index+=this.accumulatorStateCount[i];
                aggregateFunction.addInput(tuple, getContext());
                aggregateFunction.getState(accumulated);
            }
            tree.insert(accumulated, update?InsertMode.UPDATE:InsertMode.NEW, -1);
        }
        this.groupSortTupleSource.closeSource();
        this.groupSortTupleSource = tree.getTupleSource(true);
        this.phase = GROUP_SORT_OUTPUT;
    }

    /**
     * Walk the tree to produce the results
     * @return
     * @throws FunctionExecutionException
     * @throws ExpressionEvaluationException
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    private TupleBatch groupSortOutputPhase() throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException, TeiidProcessingException {
        List<?> tuple = null;
        int size = orderBy.size();
        List<Object> vals = Arrays.asList(new Object[size + groupSortfunctions.length]);
        while ((tuple = groupSortTupleSource.nextTuple()) != null) {
            for (int i = 0; i < size; i++) {
                vals.set(i, tuple.get(i));
            }
            int index = size;
            for (int i = 0; i < this.groupSortfunctions.length; i++) {
                AggregateFunction aggregateFunction = this.groupSortfunctions[i];
                aggregateFunction.setState(tuple, index);
                index+=this.accumulatorStateCount[i];
                vals.set(size + i, aggregateFunction.getResult(getContext()));
            }
            List<?> result = RelationalNode.projectTuple(projection, vals);
            addBatchRow(result);
            if (isBatchFull()) {
                return pullBatch();
            }
        }
        terminateBatches();
        return pullBatch();
    }

    private void sortPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        this.sortBuffer = this.sortUtility.sort();
        this.sortBuffer.setForwardOnly(true);
        this.groupTupleSource = this.sortBuffer.createIndexedTupleSource();
        this.phase = GROUP;
    }

    private TupleBatch groupPhase() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        CommandContext context = getContext();
        while(!doneReading) {

            if (currentGroupTuple == null) {
                currentGroupTuple = this.groupTupleSource.nextTuple();
                if (currentGroupTuple == null) {
                    doneReading = true;
                    break;
                }
            }

            if(lastRow == null) {
                // First row we've seen
                lastRow = currentGroupTuple;

            } else {
                int colDiff = sameGroup(indexes, currentGroupTuple, lastRow);
                if (colDiff != -1) {
                    // Close old group
                    closeGroup(colDiff, true, context);

                    // Reset last tuple
                    lastRow = currentGroupTuple;

                    // Save in output batch

                    if (this.isBatchFull()) {
                        return pullBatch();
                    }
                }
            }

            // Update function accumulators with new row - can throw blocked exception
            updateAggregates(currentGroupTuple);
            currentGroupTuple = null;
        }
        if(lastRow != null || orderBy == null) {
            // Close last group
            closeGroup(-1, false, context);
        }

        this.terminateBatches();
        return pullBatch();
    }

    protected void closeGroup(int colDiff, boolean reset, CommandContext context) throws FunctionExecutionException,
            ExpressionEvaluationException, TeiidComponentException,
            TeiidProcessingException {
        List<Object> row = new ArrayList<Object>(functions.length);
        for(int i=0; i<functions.length; i++) {
            row.add( functions[i][0].getResult(context) );
            if (reset && !rollup) {
                functions[i][0].reset();
            }
        }
        addBatchRow(row);
        if (rollup) {
            int rollups = orderBy.size() - colDiff;
            for (int j = 1; j < rollups; j++) {
                row = new ArrayList<Object>(functions.length);
                for(int i=0; i<functions.length; i++) {
                    if (functions[i].length == 1) {
                        int index = functions[i][0].getArgIndexes()[0];
                        Integer val = this.indexMap.get(index);
                        if (val != null && val <= j) {
                            row.add(null);
                        } else {
                            row.add(functions[i][0].getResult(context));
                        }
                    } else {
                        row.add( functions[i][j].getResult(context) );
                        if (reset) {
                            functions[i][j].reset();
                        }
                    }
                }
                addBatchRow(row);
            }
            if (reset) {
                for(int i=0; i<functions.length; i++) {
                    functions[i][0].reset();
                }
            }
        }
    }

    public static int sameGroup(int[] indexes, List<?> newTuple, List<?> oldTuple) {
        if (indexes == null) {
            return -1;
        }
        return MergeJoinStrategy.compareTuples(newTuple, oldTuple, indexes, indexes, true, true);
    }

    private void updateAggregates(List<?> tuple)
    throws TeiidComponentException, TeiidProcessingException {

        for(int i=0; i<functions.length; i++) {
            for (AggregateFunction function : functions[i]) {
                function.addInput(tuple, getContext());
            }
        }
    }

    public void closeDirect() {
        if (this.sortBuffer != null) {
            this.sortBuffer.remove();
            this.sortBuffer = null;
        }
        if (this.sortUtility != null) {
            this.sortUtility.remove();
            this.sortUtility = null;
        }
        if (this.tree != null) {
            this.tree.remove();
            this.tree = null;
        }
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append(orderBy);
        if (outputMapping != null) {
            str.append(outputMapping);
        }
    }

    public Object clone(){
        GroupingNode clonedNode = new GroupingNode(super.getID());
        super.copyTo(clonedNode);
        clonedNode.removeDuplicates = removeDuplicates;
        clonedNode.outputMapping = outputMapping;
        clonedNode.orderBy = orderBy;
        clonedNode.rollup = rollup;
        return clonedNode;
    }

    public PlanNode getDescriptionProperties() {
        // Default implementation - should be overridden
        PlanNode props = super.getDescriptionProperties();

        if(orderBy != null) {
            int elements = orderBy.size();
            List<String> groupCols = new ArrayList<String>(elements);
            for(int i=0; i<elements; i++) {
                groupCols.add(this.orderBy.get(i).toString());
            }
            props.addProperty(PROP_GROUP_COLS, groupCols);
        }
        if (outputMapping != null) {
            List<String> groupCols = new ArrayList<String>(outputMapping.asMap().size());
            for(Map.Entry<ElementSymbol, Expression> entry  : outputMapping.asMap().entrySet()) {
                groupCols.add(entry.toString());
            }
            props.addProperty(PROP_GROUP_MAPPING, groupCols);
        }
        props.addProperty(PROP_SORT_MODE, String.valueOf(this.removeDuplicates));
        if (rollup) {
            props.addProperty(PROP_ROLLUP, Boolean.TRUE.toString());
        }
        return props;
    }

    public void setRollup(boolean rollup) {
        this.rollup = rollup;
    }

}