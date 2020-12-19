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

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.id.IDGenerator;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.DependentAccessNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;

public final class RulePlanSubqueries implements OptimizerRule {

    private static final int LARGE_INDEPENDENT = 10000;

    /**
     * Used to replace correlated references
     */
    public static final class ReferenceReplacementVisitor extends
            ExpressionMappingVisitor {
        private final SymbolMap refs;
        private boolean replacedAny;

        public ReferenceReplacementVisitor(SymbolMap refs) {
            super(null);
            this.refs = refs;
        }

        public Expression replaceExpression(Expression element) {
            if (element instanceof Reference) {
                Reference r = (Reference)element;
                Expression ex = refs.getMappedExpression(r.getExpression());
                if (ex != null) {
                    if (ex instanceof ElementSymbol) {
                        ElementSymbol es = (ElementSymbol) ex.clone();
                        es.setIsExternalReference(false);
                        ex = es;
                    }
                    replacedAny = true;
                    return ex;
                }
            }
            return element;
        }

    }

    public static class PlannedResult {
        public List leftExpressions = new LinkedList();
        public List rightExpressions = new LinkedList();
        public Query query;
        public boolean not;
        public List<Criteria> nonEquiJoinCriteria = new LinkedList<Criteria>();
        public CompareCriteria additionalCritieria;
        public Class<?> type;
        public boolean mergeJoin;
        public boolean madeDistinct;
        public boolean makeInd;
        public boolean multiRow;
        public void reset() {
            this.leftExpressions.clear();
            this.rightExpressions.clear();
            this.query = null;
            this.not = false;
            this.nonEquiJoinCriteria.clear();
            this.additionalCritieria = null;
            this.type = null;
            this.mergeJoin = false;
            this.madeDistinct = false;
            this.makeInd = false;
            this.multiRow = false;
        }
    }

    /**
     * Return true if the result from the subquery may be different
     * if non-distinct rows are used as input
     * @param query
     * @return
     */
    public static boolean requiresDistinctRows(Query query) {
        Set<AggregateSymbol> aggs = new HashSet<AggregateSymbol>();
        aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(query.getSelect(), false));
        aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(query.getHaving(), false));
        if (!aggs.isEmpty() || query.getGroupBy() != null) {
            if (!AggregateSymbol.areAggregatesCardinalityDependent(aggs)) {
                return false;
            }
        } else if (query.getSelect().isDistinct()) {
            for (Expression projectSymbol : query.getSelect().getProjectedSymbols()) {
                Expression ex = SymbolMap.getExpression(projectSymbol);
                if (FunctionCollectorVisitor.isNonDeterministic(ex)) {
                    return true;
                }
                if (!ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ex).isEmpty()) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private IDGenerator idGenerator;
    private CapabilitiesFinder capFinder;
    private AnalysisRecord analysisRecord;
    private CommandContext context;
    private QueryMetadataInterface metadata;
    private boolean dependent;

    public RulePlanSubqueries(IDGenerator idGenerator, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context, QueryMetadataInterface metadata) {
        this.idGenerator = idGenerator;
        this.capFinder = capFinder;
        this.analysisRecord = analysisRecord;
        this.context = context;
        this.metadata = metadata;
    }

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, TeiidComponentException {
        dependent = false;
        processSubqueries(plan);
        if (dependent) {
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }
        return plan;
    }

    void processSubqueries(PlanNode root)
            throws QueryPlannerException, TeiidComponentException {

        PlanNode recurseRoot = root;
        if(root.getType() == NodeConstants.Types.SELECT) {

            // Walk to end of the chain and change recurse root
            while(recurseRoot.getType() == NodeConstants.Types.SELECT) {
                // Look for opportunities to replace with a semi-join
                recurseRoot = planMergeJoin(recurseRoot, root);
                if (root.getChildCount() == 0) {
                    root = recurseRoot.getFirstChild();
                    if (root.getType() != NodeConstants.Types.SELECT) {
                        root = root.getParent();
                    }
                }
                recurseRoot = recurseRoot.getFirstChild();
            }
        } else if (root.getType() == NodeConstants.Types.PROJECT) {
            PlannedResult plannedResult = new PlannedResult();
            List<Expression> symbols = (List<Expression>) root.getProperty(Info.PROJECT_COLS);
            List<Expression> output = (List<Expression>) root.getProperty(Info.OUTPUT_COLS);
            for (int i = 0; i < symbols.size(); i++) {
                Expression symbol = symbols.get(i);
                plannedResult.reset();
                findSubquery(SymbolMap.getExpression(symbol), true, plannedResult, false);
                if (plannedResult.query == null
                        || plannedResult.query.getFrom() == null
                        || plannedResult.not) {
                    continue;
                }
                PlanNode child = root.getFirstChild();
                PlanNode newRecurseRoot = planMergeJoin(child, child, symbol, plannedResult, true);
                if (newRecurseRoot != child) {
                    Expression newProjection = ((List<Expression>)newRecurseRoot.getLastChild().getProperty(Info.OUTPUT_COLS)).get(0);
                    symbols.set(i, newProjection);
                    output.set(i, newProjection);
                    ((List<Expression>)newRecurseRoot.getProperty(Info.OUTPUT_COLS)).add(newProjection);
                    //we may have used some of the state in planning, so create new
                    plannedResult = new PlannedResult();
                }
            }
        }

        if (recurseRoot.getType() != NodeConstants.Types.ACCESS) {
            for (PlanNode child : recurseRoot.getChildren()) {
                processSubqueries(child);
            }
        }
    }

    /**
     * Look for:
     * [NOT] EXISTS ( )
     * IN ( ) / SOME ( )
     *
     * and replace with a semi join
     */
    private PlanNode planMergeJoin(PlanNode current, PlanNode root) throws QueryMetadataException,
            TeiidComponentException {
        Criteria crit = (Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA);

        PlannedResult plannedResult = findSubquery(crit, true);
        if (plannedResult.query == null) {
            return current;
        }
        return planMergeJoin(current, root, crit, plannedResult, false);
    }

    private PlanNode planMergeJoin(PlanNode current, PlanNode root,
            LanguageObject obj, PlannedResult plannedResult, boolean isProjection)
            throws QueryMetadataException, TeiidComponentException {
        if (current == null || current.getFirstChild() == null) {
            //select subquery with no from or over just a source node representing a table function
            return current;
        }
        float sourceCost = NewCalculateCostUtil.computeCostForTree(current.getFirstChild(), metadata);
        if (!isProjection && sourceCost != NewCalculateCostUtil.UNKNOWN_VALUE
                && sourceCost < RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY && !plannedResult.mergeJoin) {
            //TODO: see if a dependent join applies the other direction - which we now handle in the isProjection case
            return current;
        }

        RelationalPlan originalPlan = (RelationalPlan)plannedResult.query.getProcessorPlan();
        Number originalCardinality = originalPlan.getRootNode().getEstimateNodeCardinality();
        if (!plannedResult.mergeJoin && originalCardinality.floatValue() == NewCalculateCostUtil.UNKNOWN_VALUE) {
            //TODO: this check isn't really accurate - exists and scalarsubqueries will always have cardinality 2/1
            //if it's currently unknown, removing criteria won't make it any better
            return current;
        }
        Collection<GroupSymbol> leftGroups = FrameUtil.findJoinSourceNode(current).getGroups();
        if (!planQuery(leftGroups, false, plannedResult)) {
            if (plannedResult.mergeJoin && analysisRecord != null && analysisRecord.recordAnnotations()) {
                this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, "Could not plan as a merge join: " + obj, "ignoring MJ hint", Priority.HIGH)); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return current;
        }

        //check if the child is already ordered.  TODO: see if the ordering is compatible.
        PlanNode childSort = NodeEditor.findNodePreOrder(root, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN);
        if (childSort != null) {
            if (plannedResult.mergeJoin && analysisRecord != null && analysisRecord.recordAnnotations()) {
                this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, "Could not plan as a merge join since the parent join requires a sort: " + obj, "ignoring MJ hint", Priority.HIGH)); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return current;
        }

        //add an order by, which hopefully will get pushed down
        if (!isProjection) { //we can't sort in the projection case as we can't later decline the sort from the subplan
            plannedResult.query.setOrderBy(new OrderBy(plannedResult.rightExpressions).clone());
            for (OrderByItem item : plannedResult.query.getOrderBy().getOrderByItems()) {
                int index = plannedResult.query.getProjectedSymbols().indexOf(item.getSymbol());
                if (index >= 0 && !(item.getSymbol() instanceof ElementSymbol)) {
                    item.setSymbol((Expression) plannedResult.query.getProjectedSymbols().get(index).clone());
                }
                item.setExpressionPosition(index);
            }
        }

        String id = null;
        if (isProjection) {
            //basic cost determination, without a hint don't proceed if the outer side is "large"
            //The other test below will be if a dependent join can be used, which we
            //check after planning
            if (plannedResult.rightExpressions.isEmpty()) {
                return current;
            }
            if (!plannedResult.mergeJoin) {
                if (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE) {
                    return current;
                }
                float ndv = NewCalculateCostUtil.getNDVEstimate(current.getFirstChild(), metadata, sourceCost, plannedResult.leftExpressions, true);
                //hard-coded fail-safe
                if (ndv > LARGE_INDEPENDENT) {
                    return current;
                }
            }
            //if the dep join is not created...
            id = RuleChooseDependent.nextId();
            PlanNode dep = RuleChooseDependent.getDependentCriteriaNode(id, plannedResult.leftExpressions, plannedResult.rightExpressions, root, metadata, null, false, null);
            Criteria crit  = (Criteria)dep.getProperty(Info.SELECT_CRITERIA);
            plannedResult.query.setCriteria(Criteria.combineCriteria(plannedResult.query.getCriteria(), crit));
        }

        try {
            //clone the symbols as they may change during planning
            List<? extends Expression> projectedSymbols = LanguageObject.Util.deepClone(plannedResult.query.getProjectedSymbols(), Expression.class);
            //NOTE: we could tap into the relationalplanner at a lower level to get this in a plan node form,
            //the major benefit would be to reuse the dependent join planning logic if possible.
            RelationalPlan subPlan = (RelationalPlan)QueryOptimizer.optimizePlan(plannedResult.query, metadata, idGenerator, capFinder, analysisRecord, context);
            Number planCardinality = subPlan.getRootNode().getEstimateNodeCardinality();

            if (!plannedResult.mergeJoin) {
                if (isProjection) {
                    RelationalNode rn = subPlan.getRootNode();
                    if (!isUsingDependentJoin(id, rn)) {
                        return current;
                    }
                } else //if we don't have a specific hint, then use costing
                if (planCardinality.floatValue() == NewCalculateCostUtil.UNKNOWN_VALUE
                        || planCardinality.floatValue() > 10000000
                        || (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE && planCardinality.floatValue() > 1000)
                        || (sourceCost != NewCalculateCostUtil.UNKNOWN_VALUE && sourceCost * originalCardinality.floatValue() < planCardinality.floatValue() / (100 * Math.log(Math.max(4, sourceCost))))) {
                    //bail-out if both are unknown or the new plan is too large
                    if (analysisRecord != null && analysisRecord.recordDebug()) {
                        current.recordDebugAnnotation("cost of merge join plan was not favorable", null, "semi merge join will not be used", analysisRecord, metadata); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    return current;
                }
            }

            if (isProjection) {
                plannedResult.makeInd = false;
            } else {
                //assume dependent
                plannedResult.makeInd = makeDep(sourceCost, planCardinality.floatValue());
            }

            /*if (plannedResult.makeInd
                    && plannedResult.query.getCorrelatedReferences() == null
                    && !plannedResult.not
                    && plannedResult.leftExpressions.size() == 1) {
                //TODO: this should just be a dependent criteria node to avoid sorts
            }*/

            current.recordDebugAnnotation("Conditions met (hint or cost)", null, "Converting to a semi merge join", analysisRecord, metadata); //$NON-NLS-1$ //$NON-NLS-2$

            PlanNode semiJoin = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            semiJoin.addGroups(current.getGroups());

            //create a proper projection "sub" view.  then update the right expressions
            //to the virtual group columns
            GroupSymbol v = RulePlaceAccess.recontextSymbol(new GroupSymbol("sub"), context.getGroups()); //$NON-NLS-1$
            v.setName(v.getName());
            v.setDefinition(null);
            TempMetadataStore tms = new TempMetadataStore();
            Select s = new Select(projectedSymbols);
            QueryRewriter.makeSelectUnique(s, false);
            v.setMetadataID(tms.addTempGroup(v.getName(), s.getProjectedSymbols()));
            List<ElementSymbol> virtualCols = ResolverUtil.resolveElementsInGroup(v, new TempMetadataAdapter(metadata, tms));
            projectedSymbols = virtualCols;
            SymbolMap map = SymbolMap.createSymbolMap(virtualCols, s.getProjectedSymbols());
            Map<Expression, ElementSymbol> inverseMapping = map.inserseMapping();
            List<Expression> mappedRight = new ArrayList<>(plannedResult.rightExpressions.size());
            for (Expression ex : (List<Expression>)plannedResult.rightExpressions) {
                ex = inverseMapping.get(SymbolMap.getExpression(ex));
                if (ex == null) {
                    LogManager.logWarning(LogConstants.CTX_DQP, "Could not map column from subquery optimization, backing out"); //$NON-NLS-1$
                    return current;
                }
                mappedRight.add(ex);
            }
            plannedResult.rightExpressions = mappedRight;

            Set<GroupSymbol> groups = new LinkedHashSet<>();
            groups.add(v);
            semiJoin.addGroups(groups);
            semiJoin.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
            if (isProjection) {
                semiJoin.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
                if (plannedResult.multiRow) {
                    semiJoin.setProperty(NodeConstants.Info.SINGLE_MATCH, true);
                }
            } else {
                semiJoin.setProperty(NodeConstants.Info.JOIN_TYPE, plannedResult.not?JoinType.JOIN_ANTI_SEMI:JoinType.JOIN_SEMI);
            }
            if (!plannedResult.nonEquiJoinCriteria.isEmpty()) {
                for (Criteria c : plannedResult.nonEquiJoinCriteria) {
                    ExpressionMappingVisitor.mapExpressions(c, inverseMapping);
                }
                semiJoin.setProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA, plannedResult.nonEquiJoinCriteria);
            }
            List<Criteria> joinCriteria = new ArrayList<Criteria>();
            joinCriteria.addAll(plannedResult.nonEquiJoinCriteria);
            for (int i = 0; i < plannedResult.leftExpressions.size(); i++) {
                joinCriteria.add(new CompareCriteria((Expression)plannedResult.rightExpressions.get(i), CompareCriteria.EQ, (Expression)plannedResult.leftExpressions.get(i)));
            }
            semiJoin.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
            //nested subqueries are possibly being promoted, so they need their references updated
            List<SymbolMap> refMaps = semiJoin.getAllReferences();
            SymbolMap parentRefs = plannedResult.query.getCorrelatedReferences();
            for (SymbolMap refs : refMaps) {
                for (Map.Entry<ElementSymbol, Expression> ref : refs.asUpdatableMap().entrySet()) {
                    Expression expr = ref.getValue();
                    if (expr instanceof ElementSymbol) {
                        Expression convertedExpr = parentRefs.getMappedExpression((ElementSymbol)expr);
                        if (convertedExpr != null) {
                            ref.setValue(convertedExpr);
                        }
                    }
                    semiJoin.getGroups().addAll(GroupsUsedByElementsVisitor.getGroups(ref.getValue()));
                }
            }
            semiJoin.setProperty(NodeConstants.Info.LEFT_EXPRESSIONS, plannedResult.leftExpressions);
            semiJoin.getGroups().addAll(GroupsUsedByElementsVisitor.getGroups(plannedResult.leftExpressions));
            semiJoin.setProperty(NodeConstants.Info.RIGHT_EXPRESSIONS, plannedResult.rightExpressions);
            semiJoin.setProperty(NodeConstants.Info.SORT_RIGHT, SortOption.ALREADY_SORTED);
            semiJoin.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList((List<?>)root.getProperty(NodeConstants.Info.OUTPUT_COLS)));

            List childOutput = (List)current.getFirstChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
            PlanNode toCorrect = root;
            while (toCorrect != current) {
                toCorrect.setProperty(NodeConstants.Info.OUTPUT_COLS, childOutput);
                toCorrect = toCorrect.getFirstChild();
            }

            PlanNode node = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
            node.setProperty(NodeConstants.Info.PROCESSOR_PLAN, subPlan);
            node.setProperty(NodeConstants.Info.OUTPUT_COLS, projectedSymbols);
            node.setProperty(NodeConstants.Info.EST_CARDINALITY, planCardinality);
            node.addGroups(groups);
            root.addAsParent(semiJoin);
            semiJoin.addLastChild(node);
            PlanNode result = current.getParent();
            if (!isProjection) {
                NodeEditor.removeChildNode(result, current);
            }
            RuleImplementJoinStrategy.insertSort(semiJoin.getFirstChild(), plannedResult.leftExpressions, semiJoin, metadata, capFinder, true, context);
            if (isProjection) { //this is always a dep join case, and the predicate was already added
                semiJoin.setProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE, id);
                semiJoin.setProperty(Info.SORT_RIGHT, SortOption.SORT);
                this.dependent = true;
            } else if (plannedResult.makeInd && !plannedResult.not) {
                id = RuleChooseDependent.nextId();
                //TODO: would like for an enhanced sort merge with the semi dep option to avoid the sorting
                //this is a little different than a typical dependent join in that the right is the independent side
                PlanNode dep = RuleChooseDependent.getDependentCriteriaNode(id, plannedResult.rightExpressions, plannedResult.leftExpressions, node, metadata, null, false, null);
                dep.setProperty(Info.OUTPUT_COLS, new ArrayList((List<?>)semiJoin.getFirstChild().getProperty(Info.OUTPUT_COLS)));
                semiJoin.getFirstChild().addAsParent(dep);
                semiJoin.setProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE, id);
                this.dependent = true;
            }
            return result;
        } catch (QueryPlannerException e) {
            //can't be done - probably access patterns - what about dependent
            return current;
        }
    }

    /**
     * Because we lack a cost value based upon complexity the
     * heurstic is to look that the dependent set is fully pushed.
     * there are situations where it won't be fully pushed, but
     * for which it will still be a better plan and we'll reject
     */
    private boolean isUsingDependentJoin(String id, RelationalNode rn) {
        if (rn instanceof DependentAccessNode) {
            DependentAccessNode dan = (DependentAccessNode)rn;
            Query qc = (Query) dan.getCommand();
            Criteria c = qc.getCriteria();
            for (Criteria crit : Criteria.separateCriteriaByAnd(c)) {
                if (crit instanceof DependentSetCriteria) {
                    DependentSetCriteria dsc = (DependentSetCriteria)crit;
                    if (dsc.getContextSymbol().equals(id)) {
                        return true;
                    }
                }
            }
        }
        RelationalNode[] children = rn.getChildren();
        for (int i=0; i<rn.getChildCount(); i++) {
            if (isUsingDependentJoin(id, children[i])) {
                return true;
            }
        }
        return false;
    }

    private boolean makeDep(float depSide, float indSide) {
        return (depSide != NewCalculateCostUtil.UNKNOWN_VALUE && indSide != NewCalculateCostUtil.UNKNOWN_VALUE
            && indSide < depSide / 8) || (depSide == NewCalculateCostUtil.UNKNOWN_VALUE && indSide <= 1000);
    }

    public PlannedResult findSubquery(Expression expr, boolean unnest, PlannedResult result, boolean requireSingleRow) throws QueryMetadataException, TeiidComponentException {
        if (expr instanceof ScalarSubquery) {
            ScalarSubquery scc = (ScalarSubquery)expr;
            if (scc.getSubqueryHint().isNoUnnest()) {
                return result;
            }

            Query query = (Query)scc.getCommand();

            result.multiRow = !isSingleRow(query);

            result.type = scc.getClass();
            result.mergeJoin = scc.getSubqueryHint().isMergeJoin();
            if (!unnest && !result.mergeJoin) {
                return result;
            }
            result.makeInd = scc.getSubqueryHint().isDepJoin();
            result.query = query;
        }
        return result;
    }

    private boolean isSingleRow(Query query) {
        if (query.hasAggregates() && query.getGroupBy() == null) {
            return true;
        }
        return false;
        //unique key is looked at in planQuery
    }

    public PlannedResult findSubquery(Criteria crit, boolean unnest) throws TeiidComponentException, QueryMetadataException {
        PlannedResult result = new PlannedResult();
        if (crit instanceof SubquerySetCriteria) {
            //convert to the quantified form
            SubquerySetCriteria ssc = (SubquerySetCriteria)crit;
            if (ssc.getSubqueryHint().isNoUnnest()) {
                return result;
            }
            result.not = ssc.isNegated();
            result.type = ssc.getClass();
            crit = new SubqueryCompareCriteria(ssc.getExpression(), ssc.getCommand(), SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
            ((SubqueryCompareCriteria)crit).setSubqueryHint(ssc.getSubqueryHint());
        } else if (crit instanceof CompareCriteria) {
            CompareCriteria cc = (CompareCriteria)crit;
            if (cc.getRightExpression() instanceof ScalarSubquery) {
                ScalarSubquery ss = (ScalarSubquery)cc.getRightExpression();
                if (ss.getSubqueryHint().isNoUnnest()) {
                    return result;
                }
                result.type = ss.getClass();
                if (ss.getCommand() instanceof Query) {
                    Query query = (Query)ss.getCommand();
                    result.multiRow = !isSingleRow(query);
                    crit = new SubqueryCompareCriteria(cc.getLeftExpression(), ss.getCommand(), cc.getOperator(), SubqueryCompareCriteria.SOME);
                    ((SubqueryCompareCriteria)crit).setSubqueryHint(ss.getSubqueryHint());
                }
            } else if (cc.getLeftExpression() instanceof ScalarSubquery) {
                ScalarSubquery ss = (ScalarSubquery)cc.getLeftExpression();
                if (ss.getSubqueryHint().isNoUnnest()) {
                    return result;
                }
                result.type = ss.getClass();
                if (ss.getCommand() instanceof Query) {
                    Query query = (Query)ss.getCommand();
                    result.multiRow = !isSingleRow(query);
                    crit = new SubqueryCompareCriteria(cc.getRightExpression(), ss.getCommand(), cc.getReverseOperator(), SubqueryCompareCriteria.SOME);
                    ((SubqueryCompareCriteria)crit).setSubqueryHint(ss.getSubqueryHint());
                }
            }
        }
        if (crit instanceof SubqueryCompareCriteria) {
            SubqueryCompareCriteria scc = (SubqueryCompareCriteria)crit;
            if (scc.getSubqueryHint().isNoUnnest()) {
                return result;
            }
            if (scc.getPredicateQuantifier() != SubqueryCompareCriteria.SOME
                    //TODO: could add an inline view if not a query
                    || !(scc.getCommand() instanceof Query)) {
                return result;
            }

            Query query = (Query)scc.getCommand();
            Expression rightExpr = SymbolMap.getExpression(query.getProjectedSymbols().get(0));

            if (result.not && !isNonNull(query, rightExpr)) {
                return result;
            }
            if (result.type == null) {
                result.type = scc.getClass();
            }
            result.mergeJoin = scc.getSubqueryHint().isMergeJoin();
            if (!unnest && !result.mergeJoin) {
                return result;
            }
            result.makeInd = scc.getSubqueryHint().isDepJoin();
            result.query = query;
            result.additionalCritieria = (CompareCriteria)new CompareCriteria(scc.getLeftExpression(), scc.getOperator(), rightExpr).clone();
        }
        if (crit instanceof ExistsCriteria) {
            ExistsCriteria exists = (ExistsCriteria)crit;
            if (exists.getSubqueryHint().isNoUnnest()) {
                return result;
            }
            if (!(exists.getCommand() instanceof Query)) {
                return result;
            }
            result.type = crit.getClass();
            result.not = exists.isNegated();
            //the correlations can only be in where (if no group by or aggregates) or having
            result.mergeJoin = exists.getSubqueryHint().isMergeJoin();
            result.makeInd = exists.getSubqueryHint().isDepJoin();
            if (!unnest && !result.mergeJoin) {
                return result;
            }
            result.query = (Query)exists.getCommand();
        }
        return result;
    }

    private boolean isNonNull(Query query, Expression rightExpr)
            throws TeiidComponentException, QueryMetadataException {
        if (rightExpr instanceof ElementSymbol) {
            ElementSymbol es = (ElementSymbol)rightExpr;
            if (metadata.elementSupports(es.getMetadataID(), SupportConstants.Element.NULL) || metadata.elementSupports(es.getMetadataID(), SupportConstants.Element.NULL_UNKNOWN)) {
                return false;
            }
            if (!isSimpleJoin(query)) {
                return false;
            }
        } else if (rightExpr instanceof Constant) {
            if (((Constant)rightExpr).isNull()) {
                return false;
            }
        } else if (rightExpr instanceof AggregateSymbol) {
            AggregateSymbol as = (AggregateSymbol)rightExpr;
            if (!as.isCount()) {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    private boolean isSimpleJoin(Query query) {
        if (query.getFrom() != null) {
            for (FromClause clause : query.getFrom().getClauses()) {
                if (RuleCollapseSource.hasOuterJoins(clause)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean planQuery(Collection<GroupSymbol> leftGroups, boolean requireDistinct, PlannedResult plannedResult) throws QueryMetadataException, TeiidComponentException {
        if ((plannedResult.query.getLimit() != null && !plannedResult.query.getLimit().isImplicit()) || plannedResult.query.getFrom() == null) {
            return false;
        }

        if ((plannedResult.type == ExistsCriteria.class || plannedResult.type == ScalarSubquery.class) && plannedResult.query.getCorrelatedReferences() == null) {
            //we can't really improve on this case
            //TODO: do this check earlier
            return false;
        }

        plannedResult.query = (Query)plannedResult.query.clone();
        for (Command c : CommandCollectorVisitor.getCommands(plannedResult.query)) {
            //subqueries either need to be re-resolved or replanned to maintain
            //multilevel correlated references.  it's easier for now to replan
            c.setProcessorPlan(null);
        }
        plannedResult.query.setLimit(null);

        List<GroupSymbol> rightGroups = plannedResult.query.getFrom().getGroups();

        boolean hasAggregates = plannedResult.query.hasAggregates();

        Set<Expression> requiredExpressions = new LinkedHashSet<Expression>();
        final SymbolMap refs = plannedResult.query.getCorrelatedReferences();
        boolean addGroupBy = false;
        if (refs != null) {
            Criteria where = plannedResult.query.getCriteria();
            if (plannedResult.query.getGroupBy() == null) {
                plannedResult.query.setCriteria(null);
            }
            Criteria having = plannedResult.query.getHaving();
            plannedResult.query.setHaving(null);
            if (hasCorrelatedReferences(plannedResult.query, refs)) {
                return false;
            }
            if (plannedResult.query.getGroupBy() == null) {
                processCriteria(leftGroups, plannedResult, rightGroups, requiredExpressions, refs, where, null, true);
                if (hasAggregates) {
                    if (!plannedResult.nonEquiJoinCriteria.isEmpty()) {
                        return false;
                    }
                    addGroupBy = true;
                    if (!canAddGrouping(plannedResult.query.getSelect())) {
                        return false;
                    }
                    if (!canAddGrouping(having)) {
                        boolean okToAdd = false;
                        for (Expression ex : (List<Expression>)plannedResult.rightExpressions) {
                            if (canAddGrouping(ex)) {
                                okToAdd = true;
                            }
                        }
                        if (!okToAdd) {
                            return false;
                        }
                    }
                }
            }
            processCriteria(leftGroups, plannedResult, rightGroups, requiredExpressions, refs, having, plannedResult.query.getGroupBy(), false);
        }

        if (plannedResult.additionalCritieria != null) {
            //move the additional subquery compare criteria onto the query
            //which effectively makes this an exists predicate
            if (EvaluatableVisitor.isFullyEvaluatable(plannedResult.additionalCritieria.getLeftExpression(), false)) {
                plannedResult.type = ExistsCriteria.class;
                //rewrite back to the other direction
                CompareCriteria cc = new CompareCriteria(plannedResult.additionalCritieria.getRightExpression(), plannedResult.additionalCritieria.getReverseOperator(), plannedResult.additionalCritieria.getLeftExpression());
                if (hasAggregates) {
                    plannedResult.query.setHaving(Criteria.combineCriteria(plannedResult.query.getHaving(), cc));
                } else {
                    plannedResult.query.setCriteria(Criteria.combineCriteria(plannedResult.query.getCriteria(), cc));
                }
            } else {
                RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, plannedResult.leftExpressions, plannedResult.rightExpressions, Criteria.separateCriteriaByAnd(plannedResult.additionalCritieria), plannedResult.nonEquiJoinCriteria);
            }
        }

        if (plannedResult.leftExpressions.isEmpty()) {
            return false;
        }

        plannedResult.leftExpressions = RuleChooseJoinStrategy.createExpressionSymbols(plannedResult.leftExpressions);
        plannedResult.rightExpressions = RuleChooseJoinStrategy.createExpressionSymbols(plannedResult.rightExpressions);

        if (requireDistinct && !addGroupBy) {
            //ensure that uniqueness applies to the in condition
            if (plannedResult.rightExpressions.size() > 1
                    && (plannedResult.type != SubquerySetCriteria.class || !isDistinct(plannedResult.query, plannedResult.rightExpressions.subList(plannedResult.rightExpressions.size() - 1, plannedResult.rightExpressions.size()), metadata))) {
                return false;
            }

            if (!isDistinct(plannedResult.query, plannedResult.rightExpressions, metadata)) {
                if (plannedResult.type == ExistsCriteria.class) {
                    if (requiredExpressions.size() > plannedResult.leftExpressions.size()) {
                        return false; //not an equi join
                    }
                } else if (!requiredExpressions.isEmpty() && !isDistinct(plannedResult.query, plannedResult.query.getProjectedSymbols(), metadata)) {
                    return false;
                }
                plannedResult.query.getSelect().setDistinct(true);
                plannedResult.madeDistinct = true;
            }
        }

        //it doesn't matter what the select columns are
        if (plannedResult.type == ExistsCriteria.class) {
            plannedResult.query.getSelect().clearSymbols();
        }

        if (addGroupBy) {
            LinkedHashSet<Expression> groupingSymbols = new LinkedHashSet<Expression>();
            for (Expression expr : (List<Expression>)plannedResult.rightExpressions) {
                AggregateSymbolCollectorVisitor.getAggregates(expr, null, groupingSymbols, null, null, null);
            }
            if (!groupingSymbols.isEmpty()) {
                plannedResult.query.setGroupBy((GroupBy) new GroupBy(new ArrayList<Expression>(groupingSymbols)).clone());
            }
        }
        HashSet<Expression> projectedSymbols = new HashSet<Expression>();
        for (Expression ses : plannedResult.query.getProjectedSymbols()) {
            projectedSymbols.add(SymbolMap.getExpression(ses));
        }
        for (Expression ses : requiredExpressions) {
            if (projectedSymbols.add(ses)) {
                plannedResult.query.getSelect().addSymbol((Expression)ses.clone());
            }
        }
        for (Expression ses : (List<Expression>)plannedResult.rightExpressions) {
            if (projectedSymbols.add(SymbolMap.getExpression(ses))) {
                plannedResult.query.getSelect().addSymbol((Expression)ses.clone());
            }
        }
        if (plannedResult.madeDistinct
                && plannedResult.multiRow
                && plannedResult.query.getSelect().getProjectedSymbols().size() > 1) {
            return false;
        }
        return true;
    }

    private boolean canAddGrouping(LanguageObject lo) {
        for (AggregateSymbol as : AggregateSymbolCollectorVisitor.getAggregates(lo, false)) {
            if (as.isCount() || as.getAggregateFunction() == Type.TEXTAGG) {
                //these can be non-null for no rows.
                //TODO: could include udaf as well
                return false;
            }
        }
        return true;
    }

    private void processCriteria(Collection<GroupSymbol> leftGroups,
            PlannedResult plannedResult, List<GroupSymbol> rightGroups,
            Set<Expression> requiredExpressions, final SymbolMap refs,
            Criteria joinCriteria, GroupBy groupBy, boolean where) {
        if (joinCriteria == null) {
            return;
        }
        List<Criteria> crits = Criteria.separateCriteriaByAnd((Criteria)joinCriteria.clone());

        for (Iterator<Criteria> critIter = crits.iterator(); critIter.hasNext();) {
            Criteria conjunct = critIter.next();
            List<Expression> additionalRequired = new LinkedList<Expression>();
            AggregateSymbolCollectorVisitor.getAggregates(conjunct, additionalRequired, additionalRequired, additionalRequired, null, groupBy!=null?groupBy.getSymbols():null);
            ReferenceReplacementVisitor emv = new ReferenceReplacementVisitor(refs);
            DeepPostOrderNavigator.doVisit(conjunct, emv);
            if (!emv.replacedAny) {
                //if not correlated, then leave it on the query
                critIter.remove();
                if (where) {
                    plannedResult.query.setCriteria(Criteria.combineCriteria(plannedResult.query.getCriteria(), conjunct));
                } else {
                    plannedResult.query.setHaving(Criteria.combineCriteria(plannedResult.query.getHaving(), conjunct));
                }
            } else {
                requiredExpressions.addAll(additionalRequired);
            }
        }
        RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, plannedResult.leftExpressions, plannedResult.rightExpressions, crits, plannedResult.nonEquiJoinCriteria);
    }

    public static boolean isDistinct(Query query, List<Expression> expressions, QueryMetadataInterface metadata)
            throws QueryMetadataException, TeiidComponentException {
        boolean distinct = false;
        if (query.getGroupBy() != null) {
            distinct = true;
            for (Expression groupByExpr : query.getGroupBy().getSymbols()) {
                if (!expressions.contains(groupByExpr)) {
                    distinct = false;
                    break;
                }
            }
        }
        if (distinct) {
            return true;
        }
        HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
        ResolverUtil.findKeyPreserved(query, keyPreservingGroups, metadata);
        return NewCalculateCostUtil.usesKey(expressions, keyPreservingGroups, metadata, true);
    }

    private boolean hasCorrelatedReferences(LanguageObject object, SymbolMap correlatedReferences) {
        Collection<Reference> references =  ReferenceCollectorVisitor.getReferences(object);
        for (Reference reference : references) {
            if (correlatedReferences.asMap().containsKey(reference.getExpression())) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        return "PlanSubqueries"; //$NON-NLS-1$
    }

}
