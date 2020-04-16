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

import java.util.*;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;


/**
 * @since 4.2
 */
public class RulePushAggregates implements
                               OptimizerRule {

    private IDGenerator idGenerator;
    private CommandContext context;
    private List<PlanNode> groupingNodes;

    public RulePushAggregates(IDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    /**
     * @see org.teiid.query.optimizer.relational.OptimizerRule#execute(org.teiid.query.optimizer.relational.plantree.PlanNode,
     *      org.teiid.query.metadata.QueryMetadataInterface, org.teiid.query.optimizer.capabilities.CapabilitiesFinder,
     *      org.teiid.query.optimizer.relational.RuleStack, AnalysisRecord, CommandContext)
     * @since 4.2
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext ctx) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {
        this.context = ctx;
        groupingNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.GROUP, NodeConstants.Types.ACCESS);
        outer: for (int i = 0; i < groupingNodes.size(); i++) {
            PlanNode groupNode = groupingNodes.get(i);
            if (groupNode.hasBooleanProperty(Info.ROLLUP)) {
                continue;
            }
            PlanNode child = groupNode.getFirstChild();

            List<Expression> groupingExpressions = (List<Expression>)groupNode.getProperty(NodeConstants.Info.GROUP_COLS);
            if (groupingExpressions == null) {
                groupingExpressions = Collections.emptyList();
            }

            try {
                if (child.getType() == NodeConstants.Types.SOURCE) {
                    PlanNode setOp = child.getFirstChild();
                    pushGroupNodeOverUnion(metadata, capFinder, groupNode, child, groupingExpressions, setOp, analysisRecord);
                    continue;
                } else if (child.getType() != NodeConstants.Types.JOIN) {
                    PlanNode access = NodeEditor.findNodePreOrder(child, NodeConstants.Types.ACCESS, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN | NodeConstants.Types.SET_OP);
                    if (access != null) {
                        PlanNode parent = access.getParent();
                        while (parent != groupNode) {
                            if (parent.getType() != NodeConstants.Types.SELECT) {
                                continue outer;
                            }
                            parent = parent.getParent();
                        }
                        Set<AggregateSymbol> aggregates = collectAggregates(groupNode);

                        //hybrid of the join/union/decompose pushing logic
                        if (access.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
                            if (!RuleRaiseAccess.isPartitioned(metadata, groupingExpressions, groupNode)) {
                                for (AggregateSymbol agg : aggregates) {
                                    if (!agg.canStage()) {
                                        continue outer;
                                    }
                                }
                                boolean shouldPushdown = canPushGroupByToUnionChild(metadata, capFinder, groupingExpressions, aggregates, access, analysisRecord, groupNode);
                                if (!shouldPushdown) {
                                    continue;
                                }
                                Set<Expression> stagedGroupingSymbols = new LinkedHashSet<Expression>();
                                stagedGroupingSymbols.addAll(groupingExpressions);
                                aggregates = stageAggregates(groupNode, metadata, stagedGroupingSymbols, aggregates, false);
                                if (aggregates.isEmpty() && stagedGroupingSymbols.isEmpty()) {
                                    continue;
                                }
                                addGroupBy(child, new ArrayList<Expression>(stagedGroupingSymbols), aggregates, metadata, groupNode.getParent(), capFinder, false, stagedGroupingSymbols.isEmpty() && containsNullDependent(aggregates));
                            } else if (groupNode.getFirstChild() == access
                                    && RuleRaiseAccess.canRaiseOverGroupBy(groupNode, child, aggregates, metadata, capFinder, analysisRecord, false)
                                    && canFilterEmpty(metadata, capFinder, child, groupingExpressions)) {
                                if (groupingExpressions.isEmpty()) {
                                    addEmptyFilter(aggregates, groupNode, metadata, capFinder, RuleRaiseAccess.getModelIDFromAccess(child, metadata));
                                }
                                access.getGroups().clear();
                                access.getGroups().addAll(groupNode.getGroups());
                                RuleRaiseAccess.performRaise(null, access, access.getParent());
                                if (groupingExpressions.isEmpty() && RuleRaiseAccess.canRaiseOverSelect(access, metadata, capFinder, access.getParent(), null)) {
                                    RuleRaiseAccess.performRaise(null, access, access.getParent());
                                }
                            }
                        }
                        //TODO: consider pushing aggregate in general
                    }
                    continue;
                }
            } catch (QueryResolverException e) {
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30264, e);
            }

            Set<AggregateSymbol> aggregates = collectAggregates(groupNode);

            pushGroupNode(groupNode, groupingExpressions, aggregates, metadata, capFinder, context);
        }

        return plan;
    }

    /**
     * The plan tree looks like:
     * group [agg(x), {a, b}]
     *   source
     *     set op
     *       child 1
     *       ...
     *
     * we need to make it into
     *
     * group [agg(agg(x)), {a, b}]
     *   source
     *     set op
     *       project
     *         [select]
     *           group [agg(x), {a, b}]
     *             source
     *               child 1
     *       ...
     *
     * Or if the child does not support pushdown we add dummy aggregate projection
     * count(*) = 1, count(x) = case x is null then 0 else 1 end, avg(x) = x, etc.
     */
    private void pushGroupNodeOverUnion(QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            PlanNode groupNode, PlanNode unionSourceParent,
            List<Expression> groupingExpressions, PlanNode setOp, AnalysisRecord record)
            throws TeiidComponentException, QueryMetadataException,
            QueryPlannerException, QueryResolverException {
        if (setOp == null || setOp.getProperty(NodeConstants.Info.SET_OPERATION) != Operation.UNION) {
            return;
        }
        LinkedHashSet<AggregateSymbol> aggregates = collectAggregates(groupNode);

        Map<ElementSymbol, List<Set<Constant>>> partitionInfo = (Map<ElementSymbol, List<Set<Constant>>>)unionSourceParent.getProperty(Info.PARTITION_INFO);

        //check to see if any aggregate is dependent upon cardinality
        boolean cardinalityDependent = AggregateSymbol.areAggregatesCardinalityDependent(aggregates);

        LinkedList<PlanNode> unionChildren = new LinkedList<PlanNode>();
        findUnionChildren(unionChildren, cardinalityDependent, setOp);

        SymbolMap parentMap = (SymbolMap)unionSourceParent.getProperty(NodeConstants.Info.SYMBOL_MAP);

        //partitioned union
        if (partitionInfo != null && !Collections.disjoint(partitionInfo.keySet(), groupingExpressions)) {
            decomposeGroupBy(groupNode, unionSourceParent, groupingExpressions, aggregates, unionChildren, parentMap, metadata, capFinder);
            return;
        }

        /*
         * if there are no aggregates, this is just duplicate removal
         * mark the union as not all, which should be removed later but
         * serves as a hint to distribute a distinct to the union queries
         */
        if (aggregates.isEmpty()) {
            if (!groupingExpressions.isEmpty()) {
                Set<Expression> expressions = new HashSet<Expression>();
                boolean allCols = true;
                for (Expression ex : groupingExpressions) {
                    if (!(ex instanceof ElementSymbol)) {
                        allCols = false;
                        break;
                    }
                    Expression mapped = parentMap.getMappedExpression((ElementSymbol)ex);
                    expressions.add(mapped);
                }

                if (allCols) {
                    PlanNode project = NodeEditor.findNodePreOrder(unionSourceParent, NodeConstants.Types.PROJECT);
                    boolean projectsGrouping = true;
                    for (Expression ex :(List<Expression>)project.getProperty(Info.PROJECT_COLS)) {
                        if (!expressions.contains(SymbolMap.getExpression(ex))) {
                            projectsGrouping = false;
                            break;
                        }
                    }
                    if (projectsGrouping) {
                        //since there are no expressions in the grouping cols, we know the grouping node is now not needed.
                        RuleAssignOutputElements.removeGroupBy(groupNode, metadata);
                        setOp.setProperty(NodeConstants.Info.USE_ALL, Boolean.FALSE);
                    }
                }
            }
            return;
        }
        for (AggregateSymbol agg : aggregates) {
            if (!agg.canStage()) {
                return;
            }
        }

        //TODO: merge virtual, plan unions, raise null - change the partition information

        if (unionChildren.size() < 2) {
            return;
        }

        List<AggregateSymbol> copy = new ArrayList<AggregateSymbol>(aggregates);
        aggregates.clear();
        Map<AggregateSymbol, Expression> aggMap = buildAggregateMap(copy, metadata, aggregates, false);

        boolean shouldPushdown = false;
        List<Boolean> pushdownList = new ArrayList<Boolean>(unionChildren.size());

        for (PlanNode planNode : unionChildren) {
            boolean pushdown = canPushGroupByToUnionChild(metadata, capFinder, groupingExpressions, aggregates, planNode, record, groupNode);
            pushdownList.add(pushdown);
            shouldPushdown |= pushdown;
        }

        if (!shouldPushdown) {
            return;
        }

        GroupSymbol group = unionSourceParent.getGroups().iterator().next().clone();

        Iterator<Boolean> pushdownIterator = pushdownList.iterator();
        boolean first = true;
        for (PlanNode planNode : unionChildren) {
            addUnionGroupBy(groupingExpressions, aggregates, parentMap, metadata, capFinder, group, first, planNode, !pushdownIterator.next(), false);
            first = false;
        }

        updateParentAggs(groupNode, aggMap, metadata);

        List<Expression> symbols = (List<Expression>) NodeEditor.findNodePreOrder(unionSourceParent, NodeConstants.Types.PROJECT).getProperty(Info.PROJECT_COLS);
        GroupSymbol modifiedGroup = group.clone();
        SymbolMap symbolMap = createSymbolMap(modifiedGroup, symbols, unionSourceParent, metadata);
        unionSourceParent.setProperty(Info.SYMBOL_MAP, symbolMap);

        //correct the parent frame
        Map<Expression, ElementSymbol> mapping = new HashMap<Expression, ElementSymbol>();
        Iterator<ElementSymbol> elemIter = symbolMap.getKeys().iterator();
        for (Expression expr : groupingExpressions) {
            mapping.put(expr, elemIter.next());
        }
        for (AggregateSymbol agg : aggregates) {
            mapping.put(agg, elemIter.next());
        }
        PlanNode node = unionSourceParent;
        while (node != groupNode.getParent()) {
            FrameUtil.convertNode(node, null, null, mapping, metadata, false);
            node = node.getParent();
        }
        removeUnnecessaryViews(unionSourceParent, metadata, capFinder);
    }

    private void updateParentAggs(PlanNode groupNode,
            Map<AggregateSymbol, Expression> aggMap, QueryMetadataInterface metadata)
            throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        LinkedHashSet<AggregateSymbol> compositeAggs = new LinkedHashSet<AggregateSymbol>();
        boolean hasExpressionMapping = false;
        SymbolMap oldGroupingMap = (SymbolMap) groupNode.getProperty(Info.SYMBOL_MAP);
        /* we operate over the old group node map since the aggMap is based only
         * upon the aggs for the target node and not all of the possible aggregates,
         * which will cause a failure if we introduce an expression mapping
         */
        for (Expression ex : oldGroupingMap.asMap().values()) {
            if (!(ex instanceof AggregateSymbol)) {
                continue;
            }
            Expression mappedAgg = aggMap.get(ex);
            if (mappedAgg != null) {
                if (mappedAgg instanceof AggregateSymbol) {
                    compositeAggs.add((AggregateSymbol) mappedAgg);
                } else {
                    compositeAggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(mappedAgg, false));
                    hasExpressionMapping = true;
                }
            } else {
                compositeAggs.add((AggregateSymbol) ex);
            }
        }
        if (!hasExpressionMapping) {
            //if no new expressions are created we can just modify the existing aggregates
            FrameUtil.correctSymbolMap(aggMap, groupNode);
        } else {
            //if new expressions are created we insert a view to handle the projection
            groupNode.getGroups().clear();
            GroupSymbol oldGroup = oldGroupingMap.asMap().keySet().iterator().next().getGroupSymbol();
            SymbolMap groupingMap = RelationalPlanner.buildGroupingNode(compositeAggs, (List<? extends Expression>) groupNode.getProperty(Info.GROUP_COLS), groupNode, context, idGenerator);
            ArrayList<Expression> projectCols = new ArrayList<Expression>(oldGroupingMap.asMap().size());
            SymbolMap correctedMap = new SymbolMap();
            Map<Expression, ElementSymbol> inverseMap = groupingMap.inserseMapping();
            for (Map.Entry<ElementSymbol, Expression> entry : oldGroupingMap.asMap().entrySet()) {
                Expression ses = null;
                if (entry.getValue() instanceof AggregateSymbol) {
                    Expression ex = aggMap.get(entry.getValue());
                    if (ex == null) {
                        ses = inverseMap.get(entry.getValue());
                    } else if (ex instanceof AggregateSymbol) {
                        ses = inverseMap.get(ex);
                    } else {
                        ExpressionMappingVisitor.mapExpressions(ex, inverseMap);
                        ses = new ExpressionSymbol("expr", ex); //$NON-NLS-1$
                    }
                } else {
                    ses = inverseMap.get(entry.getValue());
                }
                ses = (Expression) ses.clone();
                projectCols.add(new AliasSymbol(Symbol.getShortName(entry.getKey()), ses));
                correctedMap.addMapping(entry.getKey(), SymbolMap.getExpression(ses));
            }
            PlanNode projectNode = groupNode.getParent();
            if (projectNode.getType() != NodeConstants.Types.PROJECT) {
                projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
                groupNode.addAsParent(projectNode);
                projectNode.setProperty(Info.PROJECT_COLS, projectCols);
                RuleDecomposeJoin.createSource(oldGroup, projectNode, correctedMap);
            } else {
                FrameUtil.convertFrame(projectNode, oldGroup, null, correctedMap.asMap(), metadata);
            }
        }
    }

    /* if partitioned, then we don't need decomposition or the top level group by
     *
     *   source
     *     set op
     *       project
     *           group [agg(x), {a, b}]
     *             source
     *               child 1
     *       ...
     *
     */
    private void decomposeGroupBy(PlanNode groupNode, PlanNode sourceNode,
            List<Expression> groupingExpressions,
            LinkedHashSet<AggregateSymbol> aggregates,
            LinkedList<PlanNode> unionChildren, SymbolMap parentMap, QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder) throws QueryPlannerException, QueryMetadataException, TeiidComponentException, QueryResolverException {
        // remove the group node
        groupNode.getParent().replaceChild(groupNode, groupNode.getFirstChild());

        GroupSymbol group = sourceNode.getGroups().iterator().next().clone();

        boolean first = true;
        for (PlanNode planNode : unionChildren) {
            addUnionGroupBy(groupingExpressions, aggregates,
                    parentMap, metadata, capFinder, group, first,
                    planNode, false, true);
            first = false;
        }
        List<Expression> symbols = (List<Expression>) NodeEditor.findNodePreOrder(sourceNode, NodeConstants.Types.PROJECT).getProperty(Info.PROJECT_COLS);
        GroupSymbol modifiedGroup = group.clone();
        SymbolMap symbolMap = createSymbolMap(modifiedGroup, symbols, sourceNode, metadata);
        sourceNode.setProperty(Info.SYMBOL_MAP, symbolMap);

        //map from the anon group to the updated inline view group
        SymbolMap map = (SymbolMap)groupNode.getProperty(Info.SYMBOL_MAP);
        Map<Expression, ElementSymbol> inverse = map.inserseMapping();
        SymbolMap newMapping = (SymbolMap) NodeEditor.findNodePreOrder(sourceNode, NodeConstants.Types.GROUP).getProperty(Info.SYMBOL_MAP);

        GroupSymbol oldGroup = null;
        Map<ElementSymbol, ElementSymbol> updatedMapping = new HashMap<ElementSymbol, ElementSymbol>();
        for (Map.Entry<ElementSymbol, Expression> entry : symbolMap.asMap().entrySet()) {
            Expression ex = newMapping.getMappedExpression((ElementSymbol) entry.getValue());
            ElementSymbol orig = inverse.get(ex);
            oldGroup = orig.getGroupSymbol();
            updatedMapping.put(orig, entry.getKey());
        }
        FrameUtil.convertFrame(sourceNode, oldGroup, Collections.singleton(modifiedGroup), updatedMapping, metadata);
        removeUnnecessaryViews(sourceNode, metadata, capFinder);
    }

    /**
     * TODO: remove me - the logic in {@link #addUnionGroupBy} should be redone
     * to not use a view, but the logic there is more straight-forward there
     * and then we correct here.
     * @param sourceNode
     * @param metadata
     * @param capFinder
     * @throws QueryPlannerException
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private void removeUnnecessaryViews(PlanNode sourceNode,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {
        for (PlanNode source : NodeEditor.findAllNodes(sourceNode.getFirstChild(), NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS)) {
            PlanNode planNode = source.getFirstChild();
            if (planNode == null || planNode.getType() != NodeConstants.Types.ACCESS) {
                continue;
            }
            //temporarily remove the access node
            NodeEditor.removeChildNode(source, planNode);
            PlanNode parent = RuleMergeVirtual.doMerge(source, source.getParent(), false, metadata, capFinder);
            //add it back
            if (parent.getFirstChild() == source) {
                source.getFirstChild().addAsParent(planNode);
            } else {
                parent.getFirstChild().addAsParent(planNode);
            }
            while (RuleRaiseAccess.raiseAccessNode(planNode, planNode, metadata, capFinder, true, null, context) != null) {
                //continue to raise
            }
        }
    }

    private void addUnionGroupBy(
            List<Expression> groupingExpressions,
            LinkedHashSet<AggregateSymbol> aggregates, SymbolMap parentMap,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            GroupSymbol group, boolean first, PlanNode planNode, boolean viewOnly, boolean partitioned)
            throws QueryMetadataException, TeiidComponentException,
            QueryPlannerException, QueryResolverException {
        List<Expression> groupingColumns = LanguageObject.Util.deepClone(groupingExpressions, Expression.class);

        //branches other than the first need to have their projected column names updated
        if (!first) {
            PlanNode sortNode = NodeEditor.findNodePreOrder(planNode, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
            List<Expression> sortOrder = null;
            OrderBy orderBy = null;
            if (sortNode != null) {
                orderBy = (OrderBy)sortNode.getProperty(Info.SORT_ORDER);
                sortOrder = orderBy.getSortKeys();
            }
            List<Expression> projectCols = FrameUtil.findTopCols(planNode);
            List<ElementSymbol> virtualElements = parentMap.getKeys();
            for (int i = 0; i < virtualElements.size(); i++) {
                ElementSymbol virtualElem = virtualElements.get(i);
                Expression projectedSymbol = projectCols.get(i);
                if (!Symbol.getShortName(projectedSymbol).equals(Symbol.getShortName(virtualElem))) {
                    if (sortOrder != null) {
                        int sortIndex = sortOrder.indexOf(projectedSymbol);
                        if (sortIndex > -1) {
                            updateSymbolName(sortOrder, sortIndex, virtualElem, sortOrder.get(sortIndex));
                            orderBy.getOrderByItems().get(sortIndex).setSymbol(sortOrder.get(sortIndex));
                        }
                    }
                    updateSymbolName(projectCols, i, virtualElem, projectedSymbol);
                }
            }
        }

        PlanNode view = RuleDecomposeJoin.createSource(group, planNode, parentMap);

        PlanNode projectPlanNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        Select allSymbols = new Select();
        for (Expression expr : groupingColumns) {
            allSymbols.addSymbol(new ExpressionSymbol("expr", expr)); //$NON-NLS-1$
        }
        if (viewOnly) {
            for (AggregateSymbol agg : aggregates) {
                agg = (AggregateSymbol)agg.clone();
                if (agg.isCount()) {
                    if (isCountStar(agg)) {
                        allSymbols.addSymbol(new ExpressionSymbol("stagedAgg", newLiteral(1, agg.getType()))); //$NON-NLS-1$
                    } else {
                        SearchedCaseExpression count = new SearchedCaseExpression(Arrays.asList(new IsNullCriteria(agg.getArg(0))), Arrays.asList(newLiteral(0, agg.getType())));
                        count.setElseExpression(newLiteral(1, agg.getType()));
                        count.setType(agg.getType());
                        allSymbols.addSymbol(new ExpressionSymbol("stagedAgg", count)); //$NON-NLS-1$
                    }
                } else { //min, max, sum
                    assert agg.getArgs().length == 1; //prior canStage should ensure this is true
                    Expression ex = agg.getArg(0);
                    ex = ResolverUtil.convertExpression(ex, DataTypeManager.getDataTypeName(agg.getType()), metadata);
                    allSymbols.addSymbol(new ExpressionSymbol("stagedAgg", ex)); //$NON-NLS-1$
                }
            }
        } else {
            allSymbols.addSymbols(aggregates);
        }
        if (first) {
            QueryRewriter.makeSelectUnique(allSymbols, false);
        }
        projectPlanNode.setProperty(NodeConstants.Info.PROJECT_COLS, allSymbols.getSymbols());
        projectPlanNode.addGroups(view.getGroups());

        view.addAsParent(projectPlanNode);

        if (!viewOnly) {
            addGroupBy(view, groupingColumns, aggregates, metadata, projectPlanNode.getParent(), capFinder, true, groupingColumns.isEmpty() && (partitioned || containsNullDependent(aggregates)));
        }
    }

    private void updateSymbolName(List<Expression> projectCols, int i,
            ElementSymbol virtualElem, Expression projectedSymbol) {
        if (projectedSymbol instanceof AliasSymbol) {
            ((AliasSymbol)projectedSymbol).setShortName(Symbol.getShortName(virtualElem));
        } else {
            projectCols.set(i, new AliasSymbol(Symbol.getShortName(virtualElem), projectedSymbol));
        }
    }

    private boolean canPushGroupByToUnionChild(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder,
            List<Expression> groupingExpressions,
            Set<AggregateSymbol> aggregates,
            PlanNode planNode, AnalysisRecord record, PlanNode groupingNode)
            throws QueryMetadataException, TeiidComponentException {
        if (planNode.getType() != NodeConstants.Types.ACCESS) {
            return false;
        }
        boolean result = RuleRaiseAccess.canRaiseOverGroupBy(groupingNode, planNode, aggregates, metadata, capFinder, record, false);
        if (!result) {
            return false;
        }
        if (containsNullDependent(aggregates) && !canFilterEmpty(metadata, capFinder, planNode, groupingExpressions)) {
            return false;
        }
        return true;
    }

    private boolean containsNullDependent(Collection<AggregateSymbol> aggregates) {
        for (AggregateSymbol aggregateSymbol : aggregates) {
            //we don't consider count here as we dealing with the original aggregates, not the mapped expressions
            if (aggregateSymbol.getFunctionDescriptor() != null && aggregateSymbol.getFunctionDescriptor().isNullDependent()) {
                return true;
            }
        }
        return false;
    }

    private boolean canFilterEmpty(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, PlanNode planNode, List<Expression> groupingExpressions) throws QueryMetadataException,
            TeiidComponentException {
        if (!groupingExpressions.isEmpty()) {
            return true;
        }
        Object modelId = RuleRaiseAccess.getModelIDFromAccess(planNode, metadata);
        return (/*CapabilitiesUtil.supports(Capability.CRITERIA_COMPARE_ORDERED, modelId, metadata, capFinder)
                &&*/ CapabilitiesUtil.supports(Capability.QUERY_AGGREGATES_COUNT_STAR, modelId, metadata, capFinder)
                /*&& CapabilitiesUtil.supports(Capability.QUERY_HAVING, modelId, metadata, capFinder)*/);
    }

    /**
     * Recursively searches the union tree for all applicable source nodes
     */
    static PlanNode findUnionChildren(List<PlanNode> unionChildren, boolean carinalityDependent, PlanNode setOp) {
        if (setOp.getType() != NodeConstants.Types.SET_OP || setOp.getProperty(NodeConstants.Info.SET_OPERATION) != Operation.UNION) {
            return setOp;
        }

        if (!setOp.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
            if (carinalityDependent) {
                return setOp;
            }
            setOp.setProperty(NodeConstants.Info.USE_ALL, Boolean.TRUE);
        }

        for (PlanNode planNode : setOp.getChildren()) {
            PlanNode child = findUnionChildren(unionChildren, carinalityDependent, planNode);
            if (child != null) {
                unionChildren.add(child);
            }
        }

        return null;
    }

    static SymbolMap createSymbolMap(GroupSymbol group,
            List<? extends Expression> virtualElements,
            PlanNode child, QueryMetadataInterface metadata)
            throws TeiidComponentException, QueryMetadataException {
        List<ElementSymbol> projectedSymbols = defineNewGroup(group,
                virtualElements, metadata);
        SymbolMap symbolMap = SymbolMap.createSymbolMap(projectedSymbols,
                (List<Expression>)NodeEditor.findNodePreOrder(child, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS));
        return symbolMap;
    }

    static List<ElementSymbol> defineNewGroup(GroupSymbol group,
            List<? extends Expression> virtualElements,
            QueryMetadataInterface metadata) throws TeiidComponentException,
            QueryMetadataException {
        TempMetadataStore store = new TempMetadataStore();
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, store);
        try {
            group.setMetadataID(ResolverUtil.addTempGroup(tma, group, virtualElements, false));
        } catch (QueryResolverException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30265, e);
        }
        List<ElementSymbol> projectedSymbols = ResolverUtil.resolveElementsInGroup(group, metadata);
        return projectedSymbols;
    }

    /**
     * Walk up the plan from the GROUP node. Should encounter only (optionally) a SELECT and can stop at the PROJECT node. Need to
     * collect any AggregateSymbols used in the select criteria or projected columns.
     *
     * @param groupNode
     * @return the set of aggregate symbols found
     * @since 4.2
     */
    static LinkedHashSet<AggregateSymbol> collectAggregates(PlanNode groupNode) {
        LinkedHashSet<AggregateSymbol> aggregates = new LinkedHashSet<AggregateSymbol>();
        PlanNode currentNode = groupNode.getParent();
        SymbolMap symbolMap = (SymbolMap) groupNode.getProperty(NodeConstants.Info.SYMBOL_MAP);

        while (currentNode != null) {
            if (currentNode.getType() == NodeConstants.Types.PROJECT) {
                List<Expression> projectedSymbols = (List<Expression>)currentNode.getProperty(NodeConstants.Info.PROJECT_COLS);
                for (Expression symbol : projectedSymbols) {
                    mapAggregates(ElementCollectorVisitor.getAggregates(symbol, true), symbolMap, aggregates);
                }
                break;
            }
            if (currentNode.getType() == NodeConstants.Types.SELECT) {
                Criteria crit = (Criteria)currentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                mapAggregates(ElementCollectorVisitor.getAggregates(crit, true), symbolMap, aggregates);
            }

            currentNode = currentNode.getParent();
        }
        return aggregates;
    }

    static void mapAggregates(Collection<ElementSymbol> symbols, SymbolMap map, Collection<? super AggregateSymbol> aggs) {
        for (ElementSymbol es : symbols) {
            Expression ex = map.getMappedExpression(es);
            if (ex instanceof AggregateSymbol) {
                aggs.add((AggregateSymbol) ex);
            }
        }
    }

    /**
     * Attempt to push the group node below one or more joins, manipulating the parent plan as necessary. This may involve
     * modifying symbols in parent nodes (to account for staged aggregates).
     * @throws QueryPlannerException
     *
     * @since 4.2
     */
    private void pushGroupNode(PlanNode groupNode,
                               List<Expression> groupingExpressions,
                               Set<AggregateSymbol> allAggregates,
                               QueryMetadataInterface metadata,
                               CapabilitiesFinder capFinder, CommandContext cc) throws TeiidComponentException,
                                                            QueryMetadataException, QueryPlannerException {

        Map<PlanNode, List<AggregateSymbol>> aggregateMap = createNodeMapping(groupNode, allAggregates, true);
        if (aggregateMap == null) {
            return;
        }
        Map<PlanNode, List<Expression>> groupingMap = createNodeMapping(groupNode, groupingExpressions, false);

        Set<PlanNode> possibleTargetNodes = new LinkedHashSet<PlanNode>(aggregateMap.keySet());
        possibleTargetNodes.addAll(groupingMap.keySet());
        int cardinalityDependent = 0;
        for (Map.Entry<PlanNode, List<AggregateSymbol>> entry : aggregateMap.entrySet()) {
            if (AggregateSymbol.areAggregatesCardinalityDependent(entry.getValue())) {
                cardinalityDependent++;
                if (cardinalityDependent > 1) {
                    //we are not handling this case yet - the aggregate map expressions
                    //are more complicated as we aggregate on one side. the
                    //other side to be a multiple of the other
                    return;
                }
                //can't change the cardinality on the other side of the join -
                //unless it's a 1-1 join, in which case this optimization isn't needed
                //TODO: make a better choice if there are multiple targets
                possibleTargetNodes.clear();
                possibleTargetNodes.add(entry.getKey());
            }
        }

        boolean recollectAggregates = false;
        for (PlanNode planNode : possibleTargetNodes) {
            Set<Expression> stagedGroupingSymbols = new LinkedHashSet<Expression>();
            Collection<AggregateSymbol> aggregates = aggregateMap.get(planNode);

            planNode = canPush(groupNode, stagedGroupingSymbols, planNode, aggregates, metadata);
            if (planNode == null) {
                continue;
            }

            filterExpressions(stagedGroupingSymbols, planNode.getGroups(), groupingExpressions, false);

            if (recollectAggregates) {
                recollectAggregates = false;
                allAggregates = collectAggregates(groupNode);
            }
            collectSymbolsFromOtherAggregates(allAggregates, aggregates, planNode, stagedGroupingSymbols);

            //we cannot push staged group symbols that are null dependent across a null dependent join
            PlanNode joinParent = planNode;
            if (planNode.getType() != NodeConstants.Types.JOIN) {
                joinParent = planNode.getParent();
            }
            if (joinParent.getType() != NodeConstants.Types.JOIN
                    || containsNullDependentExpressions(stagedGroupingSymbols,
                            metadata, joinParent, (JoinType) joinParent.getProperty(NodeConstants.Info.JOIN_TYPE))) {
                continue;
            }

            //perform a costing check, if there's not a significant reduction, then don't stage
            float cardinality = NewCalculateCostUtil.computeCostForTree(planNode, metadata);
            float ndv = NewCalculateCostUtil.getNDVEstimate(planNode, metadata, cardinality, stagedGroupingSymbols, false);
            if (ndv != NewCalculateCostUtil.UNKNOWN_VALUE && cardinality / ndv < 4) {
                continue;
            }

            if (aggregates != null) {
                aggregates = stageAggregates(groupNode, metadata, stagedGroupingSymbols, aggregates, true);
            } else {
                aggregates = new ArrayList<AggregateSymbol>(1);
            }

            if (aggregates.isEmpty() && stagedGroupingSymbols.isEmpty()) {
                continue;
            }

            addGroupBy(planNode, new ArrayList<Expression>(stagedGroupingSymbols), aggregates, metadata, groupNode.getParent(), capFinder, true, stagedGroupingSymbols.isEmpty() && containsNullDependent(aggregates));
            //with the staged grouping added, the parent aggregate expressions can change due to mapping
            recollectAggregates = true;
        }
    }

    /**
     * TODO: if aggregates are empty, then could insert a dup remove node instead
     */
    private void addGroupBy(
            PlanNode child, List<Expression> stagedGroupingSymbols,
            Collection<AggregateSymbol> aggregates, QueryMetadataInterface metadata, PlanNode endNode, CapabilitiesFinder capFinder, boolean considerMultiSource, boolean filterEmpty) throws QueryMetadataException,
            TeiidComponentException, QueryPlannerException {
        PlanNode stageGroup = NodeFactory.getNewNode(NodeConstants.Types.GROUP);
        child.addAsParent(stageGroup);
        aggregates = new LinkedHashSet<AggregateSymbol>(aggregates);
        if (filterEmpty) {
            // if the source has no rows we need to insert a select node with criteria
            addEmptyFilter(aggregates, stageGroup, metadata, capFinder, RuleRaiseAccess.getModelIDFromAccess(NodeEditor.findNodePreOrder(child, NodeConstants.Types.ACCESS), metadata));
        }
        SymbolMap groupingSymbolMap = RelationalPlanner.buildGroupingNode(aggregates, stagedGroupingSymbols, stageGroup, context, idGenerator);
        Map<Expression, ElementSymbol> reverseMapping = groupingSymbolMap.inserseMapping();

        GroupSymbol newGroup = reverseMapping.values().iterator().next().getGroupSymbol();
        PlanNode node = stageGroup.getParent();
        while (node != endNode) {
            if (node.getType() == NodeConstants.Types.JOIN) {
                node.getGroups().removeAll(FrameUtil.findJoinSourceNode(stageGroup.getFirstChild()).getGroups());
                node.getGroups().add(newGroup);
            }
            FrameUtil.convertNode(node, null, null, reverseMapping, metadata, false);
            if (node.getType() == NodeConstants.Types.JOIN) {
                //reset the left/right/non-equi join criteria
                RuleChooseJoinStrategy.chooseJoinStrategy(node, metadata);
            }
            node = node.getParent();
        }
        //check for push down
        PlanNode accessNode = stageGroup.getFirstChild();
        if (accessNode.getType() != NodeConstants.Types.ACCESS) {
            groupingNodes.add(stageGroup);
        } else {
            //we need the aggregates from the symbol map, which has been cloned from the actual symbols
            //this ensures that side effects from testing pushdown will be preserved
            Collection<AggregateSymbol> aggs = new ArrayList<AggregateSymbol>(aggregates.size());
            for (Expression ex : groupingSymbolMap.asMap().values()) {
                if (ex instanceof AggregateSymbol) {
                    aggs.add((AggregateSymbol)ex);
                }
            }
            if (RuleRaiseAccess.canRaiseOverGroupBy(stageGroup, accessNode, aggs, metadata, capFinder, null, false)) {
                if (considerMultiSource && accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
                    groupingNodes.add(stageGroup);
                } else {
                    accessNode.getGroups().clear();
                    accessNode.getGroups().addAll(stageGroup.getGroups());
                    RuleRaiseAccess.performRaise(null, accessNode, stageGroup);
                    if (filterEmpty && RuleRaiseAccess.canRaiseOverSelect(accessNode, metadata, capFinder, accessNode.getParent(), null)) {
                        RuleRaiseAccess.performRaise(null, accessNode, accessNode.getParent());
                    }
                }
            }
        }
    }

    private void addEmptyFilter(Collection<AggregateSymbol> aggregates,
            PlanNode stageGroup, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, Object modelId) throws QueryMetadataException, TeiidComponentException {
        PlanNode selectNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
        AggregateSymbol count = new AggregateSymbol(NonReserved.COUNT, false, null);
        aggregates.add(count); //consider the count aggregate for the push down call below
        Criteria crit = new CompareCriteria(count, CompareCriteria.GT, new Constant(new Integer(0)));
        selectNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, crit);
        selectNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
        stageGroup.addAsParent(selectNode);
    }

    Set<AggregateSymbol> stageAggregates(PlanNode groupNode,
                                 QueryMetadataInterface metadata,
                                 Set<Expression> stagedGroupingSymbols,
                                 Collection<AggregateSymbol> aggregates, boolean join) throws TeiidComponentException, QueryPlannerException {
        //remove any aggregates that are computed over a group by column
        for (final Iterator<AggregateSymbol> iterator = aggregates.iterator(); iterator.hasNext();) {
            final AggregateSymbol symbol = iterator.next();
            if (symbol.getArgs().length != 1 || symbol.isCardinalityDependent()) {
                continue;
            }
            Expression expr = symbol.getArg(0);
            if (stagedGroupingSymbols.contains(expr)) {
                iterator.remove();
            }
        }

        if (aggregates.isEmpty()) {
            return Collections.emptySet();
        }
        // Fix any aggregate expressions so they correctly recombine the staged aggregates
        Set<AggregateSymbol> newAggs = new HashSet<AggregateSymbol>();
        Map<AggregateSymbol, Expression> aggMap;
        try {
            aggMap = buildAggregateMap(aggregates, metadata, newAggs, join);
        } catch (QueryResolverException e) {
             throw new QueryPlannerException(QueryPlugin.Event.TEIID30266, e);
        }
        updateParentAggs(groupNode, aggMap, metadata);
        return newAggs;
    }

    private void collectSymbolsFromOtherAggregates(Collection<AggregateSymbol> allAggregates,
                                                      Collection<AggregateSymbol> aggregates,
                                                      PlanNode current,
                                                      Set<Expression> stagedGroupingSymbols) {
        Set<AggregateSymbol> otherAggs = new HashSet<AggregateSymbol>(allAggregates);
        if (aggregates != null) {
            otherAggs.removeAll(aggregates);
        }

        PlanNode source = FrameUtil.findJoinSourceNode(current);

        for (AggregateSymbol aggregateSymbol : otherAggs) {
            for (ElementSymbol symbol : ElementCollectorVisitor.getElements(aggregateSymbol, true)) {
                if (source.getGroups().contains(symbol.getGroupSymbol())) {
                    stagedGroupingSymbols.add(symbol);
                }
            }
        }
    }

    /**
     * Ensures that we are only pushing through inner equi joins or cross joins.  Also collects the necessary staged grouping symbols
     * @param aggregates
     * @param metadata
     * @return null if we cannot push otherwise the target join node
     */
    private PlanNode canPush(PlanNode groupNode,
                            Set<Expression> stagedGroupingSymbols,
                            PlanNode planNode, Collection<AggregateSymbol> aggregates, QueryMetadataInterface metadata) {
        PlanNode parentJoin = planNode.getParent();

        Set<GroupSymbol> groups = FrameUtil.findJoinSourceNode(planNode).getGroups();

        PlanNode result = planNode;
        while (parentJoin != groupNode) {
            if (parentJoin.getType() != NodeConstants.Types.JOIN) {
                return null;
            }

            JoinType joinType = (JoinType)parentJoin.getProperty(NodeConstants.Info.JOIN_TYPE);
            if (containsNullDependentExpressions(aggregates, metadata, parentJoin, joinType)) {
                return null;
            }

            //check for sideways correlation
            PlanNode other = null;
            if (planNode == parentJoin.getFirstChild()) {
                other = parentJoin.getLastChild();
            } else {
                other = parentJoin.getFirstChild();
            }
            for (PlanNode node : NodeEditor.findAllNodes(other, NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE)) {
                SymbolMap map = (SymbolMap)node.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
                if (map != null) {
                    return null;
                    //TODO: handle this case. the logic would look something like below,
                    //but we would need to handle the updating of the symbol maps in addGroupBy
                    /*filterExpressions(stagedGroupingSymbols, groups, map.getKeys(), true);
                    for (ElementSymbol ex : map.getKeys()) {
                        if (DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(ex.getType()))) {
                            return null;
                        }
                    }*/
                }
            }

            if (!parentJoin.hasCollectionProperty(NodeConstants.Info.LEFT_EXPRESSIONS) || !parentJoin.hasCollectionProperty(NodeConstants.Info.RIGHT_EXPRESSIONS)) {
                List<Criteria> criteria = (List<Criteria>)parentJoin.getProperty(Info.JOIN_CRITERIA);
                if (!findStagedGroupingExpressions(groups, criteria, stagedGroupingSymbols)) {
                    return null;
                }
            } else {
                List<Criteria> criteria = (List<Criteria>)parentJoin.getProperty(Info.NON_EQUI_JOIN_CRITERIA);
                if (!findStagedGroupingExpressions(groups, criteria, stagedGroupingSymbols)) {
                    return null;
                }

                //we move the target up if the filtered expressions introduce outside groups
                if (planNode == parentJoin.getFirstChild()) {
                    if (filterExpressions(stagedGroupingSymbols, groups, (List<Expression>)parentJoin.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS), true)) {
                        result = parentJoin;
                        groups = result.getGroups();
                    }
                } else {
                    if (filterExpressions(stagedGroupingSymbols, groups, (List<Expression>)parentJoin.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS), true)) {
                        result = parentJoin;
                        groups = result.getGroups();
                    }
                }
            }

            planNode = parentJoin;
            parentJoin = parentJoin.getParent();
        }
        if (result.getParent() == groupNode) {
            //can't be pushed as we are already at the direct child
            return null;
        }
        return result;
    }

    private boolean containsNullDependentExpressions(
            Collection<? extends Expression> toCheck,
            QueryMetadataInterface metadata, PlanNode parentJoin,
            JoinType joinType) {
        if (!joinType.isOuter() || toCheck == null) {
            return false;
        }
        for (Expression ex : toCheck) {
            Collection<GroupSymbol> expressionGroups = GroupsUsedByElementsVisitor.getGroups(ex);
            Collection<GroupSymbol> innerGroups = null;
            if (joinType == JoinType.JOIN_LEFT_OUTER) {
                innerGroups = FrameUtil.findJoinSourceNode(parentJoin.getLastChild()).getGroups();
            } else {
                //full outer
                innerGroups = parentJoin.getGroups();
            }
            if (Collections.disjoint(expressionGroups, innerGroups)) {
                continue;
            }
            if (ex instanceof AggregateSymbol) {
                AggregateSymbol as = (AggregateSymbol)ex;
                if (as.getFunctionDescriptor() != null
                        && as.getFunctionDescriptor().isNullDependent()) {
                    return true;
                }
                if (as.getArgs().length == 1 && JoinUtil.isNullDependent(metadata, innerGroups, as.getArg(0))) {
                    return true;
                }
            } else if (JoinUtil.isNullDependent(metadata, innerGroups, ex)) {
                return true;
            }
        }
        return false;
    }

    private boolean findStagedGroupingExpressions(Set<GroupSymbol> groups,
            List<Criteria> criteria, Set<Expression> stagedGroupingExpressions) {
        if (criteria != null && !criteria.isEmpty()) {
            Set<Expression> subExpressions = new HashSet<Expression>();
            filterExpressions(subExpressions, groups, criteria, false);
            for (Expression ses : subExpressions) {
                if (ses.getType() == DataTypeManager.DefaultDataClasses.BOOLEAN ||
                        ses.getType() == DataTypeManager.DefaultDataClasses.BYTE ||
                        DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(ses.getType()))) {
                    return false; //need better subexpression logic, as just the element symbol is non-comparable
                }
            }
            stagedGroupingExpressions.addAll(subExpressions);
        }
        return true;
    }

    /**
     * @return true if the filtered expressions contain outside groups
     */
    private boolean filterExpressions(Set<Expression> stagedGroupingSymbols,
                                   Set<GroupSymbol> groups,
                                   Collection<? extends Expression> symbols, boolean wholeExpression) {
        boolean result = false;
        for (Expression ex : symbols) {
            Set<GroupSymbol> groups2 = GroupsUsedByElementsVisitor.getGroups(ex);
            if (!Collections.disjoint(groups, groups2)) {
                if (!result) {
                    boolean containsAll = groups.containsAll(groups2);
                    if (!wholeExpression && !containsAll) {
                        //collect only matching subexpressions - but the best that we'll currently do is elementsymbols
                        filterExpressions(stagedGroupingSymbols, groups, ElementCollectorVisitor.getElements(ex, true), true);
                        continue;
                    }
                    result = !containsAll;
                }
                stagedGroupingSymbols.add(SymbolMap.getExpression(ex));
            }
        }
        return result;
    }

    private <T extends Expression> Map<PlanNode, List<T>> createNodeMapping(PlanNode groupNode,
                                                                       Collection<T> expressions, boolean aggs) {
        Map<PlanNode, List<T>> result = new LinkedHashMap<PlanNode, List<T>>();
        if (expressions == null) {
            return result;
        }
        for (T aggregateSymbol : expressions) {
            if (aggs) {
                AggregateSymbol as = (AggregateSymbol)aggregateSymbol;
                if ((!as.canStage() && as.isCardinalityDependent())) {
                    return null;
                }
            }
            PlanNode originatingNode = null;
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(aggregateSymbol);
            if (groups.isEmpty()) {
                if (!aggs) {
                    continue;
                }
                AggregateSymbol as = (AggregateSymbol)aggregateSymbol;
                if (as.getAggregateFunction() == Type.AVG || as.isEnhancedNumeric()) {
                    continue; //don't need to map these, they will just become constants
                }
                //TODO make a better choice as to the side
                PlanNode joinNode = NodeEditor.findAllNodes(groupNode, NodeConstants.Types.JOIN).get(0);
                float left = joinNode.getFirstChild().getCardinality();
                float right = joinNode.getLastChild().getCardinality();
                boolean useLeft = true;
                if (left != NewCalculateCostUtil.UNKNOWN_VALUE && right != NewCalculateCostUtil.UNKNOWN_VALUE && right > left) {
                    useLeft = false;
                }
                groups = (useLeft?joinNode.getFirstChild():joinNode.getLastChild()).getGroups();
            }
            originatingNode = FrameUtil.findOriginatingNode(groupNode.getFirstChild(), groups);

            if (originatingNode == null) {
                if (aggs) {
                    return null;  //should never happen
                }
                continue;
            }

            PlanNode parentAccess = NodeEditor.findParent(originatingNode, NodeConstants.Types.ACCESS, NodeConstants.Types.GROUP);

            if (parentAccess != null) {
                if (!NodeEditor.findAllNodes(parentAccess, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE).isEmpty()) {
                    continue; //already did a decomposition
                }
                while (parentAccess.getType() == NodeConstants.Types.SELECT) {
                    parentAccess = parentAccess.getParent();
                }
                originatingNode = parentAccess;
            }

            if (originatingNode.getParent() == groupNode || originatingNode.getType() != NodeConstants.Types.ACCESS) {
                //anything logically applied after the join and is
                //dependent upon the cardinality prevents us from optimizing.
                if (aggs && ((AggregateSymbol)aggregateSymbol).isCardinalityDependent()) {
                    return null;
                }
                continue; //don't perform intermediate grouping either
            }

            if (aggs && ((AggregateSymbol)aggregateSymbol).isDistinct()) {
                //TODO: support distinct
                continue;
            }

            List<T> symbols = result.get(originatingNode);
            if (symbols == null) {
                symbols = new LinkedList<T>();
                result.put(originatingNode, symbols);
            }
            symbols.add(aggregateSymbol);
        }
        return result;
    }

    private static boolean isCountStar(AggregateSymbol as) {
        return as.isCount() && (as.getArgs().length == 0 || EvaluatableVisitor.willBecomeConstant(as.getArg(0)));
    }

    private static Map<AggregateSymbol, Expression> buildAggregateMap(Collection<? extends AggregateSymbol> aggregateExpressions,
                                                                        QueryMetadataInterface metadata, Set<AggregateSymbol> nestedAggregates, boolean join) throws QueryResolverException,
                                                                                                        TeiidComponentException {
        Map<AggregateSymbol, Expression> aggMap = new LinkedHashMap<AggregateSymbol, Expression>();
        for (AggregateSymbol partitionAgg : aggregateExpressions) {

            Expression newExpression = null;

            Type aggFunction = partitionAgg.getAggregateFunction();
            if (partitionAgg.isCount()) {
                //COUNT(x) -> IFNULL(CONVERT(SUM(COUNT(x)), INTEGER), 0)
                AggregateSymbol newAgg = null;
                if (isCountStar(partitionAgg) && join) {
                    //count * case (if on the inner side of an outer join)
                    Function ifnull = new Function(FunctionLibrary.IFNULL, new Expression[] {partitionAgg, newLiteral(1, partitionAgg.getType())});
                    newAgg = new AggregateSymbol(NonReserved.SUM, false, ifnull);
                } else {
                    newAgg = new AggregateSymbol(NonReserved.SUM, false, partitionAgg);
                }
                newExpression = newAgg;
                if (partitionAgg.getAggregateFunction() == Type.COUNT) {
                    // Build conversion function to convert SUM (which returns LONG) back to INTEGER
                    newExpression = new Function(FunctionLibrary.CONVERT, new Expression[] {newAgg, new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()))});
                }
                if (join) {
                    newExpression = new Function(FunctionLibrary.IFNULL,
                            new Expression[] { newExpression,
                                    newLiteral(0, partitionAgg.getType()) });
                }
                ResolverVisitor.resolveLanguageObject(newExpression, metadata);

                nestedAggregates.add(partitionAgg);
            } else if (aggFunction == Type.AVG) {
                //AVG(x) -> SUM(SUM(x)) / SUM(COUNT(x))
                AggregateSymbol countAgg = new AggregateSymbol(NonReserved.COUNT, false, partitionAgg.getArg(0));
                AggregateSymbol sumAgg = new AggregateSymbol(NonReserved.SUM, false, partitionAgg.getArg(0));

                AggregateSymbol sumSumAgg = new AggregateSymbol(NonReserved.SUM, false, sumAgg);
                AggregateSymbol sumCountAgg = new AggregateSymbol(NonReserved.SUM, false, countAgg);

                Expression convertedSum = new Function(FunctionLibrary.CONVERT, new Expression[] {sumSumAgg, new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()))});
                Expression convertCount = new Function(FunctionLibrary.CONVERT, new Expression[] {sumCountAgg, new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()))});

                Function divideFunc = new Function("/", new Expression[] {convertedSum, convertCount}); //$NON-NLS-1$
                ResolverVisitor.resolveLanguageObject(divideFunc, metadata);

                newExpression = divideFunc;
                nestedAggregates.add(countAgg);
                nestedAggregates.add(sumAgg);
            } else if (partitionAgg.isEnhancedNumeric()) {
                //e.g. STDDEV_SAMP := CASE WHEN COUNT(X) > 1 THEN SQRT((SUM(X^2) - SUM(X)^2/COUNT(X))/(COUNT(X) - 1))
                AggregateSymbol countAgg = new AggregateSymbol(NonReserved.COUNT, false, partitionAgg.getArg(0));
                AggregateSymbol sumAgg = new AggregateSymbol(NonReserved.SUM, false, partitionAgg.getArg(0));
                AggregateSymbol sumSqAgg = new AggregateSymbol(NonReserved.SUM, false, new Function(SourceSystemFunctions.POWER, new Expression[] {partitionAgg.getArg(0), new Constant(2)}));

                AggregateSymbol sumSumAgg = new AggregateSymbol(NonReserved.SUM, false, sumAgg);
                AggregateSymbol sumCountAgg = new AggregateSymbol(NonReserved.SUM, false, countAgg);
                AggregateSymbol sumSumSqAgg = new AggregateSymbol(NonReserved.SUM, false, sumSqAgg);

                Expression convertedSum = new Function(FunctionLibrary.CONVERT, new Expression[] {sumSumAgg, new Constant(DataTypeManager.DefaultDataTypes.DOUBLE)});

                Function divideFunc = new Function(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Function(SourceSystemFunctions.POWER, new Expression[] {convertedSum, new Constant(2)}), sumCountAgg});

                Function minusFunc = new Function(SourceSystemFunctions.SUBTRACT_OP, new Expression[] {sumSumSqAgg, divideFunc});
                Expression divisor = null;
                if (aggFunction == Type.STDDEV_SAMP || aggFunction == Type.VAR_SAMP) {
                    divisor = new Function(SourceSystemFunctions.SUBTRACT_OP, new Expression[] {sumCountAgg, new Constant(1)});
                } else {
                    divisor = sumCountAgg;
                }
                Expression result = new Function(SourceSystemFunctions.DIVIDE_OP, new Expression[] {minusFunc, divisor});
                if (aggFunction == Type.STDDEV_POP || aggFunction == Type.STDDEV_SAMP) {
                    result = new Function(SourceSystemFunctions.SQRT, new Expression[] {result});
                } else {
                    result = new Function(FunctionLibrary.CONVERT, new Expression[] {result, new Constant(DataTypeManager.DefaultDataTypes.DOUBLE)});
                }
                Expression n = new Constant(0);
                if (aggFunction == Type.STDDEV_SAMP || aggFunction == Type.VAR_SAMP) {
                    n = new Constant(1);
                }
                result = new SearchedCaseExpression(Arrays.asList(new CompareCriteria(sumCountAgg, CompareCriteria.GT, n)), Arrays.asList(result));
                ResolverVisitor.resolveLanguageObject(result, metadata);

                newExpression = result;
                nestedAggregates.add(countAgg);
                nestedAggregates.add(sumAgg);
                nestedAggregates.add(sumSqAgg);
            } else {
                //AGG(X) -> AGG(AGG(X))
                AggregateSymbol newAgg = null;
                if (join && partitionAgg.getArgs().length == 1 && GroupsUsedByElementsVisitor.getGroups(partitionAgg).isEmpty()) {
                    //count * case (if on the inner side of an outer join)
                    Function ifnull = new Function(FunctionLibrary.IFNULL, new Expression[] {partitionAgg, partitionAgg.getArg(0)});
                    newAgg = new AggregateSymbol(aggFunction.name(), false, ifnull);
                } else {
                    newAgg = new AggregateSymbol(aggFunction.name(), false, partitionAgg);
                }
                newExpression = newAgg;
                if (partitionAgg.getFunctionDescriptor() != null) {
                    newAgg.setFunctionDescriptor(partitionAgg.getFunctionDescriptor().clone());
                }
                ResolverVisitor.resolveLanguageObject(newAgg, metadata);
                nestedAggregates.add(partitionAgg);
            }

            aggMap.put(partitionAgg, newExpression);
        }
        return aggMap;
    }

    private static Constant newLiteral(int value, Class<?> type) {
        return new Constant(type==DefaultDataClasses.LONG?(long)value:value, type);
    }

    /**
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        return "PushAggregates"; //$NON-NLS-1$
    }
}
