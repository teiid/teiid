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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 * @since 4.2
 */
public class RulePushAggregates implements
                               OptimizerRule {

    /**
     * @see com.metamatrix.query.optimizer.relational.OptimizerRule#execute(com.metamatrix.query.optimizer.relational.plantree.PlanNode,
     *      com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder,
     *      com.metamatrix.query.optimizer.relational.RuleStack, AnalysisRecord, CommandContext)
     * @since 4.2
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   MetaMatrixComponentException {

        for (PlanNode groupNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.GROUP, NodeConstants.Types.ACCESS)) {
            PlanNode joinNode = groupNode.getFirstChild();

            if (joinNode.getType() != NodeConstants.Types.JOIN) {
                continue;
            }

            List<SingleElementSymbol> groupingExpressions = (List<SingleElementSymbol>)groupNode.getProperty(NodeConstants.Info.GROUP_COLS);
            Set<AggregateSymbol> aggregates = collectAggregates(groupNode);

            pushGroupNode(groupNode, groupingExpressions, aggregates, metadata, capFinder);
        }

        return plan;
    }

    /**
     * Walk up the plan from the GROUP node. Should encounter only (optionally) a SELECT and can stop at the PROJECT node. Need to
     * collect any AggregateSymbols used in the select criteria or projected columns.
     * 
     * @param groupNode
     * @return the set of aggregate symbols found
     * @since 4.2
     */
    static Set<AggregateSymbol> collectAggregates(PlanNode groupNode) {
        Set<AggregateSymbol> aggregates = new HashSet<AggregateSymbol>();
        PlanNode currentNode = groupNode.getParent();
        while (currentNode != null) {
            if (currentNode.getType() == NodeConstants.Types.PROJECT) {
                List<SingleElementSymbol> projectedSymbols = (List<SingleElementSymbol>)currentNode.getProperty(NodeConstants.Info.PROJECT_COLS);
                for (SingleElementSymbol symbol : projectedSymbols) {
                    aggregates.addAll(AggregateSymbolCollectorVisitor.getAggregates(symbol, true));
                }
                break;
            }
            if (currentNode.getType() == NodeConstants.Types.SELECT) {
                Criteria crit = (Criteria)currentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                aggregates.addAll(AggregateSymbolCollectorVisitor.getAggregates(crit, true));
            }

            currentNode = currentNode.getParent();
        }
        return aggregates;
    }

    /**
     * Attempt to push the group node below one or more joins, manipulating the parent plan as necessary. This may involve
     * modifying symbols in parent nodes (to account for staged aggregates).
     * @throws QueryPlannerException 
     * 
     * @since 4.2
     */
    private void pushGroupNode(PlanNode groupNode,
                               List<SingleElementSymbol> groupingExpressions,
                               Set<AggregateSymbol> allAggregates,
                               QueryMetadataInterface metadata,
                               CapabilitiesFinder capFinder) throws MetaMatrixComponentException,
                                                            QueryMetadataException, QueryPlannerException {

        Map<PlanNode, List<SingleElementSymbol>> aggregateMap = createNodeMapping(groupNode, allAggregates);
        Map<PlanNode, List<SingleElementSymbol>> groupingMap = createNodeMapping(groupNode, groupingExpressions);

        Set<PlanNode> possibleTargetNodes = new HashSet<PlanNode>(aggregateMap.keySet());
        possibleTargetNodes.addAll(groupingMap.keySet());

        for (PlanNode planNode : possibleTargetNodes) {
            Set<SingleElementSymbol> stagedGroupingSymbols = new LinkedHashSet<SingleElementSymbol>();
            List<SingleElementSymbol> aggregates = aggregateMap.get(planNode);
            List<SingleElementSymbol> groupBy = groupingMap.get(planNode);

            if (!canPush(groupNode, stagedGroupingSymbols, planNode)) {
                continue;
            }

            if (groupBy != null) {
                stagedGroupingSymbols.addAll(groupBy);
            }

            collectSymbolsFromOtherAggregates(allAggregates, aggregates, planNode, stagedGroupingSymbols);
            
            //if the grouping expressions are unique then there's no point in staging the aggregate
            //TODO: the uses key check is not really accurate.
            if (NewCalculateCostUtil.usesKey(stagedGroupingSymbols, metadata)) {
                continue;
            }

            if (aggregates != null) {
                stageAggregates(groupNode, metadata, stagedGroupingSymbols, aggregates);
            } else {
                aggregates = new ArrayList<SingleElementSymbol>();
            }

            if (aggregates.isEmpty() && stagedGroupingSymbols.isEmpty()) {
                continue;
            }
            //TODO: if aggregates is empty, then could insert a dup remove node instead
            
            PlanNode stageGroup = NodeFactory.getNewNode(NodeConstants.Types.GROUP);
            planNode.addAsParent(stageGroup);

            if (!stagedGroupingSymbols.isEmpty()) {
                stageGroup.setProperty(NodeConstants.Info.GROUP_COLS, new ArrayList<SingleElementSymbol>(stagedGroupingSymbols));
                stageGroup.addGroups(GroupsUsedByElementsVisitor.getGroups(stagedGroupingSymbols));
            } else {
                // if the source has no rows we need to insert a select node with criteria count(*)>0
                PlanNode selectNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
                AggregateSymbol count = new AggregateSymbol("stagedAgg", ReservedWords.COUNT, false, null); //$NON-NLS-1$
                aggregates.add(count); //consider the count aggregate for the push down call below
                selectNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, new CompareCriteria(count, CompareCriteria.GT,
                                                                                               new Constant(new Integer(0))));
                selectNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
                stageGroup.addAsParent(selectNode);
            }

            //check for push down
            if (stageGroup.getFirstChild().getType() == NodeConstants.Types.ACCESS 
                            && RuleRaiseAccess.canRaiseOverGroupBy(stageGroup, stageGroup.getFirstChild(), aggregates, metadata, capFinder)) {
                RuleRaiseAccess.performRaise(null, stageGroup.getFirstChild(), stageGroup);
                if (stagedGroupingSymbols.isEmpty()) {
                    RuleRaiseAccess.performRaise(null, stageGroup.getParent(), stageGroup.getParent().getParent());
                }
            }
        }
    }

    private void stageAggregates(PlanNode groupNode,
                                 QueryMetadataInterface metadata,
                                 Set<SingleElementSymbol> stagedGroupingSymbols,
                                 List<SingleElementSymbol> aggregates) throws MetaMatrixComponentException, QueryPlannerException {
        //remove any aggregates that are computed over a group by column
        Set<Expression> expressions = new HashSet<Expression>();
        for (SingleElementSymbol expression : stagedGroupingSymbols) {
            expressions.add(SymbolMap.getExpression(expression));
        }
        
        for (final Iterator<SingleElementSymbol> iterator = aggregates.iterator(); iterator.hasNext();) {
            final AggregateSymbol symbol = (AggregateSymbol)iterator.next();
            Expression expr = symbol.getExpression();
            if (expr == null) {
                continue;
            }
            if (expressions.contains(expr)) {
                iterator.remove();
            }
        } 
        
        if (!aggregates.isEmpty()) {
            // Fix any aggregate expressions so they correctly recombine the staged aggregates
            try {
                Set<AggregateSymbol> newAggs = new HashSet<AggregateSymbol>();
                Map<AggregateSymbol, Expression> aggMap = buildAggregateMap(aggregates, metadata, newAggs);
                mapExpressions(groupNode.getParent(), aggMap);
                aggregates.clear();
                aggregates.addAll(newAggs);
            } catch (QueryResolverException err) {
                throw new MetaMatrixComponentException(err);
            }
        } 
    }
    
    private void collectSymbolsFromOtherAggregates(Collection<AggregateSymbol> allAggregates,
                                                      Collection<SingleElementSymbol> aggregates,
                                                      PlanNode current,
                                                      Set<SingleElementSymbol> stagedGroupingSymbols) {
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
     */
    private boolean canPush(PlanNode groupNode,
                            Set<SingleElementSymbol> stagedGroupingSymbols,
                            PlanNode planNode) {
        PlanNode parentJoin = planNode.getParent();
        
        Set<GroupSymbol> groups = FrameUtil.findJoinSourceNode(planNode).getGroups();
        
        while (parentJoin != groupNode) {
            if (parentJoin.getType() != NodeConstants.Types.JOIN
                || parentJoin.hasCollectionProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA)
                || ((JoinType)parentJoin.getProperty(NodeConstants.Info.JOIN_TYPE)).isOuter()) {
                return false;
            }

            if (planNode == parentJoin.getFirstChild()) {
                if (parentJoin.hasCollectionProperty(NodeConstants.Info.LEFT_EXPRESSIONS) && !filterJoinColumns(stagedGroupingSymbols, groups, (List<SingleElementSymbol>)parentJoin.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS))) {
                    return false;
                }
            } else {
                if (parentJoin.hasCollectionProperty(NodeConstants.Info.RIGHT_EXPRESSIONS) && !filterJoinColumns(stagedGroupingSymbols, groups, (List<SingleElementSymbol>)parentJoin.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS))) {
                    return false;
                }
            }

            planNode = parentJoin;
            parentJoin = parentJoin.getParent();
        }
        return true;
    }

    private boolean filterJoinColumns(Set<SingleElementSymbol> stagedGroupingSymbols,
                                   Set<GroupSymbol> groups,
                                   List<SingleElementSymbol> symbols) {
        for (SingleElementSymbol singleElementSymbol : symbols) {
            if (!(singleElementSymbol instanceof ElementSymbol)) {
                return false;
            }
            if (groups.contains(((ElementSymbol)singleElementSymbol).getGroupSymbol())) {
                stagedGroupingSymbols.add(singleElementSymbol);
            }
        }
        return true;
    }

    private Map<PlanNode, List<SingleElementSymbol>> createNodeMapping(PlanNode groupNode,
                                                                       Collection<? extends SingleElementSymbol> expressions) {
        Map<PlanNode, List<SingleElementSymbol>> result = new HashMap<PlanNode, List<SingleElementSymbol>>();
        if (expressions == null) {
            return result;
        }
        for (SingleElementSymbol aggregateSymbol : expressions) {
            if (aggregateSymbol instanceof AggregateSymbol) {
                AggregateSymbol partitionAgg = (AggregateSymbol)aggregateSymbol;
                if (partitionAgg.isDistinct()) {
                    continue; //currently we cann't consider distinct aggs
                }
            }
            
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(aggregateSymbol);
            if (groups.isEmpty()) {
                continue;
            }
            PlanNode originatingNode = FrameUtil.findOriginatingNode(groupNode, groups);
            if (originatingNode == null) {
                continue;
            }

            PlanNode parentAccess = NodeEditor.findParent(originatingNode, NodeConstants.Types.ACCESS, NodeConstants.Types.GROUP);

            if (parentAccess != null) {
                while (parentAccess.getType() == NodeConstants.Types.SELECT) {
                    parentAccess = parentAccess.getParent();
                }
                originatingNode = parentAccess;
            }

            if (originatingNode.getParent() == groupNode) {
                continue;
            }

            List<SingleElementSymbol> symbols = result.get(originatingNode);
            if (symbols == null) {
                symbols = new LinkedList<SingleElementSymbol>();
                result.put(originatingNode, symbols);
            }
            symbols.add(aggregateSymbol);
        }
        return result;
    }

    private Map<AggregateSymbol, Expression> buildAggregateMap(Collection<SingleElementSymbol> aggregateExpressions,
                                                                        QueryMetadataInterface metadata, Set<AggregateSymbol> nestedAggregates) throws QueryResolverException,
                                                                                                        MetaMatrixComponentException {
        Map<AggregateSymbol, Expression> aggMap = new HashMap<AggregateSymbol, Expression>();
        for (SingleElementSymbol symbol : aggregateExpressions) {
            AggregateSymbol partitionAgg = (AggregateSymbol)symbol;
           
            Expression newExpression = null;

            String aggFunction = partitionAgg.getAggregateFunction();
            if (aggFunction.equals(ReservedWords.COUNT)) {
                //COUNT(x) -> CONVERT(SUM(COUNT(x)), INTEGER)
                AggregateSymbol newAgg = new AggregateSymbol("stagedAgg", ReservedWords.SUM, false, partitionAgg); //$NON-NLS-1$

                // Build conversion function to convert SUM (which returns LONG) back to INTEGER
                Constant convertTargetType = new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()),
                                                          DataTypeManager.DefaultDataClasses.STRING);
                Function convertFunc = new Function(FunctionLibrary.CONVERT, new Expression[] {
                    newAgg, convertTargetType
                });
                ResolverVisitor.resolveLanguageObject(convertFunc, metadata);

                newExpression = convertFunc;  
                nestedAggregates.add(partitionAgg);
            } else if (aggFunction.equals(ReservedWords.AVG)) {
                //AVG(x) -> SUM(SUM(x)) / SUM(COUNT(x))
                AggregateSymbol countAgg = new AggregateSymbol("stagedAgg", ReservedWords.COUNT, false, partitionAgg.getExpression()); //$NON-NLS-1$
                AggregateSymbol sumAgg = new AggregateSymbol("stagedAgg", ReservedWords.SUM, false, partitionAgg.getExpression()); //$NON-NLS-1$
                
                AggregateSymbol sumSumAgg = new AggregateSymbol("stagedAgg", ReservedWords.SUM, false, sumAgg); //$NON-NLS-1$
                AggregateSymbol sumCountAgg = new AggregateSymbol("stagedAgg", ReservedWords.SUM, false, countAgg); //$NON-NLS-1$

                Function divideFunc = new Function("/", new Expression[] {sumSumAgg, sumCountAgg}); //$NON-NLS-1$
                ResolverVisitor.resolveLanguageObject(divideFunc, metadata);

                newExpression = divideFunc;
                nestedAggregates.add(countAgg);
                nestedAggregates.add(sumAgg);
            } else {
                //AGG(X) -> AGG(AGG(X))
                newExpression = new AggregateSymbol("stagedAgg", aggFunction, false, partitionAgg); //$NON-NLS-1$
                nestedAggregates.add(partitionAgg);
            }

            aggMap.put(partitionAgg, newExpression);
        }
        return aggMap;
    }
    
    static void mapExpressions(PlanNode node, Map<? extends Expression, ? extends Expression> exprMap) 
    throws QueryPlannerException {
        
        while (node != null) {
            FrameUtil.convertNode(node, null, null, exprMap);
            
            switch (node.getType()) {
                case NodeConstants.Types.SOURCE:
                case NodeConstants.Types.GROUP:
                    return;
            }

            node = node.getParent();
        }
    }

    /**
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        return "PushAggregates"; //$NON-NLS-1$
    }
}
