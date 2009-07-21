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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.ExpressionMappingVisitor;
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
            PlanNode child = groupNode.getFirstChild();

        	List<SingleElementSymbol> groupingExpressions = (List<SingleElementSymbol>)groupNode.getProperty(NodeConstants.Info.GROUP_COLS);
            
            if (child.getType() == NodeConstants.Types.SOURCE) {
                PlanNode setOp = child.getFirstChild();
                
                try {
					pushGroupNodeOverUnion(plan, metadata, capFinder, groupNode, child, groupingExpressions, setOp);
				} catch (QueryResolverException e) {
					throw new MetaMatrixComponentException(e);
				}
                continue;
            }
        	
            if (child.getType() != NodeConstants.Types.JOIN) {
                continue;
            }

            Set<AggregateSymbol> aggregates = collectAggregates(groupNode);

            pushGroupNode(groupNode, groupingExpressions, aggregates, metadata, capFinder);
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
	private void pushGroupNodeOverUnion(PlanNode plan,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
			PlanNode groupNode, PlanNode child,
			List<SingleElementSymbol> groupingExpressions, PlanNode setOp)
			throws MetaMatrixComponentException, QueryMetadataException,
			QueryPlannerException, QueryResolverException {
		if (setOp.getType() != NodeConstants.Types.SET_OP || setOp.getProperty(NodeConstants.Info.SET_OPERATION) != Operation.UNION) {
			return; //must not be a union
		}
		LinkedHashSet<AggregateSymbol> aggregates = collectAggregates(groupNode);

		/*
		 * if there are no aggregates, this is just duplicate removal
		 * mark the union as not all, which should be removed later but
		 * serves as a hint to distribute a distinct to the union queries
		 */
		if (aggregates.isEmpty()) {
			if (groupingExpressions != null && !groupingExpressions.isEmpty()) {
				setOp.setProperty(NodeConstants.Info.USE_ALL, Boolean.FALSE);
			}
			return;
		}
		
		//check to see if any aggregate is dependent upon cardinality
		boolean cardinalityDependent = false;
		for (AggregateSymbol aggregateSymbol : aggregates) {
			if (aggregateSymbol.getAggregateFunction().equals(ReservedWords.COUNT)
					|| aggregateSymbol.getAggregateFunction().equals(ReservedWords.AVG)
					|| aggregateSymbol.getAggregateFunction().equals(ReservedWords.SUM)) {
				cardinalityDependent = true;
				break;
			}
		}
		
		LinkedList<PlanNode> unionChildren = new LinkedList<PlanNode>();
		findUnionChildren(unionChildren, cardinalityDependent, setOp);

		if (unionChildren.size() < 2) {
			return;
		}
		
		SymbolMap parentMap = (SymbolMap)child.getProperty(NodeConstants.Info.SYMBOL_MAP);
		List<ElementSymbol> virtualElements = parentMap.getKeys();

		Map<AggregateSymbol, Expression> aggMap = buildAggregateMap(new ArrayList<SingleElementSymbol>(aggregates), metadata, aggregates);

		boolean shouldPushdown = false;
		List<Boolean> pushdownList = new ArrayList<Boolean>(unionChildren.size());
		for (PlanNode planNode : unionChildren) {
			boolean pushdown = canPushGroupByToUnionChild(metadata, capFinder, groupingExpressions, aggregates, planNode); 
			pushdownList.add(pushdown);
			shouldPushdown |= pushdown;
		}
		
		if (!shouldPushdown) {
			return;
		}

		Iterator<Boolean> pushdownIterator = pushdownList.iterator();
		for (PlanNode planNode : unionChildren) {
		    addView(plan, planNode, pushdownIterator.next(), groupingExpressions, aggregates, virtualElements, metadata, capFinder);
		}
		
		//update the parent plan with the staged aggregates and the new projected symbols
		List<SingleElementSymbol> projectedViewSymbols = (List<SingleElementSymbol>)NodeEditor.findNodePreOrder(child, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS);
		SymbolMap newParentMap = SymbolMap.createSymbolMap(child.getGroups().iterator().next(), projectedViewSymbols);
		child.setProperty(NodeConstants.Info.SYMBOL_MAP, newParentMap);
		Map<AggregateSymbol, ElementSymbol> projectedMap = new HashMap<AggregateSymbol, ElementSymbol>();
		Iterator<AggregateSymbol> aggIter = aggregates.iterator();
		for (ElementSymbol projectedViewSymbol : newParentMap.getKeys().subList(projectedViewSymbols.size() - aggregates.size(), projectedViewSymbols.size())) {
			projectedMap.put(aggIter.next(), projectedViewSymbol);
		}
		for (Expression expr : aggMap.values()) {
			ExpressionMappingVisitor.mapExpressions(expr, projectedMap);
		}
		mapExpressions(groupNode.getParent(), aggMap);
	}

	private boolean canPushGroupByToUnionChild(QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder,
			List<SingleElementSymbol> groupingExpressions,
			LinkedHashSet<AggregateSymbol> aggregates, PlanNode planNode)
			throws QueryMetadataException, MetaMatrixComponentException {
		if (planNode.getType() != NodeConstants.Types.ACCESS) {
			return false;
		}
		Object modelId = RuleRaiseAccess.getModelIDFromAccess(planNode, metadata);
		if (!CapabilitiesUtil.supports(Capability.QUERY_FROM_INLINE_VIEWS, modelId, metadata, capFinder) 
				|| !CapabilitiesUtil.supports(Capability.QUERY_GROUP_BY, modelId, metadata, capFinder)) {
			return false;
		}
		for (AggregateSymbol aggregate : aggregates) {
			if (!CapabilitiesUtil.supportsAggregateFunction(modelId, aggregate, metadata, capFinder)) {
				return false;
			}
		}
		if ((groupingExpressions == null || groupingExpressions.isEmpty()) && !CapabilitiesUtil.supports(Capability.QUERY_AGGREGATES_COUNT_STAR, modelId, metadata, capFinder)) {
			return false;
		}
		//TODO: check to see if we are distinct
		return true;
	}
    
	/**
	 * Recursively searches the union tree for all applicable source nodes
	 */
	private PlanNode findUnionChildren(List<PlanNode> unionChildren, boolean carinalityDependent, PlanNode setOp) {
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
    
	public void addView(PlanNode root, PlanNode unionSource, boolean pushdown, List<SingleElementSymbol> groupingExpressions,
			Set<AggregateSymbol> aggregates, List<ElementSymbol> virtualElements,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
			throws MetaMatrixComponentException, QueryPlannerException, QueryResolverException {
		PlanNode originalNode = unionSource;
    	PlanNode intermediateView = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
    	unionSource.addAsParent(intermediateView);
    	unionSource = intermediateView;
    	TempMetadataStore store = new TempMetadataStore();
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, store);
        GroupSymbol group = new GroupSymbol("X"); //$NON-NLS-1$
        try {
			group.setMetadataID(ResolverUtil.addTempGroup(tma, group, virtualElements, false));
		} catch (QueryResolverException e) {
			throw new MetaMatrixComponentException(e);
		}
    	intermediateView.addGroup(group);
    	List<ElementSymbol> projectedSymbols = ResolverUtil.resolveElementsInGroup(group, metadata);
    	SymbolMap symbolMap = SymbolMap.createSymbolMap(projectedSymbols, 
				(List<Expression>)NodeEditor.findNodePreOrder(unionSource, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS));
    	intermediateView.setProperty(NodeConstants.Info.SYMBOL_MAP, symbolMap);
    	
        Set<SingleElementSymbol> newGroupingExpressions = Collections.emptySet();
        if (groupingExpressions != null) {
        	newGroupingExpressions = new HashSet<SingleElementSymbol>();
        	for (SingleElementSymbol singleElementSymbol : groupingExpressions) {
				newGroupingExpressions.add((SingleElementSymbol)symbolMap.getKeys().get(virtualElements.indexOf(singleElementSymbol)).clone());
			}
        }

        List<SingleElementSymbol> projectedViewSymbols = QueryRewriter.deepClone(projectedSymbols, SingleElementSymbol.class);

        SymbolMap viewMapping = SymbolMap.createSymbolMap(NodeEditor.findParent(unionSource, NodeConstants.Types.SOURCE).getGroups().iterator().next(), projectedSymbols);
        for (AggregateSymbol agg : aggregates) {
        	agg = (AggregateSymbol)agg.clone();
        	ExpressionMappingVisitor.mapExpressions(agg, viewMapping.asMap());
        	if (pushdown) {
        		projectedViewSymbols.add(agg);
        	} else {
        		if (agg.getAggregateFunction().equals(ReservedWords.COUNT)) {
        			SearchedCaseExpression count = new SearchedCaseExpression(Arrays.asList(new IsNullCriteria(agg.getExpression())), Arrays.asList(new Constant(Integer.valueOf(0))));
        			count.setElseExpression(new Constant(Integer.valueOf(1)));
        			count.setType(DataTypeManager.DefaultDataClasses.INTEGER);
    				projectedViewSymbols.add(new ExpressionSymbol("stagedAgg", count)); //$NON-NLS-1$
        		} else { //min, max, sum
        			Expression ex = agg.getExpression();
        			ex = ResolverUtil.convertExpression(ex, DataTypeManager.getDataTypeName(agg.getType()));
        			projectedViewSymbols.add(new ExpressionSymbol("stagedAgg", ex)); //$NON-NLS-1$
        		}
        	}
		}

        if (pushdown) {
        	unionSource = addGroupBy(unionSource, newGroupingExpressions, new LinkedList<AggregateSymbol>());
        }
        
        PlanNode projectPlanNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        unionSource.addAsParent(projectPlanNode);
        unionSource = projectPlanNode;

        //create proper names for the aggregate symbols
        Select select = new Select(projectedViewSymbols);
        QueryRewriter.makeSelectUnique(select, false);
        projectedViewSymbols = select.getProjectedSymbols();
        projectPlanNode.setProperty(NodeConstants.Info.PROJECT_COLS, projectedViewSymbols);
        projectPlanNode.addGroup(group);
        if (pushdown) {
        	while (RuleRaiseAccess.raiseAccessNode(root, originalNode, metadata, capFinder, true) != null) {
        		//continue to raise
        	}
        }
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

        Map<PlanNode, List<AggregateSymbol>> aggregateMap = createNodeMapping(groupNode, allAggregates);
        Map<PlanNode, List<SingleElementSymbol>> groupingMap = createNodeMapping(groupNode, groupingExpressions);

        Set<PlanNode> possibleTargetNodes = new HashSet<PlanNode>(aggregateMap.keySet());
        possibleTargetNodes.addAll(groupingMap.keySet());

        for (PlanNode planNode : possibleTargetNodes) {
            Set<SingleElementSymbol> stagedGroupingSymbols = new LinkedHashSet<SingleElementSymbol>();
            List<AggregateSymbol> aggregates = aggregateMap.get(planNode);
            List<SingleElementSymbol> groupBy = groupingMap.get(planNode);

            if (!canPush(groupNode, stagedGroupingSymbols, planNode)) {
                continue;
            }

            if (groupBy != null) {
                stagedGroupingSymbols.addAll(groupBy);
            }

            collectSymbolsFromOtherAggregates(allAggregates, aggregates, planNode, stagedGroupingSymbols);
            
            //if the grouping expressions are unique then there's no point in staging the aggregate
            //TODO: the uses key check is not really accurate, it doesn't take into consideration where 
            //we are in the plan.
            //if a key column is used after a non 1-1 join or a union all, then it may be non-unique.
            if (NewCalculateCostUtil.usesKey(stagedGroupingSymbols, metadata)) {
            	continue;
            }

        	//TODO: we should be doing another cost check here - especially if the aggregate cannot be pushed.
            
            if (aggregates != null) {
                stageAggregates(groupNode, metadata, stagedGroupingSymbols, aggregates);
            } else {
                aggregates = new ArrayList<AggregateSymbol>(1);
            }

            if (aggregates.isEmpty() && stagedGroupingSymbols.isEmpty()) {
                continue;
            }
            //TODO: if aggregates is empty, then could insert a dup remove node instead
            
            PlanNode stageGroup = addGroupBy(planNode, stagedGroupingSymbols, aggregates);
    		
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

	private PlanNode addGroupBy(PlanNode planNode,
			Collection<SingleElementSymbol> stagedGroupingSymbols,
			Collection<AggregateSymbol> aggregates) {
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
		return stageGroup;
	}

    static void stageAggregates(PlanNode groupNode,
                                 QueryMetadataInterface metadata,
                                 Collection<SingleElementSymbol> stagedGroupingSymbols,
                                 Collection<AggregateSymbol> aggregates) throws MetaMatrixComponentException, QueryPlannerException {
        //remove any aggregates that are computed over a group by column
        Set<Expression> expressions = new HashSet<Expression>();
        for (SingleElementSymbol expression : stagedGroupingSymbols) {
            expressions.add(SymbolMap.getExpression(expression));
        }
        
        for (final Iterator<AggregateSymbol> iterator = aggregates.iterator(); iterator.hasNext();) {
            final AggregateSymbol symbol = iterator.next();
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
                                                      Collection<AggregateSymbol> aggregates,
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

    private <T extends SingleElementSymbol> Map<PlanNode, List<T>> createNodeMapping(PlanNode groupNode,
                                                                       Collection<T> expressions) {
        Map<PlanNode, List<T>> result = new HashMap<PlanNode, List<T>>();
        if (expressions == null) {
            return result;
        }
        for (T aggregateSymbol : expressions) {
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

            List<T> symbols = result.get(originatingNode);
            if (symbols == null) {
                symbols = new LinkedList<T>();
                result.put(originatingNode, symbols);
            }
            symbols.add(aggregateSymbol);
        }
        return result;
    }

    private static Map<AggregateSymbol, Expression> buildAggregateMap(Collection<? extends SingleElementSymbol> aggregateExpressions,
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
