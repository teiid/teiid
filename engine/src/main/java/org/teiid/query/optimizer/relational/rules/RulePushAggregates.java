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

package org.teiid.query.optimizer.relational.rules;

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

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageObject.Util;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;


/**
 * @since 4.2
 */
public class RulePushAggregates implements
                               OptimizerRule {

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
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {

        for (PlanNode groupNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.GROUP, NodeConstants.Types.ACCESS)) {
            PlanNode child = groupNode.getFirstChild();

        	List<SingleElementSymbol> groupingExpressions = (List<SingleElementSymbol>)groupNode.getProperty(NodeConstants.Info.GROUP_COLS);
            
            if (child.getType() == NodeConstants.Types.SOURCE) {
                PlanNode setOp = child.getFirstChild();
                
                try {
					pushGroupNodeOverUnion(plan, metadata, capFinder, groupNode, child, groupingExpressions, setOp, context);
				} catch (QueryResolverException e) {
					throw new TeiidComponentException(e);
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
     * 
     * if partitioned, then we don't need decomposition or the top level group by
     * 
	 *   source
	 *     set op
	 *       project
	 *         [select]
	 *           group [agg(x), {a, b}]
	 *             source
	 *               child 1
	 *       ...
     * 
	 */
	private void pushGroupNodeOverUnion(PlanNode plan,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
			PlanNode groupNode, PlanNode child,
			List<SingleElementSymbol> groupingExpressions, PlanNode setOp, CommandContext context)
			throws TeiidComponentException, QueryMetadataException,
			QueryPlannerException, QueryResolverException {
		if (setOp == null || setOp.getType() != NodeConstants.Types.SET_OP || setOp.getProperty(NodeConstants.Info.SET_OPERATION) != Operation.UNION) {
			return; //must not be a union
		}
		LinkedHashSet<AggregateSymbol> aggregates = collectAggregates(groupNode);

		Map<ElementSymbol, List<Set<Constant>>> partitionInfo = (Map<ElementSymbol, List<Set<Constant>>>)child.getProperty(Info.PARTITION_INFO);

		//check to see if any aggregate is dependent upon cardinality
		boolean cardinalityDependent = AggregateSymbol.areAggregatesCardinalityDependent(aggregates);

		LinkedList<PlanNode> unionChildren = new LinkedList<PlanNode>();
		findUnionChildren(unionChildren, cardinalityDependent, setOp);

		SymbolMap parentMap = (SymbolMap)child.getProperty(NodeConstants.Info.SYMBOL_MAP);

		//partitioned union
		if (partitionInfo != null && !Collections.disjoint(partitionInfo.keySet(), groupingExpressions)) {
			decomposeGroupBy(groupNode, child, groupingExpressions, aggregates, unionChildren, parentMap, context, metadata, capFinder);
			return;
		}

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
		
		//TODO: merge virtual, plan unions, raise null - change the partition information
		
		if (unionChildren.size() < 2) {
			return;
		}
		
		List<ElementSymbol> virtualElements = parentMap.getKeys();
		List<SingleElementSymbol> copy = new ArrayList<SingleElementSymbol>(aggregates);
		aggregates.clear();
		Map<AggregateSymbol, Expression> aggMap = buildAggregateMap(copy, metadata, aggregates);

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
		List<ElementSymbol> updatedVirturalElement = new ArrayList<ElementSymbol>(virtualElements);
		
		//hack to introduce aggregate symbols to the parent view TODO: this should change the metadata properly.
		GroupSymbol virtualGroup = child.getGroups().iterator().next();
		for (int i = updatedVirturalElement.size(); i < projectedViewSymbols.size(); i++) {
			SingleElementSymbol symbol = projectedViewSymbols.get(i);
			String name = symbol.getShortName();
            String virtualElementName = virtualGroup.getCanonicalName() + ElementSymbol.SEPARATOR + name;
            ElementSymbol virtualElement = new ElementSymbol(virtualElementName);
            virtualElement.setGroupSymbol(virtualGroup);
            virtualElement.setType(symbol.getType());
            virtualElement.setMetadataID(new TempMetadataID(virtualElementName, symbol.getType()));
            updatedVirturalElement.add(virtualElement);
		}
		SymbolMap newParentMap = SymbolMap.createSymbolMap(updatedVirturalElement, projectedViewSymbols);
		child.setProperty(NodeConstants.Info.SYMBOL_MAP, newParentMap);
		Map<AggregateSymbol, ElementSymbol> projectedMap = new HashMap<AggregateSymbol, ElementSymbol>();
		Iterator<AggregateSymbol> aggIter = aggregates.iterator();
		for (ElementSymbol projectedViewSymbol : newParentMap.getKeys().subList(projectedViewSymbols.size() - aggregates.size(), projectedViewSymbols.size())) {
			projectedMap.put(aggIter.next(), projectedViewSymbol);
		}
		for (Expression expr : aggMap.values()) {
			ExpressionMappingVisitor.mapExpressions(expr, projectedMap);
		}
		mapExpressions(groupNode.getParent(), aggMap, metadata);
	}

	private void decomposeGroupBy(PlanNode groupNode, PlanNode sourceNode,
			List<SingleElementSymbol> groupingExpressions,
			LinkedHashSet<AggregateSymbol> aggregates,
			LinkedList<PlanNode> unionChildren, SymbolMap parentMap, CommandContext context, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
		// remove the group node
		groupNode.getParent().replaceChild(groupNode, groupNode.getFirstChild());
		GroupSymbol group = sourceNode.getGroups().iterator().next().clone();

		boolean first = true;
		List<SingleElementSymbol> symbols = null;
		for (PlanNode planNode : unionChildren) {
			PlanNode groupClone = NodeFactory.getNewNode(NodeConstants.Types.GROUP);
			groupClone.setProperty(Info.GROUP_COLS, LanguageObject.Util.deepClone(groupingExpressions, SingleElementSymbol.class));
			groupClone.addGroups(groupNode.getGroups());
			
			PlanNode view = RuleDecomposeJoin.createSource(group, planNode, parentMap);
			
			view.addAsParent(groupClone);
			
			PlanNode projectPlanNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
			
			Select allSymbols = new Select(groupingExpressions);
			allSymbols.addSymbols(aggregates);
			if (first) {
				first = false;
				QueryRewriter.makeSelectUnique(allSymbols, false);
				symbols = allSymbols.getSymbols();
			}
			projectPlanNode.setProperty(NodeConstants.Info.PROJECT_COLS, allSymbols.getSymbols());
		    projectPlanNode.addGroups(view.getGroups());
		    
		    groupClone.addAsParent(projectPlanNode);
		    
		    if (planNode.getType() == NodeConstants.Types.ACCESS) {
		    	//TODO: temporarily remove the access node so that the inline view could be removed if possible 
			    while (RuleRaiseAccess.raiseAccessNode(planNode, planNode, metadata, capFinder, true, null) != null) {
	        		//continue to raise
	        	}
		    }
		}
		GroupSymbol modifiedGroup = group.clone();
		SymbolMap symbolMap = createSymbolMap(modifiedGroup, symbols, sourceNode, metadata);
		sourceNode.setProperty(Info.SYMBOL_MAP, symbolMap);
		
		FrameUtil.convertFrame(sourceNode, group, Collections.singleton(modifiedGroup), symbolMap.inserseMapping(), metadata);
	}

	private boolean canPushGroupByToUnionChild(QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder,
			List<SingleElementSymbol> groupingExpressions,
			LinkedHashSet<AggregateSymbol> aggregates, PlanNode planNode)
			throws QueryMetadataException, TeiidComponentException {
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
    
	public void addView(PlanNode root, PlanNode unionSource, boolean pushdown, List<SingleElementSymbol> groupingExpressions,
			Set<AggregateSymbol> aggregates, List<ElementSymbol> virtualElements,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
			throws TeiidComponentException, QueryPlannerException, QueryResolverException {
		PlanNode originalNode = unionSource;
		//branches other than the first need to have their projected column names updated
		PlanNode sortNode = NodeEditor.findNodePreOrder(unionSource, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
		List<SingleElementSymbol> sortOrder = null;
		OrderBy orderBy = null;
		if (sortNode != null) {
			orderBy = (OrderBy)sortNode.getProperty(Info.SORT_ORDER);
			sortOrder = orderBy.getSortKeys();
		}
		List<SingleElementSymbol> projectCols = FrameUtil.findTopCols(unionSource);
		for (int i = 0; i < virtualElements.size(); i++) {
			ElementSymbol virtualElem = virtualElements.get(i);
			SingleElementSymbol projectedSymbol = projectCols.get(i);
			if (!projectedSymbol.getShortCanonicalName().equals(virtualElem.getShortCanonicalName())) {
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
		GroupSymbol group = new GroupSymbol("X"); //$NON-NLS-1$
        
		PlanNode intermediateView = createView(group, virtualElements, unionSource, metadata);
    	SymbolMap symbolMap = (SymbolMap)intermediateView.getProperty(Info.SYMBOL_MAP);
    	unionSource = intermediateView;
    	
        Set<SingleElementSymbol> newGroupingExpressions = Collections.emptySet();
        if (groupingExpressions != null) {
        	newGroupingExpressions = new HashSet<SingleElementSymbol>();
        	for (SingleElementSymbol singleElementSymbol : groupingExpressions) {
				newGroupingExpressions.add((SingleElementSymbol)symbolMap.getKeys().get(virtualElements.indexOf(singleElementSymbol)).clone());
			}
        }

        List<SingleElementSymbol> projectedViewSymbols = Util.deepClone(symbolMap.getKeys(), SingleElementSymbol.class);

        PlanNode parent = NodeEditor.findParent(unionSource, NodeConstants.Types.SOURCE);
        SymbolMap parentMap = (SymbolMap) parent.getProperty(NodeConstants.Info.SYMBOL_MAP);
        SymbolMap viewMapping = SymbolMap.createSymbolMap(parentMap.getKeys(), projectedViewSymbols);
        for (AggregateSymbol agg : aggregates) {
        	agg = (AggregateSymbol)agg.clone();
        	ExpressionMappingVisitor.mapExpressions(agg, viewMapping.asMap());
        	if (pushdown) {
        		projectedViewSymbols.add(agg);
        	} else {
        		if (agg.getAggregateFunction() == Type.COUNT) {
        			if (agg.getExpression() == null) {
	    				projectedViewSymbols.add(new ExpressionSymbol("stagedAgg", new Constant(1))); //$NON-NLS-1$
        			} else { 
	        			SearchedCaseExpression count = new SearchedCaseExpression(Arrays.asList(new IsNullCriteria(agg.getExpression())), Arrays.asList(new Constant(Integer.valueOf(0))));
	        			count.setElseExpression(new Constant(Integer.valueOf(1)));
	        			count.setType(DataTypeManager.DefaultDataClasses.INTEGER);
	    				projectedViewSymbols.add(new ExpressionSymbol("stagedAgg", count)); //$NON-NLS-1$
        			}
        		} else { //min, max, sum
        			Expression ex = agg.getExpression();
        			ex = ResolverUtil.convertExpression(ex, DataTypeManager.getDataTypeName(agg.getType()), metadata);
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
        	while (RuleRaiseAccess.raiseAccessNode(root, originalNode, metadata, capFinder, true, null) != null) {
        		//continue to raise
        	}
        }
    }
	
	static PlanNode createView(GroupSymbol group, List<? extends SingleElementSymbol> virtualElements, PlanNode child, QueryMetadataInterface metadata) throws TeiidComponentException {
		PlanNode intermediateView = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
    	SymbolMap symbolMap = createSymbolMap(group, virtualElements, child, metadata);
    	intermediateView.setProperty(NodeConstants.Info.SYMBOL_MAP, symbolMap);
    	child.addAsParent(intermediateView);
    	intermediateView.addGroup(group);
    	return intermediateView;
	}

	private static SymbolMap createSymbolMap(GroupSymbol group,
			List<? extends SingleElementSymbol> virtualElements,
			PlanNode child, QueryMetadataInterface metadata)
			throws TeiidComponentException, QueryMetadataException {
		TempMetadataStore store = new TempMetadataStore();
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, store);
        try {
			group.setMetadataID(ResolverUtil.addTempGroup(tma, group, virtualElements, false));
		} catch (QueryResolverException e) {
			throw new TeiidComponentException(e);
		}
    	List<ElementSymbol> projectedSymbols = ResolverUtil.resolveElementsInGroup(group, metadata);
    	SymbolMap symbolMap = SymbolMap.createSymbolMap(projectedSymbols, 
				(List<Expression>)NodeEditor.findNodePreOrder(child, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS));
		return symbolMap;
	}

	private void updateSymbolName(List<SingleElementSymbol> projectCols, int i,
			ElementSymbol virtualElem, SingleElementSymbol projectedSymbol) {
		if (projectedSymbol instanceof AliasSymbol) {
			((AliasSymbol)projectedSymbol).setName(virtualElem.getShortCanonicalName());
		} else {
			projectCols.set(i, new AliasSymbol(virtualElem.getShortCanonicalName(), projectedSymbol));
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
                               CapabilitiesFinder capFinder) throws TeiidComponentException,
                                                            QueryMetadataException, QueryPlannerException {

        Map<PlanNode, List<AggregateSymbol>> aggregateMap = createNodeMapping(groupNode, allAggregates, true);
        if (aggregateMap == null) {
        	return;
        }
        Map<PlanNode, List<SingleElementSymbol>> groupingMap = createNodeMapping(groupNode, groupingExpressions, false);

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
            
            //perform a costing check, if there's not a significant reduction, then don't stage
            float cardinality = NewCalculateCostUtil.computeCostForTree(planNode, metadata);
            float ndv = NewCalculateCostUtil.getNDVEstimate(planNode, metadata, cardinality, stagedGroupingSymbols, false);
        	if (ndv != NewCalculateCostUtil.UNKNOWN_VALUE && cardinality / ndv < 4) {
    			continue;
        	}
            
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
                            && RuleRaiseAccess.canRaiseOverGroupBy(stageGroup, stageGroup.getFirstChild(), aggregates, metadata, capFinder, null)) {
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
		    AggregateSymbol count = new AggregateSymbol("stagedAgg", NonReserved.COUNT, false, null); //$NON-NLS-1$
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
                                 Collection<AggregateSymbol> aggregates) throws TeiidComponentException, QueryPlannerException {
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
                mapExpressions(groupNode.getParent(), aggMap, metadata);
                aggregates.clear();
                aggregates.addAll(newAggs);
            } catch (QueryResolverException err) {
                throw new TeiidComponentException(err);
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
                                                                       Collection<T> expressions, boolean aggs) {
        Map<PlanNode, List<T>> result = new HashMap<PlanNode, List<T>>();
        if (expressions == null) {
            return result;
        }
        for (T aggregateSymbol : expressions) {
        	if (aggs && ((AggregateSymbol)aggregateSymbol).getExpression() == null) {
        		return null; //count(*) is not yet handled.  a general approach would be count(*) => count(r.col) * count(l.col), but the logic here assumes a simpler initial mapping
        	}
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(aggregateSymbol);
            if (groups.isEmpty()) {
            	continue;
            }
            PlanNode originatingNode = FrameUtil.findOriginatingNode(groupNode, groups);
            if (originatingNode == null) {
            	if (aggs) {
            		return null;  //should never happen
            	}
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
            	//anything logically applied after the join and is
            	//dependent upon the cardinality prevents us from optimizing.
            	if (aggs && ((AggregateSymbol)aggregateSymbol).isCardinalityDependent()) {
            		return null;
            	}
                continue;
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

    private static Map<AggregateSymbol, Expression> buildAggregateMap(Collection<? extends SingleElementSymbol> aggregateExpressions,
                                                                        QueryMetadataInterface metadata, Set<AggregateSymbol> nestedAggregates) throws QueryResolverException,
                                                                                                        TeiidComponentException {
        Map<AggregateSymbol, Expression> aggMap = new HashMap<AggregateSymbol, Expression>();
        for (SingleElementSymbol symbol : aggregateExpressions) {
            AggregateSymbol partitionAgg = (AggregateSymbol)symbol;
           
            Expression newExpression = null;

            Type aggFunction = partitionAgg.getAggregateFunction();
            if (aggFunction == Type.COUNT) {
                //COUNT(x) -> CONVERT(SUM(COUNT(x)), INTEGER)
                AggregateSymbol newAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, partitionAgg); //$NON-NLS-1$

                // Build conversion function to convert SUM (which returns LONG) back to INTEGER
                Function convertFunc = new Function(FunctionLibrary.CONVERT, new Expression[] {newAgg, new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()))});
                ResolverVisitor.resolveLanguageObject(convertFunc, metadata);

                newExpression = convertFunc;  
                nestedAggregates.add(partitionAgg);
            } else if (aggFunction == Type.AVG) {
                //AVG(x) -> SUM(SUM(x)) / SUM(COUNT(x))
                AggregateSymbol countAgg = new AggregateSymbol("stagedAgg", NonReserved.COUNT, false, partitionAgg.getExpression()); //$NON-NLS-1$
                AggregateSymbol sumAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, partitionAgg.getExpression()); //$NON-NLS-1$
                
                AggregateSymbol sumSumAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, sumAgg); //$NON-NLS-1$
                AggregateSymbol sumCountAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, countAgg); //$NON-NLS-1$

                Expression convertedSum = new Function(FunctionLibrary.CONVERT, new Expression[] {sumSumAgg, new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()))});
                Expression convertCount = new Function(FunctionLibrary.CONVERT, new Expression[] {sumCountAgg, new Constant(DataTypeManager.getDataTypeName(partitionAgg.getType()))});
                
                Function divideFunc = new Function("/", new Expression[] {convertedSum, convertCount}); //$NON-NLS-1$
                ResolverVisitor.resolveLanguageObject(divideFunc, metadata);

                newExpression = divideFunc;
                nestedAggregates.add(countAgg);
                nestedAggregates.add(sumAgg);
            } else if (partitionAgg.isEnhancedNumeric()) {
            	//e.g. STDDEV_SAMP := CASE WHEN COUNT(X) > 1 THEN SQRT((SUM(X^2) - SUM(X)^2/COUNT(X))/(COUNT(X) - 1))
            	AggregateSymbol countAgg = new AggregateSymbol("stagedAgg", NonReserved.COUNT, false, partitionAgg.getExpression()); //$NON-NLS-1$
                AggregateSymbol sumAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, partitionAgg.getExpression()); //$NON-NLS-1$
                AggregateSymbol sumSqAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, new Function(SourceSystemFunctions.POWER, new Expression[] {partitionAgg.getExpression(), new Constant(2)})); //$NON-NLS-1$
                
                AggregateSymbol sumSumAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, sumAgg); //$NON-NLS-1$
                AggregateSymbol sumCountAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, countAgg); //$NON-NLS-1$
                AggregateSymbol sumSumSqAgg = new AggregateSymbol("stagedAgg", NonReserved.SUM, false, sumSqAgg); //$NON-NLS-1$
                
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
            } else if (aggFunction == Type.TEXTAGG) {
            	continue;
            }
            else {
                //AGG(X) -> AGG(AGG(X))
                newExpression = new AggregateSymbol("stagedAgg", aggFunction.name(), false, partitionAgg); //$NON-NLS-1$
                nestedAggregates.add(partitionAgg);
            }

            aggMap.put(partitionAgg, newExpression);
        }
        return aggMap;
    }
    
    static void mapExpressions(PlanNode node, Map<? extends Expression, ? extends Expression> exprMap, QueryMetadataInterface metadata) 
    throws QueryPlannerException {
        
        while (node != null) {
            FrameUtil.convertNode(node, null, null, exprMap, metadata, true);
            
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
