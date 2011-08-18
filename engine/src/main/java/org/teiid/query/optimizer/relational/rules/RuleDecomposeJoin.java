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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;

/**
 * Perform the optimization:<pre>
 *                  source
 * inner join         union all
 *   source             inner join
 *     union all  =>      source
 *       a                  a
 *       b                source
 *   source                 c
 *     union all        inner join
 *       c                source
 *       d                  b
 *                        source
 *                          d
 * </pre>
 * 
 * TODO: non-ansi joins
 */
public class RuleDecomposeJoin implements OptimizerRule {

	@Override
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder, RuleStack rules,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryPlannerException, QueryMetadataException,
			TeiidComponentException {
		
		for (PlanNode joinNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)) {
			plan = decomposeJoin(joinNode, plan, metadata, context);
		}
		
		return plan;
	}
	
	public PlanNode decomposeJoin(PlanNode joinNode, PlanNode root, QueryMetadataInterface metadata, CommandContext context) throws TeiidComponentException, QueryPlannerException {
		//TODO: should be done based upon join region to allow more than a 2-way non-ansi join
		JoinType joinType = (JoinType)joinNode.getProperty(Info.JOIN_TYPE);
		if (joinType != JoinType.JOIN_INNER) {
			return root;
		}

		PlanNode left = joinNode.getFirstChild();
		if (left.getType() != NodeConstants.Types.SOURCE) {
			return root;
		}
		
		Map<ElementSymbol, List<Set<Constant>>> partitionInfo = (Map<ElementSymbol, List<Set<Constant>>>)left.getProperty(Info.PARTITION_INFO);
		
		if (partitionInfo == null) {
			return root;
		}

		PlanNode unionNode = left.getFirstChild();
		if (unionNode.getType() != NodeConstants.Types.SET_OP) {
			return root;
		}
		
		PlanNode right = joinNode.getLastChild();
		
		if (right.getType() != NodeConstants.Types.SOURCE) {
			return root;
		}
		
		Map<ElementSymbol, List<Set<Constant>>> rightPartionInfo = (Map<ElementSymbol, List<Set<Constant>>>)right.getProperty(Info.PARTITION_INFO);
		
		if (rightPartionInfo == null) {
			return root;
		}
		
		List<Criteria> criteria = (List<Criteria>)joinNode.getProperty(Info.JOIN_CRITERIA);
		
		List<Expression> expr = new ArrayList<Expression>();
		List<Expression> exprOther = new ArrayList<Expression>();
		RuleChooseJoinStrategy.separateCriteria(unionNode.getParent().getGroups(), right.getGroups(), expr, exprOther, criteria, new LinkedList<Criteria>());
		
		if (expr.isEmpty()) {
			return root; //no equi-join
		}
		
		List<int[]> matches = findMatches(partitionInfo, rightPartionInfo, expr, exprOther);
		
		if (matches == null) {
			return root; //no non-overlapping partitions
		}

		int branchSize = partitionInfo.values().iterator().next().size();
		int otherBranchSize = rightPartionInfo.values().iterator().next().size();
		
		if (matches.isEmpty()) {
			//no matches mean that we can just insert a null node (false criteria) and be done with it
			PlanNode critNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
			critNode.setProperty(Info.SELECT_CRITERIA, QueryRewriter.FALSE_CRITERIA);
			unionNode.addAsParent(critNode);
			return root;
		}
		
		List<PlanNode> branches = new ArrayList<PlanNode>();
		//TODO: find union children from RulePushAggregates
		RulePushSelectCriteria.collectUnionChildren(unionNode, branches);
		
		if (branches.size() != branchSize) {
			return root; //sanity check 
		}
		
		List<PlanNode> otherBranches = new ArrayList<PlanNode>();
		RulePushSelectCriteria.collectUnionChildren(right.getFirstChild(), otherBranches);
		
		if (otherBranches.size() != otherBranchSize) {
			return root; //sanity check 
		}

		PlanNode newUnion = buildUnion(unionNode, right, criteria, matches, branches, otherBranches);
		PlanNode view = rebuild(left.getGroups().iterator().next(), joinNode, newUnion, metadata, context, left, right);

		SymbolMap symbolmap = (SymbolMap)view.getProperty(Info.SYMBOL_MAP);
		HashMap<ElementSymbol, List<Set<Constant>>> newPartitionInfo = new LinkedHashMap<ElementSymbol, List<Set<Constant>>>();
		for (int[] match : matches) {
			updatePartitionInfo(partitionInfo, matches, symbolmap, newPartitionInfo, 0, match[0]);
			updatePartitionInfo(rightPartionInfo, matches, symbolmap, newPartitionInfo, partitionInfo.size(), match[1]);
		}
		view.setProperty(Info.PARTITION_INFO, newPartitionInfo);
	
		//since we've created a new union node, there's a chance we can decompose again
		return decomposeJoin(newUnion, root, metadata, context);
	}

	private void updatePartitionInfo(
			Map<ElementSymbol, List<Set<Constant>>> partitionInfo,
			List<int[]> matches, SymbolMap symbolmap,
			HashMap<ElementSymbol, List<Set<Constant>>> newPartitionInfo, int start, int index) {
		for (Map.Entry<ElementSymbol, List<Set<Constant>>> entry : partitionInfo.entrySet()) {
			ElementSymbol newSymbol = symbolmap.getKeys().get(start++);
			List<Set<Constant>> values = newPartitionInfo.get(newSymbol);
			if (values == null) {
				values = new ArrayList<Set<Constant>>(matches.size());
				newPartitionInfo.put(newSymbol, values);
			}
			values.add(entry.getValue().get(index));
		}
	}

	/**
	 * Add the new union back in under a view 
	 */
	static PlanNode rebuild(GroupSymbol group, PlanNode toReplace, PlanNode newUnion, QueryMetadataInterface metadata, CommandContext context,
			PlanNode... toMap)
			throws TeiidComponentException, QueryPlannerException,
			QueryMetadataException {
		Set<String> groups = context.getGroups();
		
		group = RulePlaceAccess.recontextSymbol(group, groups);
		
		PlanNode projectNode = NodeEditor.findNodePreOrder(newUnion, NodeConstants.Types.PROJECT);
		List<? extends SingleElementSymbol> projectedSymbols = (List<? extends SingleElementSymbol>)projectNode.getProperty(Info.PROJECT_COLS);

    	SymbolMap newSymbolMap = RulePushAggregates.createSymbolMap(group, projectedSymbols, newUnion, metadata);
	    PlanNode view = RuleDecomposeJoin.createSource(group, newUnion, newSymbolMap);
		
		Map<Expression, ElementSymbol> inverseMap = newSymbolMap.inserseMapping();
		toReplace.getParent().replaceChild(toReplace, view);
		Set<GroupSymbol> newGroups = Collections.singleton(group);
		for (PlanNode node : toMap) {
			FrameUtil.convertFrame(view, node.getGroups().iterator().next(), newGroups, inverseMap, metadata);
		}
		
		return view;
	}

	/**
	 * Search each equi-join for partitioning
	 */
	private List<int[]> findMatches(
			Map<ElementSymbol, List<Set<Constant>>> partitionInfo,
			Map<ElementSymbol, List<Set<Constant>>> partitionInfoOther,
			List<Expression> expr, List<Expression> exprOther) {
		List<int[]> matches = null;
		for (int i = 0; i < expr.size() && matches == null; i++) {
			if (!(expr.get(i) instanceof ElementSymbol) || !(exprOther.get(i) instanceof ElementSymbol)) {
				continue;
			}
			ElementSymbol es = (ElementSymbol)expr.get(i);
			ElementSymbol esOther = (ElementSymbol)exprOther.get(i);
			List<Set<Constant>> partLists = partitionInfo.get(es);
			List<Set<Constant>> partListsOther = partitionInfoOther.get(esOther);
			if (partLists == null || partListsOther == null) {
				continue;
			}
			matches = findMatches(partLists, partListsOther);
		}
		return matches;
	}
	
	/**
	 * Find overlaps in the given partition lists
	 */
	private List<int[]> findMatches(List<Set<Constant>> partLists,
			List<Set<Constant>> partListsOther) {
		List<int[]> matches = new LinkedList<int[]>();
		for (int j = 0; j < partLists.size(); j++) {
			int[] match = null;
			Set<Constant> vals = partLists.get(j);
			for (int k = 0; k < partListsOther.size(); k++) {
				if (!Collections.disjoint(vals, partListsOther.get(k))) {
					if (match == null) {
						match = new int[] {j, k};
					} else {
						//TODO: we currently do handle a situation where multiple 
						//partitions overlap.
						return null;
					}
				}
			}
			if (match != null) {
				matches.add(match);
			}
		}
		return matches;
	}

	private PlanNode buildUnion(PlanNode unionNode, PlanNode otherSide,
			List<Criteria> criteria, List<int[]> matches,
			List<PlanNode> branches, List<PlanNode> otherBranches) {
		SymbolMap symbolMap = (SymbolMap)unionNode.getParent().getProperty(Info.SYMBOL_MAP);
		SymbolMap otherSymbolMap = (SymbolMap)otherSide.getProperty(Info.SYMBOL_MAP);

		List<PlanNode> joins = new LinkedList<PlanNode>();
		for (int i = 0; i < matches.size(); i++) {
			int[] is = matches.get(i);
			PlanNode branch = branches.get(is[0]);
			PlanNode branchSource = createSource(unionNode.getParent().getGroups().iterator().next(), branch, symbolMap);
			
			PlanNode otherBranch = otherBranches.get(is[1]);
			PlanNode otherBranchSource = createSource(otherSide.getGroups().iterator().next(), otherBranch, otherSymbolMap);
			
			PlanNode newJoinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
			newJoinNode.addLastChild(branchSource);
			newJoinNode.addLastChild(otherBranchSource);
			
			newJoinNode.setProperty(Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
			newJoinNode.setProperty(Info.JOIN_TYPE, JoinType.JOIN_INNER);
			newJoinNode.setProperty(Info.JOIN_CRITERIA, LanguageObject.Util.deepClone(criteria, Criteria.class));
			newJoinNode.addGroups(branchSource.getGroups());
			newJoinNode.addGroups(otherBranchSource.getGroups());
			
			PlanNode projectPlanNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
			newJoinNode.addAsParent(projectPlanNode);
			
			Select allSymbols = new Select(symbolMap.getKeys());
			allSymbols.addSymbols(otherSymbolMap.getKeys());
			if (i == 0) {
				QueryRewriter.makeSelectUnique(allSymbols, false);
			}
			projectPlanNode.setProperty(NodeConstants.Info.PROJECT_COLS, allSymbols.getSymbols());
	        projectPlanNode.addGroups(newJoinNode.getGroups());
			
			joins.add(projectPlanNode);
		}
		
		PlanNode newUnion = RulePlanUnions.buildUnionTree(unionNode, joins);
		return newUnion;
	}

	static PlanNode createSource(GroupSymbol group, PlanNode unionNode, SymbolMap symbolMap) {
		PlanNode branchSource = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
		branchSource.addGroup(group);
		PlanNode projectNode = NodeEditor.findNodePreOrder(unionNode, NodeConstants.Types.PROJECT);
		branchSource.setProperty(Info.SYMBOL_MAP, SymbolMap.createSymbolMap(symbolMap.getKeys(), (List<? extends SingleElementSymbol>)projectNode.getProperty(Info.PROJECT_COLS)));
		unionNode.addAsParent(branchSource);
		return branchSource;
	}

	@Override
	public String toString() {
		return "DecomposeJoin"; //$NON-NLS-1$
	}

}
