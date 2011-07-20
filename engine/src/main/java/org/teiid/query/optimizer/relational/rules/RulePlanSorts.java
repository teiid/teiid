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
import java.util.List;

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
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


/**
 * Attempts to minimize the cost of sorting operations across the plan.
 * 
 * Must be run after output elements are assigned
 */
public class RulePlanSorts implements OptimizerRule {
	
	@Override
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder, RuleStack rules,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryPlannerException, QueryMetadataException,
			TeiidComponentException {
		return optimizeSorts(false, plan, plan, metadata, capabilitiesFinder, analysisRecord);
	}

	private PlanNode optimizeSorts(boolean parentBlocking, PlanNode node, PlanNode root, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
		node = NodeEditor.findNodePreOrder(node, 
				NodeConstants.Types.SORT 
				| NodeConstants.Types.DUP_REMOVE 
				| NodeConstants.Types.GROUP 
				| NodeConstants.Types.JOIN 
				| NodeConstants.Types.SET_OP, NodeConstants.Types.ACCESS);
		if (node == null) {
			return root;
		}
		switch (node.getType()) {
		case NodeConstants.Types.SORT:
			parentBlocking = true;
			if (node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
				break;
			}
			if (mergeSortWithDupRemoval(node)) {
				node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
			} else {
				root = checkForProjectOptimization(node, root, metadata, capFinder, record);
			}
			List<SingleElementSymbol> orderColumns = ((OrderBy)node.getProperty(NodeConstants.Info.SORT_ORDER)).getSortKeys();
			List<Expression> sortExpressions = new ArrayList<Expression>(orderColumns.size());
			PlanNode possibleSort = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.ACCESS);
			if (possibleSort != null) {
				boolean otherExpression = false;
				SymbolMap groupMap = (SymbolMap)possibleSort.getProperty(Info.SYMBOL_MAP);
				for (SingleElementSymbol singleElementSymbol : orderColumns) {
					Expression ex = SymbolMap.getExpression(singleElementSymbol);
					if (ex instanceof ElementSymbol) {
						sortExpressions.add(groupMap.getMappedExpression((ElementSymbol) ex));						
					} else {
						otherExpression = true;
						break;
					}
				}

				List<Expression> exprs = (List<Expression>)possibleSort.getProperty(Info.GROUP_COLS);
				if (!otherExpression && exprs != null && exprs.containsAll(sortExpressions)) {
					exprs.removeAll(sortExpressions);
					exprs.addAll(0, sortExpressions);
					if (node.getParent() == null) {
						root = node.getFirstChild();
						root.removeFromParent();
						node = root;
					} else {
						PlanNode nextNode = node.getFirstChild();
						NodeEditor.removeChildNode(node.getParent(), node);
						node = nextNode;
					}
				}
				break;
			} 
			break;
		case NodeConstants.Types.DUP_REMOVE:
			if (parentBlocking) {
				node.setType(NodeConstants.Types.SORT);
				node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
			} 
			break;
		case NodeConstants.Types.GROUP:
			if (!node.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
				break;
			}
			if (mergeSortWithDupRemovalAcrossSource(node)) {
				node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
			}
			//TODO: check the join interesting order
			parentBlocking = true;
			break;
		case NodeConstants.Types.JOIN:
			if (node.getProperty(NodeConstants.Info.JOIN_STRATEGY) == JoinStrategyType.NESTED_LOOP 
					|| node.getProperty(NodeConstants.Info.JOIN_STRATEGY) == JoinStrategyType.NESTED_TABLE) {
				break;
			}
			/*
			 *  Look under the left and the right sources for a dup removal operation
			 *  join
			 *   [project]
			 *     source
			 *       dup remove | union not all
			 */
			parentBlocking = true;
			PlanNode toTest = node.getFirstChild();
			if (mergeSortWithDupRemovalAcrossSource(toTest)) {
				node.setProperty(NodeConstants.Info.SORT_LEFT, SortOption.SORT_DISTINCT);
				if (node.getProperty(NodeConstants.Info.SORT_RIGHT) != SortOption.SORT) {
					node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
				}
			}
			toTest = node.getLastChild();
			if (mergeSortWithDupRemovalAcrossSource(toTest)) {
				node.setProperty(NodeConstants.Info.SORT_RIGHT, SortOption.SORT_DISTINCT);
				if (node.getProperty(NodeConstants.Info.SORT_LEFT) != SortOption.SORT) {
					node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
				}
			}
			break;
		case NodeConstants.Types.SET_OP:
			// assumes the use of the merge algorithm
			if (node.getProperty(NodeConstants.Info.SET_OPERATION) != SetQuery.Operation.UNION) {
				parentBlocking = true;
			} else if (!node.hasBooleanProperty(NodeConstants.Info.USE_ALL) && !parentBlocking) {
				node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
			}
			break;
		}
		for (PlanNode child : node.getChildren()) {
			root = optimizeSorts(parentBlocking, child, root, metadata, capFinder, record);
		}
		return root;
	}

	private PlanNode checkForProjectOptimization(PlanNode node, PlanNode root, 
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
		PlanNode projectNode = node.getFirstChild();
		PlanNode parent = node.getParent();
		boolean raiseAccess = false;
		//special check for unrelated order by compensation
		if (projectNode.getType() == NodeConstants.Types.ACCESS && RuleRaiseAccess.canRaiseOverSort(projectNode.getFirstChild(), metadata, capFinder, node, record, true)) {
			projectNode = NodeEditor.findNodePreOrder(projectNode, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
			if (projectNode == null) {
				return root; //shouldn't happen
			}
			raiseAccess = true;
		} else if (projectNode.getType() == NodeConstants.Types.PROJECT && projectNode.getFirstChild() != null) {
			raiseAccess = projectNode.getFirstChild().getType() == NodeConstants.Types.ACCESS 
				&& RuleRaiseAccess.canRaiseOverSort(projectNode.getFirstChild(), metadata, capFinder, node, record, false);
			
			//if we can't raise the access node and this doesn't have a limit, there's no point in optimizing
			if (!raiseAccess && (parent == null || parent.getType() != NodeConstants.Types.TUPLE_LIMIT)) {
				return root;
			}
		} else {
			return root;
		}
		List<SingleElementSymbol> childOutputCols = (List<SingleElementSymbol>) projectNode.getFirstChild().getProperty(Info.OUTPUT_COLS);
		OrderBy orderBy = (OrderBy) node.getProperty(Info.SORT_ORDER);
		List<SingleElementSymbol> orderByKeys = orderBy.getSortKeys();
		for (SingleElementSymbol ss : orderByKeys) {
			if(ss instanceof AliasSymbol) {
                ss = ((AliasSymbol)ss).getSymbol();
            }
            if (ss instanceof ExpressionSymbol && !(ss instanceof AggregateSymbol)) {
                return root; //TODO: insert a new project node to handle this case
            }
			if (!childOutputCols.contains(ss)) {
				return root;
			}
		}
		NodeEditor.removeChildNode(projectNode.getParent(), projectNode);
		if (parent != null && parent.getType() == NodeConstants.Types.TUPLE_LIMIT && parent.getParent() != null) {
			parent.addAsParent(projectNode);
		} else {
			if (parent == null) {
				root = projectNode;
			}
			if (parent != null && parent.getType() == NodeConstants.Types.TUPLE_LIMIT) {
				if (root == parent) {
					root = projectNode; 
				}
				projectNode.addFirstChild(parent);
			} else {
				projectNode.addFirstChild(node);
			}
		}
		List<SingleElementSymbol> orderByOutputSymbols = (List<SingleElementSymbol>) node.getProperty(Info.OUTPUT_COLS);
		boolean unrelated = false;
		if (node.hasBooleanProperty(Info.UNRELATED_SORT)) {
			node.setProperty(Info.UNRELATED_SORT, false);
			unrelated = true;
		}
		for (OrderByItem item : orderBy.getOrderByItems()) {
			if (unrelated) {
			    //update sort order
				int index = childOutputCols.indexOf(item.getSymbol());
				item.setExpressionPosition(index);
			}
			//strip alias as project was raised
			if (item.getSymbol() instanceof AliasSymbol) {
				item.setSymbol(((AliasSymbol)item.getSymbol()).getSymbol());
			}
		}
		projectNode.setProperty(Info.OUTPUT_COLS, orderByOutputSymbols);
		projectNode.setProperty(Info.PROJECT_COLS, orderByOutputSymbols);
		node.setProperty(Info.OUTPUT_COLS, childOutputCols);
		if (parent != null) {
			parent.setProperty(Info.OUTPUT_COLS, childOutputCols);
		}
		if (raiseAccess) {
			PlanNode accessNode = node.getFirstChild();
			//instead of just calling ruleraiseaccess, we're more selective
			//we do not want to raise the access node over a project that is handling an unrelated sort
			PlanNode newRoot = RuleRaiseAccess.raiseAccessNode(root, accessNode, metadata, capFinder, true, record);
			if (newRoot != null) {
				root = newRoot;
				if (accessNode.getParent().getType() == NodeConstants.Types.TUPLE_LIMIT) {
					newRoot = RulePushLimit.raiseAccessOverLimit(root, accessNode, metadata, capFinder, accessNode.getParent());
				}
				if (newRoot != null) {
					root = newRoot;
				}
			}
		}
		return root;
	}

	private boolean mergeSortWithDupRemovalAcrossSource(PlanNode toTest) {
		PlanNode source = NodeEditor.findNodePreOrder(toTest, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS | NodeConstants.Types.JOIN);
		return source != null && mergeSortWithDupRemoval(source);
	}

	private boolean mergeSortWithDupRemoval(PlanNode node) {
		if (node.getFirstChild() == null) {
			return false;
		}
		switch (node.getFirstChild().getType()) {
		case NodeConstants.Types.SET_OP:
			if (node.getFirstChild().getProperty(NodeConstants.Info.SET_OPERATION) == SetQuery.Operation.UNION && !node.getFirstChild().hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
				node.getFirstChild().setProperty(NodeConstants.Info.USE_ALL, true);
				return true;
			}
			break;
		case NodeConstants.Types.DUP_REMOVE:
			NodeEditor.removeChildNode(node, node.getFirstChild());
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return "PlanSorts"; //$NON-NLS-1$
	}

}
