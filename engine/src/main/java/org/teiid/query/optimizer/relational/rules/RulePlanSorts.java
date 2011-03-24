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
import org.teiid.query.sql.symbol.SingleElementSymbol;
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
		return optimizeSorts(false, plan, plan);
	}

	private PlanNode optimizeSorts(boolean parentBlocking, PlanNode node, PlanNode root) {
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
			} else if (node.getParent() != null) {
			    //if we are not distinct and there is a limit, the projection could be deferred
				PlanNode parent = node.getParent();
				root = checkForProjectOptimization(node, root, parent);
			}
			List<SingleElementSymbol> orderColumns = ((OrderBy)node.getProperty(NodeConstants.Info.SORT_ORDER)).getSortKeys();
			PlanNode possibleSort = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.ACCESS);
			if (possibleSort != null) {
				List exprs = (List)possibleSort.getProperty(Info.GROUP_COLS);
				if (exprs != null && exprs.containsAll(orderColumns)) {
					exprs.removeAll(orderColumns);
					orderColumns.addAll(exprs);
					possibleSort.setProperty(Info.GROUP_COLS, orderColumns);
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
/*			possibleSort = NodeEditor.findNodePreOrder(node, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE | NodeConstants.Types.ACCESS);
			if (possibleSort == null) {
				break;
			}
			boolean left = false;
			if (possibleSort.getType() == NodeConstants.Types.JOIN) {
				if (possibleSort.getProperty(NodeConstants.Info.JOIN_STRATEGY) != JoinStrategyType.MERGE
					|| possibleSort.getProperty(NodeConstants.Info.JOIN_TYPE) != JoinType.JOIN_INNER) {
					break;
				} 
				if (FrameUtil.findJoinSourceNode(possibleSort.getFirstChild()).getGroups().containsAll(node.getGroups()) 
						&& possibleSort.getProperty(NodeConstants.Info.SORT_LEFT) == SortOption.SORT) {
					left = true;
				} else if (!FrameUtil.findJoinSourceNode(possibleSort.getLastChild()).getGroups().containsAll(node.getGroups()) 
						|| possibleSort.getProperty(NodeConstants.Info.SORT_RIGHT) != SortOption.SORT) {
					break;
				}
			}
			List exprs = (List)possibleSort.getProperty(left?Info.LEFT_EXPRESSIONS:Info.RIGHT_EXPRESSIONS);
			if (exprs != null && exprs.containsAll(orderColumns)) {
				List<Integer> indexes = new ArrayList<Integer>(orderColumns.size());
				for (Expression expr : (List<Expression>)orderColumns) {
					indexes.add(0, exprs.indexOf(expr));
				}
				exprs.removeAll(orderColumns);
				List newExprs = new ArrayList(orderColumns);
				newExprs.addAll(exprs);
				possibleSort.setProperty(left?Info.LEFT_EXPRESSIONS:Info.RIGHT_EXPRESSIONS, newExprs);
				if (node.getParent() == null) {
					root = node.getFirstChild();
					root.removeFromParent();
					node = root;
				} else {
					PlanNode nextNode = node.getFirstChild();
					NodeEditor.removeChildNode(node.getParent(), node);
					node = nextNode;
				}
				exprs = (List)possibleSort.getProperty(left?Info.RIGHT_EXPRESSIONS:Info.LEFT_EXPRESSIONS);
				List toRemove = new ArrayList();
				for (Integer index : indexes) {
					Object o = exprs.get(index);
					exprs.add(0, o);
					toRemove.add(o);
				}
				exprs.subList(indexes.size(), exprs.size()).removeAll(toRemove);
				possibleSort.setProperty(left?NodeConstants.Info.SORT_LEFT:NodeConstants.Info.SORT_RIGHT, SortOption.SORT_REQUIRED);
			}
*/
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
			root = optimizeSorts(parentBlocking, child, root);
		}
		return root;
	}

	private PlanNode checkForProjectOptimization(PlanNode node, PlanNode root,
			PlanNode parent) {
		if (parent.getType() != NodeConstants.Types.TUPLE_LIMIT) {
			return root;
		}
		PlanNode projectNode = node.getFirstChild();
		//if (child.getType() == NodeConstants.Types.ACCESS) {
			//TODO: there should be a cost based evaluation if this looks like ACCESS->PROJECT and we attempt to raise project expressions 
		//} 
		if (projectNode.getType() != NodeConstants.Types.PROJECT || projectNode.getFirstChild() == null) {
			return root;
		}
		List<SingleElementSymbol> childOutputCols = (List<SingleElementSymbol>) projectNode.getFirstChild().getProperty(Info.OUTPUT_COLS);
		OrderBy orderBy = (OrderBy) node.getProperty(Info.SORT_ORDER);
		List<SingleElementSymbol> orderByKeys = orderBy.getSortKeys();
		if (!childOutputCols.containsAll(orderByKeys)) {
			return root;
		}
		//move the project before the ordered limit
		NodeEditor.removeChildNode(node, projectNode);
		if (parent.getParent() != null) {
			parent.addAsParent(node);
		} else {
			root = projectNode;
			projectNode.addFirstChild(parent);
		}
		List<SingleElementSymbol> orderByOutputSymbols = (List<SingleElementSymbol>) node.getProperty(Info.OUTPUT_COLS);
		if (node.hasBooleanProperty(Info.UNRELATED_SORT)) {
			node.setProperty(Info.UNRELATED_SORT, false);
			//update sort order
			for (OrderByItem item : orderBy.getOrderByItems()) {
				int index = childOutputCols.indexOf(item.getSymbol());
				item.setExpressionPosition(index);
			}
		}
		projectNode.setProperty(Info.OUTPUT_COLS, orderByOutputSymbols);
		projectNode.setProperty(Info.PROJECT_COLS, orderByOutputSymbols);
		node.setProperty(Info.OUTPUT_COLS, childOutputCols);
		parent.setProperty(Info.OUTPUT_COLS, childOutputCols);
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
