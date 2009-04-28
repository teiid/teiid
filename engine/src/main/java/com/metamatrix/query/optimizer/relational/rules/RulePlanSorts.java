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

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.JoinStrategyType;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.relational.MergeJoinStrategy.SortOption;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.util.CommandContext;

/**
 * Attempts to minimize the cost of sorting operations across the plan.
 */
public class RulePlanSorts implements OptimizerRule {
	
	@Override
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder, RuleStack rules,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryPlannerException, QueryMetadataException,
			MetaMatrixComponentException {
		optimizeSorts(false, plan);
		return plan;
	}

	private void optimizeSorts(boolean parentBlocking, PlanNode node) {
		node = NodeEditor.findNodePreOrder(node, 
				NodeConstants.Types.SORT 
				| NodeConstants.Types.DUP_REMOVE 
				| NodeConstants.Types.GROUP 
				| NodeConstants.Types.JOIN 
				| NodeConstants.Types.SET_OP, NodeConstants.Types.ACCESS);
		if (node == null) {
			return;
		}
		switch (node.getType()) {
		case NodeConstants.Types.SORT:
			parentBlocking = true;
			if (node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
				break;
			}
			if (mergeSortWithDupRemoval(node)) {
				node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
			}
			break;
		case NodeConstants.Types.DUP_REMOVE:
			if (parentBlocking) {
				node.setType(NodeConstants.Types.SORT);
				node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
			} 
			break;
		case NodeConstants.Types.GROUP:
			//TODO: check the join interesting order
			parentBlocking = true;
			break;
		case NodeConstants.Types.JOIN:
			if (node.getProperty(NodeConstants.Info.JOIN_STRATEGY) != JoinStrategyType.MERGE) {
				break;
			}
			parentBlocking = true;
			PlanNode toTest = node.getFirstChild();
			if (mergeSortWithDupRemovalForJoin(toTest)) {
				node.setProperty(NodeConstants.Info.SORT_LEFT, SortOption.SORT_DISTINCT);
			}
			toTest = node.getLastChild();
			if (mergeSortWithDupRemovalForJoin(toTest)) {
				node.setProperty(NodeConstants.Info.SORT_RIGHT, SortOption.SORT_DISTINCT);
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
			optimizeSorts(parentBlocking, child);
		}
	}

	/**
	 * Look under the left and the right sources for a dup removal operation
	 *  join
	 *   [project]
	 *     source
	 *       dup remove | union not all
	 */
	private boolean mergeSortWithDupRemovalForJoin(PlanNode toTest) {
		PlanNode source = NodeEditor.findNodePreOrder(toTest, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS | NodeConstants.Types.JOIN);
		return source != null && mergeSortWithDupRemoval(source);
	}

	private boolean mergeSortWithDupRemoval(PlanNode node) {
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
