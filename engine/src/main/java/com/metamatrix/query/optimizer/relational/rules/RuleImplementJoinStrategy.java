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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.relational.JoinNode.JoinStrategyType;
import com.metamatrix.query.processor.relational.MergeJoinStrategy.SortOption;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.CommandContext;

/**
 * Inserts sort nodes for specific join strategies.
 */
public class RuleImplementJoinStrategy implements OptimizerRule {
        
    /** 
     * @see com.metamatrix.query.optimizer.relational.OptimizerRule#execute(com.metamatrix.query.optimizer.relational.plantree.PlanNode, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder, com.metamatrix.query.optimizer.relational.RuleStack, com.metamatrix.query.analysis.AnalysisRecord, com.metamatrix.query.util.CommandContext)
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capabilitiesFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   MetaMatrixComponentException {

        for (PlanNode joinNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)) {
            JoinStrategyType stype = (JoinStrategyType) joinNode.getProperty(NodeConstants.Info.JOIN_STRATEGY);
            if (!JoinStrategyType.MERGE.equals(stype)) {
            	continue;
            } 
            
            /**
             * Don't push sorts for unbalanced inner joins, we prefer to use partitioning 
             */
            boolean pushLeft = true;
            boolean pushRight = true;
            if (joinNode.getProperty(NodeConstants.Info.JOIN_TYPE) == JoinType.JOIN_INNER && context != null) {
            	float leftCost = NewCalculateCostUtil.computeCostForTree(joinNode.getFirstChild(), metadata);
            	float rightCost = NewCalculateCostUtil.computeCostForTree(joinNode.getLastChild(), metadata);
            	boolean leftSmall = leftCost < context.getProcessorBatchSize() / 4;
            	boolean rightSmall = rightCost < context.getProcessorBatchSize() / 4;
            	boolean leftLarge = leftCost > context.getProcessorBatchSize();
            	boolean rightLarge = rightCost > context.getProcessorBatchSize();
            	if (leftLarge || rightLarge) {
	                pushLeft = leftCost == NewCalculateCostUtil.UNKNOWN_VALUE || leftSmall || rightLarge;
	                pushRight = rightCost == NewCalculateCostUtil.UNKNOWN_VALUE || rightSmall || leftLarge || joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE) != null;
            	}
            }            

            boolean pushedLeft = insertSort(joinNode.getFirstChild(), (List<SingleElementSymbol>) joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS), joinNode, metadata, capabilitiesFinder, pushLeft);	
            insertSort(joinNode.getLastChild(), (List<SingleElementSymbol>) joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS), joinNode, metadata, capabilitiesFinder, pushRight);
        	
        	if (joinNode.getProperty(NodeConstants.Info.JOIN_TYPE) == JoinType.JOIN_INNER && (!pushRight || !pushedLeft)) {
        		joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.PARTITIONED_SORT);
        	}
        }
        
        return plan;
    }

    /**
     * Insert a sort node under the merge join node.  If necessary, also insert a project
     * node to handle function evaluation.  
     * @param expressions The expressions that need to be sorted on 
     * @param jnode The planner merge join node to attach to
     * @return returns true if a project node needs added
     * @throws MetaMatrixComponentException 
     * @throws QueryMetadataException 
     */
    private static boolean insertSort(PlanNode childNode, List<SingleElementSymbol> expressions, PlanNode jnode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
    		boolean attemptPush) throws QueryMetadataException, MetaMatrixComponentException {
        Set<SingleElementSymbol> orderSymbols = new LinkedHashSet<SingleElementSymbol>(expressions); 

        PlanNode sourceNode = FrameUtil.findJoinSourceNode(childNode);
        PlanNode joinNode = childNode.getParent();

        Set<SingleElementSymbol> outputSymbols = new LinkedHashSet<SingleElementSymbol>((List<SingleElementSymbol>)childNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
        
        int oldSize = outputSymbols.size();
        
        outputSymbols.addAll(expressions);
        
        boolean needsCorrection = outputSymbols.size() > oldSize;
                
        List<Boolean> directions = Collections.nCopies(orderSymbols.size(), OrderBy.ASC);
        
        PlanNode sortNode = createSortNode(orderSymbols, outputSymbols, directions);
        
        if (sourceNode.getType() == NodeConstants.Types.ACCESS) {
        	if (NodeEditor.findAllNodes(sourceNode, NodeConstants.Types.SOURCE).size() == 1 
        			&& NewCalculateCostUtil.usesKey(expressions, metadata)) {
                joinNode.setProperty(joinNode.getFirstChild() == childNode ? NodeConstants.Info.IS_LEFT_DISTINCT : NodeConstants.Info.IS_RIGHT_DISTINCT, true);
        	}
	        if (attemptPush && RuleRaiseAccess.canRaiseOverSort(sourceNode, metadata, capFinder, sortNode)) {
	            sourceNode.getFirstChild().addAsParent(sortNode);
	            
	            if (needsCorrection) {
	                correctOutputElements(joinNode, outputSymbols, sortNode);
	            }
	            return true;
	        }
        }
        
        joinNode.setProperty(joinNode.getFirstChild() == childNode ? NodeConstants.Info.SORT_LEFT : NodeConstants.Info.SORT_RIGHT, SortOption.SORT);
        
        if (needsCorrection) {
            PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
            projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, new ArrayList(outputSymbols));
            childNode.addAsParent(projectNode);
            correctOutputElements(joinNode, outputSymbols, projectNode);
        }        
        return false;
    }

    private static PlanNode createSortNode(Collection orderSymbols,
                                           Collection outputElements,
                                           List directions) {
        PlanNode sortNode = NodeFactory.getNewNode(NodeConstants.Types.SORT);
        sortNode.setProperty(NodeConstants.Info.SORT_ORDER, new ArrayList(orderSymbols));
        sortNode.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList(outputElements));
        sortNode.setProperty(NodeConstants.Info.ORDER_TYPES, directions);
        return sortNode;
    }

    private static void correctOutputElements(PlanNode endNode,
                                              Collection outputElements,
                                              PlanNode startNode) {
        while (startNode != endNode) {
            startNode.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList(outputElements));
            startNode = startNode.getParent();
        }
    }
            
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "ImplementJoinStrategy"; //$NON-NLS-1$
    }

}
