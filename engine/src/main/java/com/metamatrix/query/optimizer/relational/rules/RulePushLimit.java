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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.util.CommandContext;

/** 
 * Pushes limit nodes to their lowest points.  This rule should only be run once.  Should be run after all access nodes have been raised
 */
public class RulePushLimit implements OptimizerRule {
    

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
        
        List<PlanNode> limitNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.TUPLE_LIMIT);
        
        boolean pushRaiseNull = false;
        
        while (!limitNodes.isEmpty()) {
            PlanNode limitNode = limitNodes.get(0);
            
            Expression limit = (Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
            
            if (limit instanceof Constant && new Integer(0).equals(((Constant)limit).getValue())) {
                PlanNode childProject = NodeEditor.findNodePreOrder(limitNode, NodeConstants.Types.PROJECT);
                
                if (childProject != null && childProject.getProperty(NodeConstants.Info.INTO_GROUP) == null) {
                    FrameUtil.replaceWithNullNode(limitNode.getFirstChild());
                    PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
                    projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, childProject.getProperty(NodeConstants.Info.PROJECT_COLS));
                    limitNode.getFirstChild().addAsParent(projectNode);
                    pushRaiseNull = true;
                    limitNodes.remove(limitNode);
                    continue;
                }
            }
            
            if (NodeEditor.findAllNodes(limitNode, NodeConstants.Types.ACCESS).isEmpty()) {
                limitNodes.remove(limitNode);
                continue;
            }
            
            while (canPushLimit(plan, limitNode, limitNodes, metadata, capabilitiesFinder)) {
                plan = RuleRaiseAccess.performRaise(plan, limitNode.getFirstChild(), limitNode);
            }
            
            limitNodes.remove(limitNode);
        }
        
        if (pushRaiseNull) {
            rules.push(RuleConstants.RAISE_NULL);
        }
        
        return plan;
    }
    
    boolean canPushLimit(PlanNode rootNode, PlanNode limitNode, List<PlanNode> limitNodes, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, MetaMatrixComponentException {
        PlanNode child = limitNode.getFirstChild();
        if (child == null || child.getChildCount() == 0) {
            return false;
        }
        
        switch (child.getType()) {
            case NodeConstants.Types.TUPLE_LIMIT:
            {
                //combine the limits
                Expression minLimit = getMinValue((Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT), (Expression)child.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT)); 
                Expression offSet = getSum((Expression)limitNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT), (Expression)child.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT)); 
                
                NodeEditor.removeChildNode(limitNode, child);
                
                limitNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, minLimit);
                limitNode.setProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT, offSet);
                
                limitNodes.remove(child);
                
                return canPushLimit(rootNode, limitNode, limitNodes, metadata, capFinder);
            }
            case NodeConstants.Types.SET_OP:
            {
                if (!SetQuery.Operation.UNION.equals(child.getProperty(NodeConstants.Info.SET_OPERATION))) {
                    return false;
                }                                
                //distribute the limit
                List<PlanNode> grandChildren = new LinkedList<PlanNode>(child.getChildren());
                for (PlanNode grandChild : grandChildren) {
                    PlanNode newLimit = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
                    Expression limit = (Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
                    Expression offset = (Expression)limitNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
                    newLimit.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, getSum(limit, offset));
                    grandChild.addAsParent(newLimit);
                    limitNodes.add(newLimit);
                }
                
                return false;
            }
            case NodeConstants.Types.ACCESS:
            {
                raiseAccessOverLimit(rootNode, child, metadata, capFinder, limitNode);
                return false;
            }
            case NodeConstants.Types.PROJECT:
            {
                return child.getProperty(NodeConstants.Info.INTO_GROUP) == null;
            }
            case NodeConstants.Types.SOURCE:
            case NodeConstants.Types.SELECT:
            case NodeConstants.Types.DUP_REMOVE:
            {
                return true;
            }
            default:
            {
                return false;
            }
        }
    }
    
    static PlanNode raiseAccessOverLimit(PlanNode rootNode,
                                          PlanNode accessNode,
                                          QueryMetadataInterface metadata,
                                          CapabilitiesFinder capFinder,
                                          PlanNode parentNode) throws QueryMetadataException,
                                                              MetaMatrixComponentException {
        Object modelID = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
        if (modelID == null) {
            return null;
        }
        
        List<PlanNode> setops = NodeEditor.findAllNodes(accessNode, NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
        
        if (!setops.isEmpty()) {
            return null;
        }
        
        Expression limit = (Expression)parentNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
        
        if (limit != null && !CapabilitiesUtil.supportsRowLimit(modelID, metadata, capFinder)) {
            return null;
        }
        
        Expression offset = (Expression)parentNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
        
        if (offset != null && !CapabilitiesUtil.supportsRowOffset(modelID, metadata, capFinder)) {
            
            if (limit != null) {
                parentNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, null);
                
                PlanNode pushedLimit = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
                
                // since we're pushing underneath the offset, we want enough rows to satisfy both the limit and the row offset
                
                pushedLimit.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, getSum(limit, offset)); 
                
                if (accessNode.getChildCount() == 0) {
                    accessNode.addFirstChild(pushedLimit);
                } else {
                    accessNode.getFirstChild().addAsParent(pushedLimit);
                }
            }
            
            return null;
        }
        
        return RuleRaiseAccess.performRaise(rootNode, accessNode, parentNode);
    }

    /**
     * @param limitNode
     * @param child
     */
    static Expression getSum(Expression expr1, Expression expr2) {
        if (expr1 == null) {
            return expr2;
        }
        if (expr2 == null) {
            return expr1;
        }
        
        Function newExpr = new Function("+", new Expression[] {expr1, expr2}); //$NON-NLS-1$
        newExpr.setFunctionDescriptor(FunctionLibraryManager.getFunctionLibrary().findFunction("+", new Class[] {DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER})); //$NON-NLS-1$
        newExpr.setType(newExpr.getFunctionDescriptor().getReturnType());
        return newExpr;
    }
    
    /**
     * @param limitNode
     * @param child
     */
    static Expression getMinValue(Expression expr1, Expression expr2) {
        if (expr1 == null) {
            return expr2;
        }
        if (expr2 == null) {
            return expr1;
        }
        
        Criteria crit = new CompareCriteria(expr1, CompareCriteria.LT, expr2);
        SearchedCaseExpression sce = new SearchedCaseExpression(Arrays.asList(new Object[] {crit}), Arrays.asList(new Object[] {expr1}));
        sce.setElseExpression(expr2);
        return sce;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PushLimit"; //$NON-NLS-1$
    }
}
