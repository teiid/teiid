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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;


/** 
 * Pushes limit nodes to their lowest points.  This rule should only be run once.  Should be run after all access nodes have been raised
 */
public class RulePushLimit implements OptimizerRule {
    

    /** 
     * @see org.teiid.query.optimizer.relational.OptimizerRule#execute(org.teiid.query.optimizer.relational.plantree.PlanNode, org.teiid.query.metadata.QueryMetadataInterface, org.teiid.query.optimizer.capabilities.CapabilitiesFinder, org.teiid.query.optimizer.relational.RuleStack, org.teiid.query.analysis.AnalysisRecord, org.teiid.query.util.CommandContext)
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capabilitiesFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {
        
        List<PlanNode> limitNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.ACCESS);
        
        boolean pushRaiseNull = false;
        
        while (!limitNodes.isEmpty()) {
            PlanNode limitNode = limitNodes.get(0);
            
            Expression limit = (Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
            
            if (limit instanceof Constant && new Integer(0).equals(((Constant)limit).getValue())) {
                PlanNode childProject = NodeEditor.findNodePreOrder(limitNode, NodeConstants.Types.PROJECT);
                
                if (childProject != null && childProject.getProperty(NodeConstants.Info.INTO_GROUP) == null) {
                	limitNodes.removeAll(NodeEditor.findAllNodes(limitNode.getFirstChild(), NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.ACCESS));
                    FrameUtil.replaceWithNullNode(limitNode.getFirstChild());
                    PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
                    projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, childProject.getProperty(NodeConstants.Info.PROJECT_COLS));
                    limitNode.getFirstChild().addAsParent(projectNode);
                    pushRaiseNull = true;
                    limitNodes.remove(limitNode);
                    continue;
                }
            }
            
            while (canPushLimit(plan, limitNode, limitNodes, metadata, capabilitiesFinder, analysisRecord, context)) {
                plan = RuleRaiseAccess.performRaise(plan, limitNode.getFirstChild(), limitNode);
                //makes this rule safe to run after the final rule assign output elements
                limitNode.setProperty(Info.OUTPUT_COLS, limitNode.getFirstChild().getProperty(Info.OUTPUT_COLS));
            }
            
            limitNodes.remove(limitNode);
            
            if (limitNode.hasBooleanProperty(Info.IS_COPIED)) {
            	limitNode.getParent().replaceChild(limitNode, limitNode.getFirstChild());
            }
        }
        
        if (pushRaiseNull) {
            rules.push(RuleConstants.RAISE_NULL);
        }
        
        return plan;
    }
    
    boolean canPushLimit(PlanNode rootNode, PlanNode limitNode, List<PlanNode> limitNodes, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record, CommandContext context) throws QueryMetadataException, TeiidComponentException {
        PlanNode child = limitNode.getFirstChild();
        if (child == null || child.getChildCount() == 0) {
            return false;
        }
        
        Expression parentLimit = (Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
		Expression parentOffset = (Expression)limitNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
		switch (child.getType()) {
            case NodeConstants.Types.TUPLE_LIMIT:
            {
            	
                //combine the limits
                Expression childLimit = (Expression)child.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
				Expression childOffset = (Expression)child.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
				
				combineLimits(limitNode, metadata, parentLimit, parentOffset, childLimit, childOffset);
                if (child.hasBooleanProperty(Info.IS_NON_STRICT)) {
                	limitNode.setProperty(Info.IS_NON_STRICT, true);
                }
                NodeEditor.removeChildNode(limitNode, child);
                limitNodes.remove(child);
                
                return canPushLimit(rootNode, limitNode, limitNodes, metadata, capFinder, record, context);
            }
            case NodeConstants.Types.SET_OP:
            {
                if (!canPushToBranches(limitNode, child)) {
                	return false;
                }
                //distribute the limit
                List<PlanNode> grandChildren = new LinkedList<PlanNode>(child.getChildren());
                for (PlanNode grandChild : grandChildren) {
                    addBranchLimit(limitNode, limitNodes, metadata,
							parentLimit, parentOffset, grandChild);
                }
                
                return false;
            }
            case NodeConstants.Types.JOIN:
            	JoinType joinType = (JoinType)child.getProperty(Info.JOIN_TYPE);
            	boolean pushLeft = false;
            	boolean pushRight = false;
            	if (joinType == JoinType.JOIN_CROSS) {
            		pushLeft = true;
            		pushRight = true;
            	} else if (joinType == JoinType.JOIN_LEFT_OUTER || joinType == JoinType.JOIN_FULL_OUTER) {
            		//we're allowed to do this based upon two conditions
            		//1 - we're not going to further change the join type/structure
            		//2 - outer results will be produced using the left product first
            		pushLeft = true;
            	} 
            	if (pushLeft && !FrameUtil.findJoinSourceNode(child.getLastChild()).hasProperty(NodeConstants.Info.CORRELATED_REFERENCES)) {
            		PlanNode newLimit = newLimit(limitNode);
                    newLimit.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, op(SourceSystemFunctions.ADD_OP, parentLimit, parentOffset, metadata.getFunctionLibrary()));
                    child.getFirstChild().addAsParent(newLimit);
                	limitNodes.add(newLimit);
            	}
            	if (pushRight) {
            		PlanNode newLimit = newLimit(limitNode);
                    newLimit.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, op(SourceSystemFunctions.ADD_OP, parentLimit, parentOffset, metadata.getFunctionLibrary()));
                    child.getLastChild().addAsParent(newLimit);
                	limitNodes.add(newLimit);
            	}
            	return false;
            case NodeConstants.Types.ACCESS:
            {
                raiseAccessOverLimit(rootNode, child, metadata, capFinder, limitNode, record);
                return false;
            }
            case NodeConstants.Types.PROJECT:
            {
                return child.getProperty(NodeConstants.Info.INTO_GROUP) == null && !child.hasProperty(Info.HAS_WINDOW_FUNCTIONS);
            }
            case NodeConstants.Types.SOURCE:
            {
                GroupSymbol virtualGroup = child.getGroups().iterator().next();
                if (virtualGroup.isProcedure()) {
                    return false;
                }
                if (FrameUtil.isProcedure(child.getFirstChild())) {
                    return false;
                }
                if (child.hasProperty(Info.TABLE_FUNCTION)) {
                	return false;
                }
                return true;
            }
            case NodeConstants.Types.SELECT:
            case NodeConstants.Types.DUP_REMOVE:
            	return limitNode.hasBooleanProperty(Info.IS_NON_STRICT);
            case NodeConstants.Types.SORT:
            	if (child.getFirstChild().getType() == NodeConstants.Types.SET_OP) {
            		PlanNode setOp = child.getFirstChild();
            		if (!canPushToBranches(limitNode, setOp)) {
            			return false;
            		}
            		OrderBy parentOrderBy = (OrderBy) child.getProperty(NodeConstants.Info.SORT_ORDER);
            		distributeLimit(limitNode, setOp, parentOrderBy, metadata, limitNodes, parentLimit, parentOffset, capFinder, context);
            	}
            	return false;
            default:
            {
                return false;
            }
        }
    }

	private void addBranchLimit(PlanNode limitNode, List<PlanNode> limitNodes,
			QueryMetadataInterface metadata, Expression parentLimit,
			Expression parentOffset, PlanNode grandChild) {
		PlanNode newLimit = newLimit(limitNode);
		newLimit.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, op(SourceSystemFunctions.ADD_OP, parentLimit, parentOffset, metadata.getFunctionLibrary()));
		grandChild.addAsParent(newLimit);
		limitNodes.add(newLimit);
		if (grandChild.getType() == NodeConstants.Types.SET_OP) {
			newLimit.setProperty(Info.IS_COPIED, true);
		}
	}

	/**
	 * Push the limit and order by to each union branch
	 * TODO: check if the top limit is smaller and implement sorted sublist processing, rather than performing a full resort
	 * @param context 
	 */
	private void distributeLimit(PlanNode limitNode, PlanNode setOp,
			OrderBy parentOrderBy, QueryMetadataInterface metadata, List<PlanNode> limitNodes, Expression parentLimit, Expression parentOffset, CapabilitiesFinder capFinder, CommandContext context) throws QueryMetadataException, TeiidComponentException {
		outer: for (PlanNode branch : setOp.getChildren()) {
			PlanNode branchSort = NodeEditor.findNodePreOrder(branch, NodeConstants.Types.SORT, NodeConstants.Types.SET_OP | NodeConstants.Types.SOURCE);
			if (branchSort != null) {
				//implies there is a limit
				OrderBy orderBy = (OrderBy) branchSort.getProperty(NodeConstants.Info.SORT_ORDER);
				//can only proceed if order by matches
				if (parentOrderBy.getOrderByItems().size() > orderBy.getOrderByItems().size()) {
					continue;
				}
				
				List<OrderByItem> parentkeys = parentOrderBy.getOrderByItems();
				List<OrderByItem> keys = orderBy.getOrderByItems();
				
				for (int i = 0; i < parentkeys.size(); i++) {
					int pos1 = parentkeys.get(i).getExpressionPosition();
					int pos2 = keys.get(i).getExpressionPosition();
					if (pos1 == -1 || pos2 == -1 || pos1 != pos2) {
						continue outer;
					}
				}
				addBranchLimit(limitNode, limitNodes, metadata, parentLimit, parentOffset, branch);
			} else {
				if (branch.getType() == NodeConstants.Types.SET_OP && canPushToBranches(limitNode, branch)) {
					//go to the children
					distributeLimit(limitNode, branch, parentOrderBy, metadata, limitNodes, parentLimit, parentOffset, capFinder, context);
					continue;
				}
				PlanNode newSort = NodeFactory.getNewNode(NodeConstants.Types.SORT);
				
				//push both the limit and order by
				
				List<OrderByItem> parentkeys = parentOrderBy.getOrderByItems();
				List<Expression> cols = (List<Expression>) NodeEditor.findNodePreOrder(branch, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS);
				
				OrderBy newOrderBy = new OrderBy();
				for (int i = 0; i < parentkeys.size(); i++) {
					OrderByItem item = parentkeys.get(i).clone();
					if (item.getExpressionPosition() == -1) {
						continue outer;
					}
					Expression ex = cols.get(item.getExpressionPosition());
					item.setSymbol((Expression) ex.clone());
					newOrderBy.getOrderByItems().add(item);
				}
				newSort.setProperty(Info.SORT_ORDER, newOrderBy);
				PlanNode childLimit = NodeEditor.findNodePreOrder(branch, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.SET_OP | NodeConstants.Types.SOURCE);
				if (childLimit != null) {
					PlanNode parentAccess = NodeEditor.findParent(childLimit, NodeConstants.Types.ACCESS, NodeConstants.Types.SET_OP);
					if (parentAccess != null) {
						//if there is a parent access, we need to handle pushing the sort
						boolean removedLimit = false;
						if (parentAccess.getFirstChild() == childLimit) {
							parentAccess.removeChild(childLimit);
							parentAccess.addFirstChild(childLimit.getFirstChild());
							removedLimit = true;
						}
						boolean canRaise = RuleRaiseAccess.canRaiseOverSort(parentAccess, metadata, capFinder, newSort, null, false, context);
						if (removedLimit) {
							childLimit.addFirstChild(parentAccess.getFirstChild());
							parentAccess.addFirstChild(childLimit);
						}
						if (canRaise) {
							//put under the limit
							//TODO: check to make sure that we're narrowing the limit
							//we won't know this in all cases since it could be parameterized
							childLimit.getFirstChild().addAsParent(newSort);	
						} else {
							continue outer;
							//TODO - once we support sorted sublist processing, then we'll want to push
							//put over the access node
							//parentAccess.addAsParent(newSort);
							//branch = newSort;
						}
					} else {
						continue outer;
						//TODO - once we support sorted sublist processing, then we'll want to push
						//branch.addAsParent(newSort);
						//branch = newSort;
					}
				} else {
					if (branch.getType() == NodeConstants.Types.ACCESS &&
							RuleRaiseAccess.canRaiseOverSort(branch, metadata, capFinder, newSort, null, false, context)) {
						branch.getFirstChild().addAsParent(newSort);
					} else {
						//TODO: if the limit is too large we shouldn't add it in here
						branch.addAsParent(newSort);
						branch = newSort;
					}
				}
				addBranchLimit(limitNode, limitNodes, metadata, parentLimit, parentOffset, branch);
			}
		}
	}

	private boolean canPushToBranches(PlanNode limitNode, PlanNode child) {
		if (!SetQuery.Operation.UNION.equals(child.getProperty(NodeConstants.Info.SET_OPERATION))) {
			return false;
		}   
		if (!child.hasBooleanProperty(NodeConstants.Info.USE_ALL) && !limitNode.hasBooleanProperty(Info.IS_NON_STRICT)) {
			return false;
		}
		return true;
	}

	private static PlanNode newLimit(PlanNode limitNode) {
		PlanNode newLimit = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
		if (limitNode.hasBooleanProperty(Info.IS_NON_STRICT)) {
			newLimit.setProperty(Info.IS_NON_STRICT, Boolean.TRUE);
		}
		return newLimit;
	}

	static void combineLimits(PlanNode limitNode,
			QueryMetadataInterface metadata, Expression parentLimit,
			Expression parentOffset, Expression childLimit,
			Expression childOffset) {
		Expression minLimit = null;
		Expression offSet = null;
		
		if (childLimit == null) {
			minLimit = parentLimit;
			offSet = op(SourceSystemFunctions.ADD_OP, childOffset, parentOffset, metadata.getFunctionLibrary());
		} else {
			minLimit = getMinValue(parentLimit, op(SourceSystemFunctions.SUBTRACT_OP, childLimit, parentOffset, metadata.getFunctionLibrary())); 
		    offSet = childOffset;
		    if (offSet == null) {
		    	offSet = parentOffset;
		    }
		}
		
		limitNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, minLimit);
		limitNode.setProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT, offSet);
	}
    
    static PlanNode raiseAccessOverLimit(PlanNode rootNode,
                                          PlanNode accessNode,
                                          QueryMetadataInterface metadata,
                                          CapabilitiesFinder capFinder,
                                          PlanNode parentNode, AnalysisRecord analysisRecord) throws QueryMetadataException,
                                                              TeiidComponentException {
        Object modelID = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
        if (modelID == null) {
            return null;
        }
        
        Expression limit = (Expression)parentNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
        
        if (limit != null && !CapabilitiesUtil.supportsRowLimit(modelID, metadata, capFinder)) {
        	parentNode.recordDebugAnnotation("limit not supported by source", modelID, "limit node not pushed", analysisRecord, metadata); //$NON-NLS-1$ //$NON-NLS-2$
            return null;
        }
        
        boolean multiSource = accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE);
        
        Expression offset = (Expression)parentNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
        
        if (multiSource || (offset != null && !CapabilitiesUtil.supportsRowOffset(modelID, metadata, capFinder))) {
            
            if (limit != null) {
            	if (!multiSource) {
            		parentNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, null);
            	}
                
                PlanNode pushedLimit = newLimit(parentNode);
                
                // since we're pushing underneath the offset, we want enough rows to satisfy both the limit and the row offset
                
                pushedLimit.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, op(SourceSystemFunctions.ADD_OP, limit, offset, metadata.getFunctionLibrary())); 
                
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

    static Expression op(String op, Expression expr1, Expression expr2, FunctionLibrary functionLibrary) {
        if (expr1 == null) {
            return expr2;
        }
        if (expr2 == null) {
            return expr1;
        }
        
        Function newExpr = new Function(op, new Expression[] {expr1, expr2});
        newExpr.setFunctionDescriptor(functionLibrary.findFunction(op, new Class[] {DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.INTEGER}));
        newExpr.setType(newExpr.getFunctionDescriptor().getReturnType());
        return evaluateIfPossible(newExpr);
    }

	private static Expression evaluateIfPossible(Expression newExpr) {
		if (EvaluatableVisitor.isFullyEvaluatable(newExpr, true)) {
        	try {
				return new Constant(Evaluator.evaluate(newExpr), newExpr.getType());
			} catch (TeiidException e) {
				 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30269, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30269));
			}
        }
        return newExpr;
	}
    
    /**
     * @param limitNode
     * @param child
     * @throws TeiidComponentException 
     * @throws BlockedException 
     * @throws ExpressionEvaluationException 
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
        sce.setType(expr1.getType());
        return evaluateIfPossible(sce);
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PushLimit"; //$NON-NLS-1$
    }
}
