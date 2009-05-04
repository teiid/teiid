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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.relational.JoinNode.JoinStrategyType;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.PredicateCriteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.LogConstants;


/** 
 * @since 4.3
 */
public class NewCalculateCostUtil {

    public static final float UNKNOWN_VALUE = -1;
    
    public static final int DEFAULT_STRONG_COST = 10;
    
    // These batch size should generally not be used as they should be retrieved from the 
    // command context, however they might be used in test scenarios where that is undefined
    static final int DEFAULT_PROCESSOR_BATCH_SIZE = 2000;
    static final int DEFAULT_CONNECTOR_BATCH_SIZE = 2000;
    
    // the following variables are used to hold cost estimates (roughly in milliseconds)
    private final static float compareTime = .05f; //TODO: a better estimate would be based upon the number of conjuncts
    private final static float readTime = .001f;
    private final static float procNewRequestTime = 100;
    private final static float procMoreRequestTime = 15;
    
    /**
     * Calculate cost of a node and all children, recursively from the bottom up.
     * @param node
     * @param metadata
     * @return Cost computed at the passed node
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    static float computeCostForTree(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        recursiveComputeCost(node, metadata);
        
        Float cost = (Float) node.getProperty(NodeConstants.Info.EST_CARDINALITY);
        if(cost != null) {
            return cost.floatValue();
        } 
        
        return UNKNOWN_VALUE;
    }
   
    /**
     * This method recursively estimates, from the bottom up, the cost of each node in
     * the plan subtree.  If a cost can't be estimated for any node, the whole recursive
     * operation is aborted.
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    private static void recursiveComputeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        // check if already computed
        if(node.getProperty(NodeConstants.Info.EST_CARDINALITY) != null) {
            return;
        }
        
        Iterator children = node.getChildren().iterator();
        while (children.hasNext()) {
            PlanNode child = (PlanNode)children.next();
            recursiveComputeCost(child, metadata);
        }
        computeNodeCost(node, metadata);
    }

    /**
     * This method attempts to estimate a cost for each type of node
     * that can be below an access node at this podouble in planning.
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    private static void computeNodeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        switch(node.getType()) {
            case NodeConstants.Types.SOURCE:
                estimateSourceNodeCost(node, metadata);
                break;

            case NodeConstants.Types.SELECT:
                estimateSelectNodeCost(node, metadata);
                break;
                           
            case NodeConstants.Types.JOIN:
                estimateJoinNodeCost(node, metadata);
                break;

            case NodeConstants.Types.DUP_REMOVE:
                estimateNodeCost(node, FrameUtil.findTopCols(node), metadata);
                break;

            case NodeConstants.Types.GROUP:
                estimateNodeCost(node, (List)node.getProperty(NodeConstants.Info.GROUP_COLS), metadata);
                break;
            case NodeConstants.Types.ACCESS:
            case NodeConstants.Types.SORT:
            {
                //Simply record the cost of the only child
                PlanNode child = node.getFirstChild();
                Float childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                setCardinalityEstimate(node, childCost);
                break;
            }
            case NodeConstants.Types.NULL:
                setCardinalityEstimate(node, new Float(0));
                break;

            case NodeConstants.Types.PROJECT:
            {
                PlanNode child = null;
                Float childCost = null;
                //Simply record the cost of the only child
                if (node.getChildCount() != 0) {
                    child = node.getFirstChild();
                    childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                } else {
                    childCost = new Float(1);
                }
                setCardinalityEstimate(node, childCost);
                break;
            }
            case NodeConstants.Types.SET_OP: 
            {
                float cost = 0;
                
                SetQuery.Operation op = (SetQuery.Operation)node.getProperty(NodeConstants.Info.SET_OPERATION);

                if (op == SetQuery.Operation.UNION) {
	                for (PlanNode childNode : node.getChildren()) {
	                    float childCost1 = (Float)childNode.getProperty(NodeConstants.Info.EST_CARDINALITY);
	                    if (childCost1 == UNKNOWN_VALUE) {
	                        cost = UNKNOWN_VALUE;
	                        break;
	                    } 
	                    // All rows will be projected so add both costs together.
	                    cost += childCost1;
	                }
	                cost = getDistinctEstimate(node, metadata, cost);
                } else {
                	float leftCost = (Float)node.getFirstChild().getProperty(NodeConstants.Info.EST_CARDINALITY);                	
                	leftCost = getDistinctEstimate(node.getFirstChild(), metadata, leftCost);
                    
                    float rightCost = (Float)node.getLastChild().getProperty(NodeConstants.Info.EST_CARDINALITY);
                    rightCost = getDistinctEstimate(node.getLastChild(), metadata, rightCost);
                    
                    cost = leftCost;
                    
                	if (op == SetQuery.Operation.EXCEPT) {
                        if (leftCost != UNKNOWN_VALUE && rightCost != UNKNOWN_VALUE) {
                        	cost = Math.max(1, leftCost - rightCost);
                        }                    		
                	} else {
                		if (rightCost != UNKNOWN_VALUE) {
                			if (leftCost != UNKNOWN_VALUE) {
                        		cost = Math.min(leftCost, rightCost);
                			} else {
                				cost = rightCost;
                			}
                		}
                	}
                }                
                
                setCardinalityEstimate(node, new Float(cost));
                break;
            }
            case NodeConstants.Types.TUPLE_LIMIT: 
            {
                PlanNode child = node.getFirstChild();
                Float childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                
                Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
                Float cost = childCost;
                
                if (childCost.floatValue() != UNKNOWN_VALUE && offset instanceof Constant) {
                    float offsetCost = childCost.floatValue() - ((Number)((Constant)offset).getValue()).floatValue();
                    cost = new Float((offsetCost < 0) ? 0 : offsetCost);
                }
                
                Expression limit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
                if (limit instanceof Constant) {
                    float limitCost = ((Number)((Constant)limit).getValue()).floatValue();
                    if (cost.floatValue() != UNKNOWN_VALUE) {
                        cost = new Float(Math.min(limitCost, cost.floatValue()));
                    } else {
                        cost = new Float(limitCost);
                    }
                } 
                setCardinalityEstimate(node, cost);
                break;
            }
        }
    }

	private static float getDistinctEstimate(PlanNode node,
			QueryMetadataInterface metadata, float cost)
			throws QueryMetadataException, MetaMatrixComponentException {
		if (!node.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
			PlanNode projectNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.PROJECT);
			if (projectNode != null) {
				cost = getDistinctEstimate(projectNode, (List)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS), metadata, cost);
			}
		}
		return cost;
	}

    private static void setCardinalityEstimate(PlanNode node, Float bestEstimate) {
        if (bestEstimate != null){
            node.setProperty(NodeConstants.Info.EST_CARDINALITY, bestEstimate);
        } else {
            node.setProperty(NodeConstants.Info.EST_CARDINALITY, new Float(UNKNOWN_VALUE));
        }
    }

    /**
     * Method estimateJoinNodeCost.
     * @param node
     * @param metadata
     */
    private static void estimateJoinNodeCost(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, MetaMatrixComponentException {

        Iterator children = node.getChildren().iterator();
        PlanNode child1 = (PlanNode)children.next();
        Float childCost1 = (Float)child1.getProperty(NodeConstants.Info.EST_CARDINALITY);
        PlanNode child2 = (PlanNode)children.next();
        Float childCost2 = (Float)child2.getProperty(NodeConstants.Info.EST_CARDINALITY);
        if (childCost1 != null && childCost2 != null && childCost1.floatValue() != UNKNOWN_VALUE && childCost2.floatValue() != UNKNOWN_VALUE){

            JoinType joinType = (JoinType)node.getProperty(NodeConstants.Info.JOIN_TYPE);
            List joinCriteria = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
            
            float baseCost = childCost1.floatValue() * childCost2.floatValue();
            if (joinCriteria != null && !joinCriteria.isEmpty()) {
                Criteria crit = Criteria.combineCriteria(joinCriteria);
                baseCost = recursiveEstimateCostOfCriteria(baseCost, node, crit, metadata);
            }
            
            Float cost = null;
            if (JoinType.JOIN_CROSS.equals(joinType)){
                cost = new Float(baseCost);
            } else if (JoinType.JOIN_FULL_OUTER.equals(joinType)) {
                cost = new Float(Math.max((childCost1.floatValue()+childCost2.floatValue()),baseCost));
            } else if (JoinType.JOIN_LEFT_OUTER.equals(joinType)) {
                cost = new Float(Math.max(childCost1.floatValue(),baseCost));
            } else if (JoinType.JOIN_RIGHT_OUTER.equals(joinType)) {
                cost = new Float(Math.max(childCost2.floatValue(),baseCost));
            } else if (JoinType.JOIN_INNER.equals(joinType)) {
                cost = new Float(baseCost);
            }
            
            setCardinalityEstimate(node, cost);
        } else {
            setCardinalityEstimate(node, null);
        }
    }

    /**
     * Estimate the cost of a selection.  This is not easy to do without information 
     * about the value count for each relation attribute.  
     * @param metadata
     */
    private static void estimateSelectNodeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {

        PlanNode child = node.getFirstChild();
        Float childCostFloat = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
        float childCost = UNKNOWN_VALUE;
        if (childCostFloat != null){
            childCost = childCostFloat.floatValue();    
        }
        
        //Get list of conjuncts
        Criteria selectCriteria = (Criteria)node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        float newCost = recursiveEstimateCostOfCriteria(childCost, node, selectCriteria, metadata);
        setCardinalityEstimate(node, new Float(newCost));
    }

    /**
     * For a source node, the cost is basically the cardinality of the source
     * (if it is known).
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    private static void estimateSourceNodeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        float cost = UNKNOWN_VALUE;
        if(node.getChildCount() > 0) {
            PlanNode child = node.getFirstChild();
            Float childCostFloat = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
            if (childCostFloat != null) {
                cost = childCostFloat.floatValue();
            }
        }else {
            GroupSymbol group = node.getGroups().iterator().next();
            float cardinality = metadata.getCardinality(group.getMetadataID());
            if (cardinality <= QueryMetadataInterface.UNKNOWN_CARDINALITY){
                cardinality = UNKNOWN_VALUE;
            }
            cost = cardinality;
        }
        
        setCardinalityEstimate(node, new Float(cost));
    } 
    
    /**
     * For a Group or Dup Removal node, the cost is basically the smaller of the largest NDV of the 
     * selected columns and cost of the child node (if it is known).
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    private static void estimateNodeCost(PlanNode node, List expressions, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        PlanNode child = node.getFirstChild();
        float childCost = ((Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY)).floatValue();
        
        if(childCost == UNKNOWN_VALUE) {
            setCardinalityEstimate(node, null);
            return;
        }

        Float newCost = getDistinctEstimate(node, expressions, metadata, childCost);
        setCardinalityEstimate(node, newCost);
    }

    private static Float getDistinctEstimate(PlanNode node,
                                             List elements,
                                             QueryMetadataInterface metadata,
                                             float childCost) throws QueryMetadataException,
                                                             MetaMatrixComponentException {
        if(elements == null) {
            return new Float(childCost);
        }
        HashSet elems = new HashSet();
        for(Iterator iter = elements.iterator(); iter.hasNext();) {
            Expression expression = (Expression)iter.next();
            ElementCollectorVisitor.getElements(expression, elems);
        }
        elements = new ArrayList(elems);
        
        float ndvCost = getMaxNDV(elements, node, childCost, metadata);
        if(ndvCost == UNKNOWN_VALUE) {
            ndvCost = childCost;
        }
        
        Float newCost = new Float(Math.min(childCost, ndvCost));
        return newCost;
    }
    
    static float recursiveEstimateCostOfCriteria(float childCost, PlanNode currentNode, Criteria crit, QueryMetadataInterface metadata)
        throws QueryMetadataException, MetaMatrixComponentException {
        
        float cost = childCost; 
        if(crit instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) crit;
            if (compCrit.getOperator() == CompoundCriteria.OR) {
                cost = 0;
            } else { //check for composite key
                HashSet elements = new HashSet();
                Iterator crits = compCrit.getCriteria().iterator();
                while(crits.hasNext()) { 
                    Criteria aCrit =  (Criteria) crits.next();
                    collectElementsOfValidCriteria(aCrit, elements);
                }
                if (usesKey(elements, metadata)) {
                    return 1;
                }
            }

            Iterator iter = compCrit.getCriteria().iterator();
            while(iter.hasNext()) { 
                float nextCost = recursiveEstimateCostOfCriteria(childCost, currentNode, (Criteria) iter.next(), metadata);
                
                if(compCrit.getOperator() == CompoundCriteria.AND) {
                    if (nextCost == UNKNOWN_VALUE) {
                        continue;
                    }
                    if (childCost != UNKNOWN_VALUE) {
                        cost *= nextCost/childCost;
                    } else {
                        if (cost == UNKNOWN_VALUE) {
                            cost = nextCost;
                        } else {
                            cost = Math.min(cost, nextCost);
                        }
                    }
                    if (cost <= 1) {
                        return 1;
                    }
                } else {
                    if (nextCost == UNKNOWN_VALUE) {
                        return childCost;
                    } 
                    //this assumes that all disjuncts are completely disjoint
                    cost += nextCost;
                    if (childCost != UNKNOWN_VALUE) {
                        cost = Math.min(cost, childCost);
                    }
                }
            }
            if (cost == UNKNOWN_VALUE) {
                return childCost;
            }
        } else if(crit instanceof NotCriteria){
            if (childCost == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            float nextCost = recursiveEstimateCostOfCriteria(childCost, currentNode, ((NotCriteria)crit).getCriteria(), metadata);
            if (nextCost == UNKNOWN_VALUE){
                return childCost;
            }   
            cost -= nextCost;
        } else {
            cost = estimatePredicateCost(childCost, currentNode, (PredicateCriteria) crit, metadata);
            
            if (cost == UNKNOWN_VALUE) {
                return childCost;
            }
        }  
        
        cost = Math.max(cost, 1);
        
        return cost;
    }
    
    /** 
     * This method is a helper to examine whether a compound criteria covers
     * a compound key.  A "valid" criteria is 
     * 1) a predicate criteria
     *   1a) not negated
     *   1b) with an equality operator if it is a compare criteria
     * b) or a compound criteria containing valid criteria and an "AND" operator 
     * @param criteria
     * @param elements Collection to collect ElementSymbols in
     * @since 4.2
     */
    private static void collectElementsOfValidCriteria(Criteria criteria, Collection elements) {
       
        if(criteria instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) criteria;
            if(compCrit.getOperator() == CompoundCriteria.AND) {
                Iterator iter = compCrit.getCriteria().iterator();
                while(iter.hasNext()) { 
                    collectElementsOfValidCriteria((Criteria) iter.next(), elements);
                }
            }
        } else if(criteria instanceof CompareCriteria) {
            CompareCriteria compCrit = (CompareCriteria)criteria;
            if (compCrit.getOperator() == CompareCriteria.EQ){
                ElementCollectorVisitor.getElements(compCrit, elements);
            }                 
        } else if(criteria instanceof MatchCriteria) {
            MatchCriteria matchCriteria = (MatchCriteria)criteria;
            if (!matchCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(matchCriteria, elements);
            }

        } else if(criteria instanceof AbstractSetCriteria) {
            AbstractSetCriteria setCriteria = (AbstractSetCriteria)criteria;
            if (!setCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(setCriteria.getExpression(), elements);
            }

        } else if(criteria instanceof IsNullCriteria) {
            IsNullCriteria isNullCriteria = (IsNullCriteria)criteria;
            if (!isNullCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(isNullCriteria.getExpression(), elements);
            }
        }        
    }
    
    /** 
     * @param childCost
     * @param predicateCriteria
     * @param metadata
     * @return
     * @since 4.3
     */
    private static float estimatePredicateCost(float childCost, PlanNode currentNode, PredicateCriteria predicateCriteria, QueryMetadataInterface metadata)
        throws QueryMetadataException, MetaMatrixComponentException {
        
        HashSet elements = new HashSet();
        ElementCollectorVisitor.getElements(predicateCriteria, elements);
        
        Collection groups = GroupsUsedByElementsVisitor.getGroups(predicateCriteria);
        boolean multiGroup = groups.size() > 1;
        
        float cost = childCost;
        float ndv = getMaxNDV(elements, currentNode, childCost, metadata);
        
        boolean unknownChildCost = childCost == UNKNOWN_VALUE;
        boolean usesKey = usesKey(elements, metadata);

        if (childCost == UNKNOWN_VALUE) {
            childCost = 1;
        }

        if (ndv == UNKNOWN_VALUE) {
            ndv = 3;
            if (multiGroup) {
                if (usesKey) {
                    ndv = (float)Math.ceil(Math.sqrt(childCost));
                } else {
                    ndv = (float)Math.ceil(Math.sqrt(childCost)/4);
                }
                ndv = Math.max(ndv, 1);
            } else if (usesKey) {
                ndv = childCost;
            }
        } 
                
        boolean isNegatedPredicateCriteria = false;
        if(predicateCriteria instanceof CompareCriteria) {
            CompareCriteria compCrit = (CompareCriteria) predicateCriteria;
                        
            if (compCrit.getOperator() == CompareCriteria.EQ || compCrit.getOperator() == CompareCriteria.NE){
                if (unknownChildCost && (!usesKey || multiGroup)) {
                    return UNKNOWN_VALUE;
                }
                cost = childCost / ndv;
                if (compCrit.getOperator() == CompareCriteria.NE) {
                    isNegatedPredicateCriteria = true;
                }
            } else { //GE, LE, GT, LT
                cost = getCostForComparison(childCost, metadata, compCrit, unknownChildCost);
            }
        } else if(predicateCriteria instanceof MatchCriteria) {
            MatchCriteria matchCriteria = (MatchCriteria)predicateCriteria;
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = estimateMatchCost(childCost, ndv, matchCriteria);
            
            isNegatedPredicateCriteria = matchCriteria.isNegated();

        } else if(predicateCriteria instanceof SetCriteria) {
            SetCriteria setCriteria = (SetCriteria) predicateCriteria;
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = Math.min(childCost, childCost * setCriteria.getNumberOfValues() / ndv);
            
            isNegatedPredicateCriteria = setCriteria.isNegated();
            
        } else if(predicateCriteria instanceof SubquerySetCriteria) {
            SubquerySetCriteria setCriteria = (SubquerySetCriteria) predicateCriteria;
            
            // TODO - use inner ProcessorPlan cardinality estimates 
            // to determine the estimated number of values
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = childCost / 3;
            
            isNegatedPredicateCriteria = setCriteria.isNegated();

        } else if(predicateCriteria instanceof IsNullCriteria) {
            IsNullCriteria isNullCriteria = (IsNullCriteria)predicateCriteria;

            if(isNullable(elements, metadata)) {
                 
                float nnv = getNNV(elements, metadata);
                if (unknownChildCost) {
                    return nnv;
                } else if (nnv == UNKNOWN_VALUE) {
                    cost = childCost / ndv;
                } else {
                    cost = childCost * (nnv / Math.max(1, getCardinality(elements, metadata)));
                }
            } else {
                // No nulls allowed
                cost = 0;
            }
            
            isNegatedPredicateCriteria = isNullCriteria.isNegated();
        }

        if (cost == UNKNOWN_VALUE) {
            return UNKNOWN_VALUE;
        }
        
        if (cost > childCost) {
            cost = childCost;
        }
        
        if (isNegatedPredicateCriteria) {
            // estimate for NOT in the predicate
            cost = (cost != UNKNOWN_VALUE)
                    ? Math.max( childCost - cost, 1)
                    : UNKNOWN_VALUE;
        }
                
        return cost;
    }

    /** 
     * @param childCost
     * @param cost
     * @param ndv
     * @param matchExpression
     * @return
     */
    private static float estimateMatchCost(float childCost,
                                           float ndv,
                                           MatchCriteria criteria) {
        Expression matchExpression = criteria.getRightExpression();
        if(matchExpression instanceof Constant && ((Constant)matchExpression).getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
            String compareValue = (String) ((Constant)matchExpression).getValue();
            if(compareValue != null) {
                if(compareValue.indexOf('%') >= 0) {
                    if(compareValue.length() == 1) {
                        return childCost;
                    }
                } else {
                    return childCost / ndv;
                }
            }
        } else if (EvaluateExpressionVisitor.willBecomeConstant(criteria.getLeftExpression())) {
            return childCost / ndv;
        }
        return childCost / 3;
    }

    private static float getCostForComparison(float childCost,
                                              QueryMetadataInterface metadata,
                                              CompareCriteria compCrit, boolean unknownChildCost) throws MetaMatrixComponentException,
                                                                       QueryMetadataException {
        if (!(compCrit.getLeftExpression() instanceof ElementSymbol) || !(compCrit.getRightExpression() instanceof Constant)) {
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            return childCost/3;
        }
        ElementSymbol element = (ElementSymbol)compCrit.getLeftExpression();
        Class dataType = compCrit.getRightExpression().getType();
    
        String max = (String)metadata.getMaximumValue(element.getMetadataID());
        String min = (String)metadata.getMinimumValue(element.getMetadataID());
        if(max == null || min == null) {
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            return childCost/3;
        } 
        float cost = childCost;
        try{
            float maxValue = 0;
            float minValue = 0;

            Constant value = (Constant)compCrit.getRightExpression();
            float compareValue = 0;
        	// Case 6257 - handling added for time and date.  If the max/min values are not
            // in the expected format, NumberFormatException is thrown and reverts to default costing.
            if(dataType.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                compareValue = ((Timestamp)value.getValue()).getTime();
                maxValue = Timestamp.valueOf(max).getTime();
                minValue = Timestamp.valueOf(min).getTime();
            } else if(dataType.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                compareValue = ((Time)value.getValue()).getTime();
                maxValue = Time.valueOf(max).getTime();
                minValue = Time.valueOf(min).getTime();
            // (For date, our costing sets the max and min values using timestamp format)
            } else if(dataType.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                compareValue = ((Date)value.getValue()).getTime();
                maxValue = Timestamp.valueOf(max).getTime();
                minValue = Timestamp.valueOf(min).getTime();
            } else {
            	if(!Number.class.isAssignableFrom(dataType)) {
                    if (unknownChildCost) {
                        return UNKNOWN_VALUE;
                    }
                    return childCost/3;
                }
                compareValue = ((Number)value.getValue()).floatValue();
                maxValue = Integer.parseInt(max);
                minValue = Integer.parseInt(min);
            }
            float range = Math.max(maxValue - minValue, 1);
            
            float costMultiple = 1;
            
            if(compCrit.getOperator() == CompareCriteria.GT || compCrit.getOperator() == CompareCriteria.GE) {
            	costMultiple = (maxValue - compareValue)/range;
                if (compareValue < 0 && maxValue < 0) {
                	costMultiple = (1 - costMultiple);
                }
            } else if(compCrit.getOperator() == CompareCriteria.LT || compCrit.getOperator() == CompareCriteria.LE) {
            	costMultiple = (compareValue - minValue)/range;
                if (compareValue < 0 && minValue < 0) {
                	costMultiple = (1 - costMultiple);
                }
            }
            if (costMultiple > 1) {
            	costMultiple = 1;
            } else if (costMultiple < 0) {
            	costMultiple = 0;
            }
            cost = childCost * costMultiple;
        }catch(IllegalArgumentException e) {
            LogManager.logWarning(LogConstants.CTX_QUERY_PLANNER, e, QueryPlugin.Util.getString("NewCalculateCostUtil.badCost")); //$NON-NLS-1$
            // If we were unable to parse the timestamp we will revert to the divide by three estimate
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = childCost/3;
        }
        return cost;
    }
    
    static boolean usesKey(Collection<SingleElementSymbol> allElements, QueryMetadataInterface metadata)
        throws QueryMetadataException, MetaMatrixComponentException {
    
        if(allElements == null || allElements.size() == 0) { 
            return false;
        }    
     
        // Sort elements into groups
        Map groupMap = new HashMap();
        Iterator elementIter = allElements.iterator();
        while(elementIter.hasNext()) { 
        	SingleElementSymbol ses = (SingleElementSymbol) elementIter.next();
        	if (!(ses instanceof ElementSymbol)) {
        		continue;
        	}
        	ElementSymbol element = (ElementSymbol)ses;
            GroupSymbol group = element.getGroupSymbol();
            List elements = (List) groupMap.get(group);
            if(elements == null) { 
                elements = new ArrayList();
                groupMap.put(group, elements);
            }
            elements.add(element.getMetadataID());
        }    
             
        // Walk through each group
        Iterator groupIter = groupMap.keySet().iterator();
        while(groupIter.hasNext()) { 
            GroupSymbol group = (GroupSymbol) groupIter.next();
            List elements = (List) groupMap.get(group);
            
            // Look up keys
            Collection keys = metadata.getUniqueKeysInGroup(group.getMetadataID());
            if(keys != null && keys.size() > 0) { 
                // For each key, get key elements
                Iterator keyIter = keys.iterator();
                while(keyIter.hasNext()) { 
                    List keyElements = metadata.getElementIDsInKey(keyIter.next());
                    if(elements.containsAll(keyElements)) {
                        // Used all elements of the key
                        return true;
                    }    
                }
            }                                    
        }
        
        return false;    
    }

    /** 
     * @param elements
     * @param metadata
     * @return
     * @since 4.3
     */
    private static float getCardinality(HashSet elements, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        if(elements.size() != 1) {
            return UNKNOWN_VALUE;
        }
        ElementSymbol elem = (ElementSymbol) elements.iterator().next();
        Object groupID = elem.getGroupSymbol().getMetadataID();
        return metadata.getCardinality(groupID);
    }

    private static float getMaxNDV(Collection elements, PlanNode currentNode, float estNodeCardinality, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        if(elements.isEmpty()) {
            return UNKNOWN_VALUE;
        }

        float ndv = UNKNOWN_VALUE;
        
        Iterator elementIterator = elements.iterator();
        while(elementIterator.hasNext()) {
            ElementSymbol elem = (ElementSymbol) elementIterator.next();
            ndv = Math.max(ndv, estimateNDVForSymbol(elem, currentNode, estNodeCardinality, metadata));
            if(ndv == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
        }
                
        return ndv;
    }
    
    /** 
     * @param elements
     * @return
     * @since 4.3
     */
    private static boolean isNullable(HashSet elements, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        if(elements.size() != 1) {
            return true;
        }
        ElementSymbol elem = (ElementSymbol) elements.iterator().next();
        Object elemID = elem.getMetadataID();
        return metadata.elementSupports(elemID, SupportConstants.Element.NULL) || 
            metadata.elementSupports(elemID, SupportConstants.Element.NULL_UNKNOWN);  
    }

    /** 
     * @param elements
     * @return
     * @since 4.3
     */
    private static float getNNV(HashSet elements, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        if(elements.size() != 1) {
            return UNKNOWN_VALUE;
        }
        ElementSymbol elem = (ElementSymbol) elements.iterator().next();
        Object elemID = elem.getMetadataID();
        return metadata.getNullValues(elemID);
    }
    
    /**
     * Computes the cost of a Merge Join
     */
    public static float computeCostForJoin(PlanNode leftChildNode, PlanNode rightChildNode, JoinStrategyType joinStrategy, QueryMetadataInterface metadata, CommandContext context) 
        throws MetaMatrixComponentException, QueryMetadataException {
        
        float leftChildCardinality = computeCostForTree(leftChildNode, metadata);
        float rightChildCardinality = computeCostForTree(rightChildNode, metadata);
        
        boolean merge = JoinStrategyType.MERGE.equals(joinStrategy);
                
        // If either cardinality is unknown, we return unknown
        if(leftChildCardinality == UNKNOWN_VALUE || rightChildCardinality == UNKNOWN_VALUE) {
            return UNKNOWN_VALUE;
        }
        
        float numberComparisons = merge?(leftChildCardinality + rightChildCardinality):(leftChildCardinality * rightChildCardinality);
        
        float connectorBatchSize = DEFAULT_CONNECTOR_BATCH_SIZE;
        if(context != null) {
            connectorBatchSize = context.getConnectorBatchSize(); 
        }
        
        float totalReadTime = (leftChildCardinality + rightChildCardinality) * readTime;
        float totalCompareTime = numberComparisons * compareTime;
        float totalProcMoreRequestLeftTime = (float)Math.floor(leftChildCardinality/connectorBatchSize)*procMoreRequestTime;
        float totalProcMoreRequestRightTime = (float)Math.floor(rightChildCardinality/connectorBatchSize)*procMoreRequestTime;
        
        float cost = (totalReadTime+
                                           totalCompareTime+
                                           totalProcMoreRequestLeftTime+
                                           totalProcMoreRequestRightTime);
        
        if (merge) {
            cost += (leftChildCardinality*safeLog(leftChildCardinality) + rightChildCardinality*safeLog(rightChildCardinality)) * readTime;
        }
        
        if (isPhysicalSource(rightChildNode)) {
            cost += procNewRequestTime;
        }
        if (isPhysicalSource(leftChildNode)) {
            cost += procNewRequestTime;
        }
        return cost;
    }

    private static float safeLog(float x) {
        return (float)Math.max(1, Math.log(x));
    }
    
    /**
     * Computes the cost of a Dependent Join
     * 
     * The worst possible cost will arise from a high independent ndv (many dependent sets) and a low depenendent ndv (possibly many matches per set)
     * 
     * This logic uses the same assumption as criteria in that ndv is used as a divisor of cardinality. 
     * 
     */
    public static float computeCostForDepJoin(PlanNode joinNode, boolean leftIndependent, JoinStrategyType joinStrategy, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context) 
        throws MetaMatrixComponentException, QueryMetadataException {
        
        PlanNode independentNode = leftIndependent?joinNode.getFirstChild():joinNode.getLastChild();
        PlanNode dependentNode = leftIndependent?joinNode.getLastChild():joinNode.getFirstChild();
        
        List independentExpressions = (List)(leftIndependent?joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS));
        List dependentExpressions = (List)(leftIndependent?joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS));
        
        // compute the cost for both trees, if we don't come up with a valid cost for both we have to return unknown
        float independentCardinality = computeCostForTree(independentNode, metadata);
        float dependentCardinality = computeCostForTree(dependentNode, metadata);
        
        float indSymbolNDV = getNDV(independentNode, independentExpressions, metadata, independentCardinality, true);
        float depSymbolNDV = getNDV(dependentNode, dependentExpressions, metadata, dependentCardinality, false);
        
        //If either cardinality is unknown, we return unknown
        if(indSymbolNDV == UNKNOWN_VALUE || depSymbolNDV == UNKNOWN_VALUE || independentCardinality == UNKNOWN_VALUE || dependentCardinality == UNKNOWN_VALUE) {
            return UNKNOWN_VALUE;
        }

        float connectorBatchSize = DEFAULT_CONNECTOR_BATCH_SIZE;
        float processorBatchSize = DEFAULT_PROCESSOR_BATCH_SIZE;
        if(context != null) {
            connectorBatchSize = context.getConnectorBatchSize();
            processorBatchSize = context.getProcessorBatchSize();
        }
        
        float setCriteriaBatchSize = indSymbolNDV;
        
        PlanNode node = FrameUtil.findJoinSourceNode(dependentNode);
        
        while (node != null && node.getType() != NodeConstants.Types.ACCESS) {
            if (node.getType() == NodeConstants.Types.JOIN || node.getType() == NodeConstants.Types.SET_OP) {
                node = null;
                break;
            }
            node = node.getFirstChild();
        }
        
        if (node != null) {
            setCriteriaBatchSize = CapabilitiesUtil.getMaxInCriteriaSize(RuleRaiseAccess.getModelIDFromAccess(node, metadata), metadata, capFinder);
            if (setCriteriaBatchSize < 1) {
                setCriteriaBatchSize = indSymbolNDV;
            }
        } else {
            //don't bother making a virtual join dependent if they are likely to be large
            if (indSymbolNDV > Math.min(processorBatchSize, setCriteriaBatchSize)) {
                return UNKNOWN_VALUE;
            }
        }
        
        independentNode.setProperty(NodeConstants.Info.EST_SET_SIZE, new Float(indSymbolNDV));
        
        //for non-partitioned joins the cardinatlity of the dependentaccess should never be greater than the dependent cardinality
        //TODO: when partitioned joins are implemented, this logic will need updated
        float dependentAccessCardinality = Math.min(dependentCardinality, dependentCardinality * indSymbolNDV / depSymbolNDV);
        
        boolean merge = false;
        if (JoinStrategyType.MERGE.equals(joinStrategy)) {
            merge = true;
        } else if (!JoinStrategyType.NESTED_LOOP.equals(joinStrategy)) {
            return UNKNOWN_VALUE;
        }
        
        dependentNode.setProperty(NodeConstants.Info.EST_DEP_CARDINALITY, new Float(dependentAccessCardinality));
        
        float numberComparisons = merge?(independentCardinality + dependentAccessCardinality):(independentCardinality * dependentAccessCardinality);
        
        //account for sorting
        float totalLoadDataTime = independentCardinality * safeLog(independentCardinality) * readTime;
        if (merge) {
            totalLoadDataTime += dependentAccessCardinality * safeLog(dependentAccessCardinality) * readTime;
        }
        
        //the independentCardinality is doubled to account for the dependent setup time, which re-reads the independent values
        float totalReadTime = (2*independentCardinality + dependentAccessCardinality) * readTime;
        float totalCompareTime = numberComparisons * compareTime;
        float totalProcMoreRequestLeftTime = (float)Math.floor(independentCardinality / connectorBatchSize) * procMoreRequestTime;
        float newDependentQueries = (float)Math.ceil(indSymbolNDV / setCriteriaBatchSize);
        float totalProcMoreRequestRightTime = Math.max(dependentAccessCardinality / connectorBatchSize - newDependentQueries, 0) * procMoreRequestTime;
        
        float cost = (totalLoadDataTime +
                                           totalReadTime+
                                           totalCompareTime+
                                           totalProcMoreRequestLeftTime+
                                           totalProcMoreRequestRightTime);
       
        if (isPhysicalSource(independentNode)) {
            cost += procNewRequestTime; //independent query latency
        }
        /*estimate for dependent query latencies
         *NOTE: the initial latency estimate can be made significantly larger for queries against large dependent sets,
         *which is consistent with observed behavior in which criteria-less queries outperform those with in criteria
         *with 1000 entries.  
         */        
        if (isPhysicalSource(dependentNode)) {
            cost += newDependentQueries * (procNewRequestTime * 
                            Math.max(safeLog(dependentCardinality) - 10, 1) * 
                            Math.max(safeLog(Math.min(dependentCardinality, Math.min(setCriteriaBatchSize, indSymbolNDV))) - 2, 1)
                            ); 
        }
        
        return cost;
    }
    
    private static boolean isPhysicalSource(PlanNode node) {
        node = FrameUtil.findJoinSourceNode(node);
        if (node != null) {
            return node.getType() == NodeConstants.Types.ACCESS;
        }
        return false;
    }
    
    private static float getNDV(PlanNode node,
                                   List expressions,
                                   QueryMetadataInterface metadata,
                                   float nodeCardinality, boolean independent) throws QueryMetadataException,
                                                      MetaMatrixComponentException {
        float result = UNKNOWN_VALUE;
        for(Iterator iter = expressions.iterator(); iter.hasNext();) {
            Expression expr = (Expression)iter.next();
            Collection symbols = ElementCollectorVisitor.getElements(expr, true);
            
            float currentSymbolNDV = getMaxNDV(symbols, node, nodeCardinality, metadata);                
            
            if(currentSymbolNDV == UNKNOWN_VALUE) { 
                if (usesKey(symbols, metadata)) {
                    return nodeCardinality;
                } 
                if (independent) {
                    currentSymbolNDV = nodeCardinality / 2;
                } else {
                    currentSymbolNDV = nodeCardinality / 4;
                }
            }
            
            if(result == UNKNOWN_VALUE || currentSymbolNDV > result) {
                result = currentSymbolNDV;
            }
        }
        return Math.max(1, Math.min(nodeCardinality, result));
    }
    
    /** 
     * For virtual symbol, find a mapped physical symbol and look up it's NDV
     *  
     * @param currentDepSymbol The current virtual dependent symbol
     * @param dependentNode The dependent node starting point
     * @param metadata Metadata API
     * @return NDV for currentDepSymbol
     * @since 5.0.1
     */
    private static float getMappedPhysicalNDV(ElementSymbol currentDepSymbol,
                                            PlanNode dependentNode,
                                            QueryMetadataInterface metadata) throws MetaMatrixComponentException {
        
        PlanNode currentNode = dependentNode;
        
        while(true) {
            // Walk down to a source node
            PlanNode sourceNode = FrameUtil.findOriginatingNode(currentNode, dependentNode.getGroups());
            if(sourceNode == null) {
                break;
            }
            
            if (sourceNode.getType() == NodeConstants.Types.SOURCE) {
                SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
                
                if (symbolMap == null) {
                    return UNKNOWN_VALUE;
                }
                
                Expression currentExpr = symbolMap.getMappedExpression(currentDepSymbol);
                
                if (currentExpr == null) {
                    return UNKNOWN_VALUE;
                }
                
                if(! (currentExpr instanceof ElementSymbol)) {
                    // Not a 1-to-1 mapping - check for single contained element to use instead.  This
                    // is a bit of a hack as it will work just fine in some cases (like convert(elem, Double))
                    // and will suck badly in others that change the size of the value domain.  If there is 
                    // more than 1 element, we'll just give up for now - probably some heuristic we can use later.
                    Collection elements = ElementCollectorVisitor.getElements(currentExpr, false, false);
                    switch(elements.size()) {
                        case 0:
                        {
                            // Must be a constant, so use NDV=1
                            return 1;
                        }
                        case 1: 
                        {
                            // Just switch to using the current expression
                            currentDepSymbol = (ElementSymbol) elements.iterator().next();
                            break;
                        }
                        default:
                        {
                            // More than 1 element in lower expression - give up!
                            return UNKNOWN_VALUE;
                        }
    
                    }
                }
            }
            
            // Check for physical element and if so, break out of the loop
            try {
                if(! metadata.isVirtualGroup(currentDepSymbol.getGroupSymbol().getMetadataID())) {
                    return metadata.getDistinctValues(currentDepSymbol.getMetadataID());
                } 
                currentNode = currentNode.getFirstChild();
            } catch(QueryMetadataException e) {
                throw new MetaMatrixComponentException(e, e.getMessage());
            }
        }
        return UNKNOWN_VALUE;
    }

    /**
     * This method will return an estimate NDV of a symbol based on the cost of the input node and the ratio of
     * the input symbol's NDV value to the input symbol's Group's cardinality.
     */
    private static float estimateNDVForSymbol(ElementSymbol symbol, PlanNode currentNode, float estNodeCardinality, QueryMetadataInterface metadata) 
        throws MetaMatrixComponentException, QueryMetadataException {
        
        if(symbol == null) {
            return UNKNOWN_VALUE;
        }
        
        float ndv = 0;
        float tableCardinality = 0;
        Object elemID = symbol.getMetadataID();
        Object groupID = symbol.getGroupSymbol().getMetadataID();
        
        if (metadata.isVirtualGroup(groupID)) {
            return getMappedPhysicalNDV(symbol, currentNode, metadata);
        }
        
        if(elemID != null && groupID != null) {
            ndv = metadata.getDistinctValues(elemID);
            tableCardinality = metadata.getCardinality(groupID);
        }
        // if ndv is <= 0 the value is considered unknown and we return UNKNOWN_VALUE
        if(ndv <= 0) {
            return UNKNOWN_VALUE;
        }
        float estNDV = estNodeCardinality * ndv / tableCardinality;
        if(estNDV < ndv) {
            return Math.max(1, estNDV);
        }
        return Math.max(1, ndv);
    }
    
    static boolean isNodeStrong(PlanNode accessNode, QueryMetadataInterface metadata, CommandContext context) 
        throws QueryMetadataException, MetaMatrixComponentException {
    
        float result = computeCostForTree(accessNode, metadata);
        
        float strong_cost = DEFAULT_STRONG_COST;
        
        if(context != null) {
            strong_cost = Math.min(strong_cost, context.getProcessorBatchSize()); 
        }
        
        return result != UNKNOWN_VALUE && result <= strong_cost;
    }
    
}
