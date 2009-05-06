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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.LogConstants;

/**
 * For each join node this rule will find the set of criteria allowed to influence the join (the join criteria, and inner
 * side criteria on non full outer joins) and builds new criteria based upon the equality relationships found.
 * 
 * Equality relationships look like element symbol = expression regardless of whether they are from select or join criteria
 * 
 * Upon successfully changing a multi group join criteria into another expression with fewer groups, the original criteria
 * will be replace with the new criteria in the on clause.
 *  
 * RulePushNonJoinCriteia and CopyCriteria will be run again after this rule if any new join criteria is created.
 * 
 * This rule is not allowed to run exhaustively by the setting of the copied property on criteria nodes.  It also will not
 * discover all possible relationships, only those that can be discovered quickly.
 */
public final class RuleCopyCriteria implements OptimizerRule {

	/**
	 * Execute the rule as described in the class comments.
	 * @param plan Incoming query plan, may be modified during method and may be returned from method
	 * @param metadata Metadata source
	 * @param rules Rules from optimizer rule stack, may be manipulated during method
	 * @return Updated query plan if rule fired, else original query plan
	 */
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context) 
		throws QueryPlannerException, MetaMatrixComponentException {

        //do an initial check to see if an execution will be necessary
	    List<PlanNode> critNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SELECT);
        boolean shouldRun = false;
        for (PlanNode critNode : critNodes) {
            if (!critNode.hasBooleanProperty(NodeConstants.Info.IS_COPIED)) {
                shouldRun = true;
                break;
            }
        }
     
        if (!shouldRun) {
            return plan;
        }
        
        if (tryToCopy(plan, new Set[2])) {
            //Push any newly created criteria nodes and try to copy them afterwards
            rules.push(RuleConstants.COPY_CRITERIA);
            rules.push(RuleConstants.PUSH_NON_JOIN_CRITERIA);
        }
        
        //mark the old criteria nodes as copied.  this will prevent RulePushSelectCriteria from considering them again
        for (PlanNode critNode : critNodes) {
        	critNode.setProperty(NodeConstants.Info.IS_COPIED, Boolean.TRUE);
        }
        		
		return plan;	
	}

    /**
     * Given a criteria and a map of elements to values try to create a new single group criteria
     * 
     * If the new criteria does not have exactly one group or already exists in the combined criteria,
     * it will not be added.
     *  
     * @param crit
     * @param tgtMap
     * @param joinCriteria
     * @param combinedCriteria
     * @return true if the copy was successful
     */
    private boolean copyCriteria(Criteria crit,
                                 Map tgtMap,
                                 List joinCriteria,
                                 Set combinedCriteria,
                                 boolean checkForGroupReduction) {
        int startGroups = GroupsUsedByElementsVisitor.getGroups(crit).size();
        
        Criteria tgtCrit = (Criteria) crit.clone();
        
        try {
            tgtCrit = FrameUtil.convertCriteria(tgtCrit, tgtMap);
        } catch (QueryPlannerException err) {
            LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, err, "Could not remap target criteria in RuleCopyCriteria"); //$NON-NLS-1$
            return false;
        }
        
        int endGroups = GroupsUsedByElementsVisitor.getGroups(tgtCrit).size();
        
        if (checkForGroupReduction) {
            if (endGroups >= startGroups) {
                return false;
            }
        } else if (endGroups > startGroups) {
            return false;
        }
        
        //if this is unique or it a duplicate but reduced a current join conjunct, return true
        if (combinedCriteria.add(tgtCrit)) {
            joinCriteria.add(tgtCrit);
            return true;
        } else if (checkForGroupReduction) {
            return true;
        }
        
        return false;
    }
            
    /** 
     * Recursively tries to copy criteria across join nodes.  toCopy will contain only the single group criteria
     * that has not yet been copied.  allCriteria will contain all criteria present at the join that can effect
     * copying.
     * 
     * @param node
     * @return true if criteria has been created
     */
    private boolean tryToCopy(PlanNode node, Set[] criteriaInfo) {
        boolean changedTree = false;
        
        if (node == null) {
            return false;
        }
        
        //visit join nodes in order
        if (node.getType() == NodeConstants.Types.JOIN) {
            JoinType jt = (JoinType)node.getProperty(NodeConstants.Info.JOIN_TYPE);
            
            if (jt == JoinType.JOIN_FULL_OUTER) {
                return visitChildern(node, criteriaInfo, changedTree);
            }
            
            Set[] leftChildCriteria = new Set[2];
            Set[] rightChildCriteria = new Set[2];
            
            changedTree |= tryToCopy(node.getFirstChild(), leftChildCriteria);
            changedTree |= tryToCopy(node.getLastChild(), rightChildCriteria);

            List joinCrits = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
            Set combinedCriteria = null;
            if (joinCrits != null) {
                combinedCriteria = new HashSet(joinCrits); 
                combinedCriteria.addAll(leftChildCriteria[1]);
                combinedCriteria.addAll(rightChildCriteria[1]);
            }

            //combine the criteria
            leftChildCriteria[0].addAll(rightChildCriteria[0]);
            leftChildCriteria[1].addAll(rightChildCriteria[1]);
            //set the applicable criteria
            criteriaInfo[0] = leftChildCriteria[0];
            //set the all criteria
            criteriaInfo[1] = leftChildCriteria[1];

            //there's no join criteria here, so just let the criteria go up
            if (jt == JoinType.JOIN_CROSS) {
                return changedTree;
            }
            
            Set toCopy = criteriaInfo[0];
            Set allCriteria = criteriaInfo[1];
            
            if (!toCopy.isEmpty()) {
                Map srcToTgt = buildElementMap(joinCrits);
    
                List newJoinCrits = new LinkedList();
    
                changedTree = createCriteriaFromSelectCriteria(changedTree, combinedCriteria, toCopy, srcToTgt, newJoinCrits);
                
                srcToTgt = buildElementMap(allCriteria);
                            
                changedTree = createCriteriaFromJoinCriteria(changedTree, joinCrits, combinedCriteria, srcToTgt, newJoinCrits);
                
                joinCrits.addAll(newJoinCrits);
            }
            
            //before returning, filter out criteria that cannot go above the join node
            if (jt == JoinType.JOIN_RIGHT_OUTER) {
                criteriaInfo[0].removeAll(leftChildCriteria[0]);
                criteriaInfo[1].removeAll(leftChildCriteria[1]);
            } else if (jt == JoinType.JOIN_LEFT_OUTER) {
                criteriaInfo[0].removeAll(rightChildCriteria[0]);
                criteriaInfo[1].removeAll(rightChildCriteria[1]);
            }
            
            return changedTree;
        }
        
        changedTree = visitChildern(node, criteriaInfo, changedTree);

        //visit select nodes on the way back up
        switch (node.getType()) {
        
            case NodeConstants.Types.SELECT:
            {
                if (criteriaInfo[0] != null) {
                    visitSelectNode(node, criteriaInfo[0], criteriaInfo[1]);
                }
                break;
            }
            //clear the criteria when hitting the following
            case NodeConstants.Types.NULL:
            case NodeConstants.Types.SOURCE: 
            case NodeConstants.Types.GROUP:
            case NodeConstants.Types.SET_OP:
            case NodeConstants.Types.PROJECT: 
            {
                if (criteriaInfo[0] == null) {
                    criteriaInfo[0] = new HashSet();
                    criteriaInfo[1] = new HashSet();
                } else {
                    criteriaInfo[0].clear();
                    criteriaInfo[1].clear();
                }
                break;
            }
                
        }
        
        return changedTree;
    }

    /** 
     * @param changedTree
     * @param joinCrits
     * @param combinedCriteria
     * @param srcToTgt
     * @param newJoinCrits
     * @return
     */
    private boolean createCriteriaFromJoinCriteria(boolean changedTree,
                                                   List joinCrits,
                                                   Set combinedCriteria,
                                                   Map srcToTgt,
                                                   List newJoinCrits) {
        if (srcToTgt.size() == 0) {
            return changedTree;
        }
        Iterator i = joinCrits.iterator();
        while (i.hasNext()) {
            Criteria crit = (Criteria)i.next();
            
            if (copyCriteria(crit, srcToTgt, newJoinCrits, combinedCriteria, true)) {
            	changedTree = true;
            	if (crit instanceof CompareCriteria) {
            		CompareCriteria cc = (CompareCriteria)crit;
            		if (cc.getLeftExpression() instanceof ElementSymbol && cc.getRightExpression() instanceof ElementSymbol) {
            			//don't remove theta criteria, just mark it as optional
            			cc.setOptional(true);
            			continue;
            		}
            	}
                i.remove();
            }
        }
        return changedTree;
    }

    /** 
     * @param changedTree
     * @param combinedCriteria
     * @param toCopy
     * @param srcToTgt
     * @param newJoinCrits
     * @return
     */
    private boolean createCriteriaFromSelectCriteria(boolean changedTree,
                                                     Set combinedCriteria,
                                                     Set toCopy,
                                                     Map srcToTgt,
                                                     List newJoinCrits) {
        if (srcToTgt.size() == 0) {
            return changedTree;
        }
        Iterator i = toCopy.iterator();
        while (i.hasNext()) {
            Criteria crit = (Criteria)i.next();
            changedTree |= copyCriteria(crit, srcToTgt, newJoinCrits, combinedCriteria, false);            
        }
        return changedTree;
    }

    /** 
     * @param node
     * @param toCopy
     * @param allCriteria
     */
    private void visitSelectNode(PlanNode node,
                                 Set toCopy,
                                 Set allCriteria) {
        //First examine criteria in critNode for suitability
        Criteria crit = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        if(node.getGroups().size() == 1 && crit != null) {
            
            if(!node.hasBooleanProperty(NodeConstants.Info.IS_HAVING) && node.getSubqueryContainers().isEmpty()) {
                if (!node.hasBooleanProperty(NodeConstants.Info.IS_COPIED)) {
                    toCopy.add(crit);
                }
                allCriteria.add(crit);
            }
        }
    }

    private boolean visitChildern(PlanNode node,
                                  Set[] criteriaInfo,
                                  boolean changedTree) {
        if (node.getChildCount() > 0) {
            List children = node.getChildren();
            for (int i = 0; i < children.size(); i++) {
                PlanNode childNode = (PlanNode)children.get(i);
                changedTree |= tryToCopy(childNode, i==0?criteriaInfo:new Set[2]);
            }
        }
        return changedTree;
    }
        
    /**
     * Construct a mapping of element symbol to value map based upon equality CompareCriteria in crits
     *  
     * @param crits
     * @return
     */
    Map buildElementMap(Collection crits) {
        Map srcToTgt = null;
        Iterator critIter = crits.iterator();
        while(critIter.hasNext()) { 
            Criteria theCrit = (Criteria) critIter.next();
            if (theCrit instanceof IsNullCriteria) {
            	IsNullCriteria isNull = (IsNullCriteria)theCrit;
            	if (!isNull.isNegated() && isNull.getExpression() instanceof ElementSymbol) {
            		if (srcToTgt == null) {
            			srcToTgt = new HashMap();
            		}
            		srcToTgt.put(isNull.getExpression(), new Constant(null, isNull.getExpression().getType()));
            	}
            	continue;
            }
            if(!(theCrit instanceof CompareCriteria)) {
                continue;
            }
            CompareCriteria crit = (CompareCriteria) theCrit;
            if (crit.getOperator() == CompareCriteria.EQ) {
                if (srcToTgt == null) {
                    srcToTgt = new HashMap();
            	}
                srcToTgt.put(crit.getLeftExpression(), crit.getRightExpression());
                srcToTgt.put(crit.getRightExpression(), crit.getLeftExpression());
            }
        }
        if (srcToTgt == null) {
            return Collections.EMPTY_MAP;
        }
        return srcToTgt;
    }
    

	public String toString() {
		return "CopyCriteria"; //$NON-NLS-1$
	}
		
}
