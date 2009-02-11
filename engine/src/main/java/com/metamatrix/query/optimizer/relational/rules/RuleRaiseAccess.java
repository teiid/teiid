/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.sql.visitor.FunctionCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.CommandContext;

public final class RuleRaiseAccess implements OptimizerRule {

	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        boolean afterJoinPlanning = !rules.contains(RuleConstants.PLAN_JOINS);
        
        // Loop until nothing has been raised - plan is then stable and can be returned
        boolean raisedNode = true;
        while(raisedNode) {
            raisedNode = false;

            for (PlanNode accessNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
                PlanNode newRoot = raiseAccessNode(plan, accessNode, metadata, capFinder, afterJoinPlanning);
                if(newRoot != null) {
                    raisedNode = true;
                    plan = newRoot;
                }
            }            
        }
        
        return plan;
	}
    
    /**
     * @return null if nothing changed, and a new plan root if something changed
     */
    PlanNode raiseAccessNode(PlanNode rootNode, PlanNode accessNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, boolean afterJoinPlanning) 
    throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        
        PlanNode parentNode = accessNode.getParent();
        if(parentNode == null) {
            // Nothing to raise over
            return null;
        }
        Object modelID = getModelIDFromAccess(accessNode, metadata);
        if(modelID == null) {
            return null;
        }
        
        switch(parentNode.getType()) {
            case NodeConstants.Types.JOIN:
            {
                modelID = canRaiseOverJoin(parentNode, metadata, capFinder, afterJoinPlanning);
                if(modelID != null) {
                    raiseAccessOverJoin(parentNode, modelID, true);                    
                    return rootNode;
                }
                return null;
            }            
            case NodeConstants.Types.PROJECT:
            {         
                // Check that the PROJECT contains only functions that can be pushed                               
                List projectCols = (List) parentNode.getProperty(NodeConstants.Info.PROJECT_COLS);
                               
                for (int i = 0; i < projectCols.size(); i++) {
                    SingleElementSymbol symbol = (SingleElementSymbol)projectCols.get(i);
                    if(! canPushSymbol(symbol, true, modelID, metadata, capFinder)) {
                        return null;
                    } 
                }
                                
                return performRaise(rootNode, accessNode, parentNode);                
            }
            case NodeConstants.Types.DUP_REMOVE:
            {     
                // If model supports the support constant parameter, then move access node
                if(CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, capFinder)) {
                    return performRaise(rootNode, accessNode, parentNode);
                }
                return null;
            }
            case NodeConstants.Types.SORT:
            {         
                if (canRaiseOverSort(accessNode, metadata, capFinder, parentNode)) {
                    return performRaise(rootNode, accessNode, parentNode);
                }
                return null;
            }            
            case NodeConstants.Types.GROUP:            
            {                
                Set<AggregateSymbol> aggregates = RulePushAggregates.collectAggregates(parentNode);
                if (canRaiseOverGroupBy(parentNode, accessNode, aggregates, metadata, capFinder)) {
                    return performRaise(rootNode, accessNode, parentNode);
                }
                return null;
            } 
            case NodeConstants.Types.SET_OP:
            	if (!canRaiseOverSetQuery(parentNode, metadata, capFinder)) {
            		return null;
            	}

            	for (PlanNode node : new ArrayList<PlanNode>(parentNode.getChildren())) {
            		if (node == accessNode) {
            			continue;
            		}
        			NodeEditor.removeChildNode(parentNode, node);
            	}
                rootNode = performRaise(rootNode, accessNode, parentNode);
            	
            	return rootNode;            	
            case NodeConstants.Types.SELECT:            
            {
                if (!parentNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET) && canRaiseOverSelect(accessNode, metadata, capFinder, parentNode)) {
                    RulePushSelectCriteria.satisfyAccessPatterns(parentNode, accessNode);
                    return performRaise(rootNode, accessNode, parentNode);                      
                }
                return null;
            }   
            case NodeConstants.Types.SOURCE:
            {
                //if a source has access patterns that are unsatisfied, then the raise cannot occur
                if (parentNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
                    return null;
                }
                
                if (!CapabilitiesUtil.supportsInlineView(modelID, metadata, capFinder)) {
                	return null;
                }

                //is there another query that will be used with this source
                if (FrameUtil.getNonQueryCommand(accessNode) != null || FrameUtil.getNestedPlan(accessNode) != null) {
                	return null;
                }
                                
                //raise only if there is no intervening project into
                PlanNode parentProject = NodeEditor.findParent(parentNode, NodeConstants.Types.PROJECT);
                if (parentProject.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
                    return null;
                }
                
                //switch to inline view and change the group on the access to that of the source
            	parentNode.setProperty(NodeConstants.Info.INLINE_VIEW, Boolean.TRUE);
            	accessNode.getGroups().clear();
            	accessNode.addGroups(parentNode.getGroups());
                RulePlaceAccess.copyDependentHints(parentNode, accessNode);
            	return performRaise(rootNode, accessNode, parentNode);
            }
            case NodeConstants.Types.TUPLE_LIMIT:
            {
                return RulePushLimit.raiseAccessOverLimit(rootNode, accessNode, metadata, capFinder, parentNode);
            }
            default: 
            {
                return null;
            }                      
        }        
    }

    static boolean canRaiseOverGroupBy(PlanNode groupNode,
                                         PlanNode accessNode,
                                         Collection<? extends SingleElementSymbol> aggregates,
                                         QueryMetadataInterface metadata,
                                         CapabilitiesFinder capFinder) throws QueryMetadataException,
                                                        MetaMatrixComponentException {
        Object modelID = getModelIDFromAccess(accessNode, metadata);
        if(modelID == null) {
            return false;
        }
        List<SingleElementSymbol> groupCols = (List<SingleElementSymbol>)groupNode.getProperty(NodeConstants.Info.GROUP_COLS);
        if(!CapabilitiesUtil.supportsAggregates(groupCols, modelID, metadata, capFinder)) {
            return false;
        }
        if (groupCols != null) {
            for (SingleElementSymbol singleElementSymbol : groupCols) {
                if (!canPushSymbol(singleElementSymbol, false, modelID, metadata, capFinder)) {
                    return false;
                }
            }
        }
        if (aggregates != null) {
            for (SingleElementSymbol aggregateSymbol : aggregates) {
                if(! CriteriaCapabilityValidatorVisitor.canPushLanguageObject(aggregateSymbol, modelID, metadata, capFinder)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    static boolean canRaiseOverSort(PlanNode accessNode,
                                   QueryMetadataInterface metadata,
                                   CapabilitiesFinder capFinder,
                                   PlanNode parentNode) throws QueryMetadataException,
                                                       MetaMatrixComponentException {
        // Find the model for this node by getting ACCESS node's model
        Object modelID = getModelIDFromAccess(accessNode, metadata);
        if(modelID == null) {
            // Couldn't determine model ID, so give up
            return false;
        } 
        
        List sortCols = (List)parentNode.getProperty(NodeConstants.Info.SORT_ORDER);
        for (int i = 0; i < sortCols.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol)sortCols.get(i);
            if(! canPushSymbol(symbol, true, modelID, metadata, capFinder)) {
                return false;
            }
        }
        
        if (accessNode.getLastChild() != null) {
            //check to see if the sort applies to a union
            if (accessNode.getLastChild().getType() == NodeConstants.Types.SET_OP) {
                return CapabilitiesUtil.supportsSetQueryOrderBy(modelID, metadata, capFinder);
            }
            //check to see the plan is not in a consistent state to have a sort applied
            if (accessNode.getLastChild().getType() == NodeConstants.Types.TUPLE_LIMIT) {
                return false;
            }
        }
        
        // If model supports the support constant parameter, then move access node
        if(CapabilitiesUtil.supportsOrderBy(modelID, metadata, capFinder)) {
            return true;
        }
        return false;
    }

    /** 
     * @param accessNode
     * @param metadata
     * @param capFinder
     * @param parentNode
     * @return
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     * @throws QueryPlannerException
     */
    static boolean canRaiseOverSelect(PlanNode accessNode,
                                        QueryMetadataInterface metadata,
                                        CapabilitiesFinder capFinder,
                                        PlanNode parentNode) throws QueryMetadataException,
                                                            MetaMatrixComponentException,
                                                            QueryPlannerException {
        if (parentNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
            return true;
        }
                
        // Find the model for this node by getting ACCESS node's model
        Object modelID = getModelIDFromAccess(accessNode, metadata);
        if(modelID == null) {
            // Couldn't determine model ID, so give up
            return false;
        } 
        
        //don't push criteria into an invalid location above an ordered limit - shouldn't happen 
        PlanNode limitNode = NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.SOURCE);
        if (limitNode != null && NodeEditor.findNodePreOrder(limitNode, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE) != null) {
        	return false;
        }
        
        Criteria crit = (Criteria) parentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        
        boolean hasSubquery = FrameUtil.hasSubquery(parentNode);
        if(hasSubquery && !isEligibleSubquery(parentNode, metadata, capFinder)){
        	return false;
        }
        
        // Check criteria capabilities of source.  Criteria (even  
        // multi-group criteria) can possibly be pushed if model can 
        // support everything in the criteria.

        if(!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(crit, modelID, metadata, capFinder) ) { 
            return false;                        
        } 
        
        if (accessNode.getFirstChild() != null && accessNode.getFirstChild().getType() == NodeConstants.Types.SET_OP) {
            return false; //inconsistent select position - RulePushSelectCriteria is too greedy
        }
        
        //TODO: check for "and" support
        
        return true;
    }  
    
    /**
     * Check whether the subquery in the node is eligible to be pushed.
     */
    static boolean isEligibleSubquery(PlanNode critNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws MetaMatrixComponentException {
        List plans = (List) critNode.getProperty(NodeConstants.Info.SUBQUERY_PLANS);
        if(plans == null) {
            return false;
        }
        Iterator planIter = plans.iterator();
        while(planIter.hasNext()) {
            ProcessorPlan plan = (ProcessorPlan) planIter.next();
            if(!(plan instanceof RelationalPlan)) {
                return false;
            }
            // We are expecting the following for an eligible subquery:
            // 1. Plan should be Access, nothing else
            // 2. Access should be returning a single column
            // 3. should not be returning a constant or scalar function, 
            //    only an element or aggregate  
            // 4. Access node command should be a Query
            // 5. Access node should be for the same model as critNode
            // 6. If subquery has correlated references, model supports correlated
            
            // Check that root node is a project
            RelationalPlan rplan = (RelationalPlan) plan;
            
            // Check that the second node is an access node and that it has no children                
            RelationalNode accessNode = rplan.getRootNode();
            if(accessNode == null || ! (accessNode instanceof AccessNode) || accessNode.getChildren()[0] != null) {
                return false;
            }
            
            // Check that command in access node is a query
            Command command = ((AccessNode)accessNode).getCommand();
            if(command == null || !(command instanceof Query) || ((Query)command).getIsXML()) {
                return false;
            }
            
            // Check that query in access node is for the same model as current node
            Object critNodeModelID = null;
            try {
                PlanNode source = FrameUtil.findJoinSourceNode(critNode);
                if (source.getType() != NodeConstants.Types.ACCESS) {
                	return false;
                }
                critNodeModelID = RuleRaiseAccess.getModelIDFromAccess(source, metadata);
                
                if (critNodeModelID == null) {
                    return false;
                }
                
                Collection subQueryGroups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, false);
                if(subQueryGroups.size() == 0) {
                    // No FROM?
                    return false;
                }
                GroupSymbol subQueryGroup = (GroupSymbol)subQueryGroups.iterator().next();
                
                Object modelID = metadata.getModelID(subQueryGroup.getMetadataID());
                if(!CapabilitiesUtil.isSameConnector(critNodeModelID, modelID, metadata, capFinder)) {
                    return false;
                }
            } catch(QueryMetadataException e) {
                throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("RulePushSelectCriteria.Error_getting_modelID")); //$NON-NLS-1$
            }                
            
            // Check whether source supports correlated subqueries and if not, whether criteria has them
            Collection refs = (Collection) critNode.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            try {
                if(refs != null && !refs.isEmpty()) {
                    if(! CapabilitiesUtil.supportsCorrelatedSubquery(critNodeModelID, metadata, capFinder)) {
                        return false;
                    }
                    
                    for (Iterator i = refs.iterator(); i.hasNext();) {
                        ((Reference)i.next()).setCorrelated(true);
                    }
                    
                    if (!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(command, critNodeModelID, metadata, capFinder)) {
                        return false;
                    }
                }
            } catch(QueryMetadataException e) {
                throw new MetaMatrixComponentException(e, e.getMessage());                  
            } finally {
                if (refs != null) {
                    for (Iterator i = refs.iterator(); i.hasNext();) {
                        ((Reference)i.next()).setCorrelated(false);
                    }
                }
            }
        }

        // Found no reason why this node is not eligible
        return true;
    }
        
    /**
     *  
     * @param symbol Symbol to check
     * @param inSelectClause True if evaluating in the context of a SELECT clause
     * @param modelID Model
     * @param metadata Metadata
     * @param capFinder Capabilities finder
     * @return True if can push symbol to source
     * @throws MetaMatrixComponentException
     * @throws QueryMetadataException
     * @since 4.1.2
     */
    private static boolean canPushSymbol(SingleElementSymbol symbol, boolean inSelectClause, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws MetaMatrixComponentException, QueryMetadataException {

        Expression expr = SymbolMap.getExpression(symbol);
        
        // Do the normal checks
        if(! CriteriaCapabilityValidatorVisitor.canPushLanguageObject(expr, modelID, metadata, capFinder)) {
            return false;
        }
        
        if(inSelectClause && !(expr instanceof ElementSymbol)) {
            if(ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expr).size() > 0) {
                // Don't support for now
                return false;

            } else if(expr instanceof Constant || EvaluateExpressionVisitor.willBecomeConstant(expr)) {
                if(! CapabilitiesUtil.supportsSelectLiterals(modelID, metadata, capFinder)) {
                    return false;
                }
            }             
        }                
         
        // By default, no reason we can't push
        return true;
    }
    
    static PlanNode performRaise(PlanNode rootNode, PlanNode accessNode, PlanNode parentNode) {
        NodeEditor.removeChildNode(parentNode, accessNode);
        parentNode.addAsParent(accessNode);
        PlanNode grandparentNode = accessNode.getParent();
        if(grandparentNode != null) {
            return rootNode;
        }
        return accessNode;
    }

    /**
     * Determine whether an access node can be raised over the specified join node.
     * 
     * This method can also be used to determine if a join node "A", parent of another join
     * node "B", will have it's access raised.  This is needed to help determine if node
     * "B" will have access raised over it.  In this scenario, the parameter will be true.
     * When this method is called normally from the "execute" method, that param will be false.
     *   
     * @param joinNode Join node that might be pushed underneath the access node
     * @param metadata Metadata information
     * @param capFinder CapabilitiesFinder
     * @return The modelID if the raise can proceed and what common model these combined
     * nodes will be sent to
     */
	Object canRaiseOverJoin(PlanNode joinNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, boolean afterJoinPlanning) 
		throws QueryMetadataException, MetaMatrixComponentException {
		
        List crits = (List) joinNode.getProperty(NodeConstants.Info.JOIN_CRITERIA);
        JoinType type = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        
        //let ruleplanjoins handle this case
        if (!afterJoinPlanning && type == JoinType.JOIN_CROSS && joinNode.getParent().getType() == NodeConstants.Types.JOIN) {
            JoinType jt = (JoinType)joinNode.getParent().getProperty(NodeConstants.Info.JOIN_TYPE);
            if (!jt.isOuter()) {
                return null;
            }
        }
        
        if (joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE) != null) {
            return null;
        }
        
        //if a join has access patterns that are unsatisfied, then the raise cannot occur
        if (joinNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
            return null;
        }
        
        return canRaiseOverJoin(joinNode.getChildren(), metadata, capFinder, crits, type);		
	}

    static Object canRaiseOverJoin(List children,
                                           QueryMetadataInterface metadata,
                                           CapabilitiesFinder capFinder,
                                           List crits,
                                           JoinType type) throws QueryMetadataException,
                                                         MetaMatrixComponentException {
        Object modelID = null;
        Set groupIDs = new HashSet();
        int groupCount = 0;
        
        // Walk through each of the join node children - all of them should be access nodes
        // if the raise can occur.
		Iterator childIter = children.iterator();
		while(childIter.hasNext()) {
			PlanNode childNode = (PlanNode) childIter.next();
            
			if(childNode.getType() != NodeConstants.Types.ACCESS) {
                // If one of the children is not an access node (or, in a rare case, a join node), 
                // then we can't raise the access nodes
                return null;
            }
			Object accessModelID = getModelIDFromAccess(childNode, metadata);
            if(accessModelID == null) { 
                return null;
            }
            
            groupCount += childNode.getGroups().size();

			// Add all group metadata IDs to the list but check before each to make 
			// sure group hasn't already been seen - if so, bail out - this is a self join
            // Unless model supports self joins, in which case, don't bail out.

            boolean supportsSelfJoins = CapabilitiesUtil.supportsSelfJoins(accessModelID, metadata, capFinder);
            
            if (!supportsSelfJoins) {
                Iterator groupIter = childNode.getGroups().iterator();				
    			while(groupIter.hasNext()) { 
    			    GroupSymbol groupSymbol = (GroupSymbol) groupIter.next();
    			    Object groupID = groupSymbol.getMetadataID();
    			    if(!groupIDs.add(groupID)) {
    			        // Already seen group - can't raise access over self join
    			        return null;
    			    }
    			}
            }
            								
			if(modelID == null) {
                
				// Check that model supports join
                if(!CapabilitiesUtil.supportsJoins(accessModelID, metadata, capFinder)) {
                	return null;
                }
				// Check that if join is outer, model supports it
				
                if(type.isOuter() && !CapabilitiesUtil.supportsOuterJoin(accessModelID, type, metadata, capFinder)) {
				   //join is outer and model does not support
				   return null;
				}
			
				// Check that model supports join expressions 
				if(crits != null && !crits.isEmpty()) {
                    // Check whether has expression
                    boolean hasExpression = false; 
					Iterator critIter = crits.iterator();
					while(critIter.hasNext()) { 
                            Criteria crit = (Criteria) critIter.next();
                        if(FunctionCollectorVisitor.getFunctions(crit, false).size() > 0) {
                            hasExpression = true;
                            break;
                        }
					}
                    
                    // If expression was found and capabilities don't support, abort this join
                    if(hasExpression && ! CapabilitiesUtil.supportsJoinExpression(accessModelID, crits, metadata, capFinder)) {
                        return null;
                    }
                }
				
				modelID = accessModelID;
				
			} else if(!CapabilitiesUtil.isSameConnector(modelID, accessModelID, metadata, capFinder)) { 
				return null;							
			}
            
            if (childNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
                return null;
            }
            
            //check the group count
            
		} // end walking through join node's children

		int maxGroups = CapabilitiesUtil.getMaxFromGroups(modelID, metadata, capFinder);
		
		if (maxGroups != -1 && maxGroups < groupCount) {
		    return null;
		}
		
		return modelID;
    }
 
    static PlanNode raiseAccessOverJoin(PlanNode joinNode, Object modelID, boolean insert) {
		PlanNode leftAccess = joinNode.getFirstChild();
		PlanNode rightAccess = joinNode.getLastChild();

		// Remove old access nodes - this will automatically add children of access nodes to join node
		NodeEditor.removeChildNode(joinNode, leftAccess);
		NodeEditor.removeChildNode(joinNode, rightAccess);
        
        //Set for later possible use, even though this isn't an access node
        joinNode.setProperty(NodeConstants.Info.MODEL_ID, modelID);

		// Insert new access node above join node 
		PlanNode newAccess = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
		newAccess.setProperty(NodeConstants.Info.MODEL_ID, modelID);
		newAccess.addGroups(rightAccess.getGroups());
		newAccess.addGroups(leftAccess.getGroups());
        
        // Combine hints if necessary
        Object leftHint = leftAccess.getProperty(NodeConstants.Info.MAKE_DEP);
        if(leftHint != null) {
            newAccess.setProperty(NodeConstants.Info.MAKE_DEP, leftHint);
        } else {
            Object rightHint = rightAccess.getProperty(NodeConstants.Info.MAKE_DEP);
            if(rightHint != null) {
                newAccess.setProperty(NodeConstants.Info.MAKE_DEP, rightHint);
            }    
        }
        RulePlaceAccess.copyDependentHints(leftAccess, newAccess);
        RulePlaceAccess.copyDependentHints(rightAccess, newAccess);
        RulePlaceAccess.copyDependentHints(joinNode, newAccess);
        
        if (insert) {
            joinNode.addAsParent(newAccess);
        } else {
            newAccess.addFirstChild(joinNode);
        }
        
        return newAccess;
	}

    /**
     * Get modelID for Access node and cache the result in the Access node.
     * @param accessNode Access node
     * @param metadata Metadata access
     * @return Object Model ID or null if not found.
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    static Object getModelIDFromAccess(PlanNode accessNode, QueryMetadataInterface metadata) 
    throws QueryMetadataException, MetaMatrixComponentException {

        Object accessModelID = accessNode.getProperty(NodeConstants.Info.MODEL_ID);
        if(accessModelID == null) {
            GroupSymbol group = accessNode.getGroups().iterator().next();
            if(metadata.isVirtualGroup(group.getMetadataID())) {
                return null;
            }
            accessModelID = metadata.getModelID(group.getMetadataID());
    
            accessNode.setProperty(NodeConstants.Info.MODEL_ID, accessModelID);
        } 
        
        return accessModelID;    
    }
    
    private boolean canRaiseOverSetQuery(PlanNode setOpNode,
                                     QueryMetadataInterface metadata,
                                     CapabilitiesFinder capFinder) throws QueryMetadataException, MetaMatrixComponentException {
        
        Object modelID = null;
        
        for (PlanNode childNode : setOpNode.getChildren()) {
            if(childNode.getType() != NodeConstants.Types.ACCESS) {
                return false;
            } 
            
            if (FrameUtil.getNonQueryCommand(childNode) != null || FrameUtil.getNestedPlan(childNode) != null) {
                return false;
            }
            
            // Get model and check that it exists
            Object accessModelID = getModelIDFromAccess(childNode, metadata);
            if(accessModelID == null) {
                return false;
            }
            
            // Reconcile this access node's model ID with existing                                             
            if(modelID == null) {
                modelID = accessModelID;
                
                if(! CapabilitiesUtil.supportsSetOp(accessModelID, (Operation)setOpNode.getProperty(NodeConstants.Info.SET_OPERATION), metadata, capFinder)) {
                    return false;
                }
            } else if(!CapabilitiesUtil.isSameConnector(modelID, accessModelID, metadata, capFinder)) {
                return false;
            }      
        }
        return true;
    }
    
	public String toString() {
		return "RaiseAccess"; //$NON-NLS-1$
	}
	
}
