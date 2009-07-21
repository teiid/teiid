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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.connector.api.ConnectorCapabilities.SupportedJoinCriteria;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
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
    static PlanNode raiseAccessNode(PlanNode rootNode, PlanNode accessNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, boolean afterJoinPlanning) 
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
                modelID = canRaiseOverJoin(modelID, parentNode, metadata, capFinder, afterJoinPlanning);
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
                if(!CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, capFinder)) {
                	return null;
                }
                
                if (!CapabilitiesUtil.checkElementsAreSearchable((List)NodeEditor.findNodePreOrder(parentNode, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS), metadata, SupportConstants.Element.SEARCHABLE_COMPARE)) {
                	return null;
                }
                
                return performRaise(rootNode, accessNode, parentNode);
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
                
                //raise only if there is no intervening project into
                PlanNode parentProject = NodeEditor.findParent(parentNode, NodeConstants.Types.PROJECT);
                GroupSymbol intoGroup = (GroupSymbol)parentProject.getProperty(NodeConstants.Info.INTO_GROUP); 
                if (intoGroup != null && parentProject.getParent() == null) {
                	if (CapabilitiesUtil.supports(Capability.INSERT_WITH_QUERYEXPRESSION, modelID, metadata, capFinder) && CapabilitiesUtil.isSameConnector(modelID, metadata.getModelID(intoGroup.getMetadataID()), metadata, capFinder)) {
                    	rootNode = performRaise(rootNode, accessNode, parentNode);
                    	return performRaise(rootNode, accessNode, parentProject);
                	}
                	return null;
                } else if (!CapabilitiesUtil.supportsInlineView(modelID, metadata, capFinder)) {
                	return null;
                }

                //is there another query that will be used with this source
                if (FrameUtil.getNonQueryCommand(accessNode) != null || FrameUtil.getNestedPlan(accessNode) != null) {
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
        return CapabilitiesUtil.checkElementsAreSearchable(groupCols, metadata, SupportConstants.Element.SEARCHABLE_COMPARE);
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
        
        if (!CapabilitiesUtil.checkElementsAreSearchable(sortCols, metadata, SupportConstants.Element.SEARCHABLE_COMPARE)) {
        	return false;
        }
        
        // If model supports the support constant parameter, then move access node
        if (!CapabilitiesUtil.supportsOrderBy(modelID, metadata, capFinder)) {
        	return false;
        }
        
        /* If we have an unrelated sort it cannot be pushed down if it's not supported by the source
         * 
         * TODO: we should be able to work this
         */
        if (parentNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT) && !CapabilitiesUtil.supports(Capability.QUERY_ORDERBY_UNRELATED, modelID, metadata, capFinder)) {
        	return false;
        }
        
        return true;
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
        
        if (parentNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING) && !CapabilitiesUtil.supports(Capability.QUERY_HAVING, modelID, metadata, capFinder)) {
        	return false;
        }
        
        //don't push criteria into an invalid location above an ordered limit - shouldn't happen 
        PlanNode limitNode = NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.SOURCE);
        if (limitNode != null && NodeEditor.findNodePreOrder(limitNode, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE) != null) {
        	return false;
        }
        
        Criteria crit = (Criteria) parentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        
        if(!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(crit, modelID, metadata, capFinder) ) { 
            return false;                        
        } 
        
        if (accessNode.getFirstChild() != null && accessNode.getFirstChild().getType() == NodeConstants.Types.SET_OP) {
            return false; //inconsistent select position - RulePushSelectCriteria is too greedy
        }
                
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
			if (!CapabilitiesUtil.supportsSelectLiterals(modelID, metadata, capFinder) 
        		&& (expr instanceof Constant || EvaluateExpressionVisitor.willBecomeConstant(expr))) {
        		return false;
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
	private static Object canRaiseOverJoin(Object modelId, PlanNode joinNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, boolean afterJoinPlanning) 
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
        
        //if I'm on the inner side of an outer join, then and we have a criteria restriction, then I can't be pushed
		if (type.isOuter() && CapabilitiesUtil.getSupportedJoinCriteria(modelId, metadata, capFinder) != SupportedJoinCriteria.ANY) {
			PlanNode critNode = NodeEditor.findNodePreOrder(joinNode.getLastChild(), NodeConstants.Types.SELECT, NodeConstants.Types.SOURCE);
			if (critNode != null) {
				return null;
			}
			if (type == JoinType.JOIN_FULL_OUTER) {
				critNode = NodeEditor.findNodePreOrder(joinNode.getFirstChild(), NodeConstants.Types.SELECT, NodeConstants.Types.SOURCE);
				if (critNode != null) {
					return null;
				}	
			}
		}
        
        return canRaiseOverJoin(joinNode.getChildren(), metadata, capFinder, crits, type);		
	}

    static Object canRaiseOverJoin(List<PlanNode> children,
                                           QueryMetadataInterface metadata,
                                           CapabilitiesFinder capFinder,
                                           List<Criteria> crits,
                                           JoinType type) throws QueryMetadataException,
                                                         MetaMatrixComponentException {
        //we only want to consider binary joins
        if (children.size() != 2) {
        	return null;
        }

    	Object modelID = null;
        Set<Object> groupIDs = new HashSet<Object>();
        int groupCount = 0;
        
		for (PlanNode childNode : children) {
			if(childNode.getType() != NodeConstants.Types.ACCESS 
					|| childNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
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
                for (GroupSymbol groupSymbol : childNode.getGroups()) {
    			    Object groupID = groupSymbol.getMetadataID();
    			    if(!groupIDs.add(groupID)) {
    			        // Already seen group - can't raise access over self join
    			        return null;
    			    }
    			}
            }
            			
            //check the join criteria now that we know the model
			if(modelID == null) {
                
        		if (!CapabilitiesUtil.supportsJoin(accessModelID, type, metadata, capFinder)) {
				   return null;
        		}
        		SupportedJoinCriteria sjc = CapabilitiesUtil.getSupportedJoinCriteria(accessModelID, metadata, capFinder);
				
        		/*
        		 * Key joins must be left linear
        		 */
        		if (sjc == SupportedJoinCriteria.KEY && children.get(1).getGroups().size() != 1) {
        			return null;
        		}
        		
				if(crits != null && !crits.isEmpty()) {
					List<Object> leftIds = null;
					List<Object> rightIds = null;
					GroupSymbol leftGroup = null;
					for (Criteria crit : crits) {
				        if (!isSupportedJoinCriteria(sjc, crit, accessModelID, metadata, capFinder)) {
				        	return null;
				        }
			        	if (sjc != SupportedJoinCriteria.KEY) {
			        		continue;
			        	}
			        	//key join
			        	//TODO: this would be much simpler if we could rely on rulechoosejoinstrategy running first
			        	if (leftIds == null) {
			        		leftIds = new ArrayList<Object>();
			        		rightIds = new ArrayList<Object>();
			        	}
			        	ElementSymbol leftSymbol = (ElementSymbol)((CompareCriteria)crit).getLeftExpression();
			        	ElementSymbol rightSymbol = (ElementSymbol)((CompareCriteria)crit).getRightExpression();
			        	if ((children.get(0).getGroups().contains(leftSymbol.getGroupSymbol()) && children.get(1).getGroups().contains(rightSymbol.getGroupSymbol()))
			        			|| (children.get(1).getGroups().contains(leftSymbol.getGroupSymbol()) && children.get(0).getGroups().contains(rightSymbol.getGroupSymbol()))) {
			        		boolean left = children.get(0).getGroups().contains(leftSymbol.getGroupSymbol());
			        		if (leftGroup == null) {
			        			leftGroup = left?leftSymbol.getGroupSymbol():rightSymbol.getGroupSymbol();
			        		} else if (!leftGroup.equals(left?leftSymbol.getGroupSymbol():rightSymbol.getGroupSymbol())) {
			        			return null;
			        		}
			        		if (left) {
				        		leftIds.add(leftSymbol.getMetadataID());
				        		rightIds.add(rightSymbol.getMetadataID());
			        		} else {
			        			rightIds.add(leftSymbol.getMetadataID());
				        		leftIds.add(rightSymbol.getMetadataID());
			        		}
			        	} else {
			        		return null;
			        	}
					}
					if (leftIds != null &&
							!matchesForeignKey(metadata, leftIds, rightIds,	leftGroup) 
							&& !matchesForeignKey(metadata, rightIds, leftIds, children.get(1).getGroups().iterator().next())) {
						return null;
					}
                } else if (sjc != SupportedJoinCriteria.ANY) {
                	return null; //cross join not supported
                }
				
				modelID = accessModelID;
				
			} else if(!CapabilitiesUtil.isSameConnector(modelID, accessModelID, metadata, capFinder)) { 
				return null;							
			}
		} // end walking through join node's children

		int maxGroups = CapabilitiesUtil.getMaxFromGroups(modelID, metadata, capFinder);
		
		if (maxGroups != -1 && maxGroups < groupCount) {
		    return null;
		}
		
		return modelID;
    }
    
    /**
     * Checks criteria one predicate at a time.  Only tests up to the equi restriction.
     */
    static boolean isSupportedJoinCriteria(SupportedJoinCriteria sjc, Criteria crit, Object accessModelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
    	if(!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(crit, accessModelID, metadata, capFinder) ) { 
            return false;                        
        } 
        if (sjc == SupportedJoinCriteria.ANY) {
        	return true;
        }
        //theta join must be between elements with a compare predicate
    	if (!(crit instanceof CompareCriteria)) {
    		return false;
    	}
    	CompareCriteria cc = (CompareCriteria)crit;
    	if (!(cc.getLeftExpression() instanceof ElementSymbol)) {
    		return false;
    	}
    	if (!(cc.getRightExpression() instanceof ElementSymbol)) {
    		return false;
    	}
    	if (sjc == SupportedJoinCriteria.THETA) {
    		return true;
    	}
    	//equi must use the equality operator
    	if (cc.getOperator() != CompareCriteria.EQ) {
    		return false;
    	}
		return true;
    }

    /**
     * TODO: gracefully handle too much criteria
     */
	private static boolean matchesForeignKey(QueryMetadataInterface metadata,
			List<Object> leftIds, List<Object> rightIds, GroupSymbol leftGroup)
			throws MetaMatrixComponentException, QueryMetadataException {
		Collection fks = metadata.getForeignKeysInGroup(leftGroup.getMetadataID());
		for (Object fk : fks) {
			List fkColumns = metadata.getElementIDsInKey(fk);
			if (!leftIds.containsAll(fkColumns) || leftIds.size() != fkColumns.size()) {
				continue;
			}
			Object pk = metadata.getPrimaryKeyIDForForeignKeyID(fk);
			List pkColumns = metadata.getElementIDsInKey(pk);
			if (rightIds.containsAll(pkColumns) && rightIds.size() == pkColumns.size()) {
				return true;
			}
		}
		return false;
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
    
    private static boolean canRaiseOverSetQuery(PlanNode setOpNode,
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
            if (!setOpNode.hasBooleanProperty(NodeConstants.Info.USE_ALL) 
            		&&  !CapabilitiesUtil.checkElementsAreSearchable((List)NodeEditor.findNodePreOrder(childNode, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS), metadata, SupportConstants.Element.SEARCHABLE_COMPARE)) {
            	return false;
            }

        }
        return true;
    }
    
	public String toString() {
		return "RaiseAccess"; //$NON-NLS-1$
	}
	
}
