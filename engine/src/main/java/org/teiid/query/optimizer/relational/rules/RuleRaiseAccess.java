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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.ForeignKey;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;


public final class RuleRaiseAccess implements OptimizerRule {
	
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        boolean afterJoinPlanning = !rules.contains(RuleConstants.PLAN_JOINS);
        
        for (PlanNode accessNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
        	while (true) {
	            PlanNode newRoot = raiseAccessNode(plan, accessNode, metadata, capFinder, afterJoinPlanning, analysisRecord, context);
	            if (newRoot == null) {
	            	break;
	            }
                plan = newRoot;
        	}
        }            
        
        return plan;
	}
    
    /**
     * @return null if nothing changed, and a new plan root if something changed
     */
    static PlanNode raiseAccessNode(PlanNode rootNode, PlanNode accessNode, QueryMetadataInterface metadata, 
    		CapabilitiesFinder capFinder, boolean afterJoinPlanning, AnalysisRecord record, CommandContext context) 
    throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        
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
                modelID = canRaiseOverJoin(modelID, parentNode, metadata, capFinder, afterJoinPlanning, record, context);
                if(modelID != null) {
                    raiseAccessOverJoin(parentNode, accessNode, modelID, true);                    
                    return rootNode;
                }
                return null;
            }            
            case NodeConstants.Types.PROJECT:
            {         
                // Check that the PROJECT contains only functions that can be pushed                               
                List<Expression> projectCols = (List) parentNode.getProperty(NodeConstants.Info.PROJECT_COLS);
                
                for (int i = 0; i < projectCols.size(); i++) {
                    Expression symbol = projectCols.get(i);
                    if(! canPushSymbol(symbol, true, modelID, metadata, capFinder, record)) {
                        return null;
                    } 
                }
                
                /*
                 * TODO: this creates an extraneous project node in many circumstances.
                 * However we don't actually support project in this case, so allowing it to be pushed
                 * causes problems with stored procedures and the assumptions made for proc/relational
                 * planning. 
                 */
                if (FrameUtil.isProcedure(parentNode)) {
                	return null;
                }
                
                PlanNode orderBy = NodeEditor.findParent(parentNode, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
                if (orderBy != null && orderBy.hasBooleanProperty(Info.UNRELATED_SORT) && !canRaiseOverSort(accessNode, metadata, capFinder, orderBy, record, false)) {
                	//this project node logically has the responsibility of creating the sort keys
            		return null;
                }
                
                if (accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
                	List<WindowFunction> windowFunctions = new ArrayList<WindowFunction>(2);
                	for (Expression ex : projectCols) {
                    	AggregateSymbolCollectorVisitor.getAggregates(ex, null, null, null, windowFunctions, null);
						if (!windowFunctions.isEmpty()) {
							return null;
						}
					}
                }
                                
                return performRaise(rootNode, accessNode, parentNode);                
            }
            case NodeConstants.Types.DUP_REMOVE:
            {     
                // If model supports the support constant parameter, then move access node
                if(!CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, capFinder)) {
                	parentNode.recordDebugAnnotation("distinct is not supported by source", modelID, "cannot push dupremove", record, metadata); //$NON-NLS-1$ //$NON-NLS-2$
                	return null;
                }
                
                if (!supportsDistinct(metadata, parentNode, accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE))) {
                	parentNode.recordDebugAnnotation("not all columns are comparable at the source", modelID, "cannot push dupremove", record, metadata); //$NON-NLS-1$ //$NON-NLS-2$
                	return null;
                }
                
                return performRaise(rootNode, accessNode, parentNode);
            }
            case NodeConstants.Types.SORT:
            {         
                if (canRaiseOverSort(accessNode, metadata, capFinder, parentNode, record, false)) {
                    return performRaise(rootNode, accessNode, parentNode);
                }
                return null;
            }            
            case NodeConstants.Types.GROUP:            
            {                
                Set<AggregateSymbol> aggregates = RulePushAggregates.collectAggregates(parentNode);
                if (canRaiseOverGroupBy(parentNode, accessNode, aggregates, metadata, capFinder, record, true)) {
                	accessNode.getGroups().clear();
                	accessNode.getGroups().addAll(parentNode.getGroups());
                    return performRaise(rootNode, accessNode, parentNode);
                }
                return null;
            } 
            case NodeConstants.Types.SET_OP:
            	if (!canRaiseOverSetQuery(parentNode, metadata, capFinder)) {
            		return null;
            	}
            	String sourceName = null;
            	boolean multiSource = false;
            	for (PlanNode node : new ArrayList<PlanNode>(parentNode.getChildren())) {
            		multiSource |= accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE);
            		if (sourceName == null) {
            			sourceName = (String)accessNode.getProperty(Info.SOURCE_NAME);
            		}
            		if (node == accessNode) {
            			continue;
            		}
                    combineSourceHints(accessNode, node);
        			NodeEditor.removeChildNode(parentNode, node);
            	}
            	accessNode.getGroups().clear();
            	if (multiSource) {
            		accessNode.setProperty(Info.IS_MULTI_SOURCE, true);
            	} else if (sourceName != null) {
            		accessNode.setProperty(Info.SOURCE_NAME, sourceName);
            	}
                return performRaise(rootNode, accessNode, parentNode);
            case NodeConstants.Types.SELECT:            
            {
            	if (parentNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
            		return null;
            	}
            	if (canRaiseOverSelect(accessNode, metadata, capFinder, parentNode, record)) {
                    RulePushSelectCriteria.satisfyConditions(parentNode, accessNode, metadata);
                    return performRaise(rootNode, accessNode, parentNode);                      
            	}
            	//determine if we should push the select back up
            	if (parentNode.getParent() == null) {
            		return null;
            	}
        		PlanNode selectRoot = parentNode;
        		while (selectRoot.getParent() != null && selectRoot.getParent().getType() == NodeConstants.Types.SELECT) {
        			selectRoot = selectRoot.getParent();
        		}
        		if (selectRoot.getParent() == null || (selectRoot.getParent().getType() & (NodeConstants.Types.PROJECT|NodeConstants.Types.GROUP)) == selectRoot.getParent().getType()) {
        			return null;
        		}
    			PlanNode grandParent = selectRoot.getParent();
    			boolean isLeft = false;
				isLeft = grandParent.getFirstChild() == selectRoot;
				if (grandParent.getType() == NodeConstants.Types.JOIN) {
					JoinType jt = (JoinType)grandParent.getProperty(NodeConstants.Info.JOIN_TYPE);
					if (jt == JoinType.JOIN_FULL_OUTER || (jt == JoinType.JOIN_LEFT_OUTER && !isLeft)) {
						return null;
					}
				}
				grandParent.removeChild(selectRoot);
				if (isLeft) {
					grandParent.addFirstChild(accessNode);
				} else {
					grandParent.addLastChild(accessNode);
				}
    			PlanNode newParent = grandParent.getParent();
				//TODO: use costing or heuristics instead of always raising
    			PlanNode newRoot = raiseAccessNode(rootNode, accessNode, metadata, capFinder, afterJoinPlanning, record, context);
    			if (newRoot == null) {
					//return the tree to its original state
    				parentNode.addFirstChild(accessNode);
    				if (isLeft) {
    					grandParent.addFirstChild(selectRoot);
    				} else {
    					grandParent.addLastChild(selectRoot);
    				}
    			} else {
    				//attach the select nodes above the access node
    				accessNode = grandParent.getParent();
					if (newParent != null) {
    					isLeft = newParent.getFirstChild() == accessNode;
    					if (isLeft) {
    						newParent.addFirstChild(selectRoot);
    					} else {
    						newParent.addLastChild(selectRoot);
    					}
    				} else {
    					newRoot = selectRoot;
    				}
					parentNode.addFirstChild(accessNode);
    				return newRoot;
    			}
                return null;
            }   
            case NodeConstants.Types.SOURCE:
            {
                //if a source has access patterns that are unsatisfied, then the raise cannot occur
                if (parentNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
                    return null;
                }
                
                SymbolMap references = (SymbolMap)parentNode.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
                if (references != null) {
                	return null;
                }
                
                //raise only if there is no intervening project into
                PlanNode parentProject = NodeEditor.findParent(parentNode, NodeConstants.Types.PROJECT);
                GroupSymbol intoGroup = (GroupSymbol)parentProject.getProperty(NodeConstants.Info.INTO_GROUP); 
                if (intoGroup != null && parentProject.getParent() == null) {
                	if (!parentProject.hasProperty(Info.CONSTRAINT) && CapabilitiesUtil.supports(Capability.INSERT_WITH_QUERYEXPRESSION, modelID, metadata, capFinder) && CapabilitiesUtil.isSameConnector(modelID, metadata.getModelID(intoGroup.getMetadataID()), metadata, capFinder)) {
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
                return RulePushLimit.raiseAccessOverLimit(rootNode, accessNode, metadata, capFinder, parentNode, record);
            }
            default: 
            {
                return null;
            }                      
        }        
    }

	private static void combineSourceHints(PlanNode accessNode,
			PlanNode parentNode) {
		accessNode.setProperty(Info.SOURCE_HINT, SourceHint.combine((SourceHint)parentNode.getProperty(Info.SOURCE_HINT), (SourceHint)accessNode.getProperty(Info.SOURCE_HINT)));
	}

    static boolean canRaiseOverGroupBy(PlanNode groupNode,
                                         PlanNode accessNode,
                                         Collection<? extends AggregateSymbol> aggregates,
                                         QueryMetadataInterface metadata,
                                         CapabilitiesFinder capFinder, AnalysisRecord record, boolean considerMultiSource) throws QueryMetadataException,
                                                        TeiidComponentException {
        Object modelID = getModelIDFromAccess(accessNode, metadata);
        if(modelID == null) {
            return false;
        }
        if (considerMultiSource && accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
        	return false;
        }
        List<Expression> groupCols = (List<Expression>)groupNode.getProperty(NodeConstants.Info.GROUP_COLS);
        if(!CapabilitiesUtil.supportsAggregates(groupCols, modelID, metadata, capFinder)) {
        	groupNode.recordDebugAnnotation("group by is not supported by source", modelID, "cannot push group by", record, metadata); //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }
        if (CapabilitiesUtil.supports(Capability.QUERY_ONLY_SINGLE_TABLE_GROUP_BY, modelID, metadata, capFinder)
        		&& !NodeEditor.findAllNodes(groupNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE).isEmpty()) {
        	groupNode.recordDebugAnnotation("joined group by is not supported by source", modelID, "cannot push group by", record, metadata); //$NON-NLS-1$ //$NON-NLS-2$
        	return false;
        }
        if (groupCols != null) {
            for (Expression expr : groupCols) {
                if (!canPushSymbol(expr, false, modelID, metadata, capFinder, record)) {
                    return false;
                }
            }
        }
        if (aggregates != null) {
            for (AggregateSymbol aggregateSymbol : aggregates) {
                if(! CriteriaCapabilityValidatorVisitor.canPushLanguageObject(aggregateSymbol, modelID, metadata, capFinder, record)) {
                    return false;
                }
            }
        }
        if (!CapabilitiesUtil.checkElementsAreSearchable(groupCols, metadata, SupportConstants.Element.SEARCHABLE_COMPARE)) {
        	groupNode.recordDebugAnnotation("non-searchable group by column", modelID, "cannot push group by", record, metadata); //$NON-NLS-1$ //$NON-NLS-2$
        	return false;
        }
        return true;
    }

	static boolean canRaiseOverSort(PlanNode accessNode,
                                   QueryMetadataInterface metadata,
                                   CapabilitiesFinder capFinder,
                                   PlanNode parentNode, AnalysisRecord record, boolean compensateForUnrelated) throws QueryMetadataException,
                                                       TeiidComponentException {
        // Find the model for this node by getting ACCESS node's model
        Object modelID = getModelIDFromAccess(accessNode, metadata);
        if(modelID == null) {
            // Couldn't determine model ID, so give up
            return false;
        } 
        
        List<OrderByItem> sortCols = ((OrderBy)parentNode.getProperty(NodeConstants.Info.SORT_ORDER)).getOrderByItems();
        for (OrderByItem symbol : sortCols) {
            if(! canPushSymbol(symbol.getSymbol(), true, modelID, metadata, capFinder, record)) {
                return false;
            }
            if (!CapabilitiesUtil.supportsNullOrdering(metadata, capFinder, modelID, symbol)) {
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
        
        if (parentNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT) 
        		&& !CapabilitiesUtil.supports(Capability.QUERY_ORDERBY_UNRELATED, modelID, metadata, capFinder)
        		&& NodeEditor.findParent(accessNode, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE) == null
        		&& !compensateForUnrelated) {
        	return false;
        }
        
        if (accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
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
     * @throws TeiidComponentException
     * @throws QueryPlannerException
     */
    static boolean canRaiseOverSelect(PlanNode accessNode,
                                        QueryMetadataInterface metadata,
                                        CapabilitiesFinder capFinder,
                                        PlanNode parentNode, AnalysisRecord record) throws QueryMetadataException,
                                                            TeiidComponentException,
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
        	parentNode.recordDebugAnnotation("having is not supported by source", modelID, "cannot push having", record, metadata); //$NON-NLS-1$ //$NON-NLS-2$
        	return false;
        }
        
        //don't push criteria into an invalid location above an ordered limit - shouldn't happen 
        PlanNode limitNode = NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.SOURCE);
        if (limitNode != null && FrameUtil.isOrderedOrStrictLimit(limitNode)) {
        	return false;
        }
        
        Criteria crit = (Criteria) parentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        
        if(!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(crit, modelID, metadata, capFinder, record) ) { 
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
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @since 4.1.2
     */
    static boolean canPushSymbol(Expression symbol, boolean inSelectClause, Object modelID, 
    		QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record) 
    throws TeiidComponentException, QueryMetadataException {

        Expression expr = SymbolMap.getExpression(symbol);
        
        // Do the normal checks
        if(! CriteriaCapabilityValidatorVisitor.canPushLanguageObject(expr, modelID, metadata, capFinder, record)) {
            return false;
        }
        
        if(inSelectClause && !(expr instanceof ElementSymbol || expr instanceof AggregateSymbol) 
        		&& !CapabilitiesUtil.supportsSelectExpression(modelID, metadata, capFinder)) {
    		return false;
        }                
         
        // By default, no reason we can't push
        return true;
    }
    
    static PlanNode performRaise(PlanNode rootNode, PlanNode accessNode, PlanNode parentNode) {
    	accessNode.removeProperty(NodeConstants.Info.EST_CARDINALITY);
    	combineSourceHints(accessNode, parentNode);
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
     * @param context 
     * @return The modelID if the raise can proceed and what common model these combined
     * nodes will be sent to
     */
	private static Object canRaiseOverJoin(Object modelId, PlanNode joinNode, QueryMetadataInterface metadata, 
			CapabilitiesFinder capFinder, boolean afterJoinPlanning, AnalysisRecord record, CommandContext context) 
		throws QueryMetadataException, TeiidComponentException {
		
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
        
        return canRaiseOverJoin(joinNode.getChildren(), metadata, capFinder, crits, type, record, context);		
	}

    static Object canRaiseOverJoin(List<PlanNode> children,
                                           QueryMetadataInterface metadata,
                                           CapabilitiesFinder capFinder,
                                           List<Criteria> crits,
                                           JoinType type, AnalysisRecord record, CommandContext context) throws QueryMetadataException,
                                                         TeiidComponentException {
        //we only want to consider binary joins
        if (children.size() != 2) {
        	return null;
        }

    	Object modelID = null;
    	boolean multiSource = false;
        Set<Object> groupIDs = new HashSet<Object>();
        int groupCount = 0;
		LinkedList<CompareCriteria> thetaCriteria = new LinkedList<CompareCriteria>();
		SupportedJoinCriteria sjc = null;

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
        		sjc = CapabilitiesUtil.getSupportedJoinCriteria(accessModelID, metadata, capFinder);

        		//see if we can emulate the inner join using an outer
        		if (!type.isOuter() 
    				&& !CapabilitiesUtil.supports(Capability.QUERY_FROM_JOIN_INNER, accessModelID, metadata, capFinder) 
    				&& (crits != null) && !crits.isEmpty()) {
        			//TODO: the IS NOT NULL check is not strictly needed as we could check predicates to see if we are already null filtering
    				if (!CapabilitiesUtil.supports(Capability.CRITERIA_ISNULL, accessModelID, metadata, capFinder)
    						|| !CapabilitiesUtil.supports(Capability.CRITERIA_NOT, accessModelID, metadata, capFinder)) {
    					return null;
    				}
    				if (sjc == SupportedJoinCriteria.ANY) {
    					//quick check to see if we can find an element to be nullable
    					boolean valid = false;
    					for (Criteria crit : crits) {
        					if (!(crit instanceof CompareCriteria)) {
        			    		continue;
        			    	}
        			    	CompareCriteria cc = (CompareCriteria)crit;
        			    	if ((cc.getLeftExpression() instanceof ElementSymbol) 
        			    			|| (cc.getRightExpression() instanceof ElementSymbol)) {
        			    		valid = true;
        			    	}
						}
    					if (!valid) {
    						return null; //TODO: check if any of the already pushed predicates can satisfy
    					}
    				}
        		}
				
        		/*
        		 * Key joins must be left linear
        		 */
        		if (sjc == SupportedJoinCriteria.KEY && children.get(0).getGroups().size() != 1) {
        			return null;
        		}
        		
				if(crits != null && !crits.isEmpty()) {
					for (Criteria crit : crits) {
				        if (!isSupportedJoinCriteria(sjc, crit, accessModelID, metadata, capFinder, record)) {
				        	if (crit instanceof CompareCriteria) {
				    			CompareCriteria cc = (CompareCriteria) crit;
				    			if (cc.isOptional()) {
				    				cc.setOptional(true);
				    				continue;
				    			}
				    		}
				        	return null;
				        } else if (crit instanceof CompareCriteria) {
				        	thetaCriteria.add((CompareCriteria)crit);
				        }
					}
					if (sjc == SupportedJoinCriteria.KEY) {
						LinkedList<Expression> leftExpressions = new LinkedList<Expression>();
						LinkedList<Expression> rightExpressions = new LinkedList<Expression>();
						RuleChooseJoinStrategy.separateCriteria(children.get(0).getGroups(), children.get(1).getGroups(), leftExpressions, rightExpressions, crits, new LinkedList<Criteria>());
						ArrayList<Object> leftIds = new ArrayList<Object>(leftExpressions.size());
						ArrayList<Object> rightIds = new ArrayList<Object>(rightExpressions.size());
						for (Expression expr : leftExpressions) {
							if (expr instanceof ElementSymbol) {
								leftIds.add(((ElementSymbol) expr).getMetadataID());
							}
						}
						GroupSymbol rightGroup = null;
						for (Expression expr : rightExpressions) {
							if (expr instanceof ElementSymbol) {
								ElementSymbol es = (ElementSymbol) expr; 
								if (rightGroup == null) {
									rightGroup = es.getGroupSymbol();
								} else if (!rightGroup.equals(es.getGroupSymbol())) {
									return null;
								}
								rightIds.add(es.getMetadataID());
							}
						}
						if (rightGroup == null) {
							return null;
						}
						if (!matchesForeignKey(metadata, leftIds, rightIds,	children.get(0).getGroups().iterator().next(), true) 
								&& !matchesForeignKey(metadata, rightIds, leftIds, rightGroup, true)) {
							return null;
						}
					} 
                } 
				if (sjc != SupportedJoinCriteria.ANY && thetaCriteria.isEmpty()) {
                	return null; //cross join not supported
                }
				
				modelID = accessModelID;
				multiSource = childNode.hasBooleanProperty(Info.IS_MULTI_SOURCE);
				
			} else if(!CapabilitiesUtil.isSameConnector(modelID, accessModelID, metadata, capFinder)) { 
				return null;							
			} else if ((multiSource || childNode.hasBooleanProperty(Info.IS_MULTI_SOURCE)) && !context.getOptions().isImplicitMultiSourceJoin()) {
				//only allow raise if partitioned
				boolean multiSourceOther = childNode.hasBooleanProperty(Info.IS_MULTI_SOURCE);
				if (multiSource && multiSourceOther && (type == JoinType.JOIN_ANTI_SEMI || type == JoinType.JOIN_CROSS)) {
					return null;
				}
				ArrayList<Expression> leftExpressions = new ArrayList<Expression>();
				ArrayList<Expression> rightExpressions = new ArrayList<Expression>();
				RuleChooseJoinStrategy.separateCriteria(children.get(0).getGroups(), children.get(1).getGroups(), leftExpressions, rightExpressions, crits, new LinkedList<Criteria>());
				boolean needsOtherCrit = sjc != SupportedJoinCriteria.ANY;
				boolean partitioned = !multiSource || !multiSourceOther;
				for (int i = 0; i < leftExpressions.size() && (!partitioned || needsOtherCrit); i++) {
					boolean multi = isMultiSourceColumn(metadata, leftExpressions.get(i), children.get(0)) && isMultiSourceColumn(metadata, rightExpressions.get(i), children.get(1));
					if (multi) {
						partitioned = true;
					} else {
						needsOtherCrit = false;
					}
				}
				if (needsOtherCrit || !partitioned) {
					return null;
				}
			}
			
		} // end walking through join node's children

		int maxGroups = CapabilitiesUtil.getMaxFromGroups(modelID, metadata, capFinder);
		
		if (maxGroups != -1 && maxGroups < groupCount) {
		    return null;
		}
		
		if (sjc == SupportedJoinCriteria.KEY) {
			for (CompareCriteria criteria : thetaCriteria) {
				criteria.setOptional(false);
			}
		} else {
			//TODO: this should be done in a less arbitrary way, and what about composite keys?
			boolean hasCriteria = false;
			for (CompareCriteria criteria : thetaCriteria) {
				if (criteria.getIsOptional() == null || (!hasCriteria && criteria.getIsOptional())) {
					criteria.setOptional(false);
				}
				hasCriteria = true;
			}
		}
		
		return modelID;
    }
    
    /**
     * Checks criteria one predicate at a time.  Only tests up to the equi restriction.
     */
    static boolean isSupportedJoinCriteria(SupportedJoinCriteria sjc, Criteria crit, Object accessModelID, 
    		QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record) 
    throws QueryMetadataException, TeiidComponentException {
    	if(!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(crit, accessModelID, metadata, capFinder, record, true) ) { 
            return false;                        
        } 
        if (sjc == SupportedJoinCriteria.ANY) {
        	boolean subqueryOn = CapabilitiesUtil.supports(Capability.CRITERIA_ON_SUBQUERY, accessModelID, metadata, capFinder);
    		if (!subqueryOn && !ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(crit).isEmpty()) {
    			return false;
    		}
        	return true;
        }
        //theta join must be between elements with a compare predicate
    	if (!(crit instanceof CompareCriteria)) {
    		return false;
    	}
    	CompareCriteria cc = (CompareCriteria)crit;
    	if (!(cc.getLeftExpression() instanceof ElementSymbol) 
    			|| !(cc.getRightExpression() instanceof ElementSymbol)) {
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

	public static boolean matchesForeignKey(QueryMetadataInterface metadata,
			Collection<Object> leftIds, Collection<Object> rightIds, GroupSymbol leftGroup, boolean exact)
			throws TeiidComponentException, QueryMetadataException {
		Collection fks = metadata.getForeignKeysInGroup(leftGroup.getMetadataID());
		for (Object fk : fks) {
			String allow = metadata.getExtensionProperty(fk, ForeignKey.ALLOW_JOIN, false);
			if (allow != null && !Boolean.valueOf(allow)) {
				continue;
			}
			List fkColumns = metadata.getElementIDsInKey(fk);
			if ((exact && leftIds.size() != fkColumns.size()) || !leftIds.containsAll(fkColumns)) {
				continue;
			}
			Object pk = metadata.getPrimaryKeyIDForForeignKeyID(fk);
			List pkColumns = metadata.getElementIDsInKey(pk);
			if ((!exact || rightIds.size() == pkColumns.size()) && rightIds.containsAll(pkColumns)) {
				return true;
			}
		}
		return false;
	}
    
    static PlanNode raiseAccessOverJoin(PlanNode joinNode, PlanNode accessNode, Object modelID, boolean insert) {
		PlanNode leftAccess = joinNode.getFirstChild();
		PlanNode rightAccess = joinNode.getLastChild();
		
		PlanNode other = leftAccess == accessNode?rightAccess:leftAccess;

		// Remove old access nodes - this will automatically add children of access nodes to join node
		NodeEditor.removeChildNode(joinNode, leftAccess);
		NodeEditor.removeChildNode(joinNode, rightAccess);
        
        //Set for later possible use, even though this isn't an access node
        joinNode.setProperty(NodeConstants.Info.MODEL_ID, modelID);

		// Insert new access node above join node 
        accessNode.addGroups(other.getGroups());
        
        // Combine hints if necessary
        RulePlaceAccess.copyDependentHints(other, accessNode);
        RulePlaceAccess.copyDependentHints(joinNode, other);
        combineSourceHints(accessNode, other);
        
        if (other.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
        	accessNode.setProperty(Info.IS_MULTI_SOURCE, Boolean.TRUE);
        }
        String sourceName = (String)other.getProperty(Info.SOURCE_NAME);
        if (sourceName != null) {
        	accessNode.setProperty(Info.SOURCE_NAME, sourceName);
        }
        
        if (insert) {
            joinNode.addAsParent(accessNode);
        } else {
            accessNode.addFirstChild(joinNode);
        }
        
        return accessNode;
	}

    /**
     * Get modelID for Access node and cache the result in the Access node.
     * @param accessNode Access node
     * @param metadata Metadata access
     * @return Object Model ID or null if not found.
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    static Object getModelIDFromAccess(PlanNode accessNode, QueryMetadataInterface metadata) 
    throws QueryMetadataException, TeiidComponentException {

        Object accessModelID = accessNode.getProperty(NodeConstants.Info.MODEL_ID);
        if(accessModelID == null && accessNode.getGroups().size() > 0) {
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
                                     CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException {
        
        Object modelID = null;
        String sourceName = null;
        boolean multiSource = false;
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
        	//TODO: see if the children are actually multiSourced
            multiSource |= childNode.hasBooleanProperty(Info.IS_MULTI_SOURCE);
            String name = (String)childNode.getProperty(Info.SOURCE_NAME);
            
            // Reconcile this access node's model ID with existing                                             
            if(modelID == null) {
                modelID = accessModelID;
                
                Operation op = (Operation)setOpNode.getProperty(NodeConstants.Info.SET_OPERATION);
				if(! CapabilitiesUtil.supportsSetOp(accessModelID, op, metadata, capFinder)) {
                    return false;
                }
                if (multiSource && op != Operation.UNION) {
            		return false;
                }
            } else if(!CapabilitiesUtil.isSameConnector(modelID, accessModelID, metadata, capFinder)) {
                return false;
            } 
            if (!multiSource) {
            	if (sourceName == null) {
            		sourceName = name;
            	} else if (name != null && !sourceName.equals(name)) {
            		return false;
            	}
            }
            if (!setOpNode.hasBooleanProperty(NodeConstants.Info.USE_ALL) && !supportsDistinct(metadata, childNode, multiSource)) {
            	return false;
            }
        }
        return true;
    }

	static boolean supportsDistinct(QueryMetadataInterface metadata,
			PlanNode childNode, boolean multiSource)
			throws QueryMetadataException, TeiidComponentException {
		List<? extends Expression> project = (List)NodeEditor.findNodePreOrder(childNode, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS);

		if (multiSource) {
			boolean partitioned = isPartitioned(metadata, project, childNode);
			if (!partitioned) {
				return false;
			}
		}
		if (!CapabilitiesUtil.checkElementsAreSearchable(project, metadata, SupportConstants.Element.SEARCHABLE_COMPARE)) {
			return false;
		}
		return true;
	}

	static boolean isPartitioned(QueryMetadataInterface metadata,
			Collection<? extends Expression> project, PlanNode node) throws QueryMetadataException,
			TeiidComponentException {
		boolean partitioned = false;
		for (Expression expression : project) {
			Expression ex = SymbolMap.getExpression(expression);
			if (ex.getType() == DataTypeManager.DefaultDataClasses.STRING 
					&& isMultiSourceColumn(metadata, ex, node)) {
				partitioned = true;
				break;
			}
		}
		return partitioned;
	}

	/**
	 * Check to see if the element is a multi-source source_name column
	 * TODO: inner side of an outer join projection
	 * do this check as part of metadata validation
	 * 
	 */
	private static boolean isMultiSourceColumn(QueryMetadataInterface metadata,
			Expression ex, PlanNode node) throws QueryMetadataException,
			TeiidComponentException {
		if (!(ex instanceof ElementSymbol)) {
			return false;
		}
		ElementSymbol es = (ElementSymbol) ex;
		if (metadata.isMultiSourceElement(es.getMetadataID())) {
			return true;
		}
		if (node == null || node.getFirstChild() == null) {
			return false;
		}
		node = FrameUtil.findOriginatingNode(node.getFirstChild(), Collections.singleton(es.getGroupSymbol()));
		if (node == null || node.getType() != NodeConstants.Types.SOURCE) {
			return false;
		}
		SymbolMap map = (SymbolMap)node.getProperty(Info.SYMBOL_MAP);
		if (node.getChildren().isEmpty() || map == null) {
			return false;
		}
		PlanNode set = NodeEditor.findNodePreOrder(node.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
		if (set == null) {
			ex = map.getMappedExpression(es);
			return isMultiSourceColumn(metadata, ex, node.getFirstChild());
		}
		int index = map.getKeys().indexOf(ex);
		if (index == -1) {
			return false;
		}
		for (PlanNode child : set.getChildren()) {
			PlanNode project = NodeEditor.findNodePreOrder(child, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
			if (project == null) {
				return false;
			}
			List<Expression> cols = (List<Expression>) project.getProperty(Info.PROJECT_COLS);
			if (!isMultiSourceColumn(metadata, cols.get(index), child)) {
				return false;
			}
		}
		return true;
	}
    
	public String toString() {
		return "RaiseAccess"; //$NON-NLS-1$
	}
	
}
