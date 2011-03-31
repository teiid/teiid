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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class RulePushSelectCriteria implements OptimizerRule {
	
	private List<PlanNode> createdNodes;
	
	public List<PlanNode> getCreatedNodes() {
		return createdNodes;
	}
	
	public void setCreatedNodes(List<PlanNode> createdNodes) {
		this.createdNodes = createdNodes;
	}
    
	/**
	 * Execute the rule as described in the class comments.
	 * @param plan Incoming query plan, may be modified during method and may be returned from method
	 * @param metadata Metadata source
	 * @param rules Rules from optimizer rule stack, may be manipulated during method
	 * @return Updated query plan if rule fired, else original query plan
	 */
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

		// Initialize movedNode to true so the loop will start
		boolean movedAnyNode = true;

		// Create set of nodes that no longer need to be considered
		Set<PlanNode> deadNodes = new HashSet<PlanNode>();

		// Loop while criteria nodes are still being moved
		while(movedAnyNode) {

		    // Reset flag to false for this iteration
		    movedAnyNode = false;

    		// Find criteria nodes that could be pushed
		    List<PlanNode> critNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SELECT);
		    Collections.reverse(critNodes);
		    for (PlanNode critNode : critNodes) {
    			boolean isPhantom = critNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM);
	            boolean isCopied = critNode.hasBooleanProperty(NodeConstants.Info.IS_COPIED);
	            boolean isPushed = critNode.hasBooleanProperty(NodeConstants.Info.IS_PUSHED);
	            if (isPhantom || isCopied || isPushed || deadNodes.contains(critNode)) {
	            	continue;
	            }
	            
	            PlanNode sourceNode = findOriginatingNode(metadata, capFinder, critNode, analysisRecord);
	            
	            if(sourceNode == null) {
                    deadNodes.add(critNode);
	                continue;
	            }
	            pushTowardOriginatingNode(sourceNode, critNode, metadata, capFinder);
	            
                boolean moved = false;
                
                if((critNode.getGroups().isEmpty() && critNode.getSubqueryContainers().isEmpty()) || !atBoundary(critNode, sourceNode)) {
                    deadNodes.add(critNode);
                    continue;
                }
               
                switch (sourceNode.getType()) {
                    case NodeConstants.Types.SOURCE:
                    {
                        moved = pushAcrossFrame(sourceNode, critNode, metadata);
                        break;
                    }
                    case NodeConstants.Types.JOIN:
                    {
        				//pushing below a join is not necessary under an access node
        				if (NodeEditor.findParent(critNode, NodeConstants.Types.ACCESS) == null) {
                            moved = handleJoinCriteria(sourceNode, critNode, metadata);
                            break;
        				}
                    }
                }
                
                if (!moved) {
                    deadNodes.add(critNode);
                } else {
                    movedAnyNode = true;
                }
	    	}
		}

		return plan;
	}

	private PlanNode findOriginatingNode(QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder, PlanNode critNode, AnalysisRecord record)
			throws TeiidComponentException, QueryMetadataException {
		if (critNode.getGroups().isEmpty()) {
	        //check to see if pushing may impact cardinality
	        PlanNode groupNode = NodeEditor.findNodePreOrder(critNode, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE);
	        if (groupNode != null && !groupNode.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
	        	return groupNode;
	        }

			Object modelId = getSubqueryModelId(metadata, capFinder, critNode, record);
			if (modelId != null) {
				for (PlanNode node : NodeEditor.findAllNodes(critNode, NodeConstants.Types.SOURCE)) {
		            GroupSymbol group = node.getGroups().iterator().next();
		            Object srcModelID = metadata.getModelID(group.getMetadataID());
		            if(CapabilitiesUtil.isSameConnector(srcModelID, modelId, metadata, capFinder)) {
		                return node;
		            }
		        }
			}
		} 
		return FrameUtil.findOriginatingNode(critNode, critNode.getGroups());
	}

	private Object getSubqueryModelId(QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder, PlanNode critNode, AnalysisRecord record)
			throws TeiidComponentException, QueryMetadataException {
		Object modelId = null;
		for (SubqueryContainer subqueryContainer : critNode.getSubqueryContainers()) {
			Object validId = CriteriaCapabilityValidatorVisitor.validateSubqueryPushdown(subqueryContainer, null, metadata, capFinder, record);
			if (validId == null) {
				return null;
			}
			if (modelId == null) {
				modelId = validId;
			} else if (!CapabilitiesUtil.isSameConnector(modelId, validId, metadata, capFinder)) {
				return null;
			}
		}
		return modelId;
	}
    
    /**
     * Handles multi-group criteria originating at the given joinNode
     *  
     * @param joinNode
     * @param critNode
     * @return
     */
    private boolean handleJoinCriteria(PlanNode joinNode, PlanNode critNode, QueryMetadataInterface metadata) {
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        
        if (jt == JoinType.JOIN_CROSS || jt == JoinType.JOIN_INNER) {
            if (jt == JoinType.JOIN_CROSS) {
                joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
            }
            moveCriteriaIntoOnClause(critNode, joinNode);
        } else {
            JoinType optimized = JoinUtil.optimizeJoinType(critNode, joinNode, metadata);
            
            if (optimized == JoinType.JOIN_INNER) {
                moveCriteriaIntoOnClause(critNode, joinNode);
                return true; //return true since the join type has changed
            }
        }
        return false;
    }
    
    /** 
     * @param critNode
     * @param joinNode
     */
    private void moveCriteriaIntoOnClause(PlanNode critNode,
                                          PlanNode joinNode) {
        List joinCriteria = (List)joinNode.getProperty(NodeConstants.Info.JOIN_CRITERIA);
        Criteria criteria = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        
        //since the parser uses EMPTY_LIST, check for size 0 also
        if (joinCriteria == null || joinCriteria.size() == 0) {
            joinCriteria = new LinkedList();
            joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
        }
        
        if (!joinCriteria.contains(criteria)) {
            joinCriteria.add(criteria);
            if(critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
                joinNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
            }
        }
        NodeEditor.removeChildNode(critNode.getParent(), critNode);
    }

    /**
     *  
     * @param critNode
     * @param metadata
     * @param capFinder
     * @throws QueryPlannerException
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    void pushTowardOriginatingNode(PlanNode sourceNode, PlanNode critNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
		throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

    	boolean groupSelects = sourceNode.getParent().getType() == NodeConstants.Types.SELECT && sourceNode.getChildCount() == 0;
        
        //to keep a stable criteria ordering, move the sourceNode to the top of the criteria chain
        while (sourceNode.getParent().getType() == NodeConstants.Types.SELECT) {
            sourceNode = sourceNode.getParent();
            if (sourceNode == critNode) {
                return;
            }
        }

		// See how far we can move it towards the SOURCE node
		PlanNode destination = examinePath(critNode, sourceNode, metadata, capFinder);
        NodeEditor.removeChildNode(critNode.getParent(), critNode);
        destination.addAsParent(critNode);
        if (groupSelects && destination == sourceNode) {
        	//Help with the detection of composite keys in pushed criteria
        	RuleMergeCriteria.mergeChain(critNode, metadata);
        }
	}

    /**
	 * Examine the path from crit node to source node to determine how far down a node
	 * can be pushed.
	 * @return destinationChild
	 */
	PlanNode examinePath(PlanNode critNode, PlanNode sourceNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
		throws QueryPlannerException, TeiidComponentException {
        
		// Walk from source node up to critNode to build list of intervening nodes
		Stack<PlanNode> path = new Stack<PlanNode>();
		PlanNode currentNode = sourceNode.getParent();
		while(currentNode != critNode) {
			path.push(currentNode);
			currentNode = currentNode.getParent();
		}

		// Examine path in reverse order (by popping stack)
		while(! path.empty()) {
			currentNode = path.pop();
            
			// Look for situations where we don't allow SELECT to be pushed
			if(currentNode.getType() == NodeConstants.Types.ACCESS) {
                try {
                    if (!RuleRaiseAccess.canRaiseOverSelect(currentNode, metadata, capFinder, critNode, null)) {
                        return currentNode;
                    }
                    if (this.createdNodes == null) {
                    	satisfyAccessPatterns(critNode, currentNode);
                    }

                    if (critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
                        //once a dependent crit node is pushed, don't bother pushing it further into the command
                        //dependent access node will use this as an assumption for where dependent sets can appear in the command
                        critNode.setProperty(NodeConstants.Info.IS_PUSHED, Boolean.TRUE);
                        currentNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
                        return currentNode.getFirstChild();
                    } 
				} catch(QueryMetadataException e) {
                    throw new QueryPlannerException(e, QueryPlugin.Util.getString("ERR.015.004.0020", currentNode.getGroups())); //$NON-NLS-1$
				}
			} else if(currentNode.getType() == NodeConstants.Types.JOIN) {
				//pushing below a join is not necessary under an access node
				if (this.createdNodes == null && NodeEditor.findParent(currentNode, NodeConstants.Types.ACCESS) != null) {
					return currentNode;
				}
				
                // Check whether this criteria is on the inner side of an outer join.  
                // If so, can't push past the join
                JoinType jt = JoinUtil.getJoinTypePreventingCriteriaOptimization(currentNode, critNode);
                
                if(jt != null) {
                    //if we successfully optimized then this should no longer inhibit the criteria from being pushed
                    //since the criteria must then be on the outer side of an outer join or on either side of an inner join

                    JoinType optimized = JoinUtil.optimizeJoinType(critNode, currentNode, metadata);
                    
                    if (optimized == null || optimized.isOuter()) {
                        return currentNode;
                    }
                }  
            
                satisfyAccessPatterns(critNode, currentNode);
            } else if (FrameUtil.isOrderedLimit(currentNode)) {
                return currentNode;
            } else if (currentNode.getType() == NodeConstants.Types.GROUP && critNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING)) {
                return currentNode;
            }
		}

		return sourceNode;
	}

	boolean pushAcrossFrame(PlanNode sourceNode, PlanNode critNode, QueryMetadataInterface metadata)
		throws QueryPlannerException {
        
        //ensure that the criteria can be pushed further
        if (sourceNode.getChildCount() == 1 && FrameUtil.isOrderedLimit(sourceNode.getFirstChild())) {
            return false;
        }
        
        //check to see if this is a move across a union
        if (sourceNode.getChildCount() > 0) {
            PlanNode child = sourceNode.getFirstChild();
            child = FrameUtil.findOriginatingNode(child, child.getGroups());
            if (child != null && child.getType() == NodeConstants.Types.SET_OP) {
            	//only allow criteria without subqueires - node cloning doesn't allow for the proper creation of 
            	//multiple nodes with the same subqueries
                if (child == sourceNode.getFirstChild() && critNode.getSubqueryContainers().isEmpty()) {
                    return pushAcrossSetOp(critNode, child, metadata);
                } 
                //this could be an access node in the middle of the source and set op,
                //it is an odd case that is not supported for now
                return false;
            }
        }
        
		// See if we can move it towards the SOURCE node
        return moveNodeAcrossFrame(critNode, sourceNode, metadata);
	}

	/**
	 * All nodes between critNode and sourceNode must be SELECT nodes.
	 */
	boolean atBoundary(PlanNode critNode, PlanNode sourceNode) {
		// Walk from source node to critNode to check each intervening node
		PlanNode currentNode = sourceNode.getParent();
		while(currentNode != critNode) {
			if(currentNode.getType() != NodeConstants.Types.SELECT) {
				return false;
			}

			currentNode = currentNode.getParent();
		}

		return true;
	}

	boolean moveNodeAcrossFrame(PlanNode critNode, PlanNode sourceNode, QueryMetadataInterface metadata)
		throws QueryPlannerException {

	      // Check that sourceNode has a child to push across
        if(sourceNode.getChildCount() == 0) {
            return false;
        }

        PlanNode projectNode = NodeEditor.findNodePreOrder(sourceNode.getFirstChild(), NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
        if(FrameUtil.isProcedure(projectNode)) {
            return false;
        }
        
        SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        
        if (!createConvertedSelectNode(critNode, sourceNode.getGroups().iterator().next(), projectNode, symbolMap, metadata)) {
            return false;
        }
		
        satisfyAccessPatterns(critNode, sourceNode);
        
		// Mark critNode as a "phantom"
		critNode.setProperty(NodeConstants.Info.IS_PHANTOM, Boolean.TRUE);
		
		return true;
	}

    /** 
     * @param critNode
     * @param sourceNode
     */
    static void satisfyAccessPatterns(PlanNode critNode,
                                       PlanNode sourceNode) {
        List aps = (List)sourceNode.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
       
        if (aps == null) {
            return;
        }

        Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        
        Collection<ElementSymbol> elements = getElementsIncriteria(crit);
                        
        boolean removeAps = satisfyAccessPatterns(aps, elements);
        if (removeAps) {
            sourceNode.removeProperty(NodeConstants.Info.ACCESS_PATTERNS);
            return;
        } 
 
        Collections.sort(aps);
    }
    
    static Collection<ElementSymbol> getElementsIncriteria(Criteria crit) {
        Collection<ElementSymbol> elements = new HashSet<ElementSymbol>();
        boolean first = true;
        if(crit instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) crit;
            for(Criteria subCrit : compCrit.getCriteria()) {
                if(compCrit.getOperator() == CompoundCriteria.AND || first) {
                    first = false;
                    elements.addAll(getElementsIncriteria(subCrit));
                } else {
                    elements.retainAll(getElementsIncriteria(subCrit));
                }
            } 
        } else {
            elements.addAll(ElementCollectorVisitor.getElements(crit, true));        
        }
        return elements;
    }
    
    /** 
     * @param aps
     * @param elements
     * @return
     */
    static boolean satisfyAccessPatterns(List<AccessPattern> aps, Collection<ElementSymbol> elements) {
    	for (AccessPattern ap : aps) {
            ap.getUnsatisfied().removeAll(elements);
            if (ap.getUnsatisfied().isEmpty()) {
                return true;
            }
        }
        return false;
    }

	PlanNode copyNode(PlanNode critNode) {
		// Create new copy node
		PlanNode copyNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);

		// Copy criteria
		Criteria crit = (Criteria) critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
		Criteria copyCrit = (Criteria) crit.clone();
		copyNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, copyCrit);
		copyNode.addGroups(critNode.getGroups());
        if(critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
            copyNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
        }
        if (createdNodes != null) {
        	createdNodes.add(copyNode);
        }
	    return copyNode;
	}

	boolean pushAcrossSetOp(PlanNode critNode, PlanNode setOp, QueryMetadataInterface metadata)
		throws QueryPlannerException {
        
        // Find source node above union and grab the symbol map
        PlanNode sourceNode = NodeEditor.findParent(setOp, NodeConstants.Types.SOURCE);
        GroupSymbol virtualGroup = sourceNode.getGroups().iterator().next();
        satisfyAccessPatterns(critNode, sourceNode);
        
        SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        SymbolMap childMap = symbolMap;
        
		// Move criteria to first child of union - names are the same, so no symbol mapping
		LinkedList<PlanNode> unionChildren = new LinkedList<PlanNode>();
		collectUnionChildren(setOp, unionChildren);

        int movedCount = 0;

        for (PlanNode planNode : unionChildren) {
		      // Find first project node
	        PlanNode projectNode = NodeEditor.findNodePreOrder(planNode, NodeConstants.Types.PROJECT);
		    
	        if (childMap == null) {
	        	childMap = SymbolMap.createSymbolMap(symbolMap.getKeys(), (List) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS));
	        }
		    
			// Move the node
			if(createConvertedSelectNode(critNode, virtualGroup, projectNode, childMap, metadata)) {
                movedCount++;
            }
			
			childMap = null; //create a new symbol map for the other children
		}
        
		//TODO - the logic here could be made more intelligent about EXCEPT and INTERSECT.
        if(movedCount == unionChildren.size()) {
            critNode.setProperty(NodeConstants.Info.IS_PHANTOM, Boolean.TRUE);
            return true;
        }
        //otherwise mark it as pushed so we don't consider it again
        critNode.setProperty(NodeConstants.Info.IS_PUSHED, Boolean.TRUE);
        return false;
	}

	static void collectUnionChildren(PlanNode unionNode, List<PlanNode> unionChildren) {
	    for (PlanNode child : unionNode.getChildren()) {
	        if(child.getType() == NodeConstants.Types.SET_OP) {
	            collectUnionChildren(child, unionChildren);
	        } else {
	            unionChildren.add(child);
	        }
        }
	}

    private boolean createConvertedSelectNode(PlanNode critNode,
    							   GroupSymbol sourceGroup,
                                   PlanNode projectNode,
                                   SymbolMap symbolMap,
                                   QueryMetadataInterface metadata) throws QueryPlannerException {
        // If projectNode has children, then it is from a SELECT without a FROM and the criteria should not be pushed
        if(projectNode.getChildCount() == 0) {
            return false;
        }

        Criteria crit = (Criteria) critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);

        Boolean conversionResult = checkConversion(symbolMap, ElementCollectorVisitor.getElements(crit, true));
        
        if (conversionResult == Boolean.FALSE) {
        	return false; //not convertable
        }
        
        if (!critNode.getSubqueryContainers().isEmpty() 
        		&& checkConversion(symbolMap, critNode.getCorrelatedReferenceElements()) != null) {
    		return false; //not convertable, or has an aggregate for a correlated reference
        }
        
        PlanNode copyNode = copyNode(critNode);

        if (conversionResult == Boolean.TRUE) {
            copyNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
        }

        FrameUtil.convertNode(copyNode, sourceGroup, null, symbolMap.asMap(), metadata, true);  
        PlanNode intermediateParent = NodeEditor.findParent(projectNode, NodeConstants.Types.ACCESS, NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP);
        if (intermediateParent != null) {
            intermediateParent.addAsParent(copyNode);
        } else {
        	projectNode.getFirstChild().addAsParent(copyNode);
        }
		return true;
    }

	private Boolean checkConversion(SymbolMap symbolMap,
			Collection<ElementSymbol> elements) {
		Boolean result = null;
        
        for (ElementSymbol element : elements) {
            Expression converted = symbolMap.getMappedExpression(element);

            if(converted == null) {
                return false;
            }
            
            Collection<SubqueryContainer> scalarSubqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(converted);
            if (!scalarSubqueries.isEmpty()){
                return false;
            }
            
            if (!AggregateSymbolCollectorVisitor.getAggregates(converted, false).isEmpty()) {
                result = Boolean.TRUE;
            }
        }
		return result;
	}
    
	public String toString() {
		return "PushSelectCriteria"; //$NON-NLS-1$
	}

}
