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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.core.util.Assertion;
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
import com.metamatrix.query.resolver.util.AccessPattern;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public final class RulePushSelectCriteria implements OptimizerRule {
    
	/**
	 * Execute the rule as described in the class comments.
	 * @param plan Incoming query plan, may be modified during method and may be returned from method
	 * @param metadata Metadata source
	 * @param rules Rules from optimizer rule stack, may be manipulated during method
	 * @return Updated query plan if rule fired, else original query plan
	 */
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

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
	            
	            PlanNode sourceNode = findOriginatingNode(metadata, capFinder, critNode);
	            
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
                        moved = pushAcrossFrame(sourceNode, critNode);
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
			CapabilitiesFinder capFinder, PlanNode critNode)
			throws MetaMatrixComponentException, QueryMetadataException {
		if (critNode.getGroups().isEmpty()) {
			Object modelId = getSubqueryModelId(metadata, capFinder, critNode);
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
			CapabilitiesFinder capFinder, PlanNode critNode)
			throws MetaMatrixComponentException, QueryMetadataException {
		Object modelId = null;
		for (SubqueryContainer subqueryContainer : critNode.getSubqueryContainers()) {
			Object validId = CriteriaCapabilityValidatorVisitor.validateSubqueryPushdown(subqueryContainer, null, metadata, capFinder);
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
     * @throws MetaMatrixComponentException
     */
    void pushTowardOriginatingNode(PlanNode sourceNode, PlanNode critNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

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
	}

    /**
	 * Examine the path from crit node to source node to determine how far down a node
	 * can be pushed.
	 * @return destinationChild
	 */
	PlanNode examinePath(PlanNode critNode, PlanNode sourceNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
		throws QueryPlannerException, MetaMatrixComponentException {
        
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
                    if (!RuleRaiseAccess.canRaiseOverSelect(currentNode, metadata, capFinder, critNode)) {
                        return currentNode;
                    }
                    
                    satisfyAccessPatterns(critNode, currentNode);
                    
                    if (critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
                        //once a dependent crit node is pushed, don't bother pushing it further into the command
                        //dependent access node will use this as an assumption for where dependent sets can appear in the command
                        critNode.setProperty(NodeConstants.Info.IS_PUSHED, Boolean.TRUE);
                        currentNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
                        return currentNode.getFirstChild();
                    } 
				} catch(QueryMetadataException e) {
                    throw new QueryPlannerException(e, QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0020, currentNode.getGroups()));
				}
			} else if(currentNode.getType() == NodeConstants.Types.JOIN) {
				//pushing below a join is not necessary under an access node
				if (NodeEditor.findParent(currentNode, NodeConstants.Types.ACCESS) != null) {
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
            } else if (currentNode.getType() == NodeConstants.Types.TUPLE_LIMIT && currentNode.getChildCount() == 1 && currentNode.getFirstChild().getType() == NodeConstants.Types.SORT) {
                return currentNode;
            } else if (currentNode.getType() == NodeConstants.Types.GROUP && critNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING)) {
                return currentNode;
            }
		}

		return sourceNode;
	}

	boolean pushAcrossFrame(PlanNode sourceNode, PlanNode critNode)
		throws QueryPlannerException {
        
        //ensure that the criteria can be pushed further
        if (sourceNode.getChildCount() == 1 && sourceNode.getFirstChild().getType() == NodeConstants.Types.TUPLE_LIMIT && 
                        sourceNode.getFirstChild().getChildCount() == 1 && sourceNode.getFirstChild().getFirstChild().getType() == NodeConstants.Types.SORT) {
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
                    return pushAcrossSetOp(critNode, child);
                } 
                //this could be an access node in the middle of the source and set op,
                //it is an odd case that is not supported for now
                return false;
            }
        }
        
		// See if we can move it towards the SOURCE node
        return moveNodeAcrossFrame(critNode, sourceNode);
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

	boolean moveNodeAcrossFrame(PlanNode critNode, PlanNode sourceNode)
		throws QueryPlannerException {

		Assertion.isNotNull(critNode.getParent());
		
	      // Check that sourceNode has a child to push across
        if(sourceNode.getChildCount() == 0) {
            return false;
        }

        PlanNode projectNode = NodeEditor.findNodePreOrder(sourceNode.getFirstChild(), NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
        if(FrameUtil.isProcedure(projectNode)) {
            return false;
        }
        
        SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        
        if (!createConvertedSelectNode(critNode, sourceNode.getGroups().iterator().next(), projectNode, symbolMap)) {
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
        
        Collection elements = getElementsIncriteria(crit);
                        
        boolean removeAps = satisfyAccessPatterns(aps, elements);
        if (removeAps) {
            sourceNode.removeProperty(NodeConstants.Info.ACCESS_PATTERNS);
            return;
        } 
 
        Collections.sort(aps);
    }
    
    static Collection getElementsIncriteria(Criteria crit) {
        Collection elements = new HashSet();
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
    static boolean satisfyAccessPatterns(List aps, Collection elements) {
        for (Iterator i = aps.iterator(); i.hasNext();) {
            AccessPattern ap = (AccessPattern)i.next();
            if (ap.getCurrentElements().containsAll(elements)) {
                ap.getUnsatisfied().removeAll(elements);
                if (ap.getUnsatisfied().isEmpty()) {
                    return true;
                }
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
	    return copyNode;
	}

	boolean pushAcrossSetOp(PlanNode critNode, PlanNode setOp)
		throws QueryPlannerException {
        
        // Find source node above union and grab the symbol map
        PlanNode sourceNode = NodeEditor.findParent(setOp, NodeConstants.Types.SOURCE);
        GroupSymbol virtualGroup = sourceNode.getGroups().iterator().next();
        satisfyAccessPatterns(critNode, sourceNode);
        
        SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);

		// Move criteria to first child of union - names are the same, so no symbol mapping
		LinkedList unionChildren = new LinkedList();
		collectUnionChildren(setOp, unionChildren);

        int movedCount = 0;
		Iterator childIter = unionChildren.iterator();
		PlanNode firstChild = (PlanNode) childIter.next();
        GroupSymbol sourceGroup = sourceNode.getGroups().iterator().next();
        
        PlanNode firstBranchNode = NodeEditor.findNodePreOrder(firstChild, NodeConstants.Types.PROJECT);
        
        if(createConvertedSelectNode(critNode, virtualGroup, firstBranchNode, symbolMap)) {
            movedCount++;
        }

        // Find project cols on first branch
        List firstProjectCols = (List) firstBranchNode.getProperty(NodeConstants.Info.PROJECT_COLS);

		// For each of the remaining children of the union, push separately
		while(childIter.hasNext()) {
		    PlanNode childNode = (PlanNode) childIter.next();

		      // Find first project node
	        PlanNode projectNode = NodeEditor.findNodePreOrder(childNode, NodeConstants.Types.PROJECT);
		    
	        // Create symbol map
            symbolMap = SymbolMap.createSymbolMap(sourceGroup, firstProjectCols, (List) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS));
		    
			// Move the node
			if(createConvertedSelectNode(critNode, virtualGroup, projectNode, symbolMap)) {
                movedCount++;
            }
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

	void collectUnionChildren(PlanNode unionNode, LinkedList<PlanNode> unionChildren) {
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
                                   SymbolMap symbolMap) throws QueryPlannerException {
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

        FrameUtil.convertNode(copyNode, sourceGroup, null, symbolMap.asMap());  
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
            
            Collection scalarSubqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(converted);
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
