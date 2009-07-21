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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 * Removes optional join nodes if elements originating from that join are not used in the 
 * top level project symbols.
 */
public class RuleRemoveOptionalJoins implements
                                    OptimizerRule {

    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   MetaMatrixComponentException {

        try {
            removeOptionalJoinNodes(plan, metadata, capFinder, null);
        } catch (QueryResolverException e) {
            throw new MetaMatrixComponentException(e);
        }
        return plan;
    }
    
    /**
     * remove optional from top to bottom
     * @throws QueryResolverException 
     */ 
    private boolean removeOptionalJoinNodes(PlanNode node,
                                            QueryMetadataInterface metadata,
                                            CapabilitiesFinder capFinder, Set<ElementSymbol> elements) throws QueryPlannerException,
                                                                         QueryMetadataException,
                                                                         MetaMatrixComponentException, QueryResolverException {
        if (node.getChildCount() == 0) {
            return false;
        }
        
        boolean isRoot = false;
        
        switch (node.getType()) {
        
        	case NodeConstants.Types.JOIN:
        	{
        		if (removedJoin(node, node.getFirstChild(), elements, metadata)) {
        			return true;
        		}
        		if (removedJoin(node, node.getLastChild(), elements, metadata)) {
        			return true;
        		}
                List crits = (List)node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                ElementCollectorVisitor.getElements(crits, elements);
                elements.addAll(node.getCorrelatedReferenceElements());
                break;
        	}
            case NodeConstants.Types.PROJECT:
            {
            	//skip to the child project
                if (node.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
                	elements = null;
                	node = NodeEditor.findNodePreOrder(node.getFirstChild(), NodeConstants.Types.PROJECT);
                }
                	
                if (elements == null) {
                    isRoot = true;
                    elements = new HashSet<ElementSymbol>();
                }
                
                //if it is a grouping scenario just make it a root
                if (!isRoot && NodeEditor.findNodePreOrder(node.getFirstChild(), NodeConstants.Types.GROUP, NodeConstants.Types.PROJECT) != null) {
                    isRoot = true;
                }
                
                if (isRoot) {
                    List columns = (List)node.getProperty(NodeConstants.Info.PROJECT_COLS);
                    ElementCollectorVisitor.getElements(columns, elements);
                    elements.addAll(node.getCorrelatedReferenceElements());
                }
                
                break;
            }
            case NodeConstants.Types.SOURCE:
            {
                if (elements == null) {
                    break;
                }
                SymbolMap symbolMap = (SymbolMap)node.getProperty(NodeConstants.Info.SYMBOL_MAP);
                Set convertedElements = new HashSet();
                for (ElementSymbol element : elements) {
                    Expression convertedExpression = symbolMap.getMappedExpression(element);
                    if (convertedExpression != null) {
                        ElementCollectorVisitor.getElements(convertedExpression, convertedElements);
                    }
                }
                elements = convertedElements;
                isRoot = true;
                break;
            }
            case NodeConstants.Types.SELECT: //covers having as well
            {
                if (elements != null) {
                    Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                    ElementCollectorVisitor.getElements(crit, elements);
                    elements.addAll(node.getCorrelatedReferenceElements());
                }
                break;
            }
            case NodeConstants.Types.SORT:
            {
                if (elements != null) {
                    List sortOrder = (List)node.getProperty(NodeConstants.Info.SORT_ORDER);
                    ElementCollectorVisitor.getElements(sortOrder, elements);
                }
                break;
            }
            case NodeConstants.Types.SET_OP:
            case NodeConstants.Types.DUP_REMOVE:
            {
                //allow project nodes to be seen as roots
                elements = null;
                break;
            }
        }
        
        //if this is a root then keep removing optional nodes until none can be removed
        if (isRoot) {
            boolean optionalRemoved = false;
            do {
                optionalRemoved = removeOptionalJoinNodes(node.getFirstChild(), metadata, capFinder, elements);
            } while (optionalRemoved);
            return false;
        }

        //otherwise recurse through the children
        Iterator iter = node.getChildren().iterator();

        while (node.getChildCount() >= 1 && iter.hasNext()) {
            if (removeOptionalJoinNodes((PlanNode)iter.next(), metadata, capFinder, elements)) {
                return true;
            }
        }
        return false;
    }

    /**
     * remove the optional node if possible
     * @throws QueryPlannerException 
     */ 
    private boolean removedJoin(PlanNode joinNode, PlanNode optionalNode,
                                           Set elements, QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException, QueryPlannerException {
        Set groups = optionalNode.getGroups();
        
        Assertion.isNotNull(elements);
        
        for (Iterator i = elements.iterator(); i.hasNext();) {
            ElementSymbol symbol = (ElementSymbol)i.next();
            if (groups.contains(symbol.getGroupSymbol())) {
                return false;  //groups contain an output symbol, don't remove
            }
        }
        
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        
        if (!optionalNode.hasBooleanProperty(NodeConstants.Info.IS_OPTIONAL) && 
        		!(jt == JoinType.JOIN_LEFT_OUTER && optionalNode == joinNode.getLastChild() && isDistinct(joinNode.getParent()))) {
        	return false;
        }
    	// remove the parent node and move the sibling node upward
		PlanNode parentNode = joinNode.getParent();
		joinNode.removeChild(optionalNode);
		NodeEditor.removeChildNode(parentNode, joinNode);

		// correct the parent nodes that may be using optional elements
		for (GroupSymbol optionalGroup : optionalNode.getGroups()) {
			List<ElementSymbol> optionalElements = ResolverUtil.resolveElementsInGroup(optionalGroup, metadata);
			List<Constant> replacements = new ArrayList<Constant>(optionalElements.size());
			for (ElementSymbol elementSymbol : optionalElements) {
				replacements.add(new Constant(null, elementSymbol.getType()));
			}
			FrameUtil.convertFrame(parentNode, optionalGroup, null, SymbolMap.createSymbolMap(optionalElements, replacements).asMap(), metadata);
		}

		return true;
    }
    
    /**
     * Ensure that the needed elements come only from the left hand side and 
     * that cardinality won't matter
     */
    private boolean isDistinct(PlanNode parent) {
		while (parent != null) {
			switch (parent.getType()) {
				case NodeConstants.Types.DUP_REMOVE: {
					return true;
				}
				case NodeConstants.Types.SET_OP: {
					if (!parent.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
						return true;
					}
					break;
				}
				case NodeConstants.Types.GROUP: {
					Set<AggregateSymbol> aggs = RulePushAggregates.collectAggregates(parent);
					for (AggregateSymbol aggregateSymbol : aggs) {
						if (aggregateSymbol.getAggregateFunction().equalsIgnoreCase(ReservedWords.COUNT) || 
								aggregateSymbol.getAggregateFunction().equalsIgnoreCase(ReservedWords.AVG)) {
							return false;
						}
					}
					return true;
				}
			}
			parent = parent.getParent();
		}
		return false;
	}

    public String toString() {
        return "RuleRemoveOptionalJoins"; //$NON-NLS-1$
    }

}
