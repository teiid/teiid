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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Assertion;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.CommandContext;


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
                                                   TeiidComponentException {

        try {
            removeOptionalJoinNodes(plan, metadata, capFinder, null);
        } catch (QueryResolverException e) {
            throw new TeiidComponentException(e);
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
                                                                         TeiidComponentException, QueryResolverException {
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
                    OrderBy sortOrder = (OrderBy)node.getProperty(NodeConstants.Info.SORT_ORDER);
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
                                           Set elements, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
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
        		(jt != JoinType.JOIN_LEFT_OUTER || optionalNode != joinNode.getLastChild() || useNonDistinctRows(joinNode.getParent()))) {
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
    static boolean useNonDistinctRows(PlanNode parent) {
		while (parent != null) {
			if (parent.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
				return false;
			}
			switch (parent.getType()) {
				case NodeConstants.Types.DUP_REMOVE: {
					return false;
				}
				case NodeConstants.Types.SET_OP: {
					if (!parent.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
						return false;
					}
					break;
				}
				case NodeConstants.Types.GROUP: {
					Set<AggregateSymbol> aggs = RulePushAggregates.collectAggregates(parent);
					return areAggregatesCardinalityDependent(aggs);
				}
				case NodeConstants.Types.TUPLE_LIMIT: {
					return true;
				}
				//we assmue that projects of non-deterministic expressions do not matter
			}
			parent = parent.getParent();
		}
		return true;
	}

	static boolean areAggregatesCardinalityDependent(Set<AggregateSymbol> aggs) {
		for (AggregateSymbol aggregateSymbol : aggs) {
			if (isCardinalityDependent(aggregateSymbol)) {
				return true;
			}
		}
		return false;
	}
	
	static boolean isCardinalityDependent(AggregateSymbol aggregateSymbol) {
		if (aggregateSymbol.isDistinct()) {
			return false;
		}
		switch (aggregateSymbol.getAggregateFunction()) {
		case COUNT:
		case AVG:
		case STDDEV_POP:
		case STDDEV_SAMP:
		case VAR_POP:
		case VAR_SAMP:
		case SUM:
			return true;
		}
		return false;
	}

    public String toString() {
        return "RuleRemoveOptionalJoins"; //$NON-NLS-1$
    }

}
