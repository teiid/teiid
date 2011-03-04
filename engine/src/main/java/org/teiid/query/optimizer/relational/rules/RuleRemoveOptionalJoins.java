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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
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
    	List<PlanNode> projectNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.PROJECT);
    	HashSet<PlanNode> skipNodes = new HashSet<PlanNode>();
    	for (PlanNode projectNode : projectNodes) {
    		if (projectNode.getChildCount() == 0 || projectNode.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
    			continue;
    		}
    		PlanNode groupNode = NodeEditor.findNodePreOrder(projectNode, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN);
    		if (groupNode != null) {
    			projectNode = groupNode;
    		}
        	Set<GroupSymbol> requiredForOptional = getRequiredGroupSymbols(projectNode.getFirstChild());
    		boolean done = false;
    		while (!done) {
    			done = true;
		    	List<PlanNode> joinNodes = NodeEditor.findAllNodes(projectNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
		    	for (PlanNode planNode : joinNodes) {
		    		if (skipNodes.contains(planNode)) {
		    			continue;
		    		}
		    		if (!planNode.getExportedCorrelatedReferences().isEmpty()) {
		    			skipNodes.add(planNode);
		    			continue;
		    		}
		    		Set<GroupSymbol> required = getRequiredGroupSymbols(planNode);
		    		
		    		List<PlanNode> removed = removeJoin(required, requiredForOptional, planNode, planNode.getFirstChild(), analysisRecord);
		    		if (removed != null) {
		    			skipNodes.addAll(removed);
		    			done = false;
		    			continue;
		    		}
		    		removed = removeJoin(required, requiredForOptional, planNode, planNode.getLastChild(), analysisRecord);
		    		if (removed != null) {
		    			skipNodes.addAll(removed);
		    			done = false;
		    		}
				}
    		}
    	}
        return plan;
    }

	private Set<GroupSymbol> getRequiredGroupSymbols(PlanNode planNode) {
		return GroupsUsedByElementsVisitor.getGroups((Collection<? extends LanguageObject>)planNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
	}
    
    /**
     * remove the optional node if possible
     * @throws QueryPlannerException 
     * @throws TeiidComponentException 
     * @throws QueryMetadataException 
     */ 
    private List<PlanNode> removeJoin(Set<GroupSymbol> required, Set<GroupSymbol> requiredForOptional, PlanNode joinNode, PlanNode optionalNode, AnalysisRecord record) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
    	boolean correctFrame = false;
    	boolean isOptional = optionalNode.hasBooleanProperty(NodeConstants.Info.IS_OPTIONAL);
    	if (isOptional) {
    		required = requiredForOptional;
			correctFrame = true;
			//prevent bridge table removal
			HashSet<GroupSymbol> joinGroups = new HashSet<GroupSymbol>();
    		PlanNode parentNode = joinNode;
    		while (parentNode.getType() != NodeConstants.Types.PROJECT) {
    			PlanNode current = parentNode;
    			parentNode = parentNode.getParent();
    			if (current.getType() != NodeConstants.Types.SELECT && current.getType() != NodeConstants.Types.JOIN) {
    				continue;
    			}
    			Set<GroupSymbol> currentGroups = current.getGroups();
				if (current.getType() == NodeConstants.Types.JOIN) {
					currentGroups = GroupsUsedByElementsVisitor.getGroups((List<Criteria>)current.getProperty(NodeConstants.Info.JOIN_CRITERIA));
				}
				if (!Collections.disjoint(currentGroups, optionalNode.getGroups()) && !optionalNode.getGroups().containsAll(currentGroups)) {
					//we're performing a join
					boolean wasEmpty = joinGroups.isEmpty();
					boolean modified = joinGroups.addAll(current.getGroups());
					if (!wasEmpty && modified) {
						return null;
					}
				}
    		}
		}
        if (!Collections.disjoint(optionalNode.getGroups(), required)) {
        	return null;
        }
    	
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        
        if (!isOptional && 
        		(jt != JoinType.JOIN_LEFT_OUTER || optionalNode != joinNode.getLastChild() || useNonDistinctRows(joinNode.getParent()))) {
        	return null;
        }
    	// remove the parent node and move the sibling node upward
		PlanNode parentNode = joinNode.getParent();
		joinNode.removeChild(optionalNode);
		joinNode.getFirstChild().setProperty(NodeConstants.Info.OUTPUT_COLS, joinNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
		NodeEditor.removeChildNode(parentNode, joinNode);
		if (record != null && record.recordDebug()) {
			record.println("Removing join node since " + (isOptional?"it was marked as optional ":"it will not affect the results") + joinNode); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}

		while (parentNode.getType() != NodeConstants.Types.PROJECT) {
			PlanNode current = parentNode;
			parentNode = parentNode.getParent();
			if (correctFrame) {
				if (current.getType() == NodeConstants.Types.SELECT) {
					if (!Collections.disjoint(current.getGroups(), optionalNode.getGroups())) {
						current.getFirstChild().setProperty(NodeConstants.Info.OUTPUT_COLS, current.getProperty(NodeConstants.Info.OUTPUT_COLS));
						NodeEditor.removeChildNode(parentNode, current);
					}
				} else if (current.getType() == NodeConstants.Types.JOIN) {
					if (!Collections.disjoint(current.getGroups(), optionalNode.getGroups())) {
						List<Criteria> crits = (List<Criteria>) current.getProperty(NodeConstants.Info.JOIN_CRITERIA);
						if (crits != null && !crits.isEmpty()) {
							for (Iterator<Criteria> iterator = crits.iterator(); iterator.hasNext();) {
								Criteria criteria = iterator.next();
								if (!Collections.disjoint(GroupsUsedByElementsVisitor.getGroups(criteria), optionalNode.getGroups())) {
									iterator.remove();
								}
							}
							if (crits.isEmpty()) {
								JoinType joinType = (JoinType) current.getProperty(NodeConstants.Info.JOIN_TYPE);
								if (joinType == JoinType.JOIN_INNER) {
									current.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
								}
							}
						}
					}
				}
			} else if (current.getType() != NodeConstants.Types.JOIN) { 
				break;
			}
			if (current.getType() == NodeConstants.Types.JOIN) { 
				current.getGroups().removeAll(optionalNode.getGroups());
			}
		}
		
		return NodeEditor.findAllNodes(optionalNode, NodeConstants.Types.JOIN);
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
