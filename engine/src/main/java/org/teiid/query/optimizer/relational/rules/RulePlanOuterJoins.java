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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;

/*
 * TODO: may need to rerun plan joins sequence after this
 */
public class RulePlanOuterJoins implements OptimizerRule {
	
	@Override
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder, RuleStack rules,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryPlannerException, QueryMetadataException,
			TeiidComponentException {
		while (planLeftOuterJoinAssociativity(plan, metadata, capabilitiesFinder, analysisRecord, context)) {
			//repeat
		}
		return plan;
	}

	private boolean planLeftOuterJoinAssociativity(PlanNode plan,
			QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryMetadataException, TeiidComponentException {
		
		boolean changedAny = false;
    	LinkedHashSet<PlanNode> joins = new LinkedHashSet<PlanNode>(NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)); 
    	while (!joins.isEmpty()) {
    		Iterator<PlanNode> i = joins.iterator();
    		PlanNode join = i.next();
    		i.remove();
    		if (!join.getProperty(Info.JOIN_TYPE).equals(JoinType.JOIN_LEFT_OUTER)) {
    			continue;
    		}
    		PlanNode childJoin = null;
    		PlanNode other = null;
    		PlanNode left = join.getFirstChild();
    		PlanNode right = join.getLastChild();

    		if (left.getType() == NodeConstants.Types.JOIN && left.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER) {
    			childJoin = left;
    			other = right;
    		} else if (right.getType() == NodeConstants.Types.JOIN && (right.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER || right.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_INNER)) {
    			childJoin = right;
    			other = left;
    		} else {
    			continue;
    		}
    		
    		PlanNode cSource = other;
    		
			if (cSource.getType() != NodeConstants.Types.ACCESS) {
				continue;
			}
    		
    		List<Criteria> joinCriteria = (List<Criteria>) join.getProperty(Info.JOIN_CRITERIA);
    		if (!isCriteriaValid(joinCriteria, metadata, join)) {
    			continue;
    		}
    		
    		List<Criteria> childJoinCriteria = (List<Criteria>) childJoin.getProperty(Info.JOIN_CRITERIA);
    		if (!isCriteriaValid(childJoinCriteria, metadata, childJoin)) {
    			continue;
    		}
    		
    		//there are three forms we can take
    		// (a b) c -> a (b c) or (a c) b
    		// c (b a) -> (c b) a or (c a) b
    		Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(joinCriteria);
			if (Collections.disjoint(groups, FrameUtil.findJoinSourceNode(childJoin == left?childJoin.getFirstChild():childJoin.getLastChild()).getGroups())) {
				//case where absolute order remains the same
				PlanNode bSource = childJoin == left?childJoin.getLastChild():childJoin.getFirstChild();
				if (bSource.getType() != NodeConstants.Types.ACCESS) {
    				continue;
    			}
    			Object modelId = RuleRaiseAccess.canRaiseOverJoin(childJoin == left?Arrays.asList(bSource, cSource):Arrays.asList(cSource, bSource), metadata, capabilitiesFinder, joinCriteria, JoinType.JOIN_LEFT_OUTER, analysisRecord, context, false, false);
    			if (modelId == null) {
    				continue;
    			}
    			//rearrange
    			PlanNode newParent = RulePlanJoins.createJoinNode();
    			newParent.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
    			PlanNode newChild = RulePlanJoins.createJoinNode();
    			newChild.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
    			joins.remove(childJoin);
    			if (childJoin == left) {
    				//a (b c)
    				newChild.addFirstChild(childJoin.getLastChild());
    				newChild.addLastChild(other);
    				newChild.setProperty(Info.JOIN_CRITERIA, joinCriteria);
    				newParent.addFirstChild(childJoin.getFirstChild());
    				newParent.addLastChild(newChild);
    				newParent.setProperty(Info.JOIN_CRITERIA, childJoinCriteria);
    			} else {
    				//(c b) a
    				newChild.addFirstChild(other);
    				newChild.addLastChild(childJoin.getFirstChild());
    				newChild.setProperty(Info.JOIN_CRITERIA, joinCriteria);
    				newParent.addFirstChild(newChild);
    				newParent.addLastChild(childJoin.getLastChild());
    				newParent.setProperty(Info.JOIN_CRITERIA, childJoinCriteria);
    			}
				updateGroups(newChild);
				updateGroups(newParent);
				join.getParent().replaceChild(join, newParent);
				if(RuleRaiseAccess.checkConformedSubqueries(newChild.getFirstChild(), newChild, true)) {
                	RuleRaiseAccess.raiseAccessOverJoin(newChild, newChild.getFirstChild(), modelId, capabilitiesFinder, metadata, true);                    
    				changedAny = true;
                }
    		} else if (Collections.disjoint(groups, FrameUtil.findJoinSourceNode(childJoin == right?childJoin.getFirstChild():childJoin.getLastChild()).getGroups())) {
				PlanNode aSource = childJoin == left?childJoin.getFirstChild():childJoin.getLastChild();
				if (aSource.getType() != NodeConstants.Types.ACCESS) {
    				continue;
    			}

    			if (!join.getExportedCorrelatedReferences().isEmpty()) {
        			//TODO: we are not really checking that specifically
    				continue;
    			}
    			Object modelId = RuleRaiseAccess.canRaiseOverJoin(childJoin == left?Arrays.asList(aSource, cSource):Arrays.asList(cSource, aSource), metadata, capabilitiesFinder, joinCriteria, JoinType.JOIN_LEFT_OUTER, analysisRecord, context, false, false);
    			if (modelId == null) {
    				continue;
    			}
    			
    			//rearrange
    			PlanNode newParent = RulePlanJoins.createJoinNode();
    			newParent.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
    			PlanNode newChild = RulePlanJoins.createJoinNode();
    			newChild.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
    			joins.remove(childJoin);
    			
    			if (childJoin == left) {
    				newChild.addFirstChild(childJoin.getFirstChild());
	    			newChild.addLastChild(other);
	    			newParent.addLastChild(childJoin.getLastChild());
    			} else {
    				newChild.addFirstChild(other);
					newChild.addLastChild(childJoin.getLastChild());
					newParent.addLastChild(childJoin.getFirstChild());
    			}
    			newChild.addGroups(newChild.getFirstChild().getGroups());
				newChild.setProperty(Info.JOIN_CRITERIA, joinCriteria);
				newParent.addFirstChild(newChild);
				newParent.setProperty(Info.JOIN_CRITERIA, childJoinCriteria);
				updateGroups(newChild);
				updateGroups(newParent);
				join.getParent().replaceChild(join, newParent);
                if(RuleRaiseAccess.checkConformedSubqueries(newChild.getFirstChild(), newChild, true)) {
                	RuleRaiseAccess.raiseAccessOverJoin(newChild, newChild.getFirstChild(), modelId, capabilitiesFinder, metadata, true);                    
    				changedAny = true;
                }
    		}
    	}
    	return changedAny;
	}

	private void updateGroups(PlanNode node) {
		node.addGroups(GroupsUsedByElementsVisitor.getGroups(node.getCorrelatedReferenceElements()));
		node.addGroups(FrameUtil.findJoinSourceNode(node.getFirstChild()).getGroups());
		node.addGroups(FrameUtil.findJoinSourceNode(node.getLastChild()).getGroups());
	}
    
    private boolean isCriteriaValid(List<Criteria> joinCriteria, QueryMetadataInterface metadata, PlanNode join) {
    	if (joinCriteria.isEmpty()) {
    		return false;
    	}
		Set<GroupSymbol> groups = join.getGroups();
    	for (Criteria crit : joinCriteria) {
			if (JoinUtil.isNullDependent(metadata, groups, crit)) {
				return false;
			}
		}
    	return true;
    }
    
    @Override
    public String toString() {
    	return "PlanOuterJoins"; //$NON-NLS-1$
    }
    
}
