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
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 */
public final class RuleMergeCriteria implements OptimizerRule {

    /**
     * @see OptimizerRule#execute(PlanNode, QueryMetadataInterface, RuleStack)
     */
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, MetaMatrixComponentException {

        // Find strings of criteria and merge them, removing duplicates
        List<PlanNode> criteriaChains = new ArrayList<PlanNode>();
        findCriteriaChains(plan, criteriaChains);

        // Merge chains
        for (PlanNode critNode : criteriaChains) {
            mergeChain(critNode);
        }

        return plan;
    }

    /**
     * Walk the tree pre-order, looking for any chains of criteria
     * @param node Root node to search
     * @param foundNodes Roots of criteria chains
     */
     void findCriteriaChains(PlanNode root, List<PlanNode> foundNodes)
        throws QueryPlannerException, MetaMatrixComponentException {

        PlanNode recurseRoot = root;
        if(root.getType() == NodeConstants.Types.SELECT) {
            // Walk to end of the chain and change recurse root
            while(recurseRoot.getType() == NodeConstants.Types.SELECT) {
                recurseRoot = recurseRoot.getLastChild();
            }

            // Ignore trivial 1-node case
            if(recurseRoot.getParent() != root) {
                // Found root for chain
                foundNodes.add(root);
            }
        }
        
        if (recurseRoot.getType() != NodeConstants.Types.ACCESS) {
            for (PlanNode child : recurseRoot.getChildren()) {
                findCriteriaChains(child, foundNodes);
            }
        }
    }

    void mergeChain(PlanNode chainRoot) {

        // Remove all of chain except root, collect crit from each
        CompoundCriteria critParts = new CompoundCriteria();
        PlanNode current = chainRoot;
        boolean isDependentSet = false;
        while(current.getType() == NodeConstants.Types.SELECT) {
            critParts.addCriteria((Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA)); 
            
            isDependentSet |= current.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET);
            
            // Recurse
            PlanNode last = current;
            current = current.getLastChild();

            // Remove current
            if(last != chainRoot) {
                NodeEditor.removeChildNode(last.getParent(), last);
            }

        }
        
        Criteria combinedCrit = QueryRewriter.optimizeCriteria(critParts);

        if (isDependentSet) {
            chainRoot.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
        }
        
        // Replace criteria at root with new combined criteria
        chainRoot.setProperty(NodeConstants.Info.SELECT_CRITERIA, combinedCrit);
        
        // Reset group for node based on combined criteria
        chainRoot.getGroups().clear();
        
        chainRoot.addGroups(GroupsUsedByElementsVisitor.getGroups(combinedCrit));
        chainRoot.addGroups(GroupsUsedByElementsVisitor.getGroups(chainRoot.getCorrelatedReferenceElements()));
    }

    public String toString() {
        return "MergeCriteria"; //$NON-NLS-1$
    }

}
