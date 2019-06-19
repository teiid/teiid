/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;

public final class RuleMergeCriteria implements OptimizerRule {

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, TeiidComponentException {
        // Find strings of criteria and merge them, removing duplicates
        List<PlanNode> criteriaChains = new ArrayList<PlanNode>();
        findCriteriaChains(plan, criteriaChains);

        // Merge chains
        for (PlanNode critNode : criteriaChains) {
            mergeChain(critNode, metadata);
        }
        return plan;
    }

    /**
     * Walk the tree pre-order, looking for any chains of criteria
     * @param root Root node to search
     * @param foundNodes Roots of criteria chains
     */
     void findCriteriaChains(PlanNode root, List<PlanNode> foundNodes)
        throws QueryPlannerException, TeiidComponentException {

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

    static void mergeChain(PlanNode chainRoot, QueryMetadataInterface metadata) {
        // Remove all of chain except root, collect crit from each
        CompoundCriteria critParts = new CompoundCriteria();
        LinkedList<Criteria> subqueryCriteria = new LinkedList<Criteria>();
        PlanNode current = chainRoot;
        boolean isDependentSet = false;
        while(current.getType() == NodeConstants.Types.SELECT) {
            if (!current.getCorrelatedReferenceElements().isEmpty()) {
                //add at the end for delayed evaluation
                subqueryCriteria.add(0, (Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA));
            } else {
                critParts.getCriteria().add(0, (Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA));
            }

            isDependentSet |= current.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET);

            // Recurse
            PlanNode last = current;
            current = current.getLastChild();

            // Remove current
            if(last != chainRoot) {
                NodeEditor.removeChildNode(last.getParent(), last);
            }
        }
        critParts.getCriteria().addAll(subqueryCriteria);
        Criteria combinedCrit = QueryRewriter.optimizeCriteria(critParts, metadata);

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
