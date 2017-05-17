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

import java.util.Collections;

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
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.util.CommandContext;

/**
 * Look for large in predicates that were not pushed and push them as dependent set criteria
 */
public class RulePushLargeIn implements OptimizerRule {

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, RuleStack rules,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {
        for (PlanNode critNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SELECT, NodeConstants.Types.ACCESS)) {
            if (critNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING) || critNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
                continue;
            }
            Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            if (!(crit instanceof SetCriteria)) {
                continue;
            }
            SetCriteria setCriteria = (SetCriteria)crit;
            if (setCriteria.isNegated() || !setCriteria.isAllConstants()) {
                continue;
            }
            //we need to be directly over an access node
            PlanNode childAccess = critNode.getFirstChild();
            accessLoop: while (true) {
                switch (childAccess.getType()) {
                case NodeConstants.Types.ACCESS:
                    break accessLoop;
                case NodeConstants.Types.SELECT:
                    break;
                default:
                    break accessLoop; 
                }
                childAccess = childAccess.getFirstChild();
            }
            if (childAccess.getType() != NodeConstants.Types.ACCESS) {
                continue;
            }
            
            //use a dummy value to test if we can raise
            critNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, new SetCriteria(setCriteria.getExpression(), Collections.EMPTY_LIST));
            boolean canRaise = RuleRaiseAccess.canRaiseOverSelect(childAccess, metadata, capabilitiesFinder, critNode, analysisRecord);
            critNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, crit);
            if (!canRaise) {
                continue;
            }
            
            //push the crit node and mark as dependent set
            critNode.getParent().replaceChild(critNode, critNode.getFirstChild());
            childAccess.addAsParent(critNode);
            RuleRaiseAccess.performRaise(plan, childAccess, critNode);
            
            childAccess.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, true);
            childAccess.setProperty(NodeConstants.Info.EST_CARDINALITY, null);
        }
        return plan;
    }
    
    @Override
    public String toString() {
        return "PushLargeIn"; //$NON-NLS-1$
    }

}
