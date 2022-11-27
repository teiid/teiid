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

import java.util.Collection;
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
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
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
            if (setCriteria.isNegated()) {
                continue;
            }
            if (!setCriteria.isAllConstants()) {
                boolean allowed = true;
                for (Expression ex : (Collection<Expression>)setCriteria.getValues()) {
                    if (!EvaluatableVisitor.willBecomeConstant(ex)) {
                        allowed = false;
                        break;
                    }
                }
                if (!allowed) {
                    continue;
                }
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
