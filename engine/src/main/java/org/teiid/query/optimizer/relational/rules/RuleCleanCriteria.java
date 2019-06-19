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

import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Removes phantom and TRUE or FALSE criteria
 */
public final class RuleCleanCriteria implements OptimizerRule {

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, TeiidComponentException {

        boolean pushRaiseNull = false;

        pushRaiseNull = clean(plan, !rules.contains(RuleConstants.COPY_CRITERIA));

        if (pushRaiseNull) {
            rules.push(RuleConstants.RAISE_NULL);
        }

        return plan;
    }

    private boolean clean(PlanNode plan, boolean removeAllPhantom)
            throws TeiidComponentException {
        boolean pushRaiseNull = false;
        List<? extends Expression> outputCols = (List<? extends Expression>) plan.setProperty(Info.OUTPUT_COLS, null);
        if (outputCols != null) {
            //save the approximate total
            plan.setProperty(Info.APPROXIMATE_OUTPUT_COLUMNS, outputCols.size());
        }
        for (PlanNode node : plan.getChildren()) {
            pushRaiseNull |= clean(node, removeAllPhantom);
        }
        if (plan.getType() == NodeConstants.Types.SELECT) {
            pushRaiseNull |= cleanCriteria(plan, removeAllPhantom);
        }
        return pushRaiseNull;
    }

    boolean cleanCriteria(PlanNode critNode, boolean removeAllPhantom) throws TeiidComponentException {
        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_TEMPORARY) || (critNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)
                && (removeAllPhantom || critNode.hasBooleanProperty(Info.IS_COPIED)))) {
            NodeEditor.removeChildNode(critNode.getParent(), critNode);
            return false;
        }

        //TODO: remove dependent set criteria that has not been meaningfully pushed from its parent join

        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING) || critNode.getGroups().size() != 0) {
            return false;
        }

        Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        //if not evaluatable, just move on to the next criteria
        if (!EvaluatableVisitor.isFullyEvaluatable(crit, true)) {
            return false;
        }
        //if evaluatable
        try {
            boolean eval = Evaluator.evaluate(crit);
            if(eval) {
                NodeEditor.removeChildNode(critNode.getParent(), critNode);
            } else {
                FrameUtil.replaceWithNullNode(critNode);
                return true;
            }
        //none of the following exceptions should ever occur
        } catch(BlockedException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30273, e);
        } catch (ExpressionEvaluationException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30274, e);
        }
        return false;
    }

    public String toString() {
        return "CleanCriteria"; //$NON-NLS-1$
    }

}
