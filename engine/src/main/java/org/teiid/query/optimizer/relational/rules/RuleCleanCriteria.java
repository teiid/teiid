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

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Removes phantom and TRUE or FALSE criteria
 */
public final class RuleCleanCriteria implements OptimizerRule {

    /**
     * @see OptimizerRule#execute(PlanNode, QueryMetadataInterface, RuleStack)
     */
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
		plan.setProperty(Info.OUTPUT_COLS, null);
        for (PlanNode node : plan.getChildren()) {
        	pushRaiseNull |= clean(node, removeAllPhantom);
        }
        if (plan.getType() == NodeConstants.Types.SELECT) {
        	pushRaiseNull |= cleanCriteria(plan, removeAllPhantom);
        }
		return pushRaiseNull;
	}
    
    boolean cleanCriteria(PlanNode critNode, boolean removeAllPhantom) throws TeiidComponentException {
        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM) 
        		&& (removeAllPhantom || critNode.hasBooleanProperty(Info.IS_COPIED))) {
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
            throw new TeiidComponentException(e);
        } catch (ExpressionEvaluationException e) {
            throw new TeiidComponentException(e);
        }
        return false;
    }
    
    public String toString() {
        return "CleanCriteria"; //$NON-NLS-1$
    }

}
