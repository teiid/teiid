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

import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 * Removes phantom and TRUE or FALSE criteria
 */
public final class RuleCleanCriteria implements OptimizerRule {

    /**
     * @see OptimizerRule#execute(PlanNode, QueryMetadataInterface, RuleStack)
     */
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, MetaMatrixComponentException {

        List criteria = NodeEditor.findAllNodes(plan, NodeConstants.Types.SELECT);
        
        Iterator critIter = criteria.iterator();
        
        boolean pushRaiseNull = false;
        
        while (critIter.hasNext()) {
            PlanNode critNode = (PlanNode)critIter.next();
            
            if (critNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
                NodeEditor.removeChildNode(critNode.getParent(), critNode);
                continue;
            }
            
            //TODO: remove dependent set criteria that has not been meaningfully pushed from its parent join
            
            if (critNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING) || critNode.getGroups().size() != 0) {
                continue;
            }
            
            Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            //if not evaluatable, just move on to the next criteria
            if (!EvaluateExpressionVisitor.isFullyEvaluatable(crit, true)) {
                continue;
            }
            //if evaluatable
            try {
                boolean eval = Evaluator.evaluate(crit);
                if(eval) {
                    NodeEditor.removeChildNode(critNode.getParent(), critNode);
                } else {
                    FrameUtil.replaceWithNullNode(critNode);
                    pushRaiseNull = true;
                }
            //none of the following exceptions should ever occur
            } catch(BlockedException e) {
                throw new MetaMatrixComponentException(e);
            } catch (CriteriaEvaluationException e) {
                throw new MetaMatrixComponentException(e);
            } 
        } 
        
        if (pushRaiseNull) {
            rules.push(RuleConstants.RAISE_NULL);
        }

        return plan;        
    }
    
    public String toString() {
        return "CleanCriteria"; //$NON-NLS-1$
    }

}
