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

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Validates that the any atomic query being sent to a model where the model
 * requires a criteria actually has a criteria.  This rule will detect this
 * case and throw an exception.  This rule does not actually modify anything
 * in the plan itself.
 */
public final class RuleValidateWhereAll implements OptimizerRule {

	/**
     * Verifies that a model with "supports where all" is being passed an atomic
     * query with a criteria.
     * @throws QueryPlannerException if property is not satisfied
	 * @see com.metamatrix.query.optimizer.OptimizerRule#execute(PlanNode, QueryMetadataInterface, RuleStack)
	 */
	public PlanNode execute(
		PlanNode plan,
		QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
        RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws
			QueryPlannerException,
			QueryMetadataException,
			MetaMatrixComponentException {

		for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
            Object modelID = RuleRaiseAccess.getModelIDFromAccess(node, metadata);

            if(CapabilitiesUtil.requiresCriteria(modelID, metadata, capFinder) 
            		&& hasNoCriteria((Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST))) {
                String modelName = metadata.getFullName(modelID);
                throw new QueryPlannerException(QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0024, modelName));
            }
		}

		return plan;
	}

	/**
     * Determine whether a command is a query without a criteria
     * @param command
     * @return
     */
    static boolean hasNoCriteria(Command command) {
    	if(command instanceof Query) {
            Query query = (Query) command;
            return query.getCriteria() == null;
        }
        if (command instanceof Delete) {
        	Delete query = (Delete) command;
            return query.getCriteria() == null;
        }
        if (command instanceof Update) {
        	Update query = (Update) command;
            return query.getCriteria() == null;
        }
        if (command instanceof SetQuery) {
        	SetQuery query = (SetQuery)command;
        	return hasNoCriteria(query.getLeftQuery()) || hasNoCriteria(query.getRightQuery());
        }
        return false;
    }

    public String toString() {
		return "ValidateWhereAll"; //$NON-NLS-1$
	}

}
