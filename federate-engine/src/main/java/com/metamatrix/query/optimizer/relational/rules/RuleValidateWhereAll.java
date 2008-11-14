/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Stack;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
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


		//All nodes will be pushed onto stack,
		//then checked for validity as they are popped
		Stack nodes = new Stack();
		nodes.push(plan);
		while (!nodes.isEmpty()){
			PlanNode node = (PlanNode)nodes.pop();

			if (node.getType()==NodeConstants.Types.ACCESS){

                Object modelID = RuleRaiseAccess.getModelIDFromAccess(node, metadata);

                if(modelID != null) {
                    boolean supportsWhereAll = metadata.modelSupports(modelID, SupportConstants.Model.NO_CRITERIA);
                    if(! supportsWhereAll) {
                        // This model does not support queries without a criteria, so we must check for that
                        boolean hasNoCriteria = hasNoCriteria((Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST));

                        if(hasNoCriteria) {
                            String modelName = metadata.getFullName(modelID);
                            throw new QueryPlannerException(QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0024, modelName));
                        }
                    }
                }
			}

			//Push children onto the stack
			Iterator children = node.getChildren().iterator();
			while (children.hasNext()) {
				nodes.push(children.next());
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
            if (query.getCriteria() == null) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
		return "ValidateWhereAll"; //$NON-NLS-1$
	}

}
