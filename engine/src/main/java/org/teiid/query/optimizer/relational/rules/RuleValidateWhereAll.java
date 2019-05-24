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

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.util.CommandContext;


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
     */
    public PlanNode execute(
        PlanNode plan,
        QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
        RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws
            QueryPlannerException,
            QueryMetadataException,
            TeiidComponentException {

        for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
            Object modelID = RuleRaiseAccess.getModelIDFromAccess(node, metadata);

            if(CapabilitiesUtil.requiresCriteria(modelID, metadata, capFinder)
                    && hasNoCriteria((Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST))) {
                String modelName = metadata.getFullName(modelID);
                 throw new QueryPlannerException(QueryPlugin.Event.TEIID30268, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30268, modelName));
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
