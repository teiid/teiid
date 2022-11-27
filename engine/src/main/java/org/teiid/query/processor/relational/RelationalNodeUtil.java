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

package org.teiid.query.processor.relational;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.visitor.EvaluatableVisitor;



/**
 * @since 4.2
 */
public class RelationalNodeUtil {

    private RelationalNodeUtil() {
    }

    /**
     * Decides whether a command needs to be executed.
     * <br><b>NOTE: This method has a side-effect.</b> If the criteria of this command always evaluate to true,
     * and the simplifyCriteria flag is true, then the command criteria are set to null.
     * @param command
     * @param simplifyCriteria whether to simplify the criteria of the command if they always evaluate to true
     * @return true if this command should be executed by the connector; false otherwise.
     * @throws TeiidComponentException
     * @throws ExpressionEvaluationException
     * @since 4.2
     */
    public static boolean shouldExecute(Command command, boolean simplifyCriteria) throws TeiidComponentException, ExpressionEvaluationException {
        return shouldExecute(command, simplifyCriteria, false);
    }

    public static boolean shouldExecute(Command command, boolean simplifyCriteria, boolean duringPlanning) throws TeiidComponentException, ExpressionEvaluationException {
        int cmdType = command.getType();
        Criteria criteria = null;
        switch(cmdType) {
            case Command.TYPE_QUERY:

                QueryCommand queryCommand = (QueryCommand) command;

                Limit limit = queryCommand.getLimit();

                if (limit != null && limit.getRowLimit() instanceof Constant) {
                    Constant rowLimit = (Constant)limit.getRowLimit();
                    if (Integer.valueOf(0).equals(rowLimit.getValue())) {
                        return false;
                    }
                }

                if(queryCommand instanceof SetQuery) {
                    SetQuery union = (SetQuery) queryCommand;
                    boolean shouldExecute = false;
                    for (QueryCommand innerQuery : union.getQueryCommands()) {
                        boolean shouldInner = shouldExecute(innerQuery, simplifyCriteria, duringPlanning);
                        if(shouldInner) {
                            shouldExecute = true;
                            break;
                        }
                    }
                    return shouldExecute;
                }

                // Else this is a query
                Query query = (Query) queryCommand;
                criteria = query.getCriteria();

                boolean shouldEvaluate = shouldEvaluate(simplifyCriteria, duringPlanning, criteria, query, false);

                if (shouldEvaluate) {
                    //check for false having as well
                    shouldEvaluate = shouldEvaluate(simplifyCriteria, duringPlanning, query.getHaving(), query, true);
                }

                if (shouldEvaluate) {
                    return true;
                }

                if (query.hasAggregates() && query.getGroupBy() == null) {
                    return true;
                }

                break;
            case Command.TYPE_INSERT:
                Insert insert = (Insert) command;
                QueryCommand expr = insert.getQueryExpression();
                if (expr != null) {
                    return shouldExecute(expr, simplifyCriteria);
                }
                return true;
            case Command.TYPE_UPDATE:
                Update update = (Update) command;

                if (update.getChangeList().isEmpty()) {
                    return false;
                }

                criteria = update.getCriteria();
                // If there are elements present in the criteria,
                // then we don't know the result, so assume we need to execute
                if (criteria == null) {
                    return true;
                }
                if(!EvaluatableVisitor.isFullyEvaluatable(criteria, duringPlanning)) {
                    return true;
                } else if(Evaluator.evaluate(criteria)) {
                    if (simplifyCriteria) {
                        update.setCriteria(null);
                    }
                    return true;
                }
                break;
            case Command.TYPE_DELETE:
                Delete delete = (Delete) command;
                criteria = delete.getCriteria();
                // If there are elements present in the criteria,
                // then we don't know the result, so assume we need to execute
                if (criteria == null) {
                    return true;
                }
                if(!EvaluatableVisitor.isFullyEvaluatable(criteria, duringPlanning)) {
                    return true;
                } else if(Evaluator.evaluate(criteria)) {
                    if (simplifyCriteria) {
                        delete.setCriteria(null);
                    }
                    return true;
                }
                break;
            default:
                return true;
        }
        return false;
    }

    private static boolean shouldEvaluate(boolean simplifyCriteria,
            boolean duringPlanning, Criteria criteria, Query query, boolean having)
            throws ExpressionEvaluationException, BlockedException,
            TeiidComponentException {
        if(criteria == null) {
            return true;
        } else if(!EvaluatableVisitor.isFullyEvaluatable(criteria, duringPlanning)) {
            // If there are elements present in the criteria,
            // then we don't know the result, so assume we need to execute
            return true;
        } else if(Evaluator.evaluate(criteria)) {
            if (simplifyCriteria) {
                if (having) {
                    query.setHaving(null);
                } else {
                    query.setCriteria(null);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns whether the relational command is an update.
     * @param command
     * @return
     * @since 4.2
     */
    public static boolean isUpdate(Command command) {
        int commandType = command.getType();
        return commandType == Command.TYPE_INSERT ||
               commandType == Command.TYPE_UPDATE ||
               commandType == Command.TYPE_DELETE;
    }

    public static boolean hasOutputParams(Command command) {
        boolean hasOutParams = false;
        if (command instanceof StoredProcedure) {
            StoredProcedure sp = (StoredProcedure)command;
            hasOutParams = sp.returnParameters() && sp.getProjectedSymbols().size() > sp.getResultSetColumns().size();
        }
        return hasOutParams;
    }

}
