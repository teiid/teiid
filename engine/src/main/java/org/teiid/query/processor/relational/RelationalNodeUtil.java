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

package org.teiid.query.processor.relational;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.Update;
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
     * <br/><b>NOTE: This method has a side-effect.</b> If the criteria of this command always evaluate to true,
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

                if(criteria == null) {
                    return true;
                } else if(!EvaluatableVisitor.isFullyEvaluatable(criteria, duringPlanning)) {
                    // If there are elements present in the criteria,
                    // then we don't know the result, so assume we need to execute
                    return true;
                } else if(Evaluator.evaluate(criteria)) {
                    if (simplifyCriteria) {
                        query.setCriteria(null);
                    }
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

}
