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

package com.metamatrix.query.validator;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.visitor.FunctionCollectorVisitor;

public class ValueValidationVisitor extends AbstractValidationVisitor {
	
	public void visit(CompareCriteria obj) {
        // Validate use of 'rowlimit' and 'rowlimitexception' pseudo-functions - they cannot be nested within another
        // function, and their operands must be a nonnegative integers

        // Collect all occurrances of rowlimit function
        List rowLimitFunctions = new ArrayList();
        FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMIT);
        PreOrderNavigator.doVisit(obj, visitor);   
        visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMITEXCEPTION);
        PreOrderNavigator.doVisit(obj, visitor);            
        final int functionCount = rowLimitFunctions.size();
        if (functionCount > 0) {
            Function function = null;
            Expression expr = null;
            if (obj.getLeftExpression() instanceof Function) {
                Function leftExpr = (Function)obj.getLeftExpression();
                if (leftExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMIT) ||
                    leftExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMITEXCEPTION)) {
                    function = leftExpr;
                    expr = obj.getRightExpression();
                }
            } 
            if (function == null && obj.getRightExpression() instanceof Function) {
                Function rightExpr = (Function)obj.getRightExpression();
                if (rightExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMIT) ||
                    rightExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMITEXCEPTION)) {
                    function = rightExpr;
                    expr = obj.getLeftExpression();
                }
            }
            if (function == null) {
                // must be nested, which is invalid
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.0"), obj); //$NON-NLS-1$
            } else {
            	if (expr instanceof Reference) {
            		expr = ((Reference)expr).getExpression();
            	}
                if (expr instanceof Constant) {
                    Constant constant = (Constant)expr;
                    if (constant.getValue() instanceof Integer) {
                        Integer integer = (Integer)constant.getValue();
                        if (integer.intValue() < 0) {
                            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.1"), obj); //$NON-NLS-1$
                        }
                    } else {
                        handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.1"), obj); //$NON-NLS-1$
                    }
                } else {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.1"), obj); //$NON-NLS-1$
                }
            }                 
        }
	}
	
    public void visit(Limit obj) {
        Expression offsetExpr = obj.getOffset();
        if (offsetExpr instanceof Reference) {
            offsetExpr = ((Reference)offsetExpr).getExpression();
        }
        if (offsetExpr instanceof Constant) {
            Integer offset = (Integer)((Constant)offsetExpr).getValue();
            if (offset == null) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.badoffset1"), obj); //$NON-NLS-1$
            } else if (offset.intValue() < 0) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.badoffset2"), obj); //$NON-NLS-1$
            }
        }
        Expression limitExpr = obj.getRowLimit();
        if (limitExpr instanceof Reference) {
            limitExpr = ((Reference)limitExpr).getExpression();
        }
        if (limitExpr instanceof Constant) {
            Integer limit = (Integer)((Constant)limitExpr).getValue();
            if (limit == null) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.badlimit1"), obj); //$NON-NLS-1$
            } else if (limit.intValue() < 0) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.badlimit2"), obj); //$NON-NLS-1$
            }
        }
    }
	
}
