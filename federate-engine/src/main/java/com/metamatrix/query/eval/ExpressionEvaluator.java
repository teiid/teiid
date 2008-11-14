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

package com.metamatrix.query.eval;

import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public class ExpressionEvaluator {

	private ExpressionEvaluator() {}

    public static Object evaluate(Expression expression, Map elements, List tuple)
        throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {

        return evaluate(expression, elements, tuple, null, null);
    }

	public static Object evaluate(Expression expression, Map elements, List tuple, LookupEvaluator dataMgr, CommandContext context)
		throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {

	    try {
    		return internalEvaluate(expression, elements, tuple, dataMgr, context);
	    } catch (ExpressionEvaluationException e) {
	        throw new ExpressionEvaluationException(e, QueryPlugin.Util.getString("ExpressionEvaluator.Eval_failed", new Object[] {expression, e.getMessage()})); //$NON-NLS-1$
	    }
	}
	
	private static Object internalEvaluate(Expression expression, Map elements, List tuple, LookupEvaluator dataMgr, CommandContext context)
       throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {

       if(expression instanceof SingleElementSymbol) {
           // Case 5155: elements must be non-null
           Assertion.isNotNull( elements );
   
           // Try to evaluate by lookup in the elements map (may work for both ElementSymbol and ExpressionSymbol
           Integer index = (Integer) elements.get(expression);
           if(index != null) {
               return tuple.get(index.intValue());
           }
           // Otherwise this should be an ExpressionSymbol and we just need to dive in and evaluate the expression itself
           if (expression instanceof ExpressionSymbol && !(expression instanceof AggregateSymbol)) {            
               ExpressionSymbol exprSyb = (ExpressionSymbol) expression;
               Expression expr = exprSyb.getExpression();
               return internalEvaluate(expr, elements, tuple, dataMgr, context);
           } 
           // instead of assuming null, throw an exception.  a problem in planning has occurred
           throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, expression, "No value was available")); //$NON-NLS-1$
       } 
       if(expression instanceof Constant) {
           return ((Constant) expression).getValue();
       } else if(expression instanceof Function) {
           return evaluate((Function) expression, elements, tuple, dataMgr, context);
       } else if(expression instanceof CaseExpression) {
           return evaluate((CaseExpression) expression, elements, tuple, dataMgr, context);
       } else if(expression instanceof SearchedCaseExpression) {
           return evaluate((SearchedCaseExpression) expression, elements, tuple, dataMgr, context);
       } else if(expression instanceof Reference) {
           return ((Reference) expression).getValue(dataMgr, context);
       } else if(expression instanceof ScalarSubquery) {
           return evaluate((ScalarSubquery) expression, elements, tuple, dataMgr, context);
       } else {
           throw new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0016, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0016, expression.getClass().getName()));
       }
   }

    private static Object evaluate(CaseExpression expr, Map elements, List tuple, LookupEvaluator dataMgr, CommandContext context)
    throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
        Object exprVal = internalEvaluate(expr.getExpression(), elements, tuple, dataMgr, context);
        for (int i = 0; i < expr.getWhenCount(); i++) {
            if (EquivalenceUtil.areEqual(exprVal, internalEvaluate(expr.getWhenExpression(i), elements, tuple, dataMgr, context))) {
                return internalEvaluate(expr.getThenExpression(i), elements, tuple, dataMgr, context);
            }
        }
        if (expr.getElseExpression() != null) {
            return internalEvaluate(expr.getElseExpression(), elements, tuple, dataMgr, context);
        }
        return null;
    }

    private static Object evaluate(SearchedCaseExpression expr, Map elements, List tuple, LookupEvaluator dataMgr, CommandContext context)
    throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
        for (int i = 0; i < expr.getWhenCount(); i++) {
            try {
                if (CriteriaEvaluator.evaluate(expr.getWhenCriteria(i), elements, tuple, dataMgr, context)) {
                    return internalEvaluate(expr.getThenExpression(i), elements, tuple, dataMgr, context);
                }
            } catch (CriteriaEvaluationException e) {
                throw new ExpressionEvaluationException(e, ErrorMessageKeys.PROCESSOR_0033, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0033, "CASE", expr.getWhenCriteria(i))); //$NON-NLS-1$
            }
        }
        if (expr.getElseExpression() != null) {
            return internalEvaluate(expr.getElseExpression(), elements, tuple, dataMgr, context);
        }
        return null;
    }

	private static Object evaluate(Function function, Map elements, List tuple, LookupEvaluator dataMgr, CommandContext context)
		throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {

        // Get function based on resolved function info
        FunctionDescriptor fd = function.getFunctionDescriptor();
        
		// Evaluate args
		Expression[] args = function.getArgs();
        Object[] values = null;
        int start = 0;
        
        if (fd.requiresContext()) {
    		values = new Object[args.length+1];
            values[0] = context;
            start = 1;
        }
        else {
            values = new Object[args.length];
        }
        
        for(int i=0; i < args.length; i++) {
            values[i+start] = internalEvaluate(args[i], elements, tuple, dataMgr, context);
        }            
        
        // Check for function we can't evaluate
        if(fd.getPushdown() == FunctionMethod.MUST_PUSHDOWN) {
            throw new MetaMatrixComponentException(QueryPlugin.Util.getString("ExpressionEvaluator.Must_push", fd.getName())); //$NON-NLS-1$
        }

        // Check for special lookup function
        if(fd.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)) {
            if(dataMgr == null) {
                throw new ComponentNotFoundException(ErrorMessageKeys.PROCESSOR_0055, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0055));
            }

            String codeTableName = (String) values[0];
            String returnElementName = (String) values[1];
            String keyElementName = (String) values[2];
            
            return dataMgr.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, values[3]);
        } 
        
		// Execute function
		FunctionLibrary library = FunctionLibraryManager.getFunctionLibrary();
		Object result = library.invokeFunction(fd, values);
		return result;        
	}

    private static Object evaluate(ScalarSubquery scalarSubquery, Map elements, List tuple, LookupEvaluator dataMgr, CommandContext context)
        throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {

        Object result = null;
        ValueIterator valueIter = scalarSubquery.getValueIterator();
        if(valueIter.hasNext()) {
            result = valueIter.next();
            if(valueIter.hasNext()) {
                // The subquery should be scalar, but has produced
                // more than one result value - this is an exception case
                throw new ExpressionEvaluationException(ErrorMessageKeys.PROCESSOR_0058, QueryPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0058, scalarSubquery.getCommand()));
            }
        }
        return result;
    }        
}
