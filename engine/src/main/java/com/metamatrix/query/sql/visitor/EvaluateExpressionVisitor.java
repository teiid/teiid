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

package com.metamatrix.query.sql.visitor;

import java.util.Collections;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.navigator.DeepPostOrderNavigator;
import com.metamatrix.query.sql.navigator.PostOrderNavigator;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.visitor.EvaluatableVisitor.EvaluationLevel;
import com.metamatrix.query.util.CommandContext;

/**
 * <p>This visitor simplifies Language Objects by evaluating and/or replacing 
 * Expressions within them.</p>
 * 
 * <p>The easiest way to use this visitor is to call the static methods which create 
 * the visitor and run it.
 * The public visit() methods should NOT be called directly.</p>
 */
public class EvaluateExpressionVisitor extends ExpressionMappingVisitor {

    private CommandContext context;
    private ProcessorDataManager dataMgr;
        
    EvaluateExpressionVisitor() {
        super(null);
    }

    public void setContext(CommandContext context) {
        this.context = context;
    }

    /**
     * Evaluate the expression.  This method takes into account whether the 
     * Expression CAN be evaluated or not.  
     * 
     * This method also takes into account if the Expression is a 
     * Reference.  The Reference may not be evaluatable (may not have data tuple and
     * element map set on it), in which case this method may return the Reference itself
     * or the Expression inside the Reference, depending on the instance variables of this
     * visitor.
     * @param expr
     * @return
     */
    public Expression replaceExpression(Expression expr) {
        //if the expression is a constant or is not evaluatable, just return
        if (expr instanceof Constant || expr instanceof ScalarSubquery || (!(expr instanceof Reference) && !EvaluatableVisitor.isEvaluatable(expr, EvaluationLevel.PROCESSING))) {
            return expr;
        }

		Object value;
        try {
            value = new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(expr, Collections.emptyList());
        } catch (MetaMatrixException err) {
            throw new MetaMatrixRuntimeException(err);
        }
        if (value instanceof Constant) {
        	return (Constant)value; //multi valued substitution
        }
		return new Constant(value, expr.getType());
    }
    
    /**
     *  Will return true if the expression can be deterministically evaluated at runtime, but it may not be
     *  evaluatable during planning
     */
    public static final boolean willBecomeConstant(LanguageObject obj) {
        return EvaluatableVisitor.willBecomeConstant(obj, false);
    }
    
    /**
     *  Should be called to check if the object can fully evaluated
     */
    public static final boolean isFullyEvaluatable(LanguageObject obj, boolean duringPlanning) {
        return EvaluatableVisitor.isEvaluatable(obj, duringPlanning?EvaluationLevel.PLANNING:EvaluationLevel.PROCESSING);
    }
        
    public static final void replaceExpressions(LanguageObject obj, boolean deep, ProcessorDataManager dataMgr, CommandContext context)
    throws ExpressionEvaluationException, MetaMatrixComponentException {
        EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor();
        visitor.setContext(context);
        visitor.dataMgr = dataMgr;
        
        try {
            if(deep) {
                DeepPostOrderNavigator.doVisit(obj, visitor);
            } else {
                PostOrderNavigator.doVisit(obj, visitor);
            }
        } catch (MetaMatrixRuntimeException err) {
            Throwable e = err.getChild();
            
            if (e == null) {
                throw err;
            }
            
            if(e instanceof ExpressionEvaluationException) {
                throw (ExpressionEvaluationException) e;
            } else if(e instanceof MetaMatrixComponentException) {
                throw (MetaMatrixComponentException) e;                    
            } else {
                throw new MetaMatrixComponentException(e, e.getMessage());    
            }
        }
    }
}
