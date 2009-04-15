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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.eval.LookupEvaluator;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.navigator.DeepPostOrderNavigator;
import com.metamatrix.query.sql.navigator.PostOrderNavigator;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.util.ValueIterator;
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
    private LookupEvaluator dataMgr;
        
    public EvaluateExpressionVisitor() {
        super(null);
    }

    public void setContext(CommandContext context) {
        this.context = context;
    }
                                
    // ######### Visit methods that are replacing DependentSetCriteria with resolved SetCriteria ########
    
    public void visit(CompoundCriteria obj) {
        List crit = obj.getCriteria();
        List newCrit = new ArrayList(crit.size());
        Iterator iter = crit.iterator();
        while(iter.hasNext()) {
            newCrit.add(checkDependentCriteria((Criteria) iter.next()));
        }
        obj.setCriteria(newCrit);
    }

    public void visit(JoinPredicate obj) {
        List joinCrit = obj.getJoinCriteria();
        List newCrit = new ArrayList(joinCrit.size());
        Iterator iter = joinCrit.iterator();
        while(iter.hasNext()) {
            newCrit.add(checkDependentCriteria((Criteria) iter.next()));
        }
        obj.setJoinCriteria(newCrit);
    }

    public void visit(Query obj) {
        obj.setCriteria(checkDependentCriteria(obj.getCriteria()));
        obj.setHaving(checkDependentCriteria(obj.getHaving()));
    }

    private Criteria checkDependentCriteria(Criteria crit) {
        if (!(crit instanceof DependentSetCriteria)) {
            return crit;
        }
        return replaceDependentCriteria((DependentSetCriteria)crit);            
    }

    public Criteria replaceDependentCriteria(DependentSetCriteria crit) {
        ValueIterator iter = crit.getValueIterator();
        if(iter == null) {
            // Something has gone horribly wrong!  This should never happen
            Assertion.failed(QueryPlugin.Util.getString("EvaluateExpressionVisitor.Cant_get_iterator", crit.getValueExpression().toString())); //$NON-NLS-1$
        }
            
        try {
            List vals = new ArrayList();
            while(iter.hasNext()) {
                Object val = iter.next();
                if(val != null) {
                    vals.add(new Constant(val));
                }
            }
            
            if(vals.size() > 0) {                    
                SetCriteria sc = new SetCriteria();
                sc.setExpression(crit.getExpression());
                sc.setValues(vals);
                return sc;
            }
            
            // No values - return criteria that is always false
            return new CompareCriteria(new Constant(new Integer(0)), CompareCriteria.EQ, new Constant(new Integer(1)));
            
        } catch(MetaMatrixComponentException e) {
            throw new MetaMatrixRuntimeException(e);
        }
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
        if(!ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expr).isEmpty()) {
            return expr;
        }
               
        //if the expression is a constant or is not evaluatable, just return
        if (expr instanceof Constant || (!(expr instanceof Reference) && !EvaluatableVisitor.isEvaluatable(expr, false, true, false, false))) {
            return expr;
        }

		Object value;
        try {
            value = new Evaluator(Collections.emptyMap(), dataMgr, context).evaluate(expr, Collections.emptyList());
        } catch (MetaMatrixException err) {
        	if (expr instanceof Reference) {
        		return expr;
        	}
            throw new MetaMatrixRuntimeException(err);
        }
		return new Constant(value, expr.getType());			 
    }
    
    /**
     *  Will return true if the expression can be deterministically evaluated at runtime, but it may not be
     *  evaluatable during planning
     */
    public static final boolean willBecomeConstant(LanguageObject obj) {
        return willBecomeConstant(obj, false);
    }
    
    public static final boolean willBecomeConstant(LanguageObject obj, boolean pushdown) {
        return EvaluatableVisitor.isEvaluatable(obj, false, false, true, pushdown);
    }
    
    /**
     *  Should be called to check if the object can fully evaluated
     */
    public static final boolean isFullyEvaluatable(LanguageObject obj, boolean duringPlanning) {
        return EvaluatableVisitor.isEvaluatable(obj, duringPlanning, true, true, false);
    }
        
    public static final void replaceExpressions(LanguageObject obj, boolean deep, LookupEvaluator dataMgr, CommandContext context)
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
