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

package com.metamatrix.query.rewriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.visitor.ExpressionMappingVisitor;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.sql.visitor.VariableCollectorVisitor;

/**
 * <p>This visitor class will traverse a language object tree, it finds variables in the language
 * object and replaces the variable with a <code>Reference</code> obj.,/p>
 *
 * <p>This will also evaluate special variables INPUT and CHANGING by looking at the user's query,
 *  it finds these special variables it replaces the variable with a value(in case of INPUT) of the
 *  corresponding variable in set the user command or replace it with a constant of boolean value
 *  (in case CHANGING) indicating if the variable value is set on the user's command.</p>
 *
 * <p>The easiest way to use this visitor is to call the static method which creates the
 * the visitor by passing it the Language Object and the variable context to be looked up.
 * The public visit() methods should NOT be called directly.</p>
 */
public class VariableSubstitutionVisitor extends ExpressionMappingVisitor {

    // INPUT variables for which problems were identified during
    // substitution
    private Collection invalidInput;
    private int commandType;
    
    private QueryResolverException conversionException = null;
    
	/**
	 * Construct a new visitor with the list of references.
	 * @param references A list of references on to be collected
	 */
	public VariableSubstitutionVisitor(Map variableValues, int commandType) {
        super(variableValues);
        this.commandType = commandType;
	}

    public void visit(XQuery obj) {
        obj.getCompiledXQuery().setParameters(getVariableValues());
    }
    
    public void visit(Update obj) {
        if (commandType != Command.TYPE_UPDATE) {
            return;
        }

        SetClauseList newChangeList = new SetClauseList();
        for (SetClause entry : obj.getChangeList().getClauses()) {
            Expression rightExpr = entry.getValue();
            boolean retainChange = checkInputVariables(rightExpr);
            if (retainChange) {
                newChangeList.addClause(entry.getSymbol(), entry.getValue());
            }
        }
        obj.setChangeList(newChangeList);
    }
    
    /**
     * Checks variables in an expression, if the variables are INPUT variables and if
     * none of them are changing, then this method returns a false, if all of them
     * are changing this returns a true, if some are changing and some are not, then
     * that is an invalid case and the method adds to the list of invalid variables.
     */
    private boolean checkInputVariables(Expression expr) {
        Iterator varIter =
            VariableCollectorVisitor.getVariables(expr, false).iterator();
        
        Boolean result = null;
        
        while (varIter.hasNext()) {
            ElementSymbol var = (ElementSymbol) varIter.next();
            String grpName = var.getGroupSymbol().getName();
            if (grpName.equals(ProcedureReservedWords.INPUT)) {
                
                String changingKey = ProcedureReservedWords.CHANGING + ElementSymbol.SEPARATOR + var.getShortCanonicalName();
                
                Boolean changingValue = (Boolean)((Constant)getVariableValues().get(changingKey)).getValue();
                
                if (result == null) {
                    result = changingValue;
                } else if (!result.equals(changingValue)) {
                    if (invalidInput == null) {
                        invalidInput = new ArrayList();
                    }
                    invalidInput.add(expr);
                }
            }
        }
        
        if (result != null) {
            return result.booleanValue();
        }
        
        return true;
    }

    /**
     * Common pattern used by visit methods in this Visitor
     * @param expr
     * @return
     */
    public Expression replaceExpression(Expression expr) {
        if (expr == null) {
            return null;
        }
        
        Class type = expr.getType();
        
        if (expr instanceof ElementSymbol) {
            ElementSymbol symbol = (ElementSymbol)expr;
            if (symbol.isExternalReference()) {
                String grpName = symbol.getGroupSymbol().getCanonicalName();
                
                Expression value = (Expression)this.getVariableValues().get(symbol.getCanonicalName());

                if (value != null) {
                    //don't forward references
                    if (!ReferenceCollectorVisitor.getReferences(value).isEmpty()) {
                        return expr;
                    }
                    expr = value;
                } else if (grpName.equals(ProcedureReservedWords.INPUT)) {
                    expr =  new Constant(null, symbol.getType());
                } else if (grpName.equals(ProcedureReservedWords.CHANGING)) {
                    Assertion.failed("Changing value should not be null"); //$NON-NLS-1$
                } 
            }
        }
        
        try {
            expr = ResolverUtil.convertExpression(expr, DataTypeManager.getDataTypeName(type));
        } catch (QueryResolverException err) {
            this.conversionException = err;
            setAbort(true);
        }
        
        return expr;
    }
    
	/**
	 * <p>Helper to visit the language object specified and replace any variables a Reference obj,
	 * and collect the references returned.</p>
	 * @param obj The Language object that is to be visited
     * @param commandType The command type of the user command invoking this procedure
     * @param forwardReferences If references in the variable values should be substituted into expressions in this command
	 * @return true if a reference was not forwarded
	 * @throws QueryValidatorException 
	 * @throws QueryValidatorException
	 */
	public static final void substituteVariables(
		LanguageObject obj,
		Map variableValues,
        int commandType) throws QueryValidatorException {
		VariableSubstitutionVisitor visitor =
			new VariableSubstitutionVisitor(variableValues, commandType);
        if (obj == null) {
            return;
        }
        
		DeepPreOrderNavigator.doVisit(obj, visitor);
        
        if (visitor.invalidInput != null) {
            throw new QueryValidatorException(QueryExecPlugin.Util.getString("VariableSubstitutionVisitor.Input_vars_should_have_same_changing_state", visitor.invalidInput)); //$NON-NLS-1$
        }
        if (visitor.conversionException != null) {
            throw new QueryValidatorException(visitor.conversionException, visitor.conversionException.getMessage());
        }
    }
    
}
