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

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;

/**
 * <p>This visitor class will traverse a language object tree, finds variables in the language
 * object and replaces the variable with a <code>Reference</code> obj.  This visitor is
 * needed for correlated subqueries.</p>
 * 
 * <p>The easiest way to use this visitor is to call the static method which creates the 
 * the visitor by passing it the Langiuage Object and the variable context to be looked up.
 * The public visit() methods should NOT be called directly.</p>
 */
public class CorrelatedVariableSubstitutionVisitor extends ExpressionMappingVisitor {

    public CorrelatedVariableSubstitutionVisitor() {
        super(Collections.EMPTY_MAP);
    }

    /** 
     * @see com.metamatrix.query.sql.visitor.ExpressionMappingVisitor#replaceExpression(com.metamatrix.query.sql.symbol.Expression)
     */
    public Expression replaceExpression(Expression expression) {
        
        if (expression instanceof ElementSymbol) {
            ElementSymbol variable = (ElementSymbol)expression;

            if (variable.isExternalReference()) {
                return new Reference(variable);
            }
        } 

        return expression;
    }
    
    /**
     * <p>Helper to visit the language object specified and replace any variables with a Reference obj, 
     * and collect the references returned.</p>
     * @param obj The Language object that is to be visited
     * that the client (outer query) is interested in references to from the correlated subquery
     * @param metadata QueryMetadataInterface
     * @return a List of References collected
     */
    public static final void substituteVariables(LanguageObject obj) {

        CorrelatedVariableSubstitutionVisitor visitor =
            new CorrelatedVariableSubstitutionVisitor();
        PreOrderNavigator.doVisit(obj, visitor);
    }
}
