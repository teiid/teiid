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

package org.teiid.translator.jdbc.hana;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.AndOr.Operator;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;



/**
 * This Function modifier used to support ANSI concat on Oracle 9i.
 * <code>
 * CONCAT(a, b) ==> CASE WHEN (a is NULL OR b is NULL) THEN NULL ELSE CONCAT(a, b)
 * </code>   
 */
public class ConcatFunctionModifier extends FunctionModifier {
    private LanguageFactory langFactory;
    
    /** 
     * @param langFactory
     */
    public ConcatFunctionModifier(LanguageFactory langFactory) {
        this.langFactory = langFactory;
    }
    
    @Override
    public List<?> translate(Function function) {
        Expression a = function.getParameters().get(0);
        Expression b = function.getParameters().get(1);
        List<Condition> crits = new ArrayList<Condition>();
        
        Literal nullValue = langFactory.createLiteral(null, TypeFacility.RUNTIME_TYPES.STRING);
        if (isNull(a)) {
        	return Arrays.asList(nullValue);
        } else if (!isNotNull(a)) {
        	crits.add(langFactory.createIsNullCriteria(a, false));
        }
        if (isNull(b)) {
        	return Arrays.asList(nullValue);
        } else if (!isNotNull(b)) {
        	crits.add(langFactory.createIsNullCriteria(b, false));
        }
        
        Condition crit = null;
        
        if (crits.isEmpty()) {
        	return null;
        } else if (crits.size() == 1) {
        	crit = crits.get(0);
        } else {
        	crit = langFactory.createAndOr(Operator.OR, crits.get(0), crits.get(1));
        }
        List<SearchedWhenClause> cases = Arrays.asList(langFactory.createSearchedWhenCondition(crit, nullValue));
        return Arrays.asList(langFactory.createSearchedCaseExpression(cases, function, TypeFacility.RUNTIME_TYPES.STRING));
    }
    
    public static boolean isNotNull(Expression expr) {
    	if (expr instanceof Literal) {
    		Literal literal = (Literal)expr;
    		return literal.getValue() != null;
    	}
    	if (expr instanceof Function) {
    		Function function = (Function)expr;
    		if (function.getName().equalsIgnoreCase("IFNULL") || function.getName().equalsIgnoreCase(SourceSystemFunctions.IFNULL)) { //$NON-NLS-1$
    			return isNotNull(function.getParameters().get(1));
    		}
    	}
    	return false;
    }
    
    private boolean isNull(Expression expr) {
    	if (expr instanceof Literal) {
    		Literal literal = (Literal)expr;
    		return literal.getValue() == null;
    	}
    	return false;
    }
        
}
